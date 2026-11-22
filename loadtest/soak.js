import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import {
  buildUserTokenPool,
  mintAdminToken,
  generateUuidV4,
  assertPerUserRateWithinBucket,
} from './lib/jwt.js';
import {
  setupMultipleEvents,
  postHold,
  postCheckout,
  getOrder,
} from './lib/api.js';
import {
  safeJson,
  checkJourneyHold,
  checkJourneyCheckout,
  checkJourneyFinalStatus,
  parseRetryAfterSeconds,
} from './lib/checks.js';
import { CHECKOUT_JOURNEY_THRESHOLDS } from './lib/thresholds.js';
import {
  holdCreated,
  holdUnknownOutcome,
  holdDuration,
  orderMaterializationLatency,
  checkoutDuration,
  sagaLatency,
  orderNotReadyRetries,
  journeyConfirmed,
  journeyPaymentFailed,
  journeyAbandoned,
  journeysTimedOut,
  unexpectedStatuses,
  plateau1Reqs,
  initJourneyCounters,
  isUnknownHoldOutcome,
} from './lib/metrics.js';

const BASE_URL = __ENV.BASE_URL || 'http://gateway-service:8080';
const JWT_SECRET = __ENV.JWT_SECRET || '';
const RUN_ID = __ENV.LOADTEST_RUN_ID || `soak-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SEATS_PER_EVENT = parseInt(__ENV.SEATS_PER_EVENT || '5000', 10);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/soak-summary.json';
const PROFILE_PATH = __ENV.SUSTAINABLE_PROFILE_PATH || './sustainable-profile.json';
const SOAK_MINUTES = parseFloat(__ENV.SOAK_MINUTES || '20');

const profile = JSON.parse(open(PROFILE_PATH));
if (!profile || !profile.journey || !profile.journey.sustainable_jps || profile.journey.sustainable_jps <= 0) {
  throw new Error(`Invalid or missing journey.sustainable_jps in ${PROFILE_PATH}`);
}

const SUSTAINED_JPS = parseInt(profile.journey.sustainable_jps, 10);
const RAMP_SEC = 20;
const PLATEAU_SEC = Math.max(60, Math.round(SOAK_MINUTES * 60));
const RAMPDOWN_SEC = 10;
const JWT_TTL_SEC = Math.max(7200, PLATEAU_SEC + 3600);

const POLL_INTERVAL_MS = 250;
const POLL_TIMEOUT_MS = 30000;
const CHECKOUT_RETRY_TIMEOUT_MS = 25000;

const PRE_ALLOCATED_VUS = Math.max(20, Math.round(SUSTAINED_JPS * 4));
const MAX_VUS = Math.max(80, Math.round(SUSTAINED_JPS * 15));

const userTokens = new SharedArray('soak_user_tokens', function () {
  return buildUserTokenPool(JWT_SECRET, USER_POOL_SIZE, JWT_TTL_SEC);
});

const holdExpected = http.expectedStatuses(201);
const checkoutExpected = http.expectedStatuses(202, 409);
const getOrderExpected = http.expectedStatuses(200);

export const options = {
  setupTimeout: '300s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    soak_journey: {
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '35s',
      stages: [
        { duration: `${RAMP_SEC}s`, target: SUSTAINED_JPS },
        { duration: `${PLATEAU_SEC}s`, target: SUSTAINED_JPS },
        { duration: `${RAMPDOWN_SEC}s`, target: 0 },
      ],
    },
  },
  thresholds: CHECKOUT_JOURNEY_THRESHOLDS,
};

export function setup() {
  initJourneyCounters();
  plateau1Reqs.add(0);

  const singleUserPollRps = 1000.0 / POLL_INTERVAL_MS;
  assertPerUserRateWithinBucket(SUSTAINED_JPS * 5.0, userTokens.length, 'soak', singleUserPollRps);

  const maxExpectedJourneys = Math.ceil(SUSTAINED_JPS * (RAMP_SEC + PLATEAU_SEC + RAMPDOWN_SEC) * 1.15);
  const eventCount = Math.max(1, Math.ceil(maxExpectedJourneys / SEATS_PER_EVENT));
  const admin = mintAdminToken(JWT_SECRET, JWT_TTL_SEC);
  const events = setupMultipleEvents(BASE_URL, admin.token, RUN_ID, 'soak', eventCount, SEATS_PER_EVENT);

  const totalSeats = events.reduce((acc, ev) => acc + ev.seatIds.length, 0);
  if (totalSeats < maxExpectedJourneys) {
    throw new Error(`[soak] Insufficient seats: have ${totalSeats}, need ${maxExpectedJourneys}`);
  }

  return {
    events: events.map((ev) => ({ eventId: ev.eventId, seatIds: ev.seatIds })),
    totalSeats: totalSeats,
    seatsPerEvent: SEATS_PER_EVENT,
    scenarioStartMs: Date.now(),
  };
}

export default function (data) {
  const iter = exec.scenario.iterationInTest;
  if (iter >= data.totalSeats) {
    unexpectedStatuses.add(1);
    return;
  }

  const journeyStartMs = Date.now();
  const elapsedSec = (journeyStartMs - data.scenarioStartMs) / 1000.0;
  if (elapsedSec >= RAMP_SEC && elapsedSec < RAMP_SEC + PLATEAU_SEC) {
    plateau1Reqs.add(1);
  }

  const user = userTokens[iter % userTokens.length];
  const ev = data.events[Math.floor(iter / data.seatsPerEvent)];
  const target = { eventId: ev.eventId, seatId: ev.seatIds[iter % data.seatsPerEvent] };

  // Step 1: Hold
  const holdRes = postHold(BASE_URL, user.token, target.eventId, target.seatId, generateUuidV4(), holdExpected);
  const holdBody = safeJson(holdRes);
  holdDuration.add(holdRes.timings.duration, { name: 'hold' });
  checkJourneyHold(holdRes, holdBody);

  if (isUnknownHoldOutcome(holdRes.status)) {
    holdUnknownOutcome.add(1);
  }

  if (holdRes.status !== 201 || !holdBody || !holdBody.reservationId) {
    unexpectedStatuses.add(1);
    return;
  }

  holdCreated.add(1);
  const holdDoneMs = Date.now();
  const orderId = holdBody.orderId || holdBody.reservationId;

  const bucket = iter % 20;
  if (bucket === 19) {
    journeyAbandoned.add(1);
    return;
  }

  const paymentToken = bucket === 18 ? 'tok_decline' : 'tok_success';
  const expectedFinalStatus = bucket === 18 ? 'CANCELLED_PAYMENT_FAILED' : 'CONFIRMED';

  // Step 2: Checkout
  const checkoutIdemKey = generateUuidV4();
  const checkoutStartLoopMs = Date.now();
  let checkoutAccepted = false;
  let orderVisibleMs = null;

  while (Date.now() - checkoutStartLoopMs < CHECKOUT_RETRY_TIMEOUT_MS) {
    const chkRes = postCheckout(BASE_URL, user.token, orderId, paymentToken, checkoutIdemKey, checkoutExpected);
    const chkBody = safeJson(chkRes);
    checkoutDuration.add(chkRes.timings.duration, { name: 'checkout' });

    if (chkRes.status === 202) {
      checkJourneyCheckout(chkRes, chkBody);
      orderVisibleMs = Date.now();
      checkoutAccepted = true;
      break;
    }

    if (chkRes.status === 409 && chkBody && chkBody.errorCode === 'ORDER_NOT_READY') {
      orderNotReadyRetries.add(1);
      sleep(parseRetryAfterSeconds(chkRes, 1.0));
      continue;
    }

    unexpectedStatuses.add(1);
    return;
  }

  if (!checkoutAccepted) {
    journeysTimedOut.add(1);
    return;
  }

  orderMaterializationLatency.add(orderVisibleMs - holdDoneMs);

  // Step 3: Poll order status
  const pollStartMs = Date.now();
  let finalStatus = null;

  while (Date.now() - pollStartMs < POLL_TIMEOUT_MS) {
    sleep(POLL_INTERVAL_MS / 1000.0);

    const ordRes = getOrder(BASE_URL, user.token, orderId, getOrderExpected);
    const ordBody = safeJson(ordRes);

    if (ordRes.status !== 200 || !ordBody) {
      unexpectedStatuses.add(1);
      return;
    }

    if (ordBody.status === 'CONFIRMED' || ordBody.status === 'CANCELLED_PAYMENT_FAILED') {
      finalStatus = ordBody.status;
      break;
    }
  }

  if (!finalStatus) {
    journeysTimedOut.add(1);
    return;
  }

  sagaLatency.add(Date.now() - journeyStartMs);
  checkJourneyFinalStatus(finalStatus, expectedFinalStatus);

  if (finalStatus === 'CONFIRMED') {
    journeyConfirmed.add(1);
  } else if (finalStatus === 'CANCELLED_PAYMENT_FAILED') {
    journeyPaymentFailed.add(1);
  }
}

export function handleSummary(data) {
  const startMs = (data.setup_data && data.setup_data.scenarioStartMs) || Date.now();
  const compact = Object.assign({}, data);
  if (compact.setup_data) {
    compact.setup_data = {
      seatCount: compact.setup_data.totalSeats || 0,
      eventCount: Array.isArray(compact.setup_data.events) ? compact.setup_data.events.length : 0,
      scenarioStartMs: startMs,
    };
  }
  const enriched = Object.assign({}, compact, {
    _meta: {
      scenario: 'soak',
      run_id: RUN_ID,
      user_pool_size: USER_POOL_SIZE,
      seats_per_event: SEATS_PER_EVENT,
      soak_minutes: SOAK_MINUTES,
      journey_rate_target: SUSTAINED_JPS,
      journey_duration_s: PLATEAU_SEC,
      ramp_s: RAMP_SEC,
      rampdown_s: RAMPDOWN_SEC,
      scenario_start_epoch: Math.floor(startMs / 1000),
      plateau_start_epoch: Math.floor(startMs / 1000) + RAMP_SEC,
      plateau_end_epoch: Math.floor(startMs / 1000) + RAMP_SEC + PLATEAU_SEC,
      pre_allocated_vus: PRE_ALLOCATED_VUS,
      max_vus: MAX_VUS,
      poll_interval_ms: POLL_INTERVAL_MS,
      poll_timeout_ms: POLL_TIMEOUT_MS,
      profile_source: profile.journey,
      mix: { tok_success: 0.90, tok_decline: 0.05, abandoned: 0.05 },
    },
  });

  const out = {};
  out[SUMMARY_PATH] = JSON.stringify(enriched, null, 2);
  return out;
}
