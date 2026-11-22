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
  setupTwoEvents,
  fetchAllSeatIds,
  waitEventVisible,
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
  initJourneyCounters,
  isUnknownHoldOutcome,
} from './lib/metrics.js';

const BASE_URL = __ENV.BASE_URL || 'http://gateway-service:8080';
const JWT_SECRET = __ENV.JWT_SECRET || '';
const LOAD_SCALE = parseFloat(__ENV.LOAD_SCALE || '1.0');
const RUN_ID = __ENV.LOADTEST_RUN_ID || `journey-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SEATS_PER_EVENT = parseInt(__ENV.SEATS_PER_EVENT || '5000', 10);
const REUSE_EVENT_ID = __ENV.REUSE_EVENT_ID || '';
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/checkout-journey-summary.json';

function scaledInt(envName, baseVal, minVal = 1) {
  if (__ENV[envName] !== undefined && __ENV[envName] !== '') {
    return parseInt(__ENV[envName], 10);
  }
  return Math.max(minVal, Math.round(baseVal * LOAD_SCALE));
}

const JOURNEY_RATE = scaledInt('JOURNEY_RATE', 25, 2);
const JOURNEY_DURATION_SEC = scaledInt('JOURNEY_DURATION_SEC', 180, 15);
const PRE_ALLOCATED_VUS = scaledInt('JOURNEY_PRE_ALLOCATED_VUS', 100, 15);
const MAX_VUS = scaledInt('JOURNEY_MAX_VUS', 300, 50);

const POLL_INTERVAL_MS = 250;
const POLL_TIMEOUT_MS = 30000;
const CHECKOUT_RETRY_TIMEOUT_MS = 25000;

// Pre-generate 5000 distinct user tokens once in the init stage
const userTokens = new SharedArray('journey_user_tokens', function () {
  return buildUserTokenPool(JWT_SECRET, USER_POOL_SIZE, 7200);
});

const holdExpected = http.expectedStatuses(201);
const checkoutExpected = http.expectedStatuses(202, 409);
const getOrderExpected = http.expectedStatuses(200);

export const options = {
  setupTimeout: '180s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    checkout_journey: {
      executor: 'constant-arrival-rate',
      rate: JOURNEY_RATE,
      timeUnit: '1s',
      duration: `${JOURNEY_DURATION_SEC}s`,
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '35s',
    },
  },
  thresholds: {
    // Documented SLOs (docs/arch/09-conventions.md & user specification)
    'http_req_duration{name:checkout}': ['p(99)<500'],
    'http_req_duration{name:hold}': ['p(99)<300'],
    // Chosen target (documented as chosen, not a formal SLO, in loadtest/README.md)
    'saga_latency': ['p(95)<5000'],
    'journeys_timed_out': ['count==0'],
    'unexpected_statuses': ['count==0'],
  },
};

export function setup() {
  initJourneyCounters();

  // Active single-user polling rate is 1000/250 = 4.0 req/s (< 5.0 req/s sustained bucket).
  // Average rate across 5000 users is also far below 5.0 req/s.
  const singleUserPollRps = 1000.0 / POLL_INTERVAL_MS;
  assertPerUserRateWithinBucket(JOURNEY_RATE * 5.0, userTokens.length, 'checkout-journey', singleUserPollRps);

  const neededSeats = Math.ceil(JOURNEY_RATE * JOURNEY_DURATION_SEC * 1.05);
  const seatsToCreate = Math.max(SEATS_PER_EVENT, neededSeats);

  let eventId;
  let seatIds;

  if (REUSE_EVENT_ID) {
    waitEventVisible(BASE_URL, REUSE_EVENT_ID);
    seatIds = fetchAllSeatIds(BASE_URL, REUSE_EVENT_ID);
    eventId = REUSE_EVENT_ID;
    console.log(`[checkout-journey] Reusing pre-created event ${eventId} with ${seatIds.length} seats`);
  } else {
    const admin = mintAdminToken(JWT_SECRET, 7200);
    const events = setupTwoEvents(BASE_URL, admin.token, RUN_ID, seatsToCreate);
    eventId = events.journeyEvent.eventId;
    seatIds = events.journeyEvent.seatIds;
    console.log(`LOADTEST_JOURNEY_EVENT_ID=${eventId}`);
  }

  if (seatIds.length < JOURNEY_RATE * JOURNEY_DURATION_SEC) {
    throw new Error(
      `Insufficient seats for checkout-journey: have ${seatIds.length}, need at least ${JOURNEY_RATE * JOURNEY_DURATION_SEC}`
    );
  }

  return {
    eventId: eventId,
    seatIds: seatIds,
  };
}

export default function (data) {
  const iter = exec.scenario.iterationInTest;
  if (iter >= data.seatIds.length) {
    unexpectedStatuses.add(1);
    console.error(`[checkout-journey] Seat pool exhausted at iteration ${iter} (seats=${data.seatIds.length})`);
    return;
  }

  const user = userTokens[iter % userTokens.length];
  const seatId = data.seatIds[iter];
  const journeyStartMs = Date.now();

  // Step 1: Create seat hold (201)
  const holdRes = postHold(BASE_URL, user.token, data.eventId, seatId, generateUuidV4(), holdExpected);
  const holdBody = safeJson(holdRes);
  holdDuration.add(holdRes.timings.duration, { name: 'hold' });
  checkJourneyHold(holdRes, holdBody);

  if (isUnknownHoldOutcome(holdRes.status)) {
    holdUnknownOutcome.add(1);
  }

  if (holdRes.status !== 201 || !holdBody || !holdBody.reservationId) {
    unexpectedStatuses.add(1);
    console.error(`[checkout-journey] Unexpected hold status=${holdRes.status} iter=${iter} body=${holdRes.body}`);
    return;
  }

  holdCreated.add(1);
  const holdDoneMs = Date.now();
  const orderId = holdBody.orderId || holdBody.reservationId;

  // Deterministic 90% tok_success / 5% tok_decline / 5% abandon (mod 20)
  const bucket = iter % 20;
  if (bucket === 19) {
    // 5% abandoned holds: do not checkout, do not force expiry, do not poll
    journeyAbandoned.add(1);
    return;
  }

  const paymentToken = bucket === 18 ? 'tok_decline' : 'tok_success';
  const expectedFinalStatus = bucket === 18 ? 'CANCELLED_PAYMENT_FAILED' : 'CONFIRMED';

  // Step 2: Checkout with early-checkout race handling (409 ORDER_NOT_READY + Retry-After)
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
      const retryAfterSec = parseRetryAfterSeconds(chkRes, 1.0);
      sleep(retryAfterSec);
      continue;
    }

    unexpectedStatuses.add(1);
    console.error(
      `[checkout-journey] Unexpected checkout status=${chkRes.status} orderId=${orderId} body=${chkRes.body}`
    );
    return;
  }

  if (!checkoutAccepted) {
    journeysTimedOut.add(1);
    console.error(`[checkout-journey] Checkout timed out waiting for ORDER_NOT_READY orderId=${orderId}`);
    return;
  }

  orderMaterializationLatency.add(orderVisibleMs - holdDoneMs);

  // Step 3: Poll GET /api/v1/orders/{id} every 250ms up to 30s until CONFIRMED or CANCELLED_PAYMENT_FAILED
  const pollStartMs = Date.now();
  let finalStatus = null;

  while (Date.now() - pollStartMs < POLL_TIMEOUT_MS) {
    sleep(POLL_INTERVAL_MS / 1000.0);

    const ordRes = getOrder(BASE_URL, user.token, orderId, getOrderExpected);
    const ordBody = safeJson(ordRes);

    if (ordRes.status !== 200 || !ordBody) {
      unexpectedStatuses.add(1);
      console.error(`[checkout-journey] Unexpected GET order status=${ordRes.status} orderId=${orderId} body=${ordRes.body}`);
      return;
    }

    if (ordBody.status === 'CONFIRMED' || ordBody.status === 'CANCELLED_PAYMENT_FAILED') {
      finalStatus = ordBody.status;
      break;
    }
  }

  if (!finalStatus) {
    journeysTimedOut.add(1);
    console.error(`[checkout-journey] Timed out waiting 30s for terminal status orderId=${orderId}`);
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
  const compact = Object.assign({}, data);
  if (compact.setup_data && Array.isArray(compact.setup_data.seatIds)) {
    compact.setup_data = Object.assign({}, compact.setup_data, {
      seatCount: compact.setup_data.seatIds.length,
    });
    delete compact.setup_data.seatIds;
  }
  const enriched = Object.assign({}, compact, {
    _meta: {
      scenario: 'checkout-journey',
      run_id: RUN_ID,
      load_scale: LOAD_SCALE,
      user_pool_size: USER_POOL_SIZE,
      seats_per_event: SEATS_PER_EVENT,
      journey_rate_target: JOURNEY_RATE,
      journey_duration_s: JOURNEY_DURATION_SEC,
      pre_allocated_vus: PRE_ALLOCATED_VUS,
      max_vus: MAX_VUS,
      poll_interval_ms: POLL_INTERVAL_MS,
      poll_timeout_ms: POLL_TIMEOUT_MS,
      mix: { tok_success: 0.90, tok_decline: 0.05, abandoned: 0.05 },
    },
  });

  const out = {};
  out[SUMMARY_PATH] = JSON.stringify(enriched, null, 2);
  return out;
}
