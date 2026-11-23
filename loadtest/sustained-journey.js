import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import {
  buildUserTokenPool,
  generateUuidV4,
  assertPerUserRateWithinBucket,
} from './lib/jwt.js';
import { postHold, postCheckout, getOrder } from './lib/api.js';
import {
  safeJson,
  checkJourneyHold,
  checkJourneyCheckout,
  checkJourneyFinalStatus,
  parseRetryAfterSeconds,
} from './lib/checks.js';
import { CHECKOUT_JOURNEY_THRESHOLDS } from './lib/thresholds.js';
import { loadSeatManifest, seatAt } from './lib/seats.js';
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
  warmupReqs,
  warmupHoldCreated,
  warmupHoldUnknownOutcome,
  warmupErrors,
  initJourneyCounters,
  initWarmupCounters,
  isUnknownHoldOutcome,
  classifyHoldOutcome,
} from './lib/metrics.js';

const BASE_URL = __ENV.BASE_URL || 'http://gateway-service:8080';
const JWT_SECRET = __ENV.JWT_SECRET || '';
const RUN_ID = __ENV.LOADTEST_RUN_ID || `sustained-journey-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/sustained-journey-summary.json';
const PROFILE_PATH = __ENV.SUSTAINABLE_PROFILE_PATH || './sustainable-profile.json';

const profile = JSON.parse(open(PROFILE_PATH));
if (!profile || !profile.journey || !profile.journey.sustainable_jps || profile.journey.sustainable_jps <= 0) {
  throw new Error(`Invalid or missing journey.sustainable_jps in ${PROFILE_PATH}`);
}

const SUSTAINED_JPS = parseInt(profile.journey.sustainable_jps, 10);
const RAMP_SEC = parseInt(__ENV.SUSTAINED_RAMP_SEC || '20', 10);
const PLATEAU_SEC = parseInt(__ENV.SUSTAINED_PLATEAU_SEC || '300', 10);
const RAMPDOWN_SEC = parseInt(__ENV.SUSTAINED_RAMPDOWN_SEC || '10', 10);

const WARMUP_SECONDS = __ENV.WARMUP_SECONDS !== undefined && __ENV.WARMUP_SECONDS !== ''
  ? parseInt(__ENV.WARMUP_SECONDS, 10) : 90;
const WARMUP_RATE = __ENV.WARMUP_RATE !== undefined && __ENV.WARMUP_RATE !== ''
  ? parseInt(__ENV.WARMUP_RATE, 10) : 4;
const WARMUP_GAP_SEC = WARMUP_SECONDS > 0 ? 15 : 0;
const MEASURED_START_OFFSET_SEC = WARMUP_SECONDS + WARMUP_GAP_SEC;

const POLL_INTERVAL_MS = 250;
const POLL_TIMEOUT_MS = 30000;
const CHECKOUT_RETRY_TIMEOUT_MS = 25000;

const manifest = loadSeatManifest(__ENV.SEATS_MANIFEST || '/results/seats.json');
const PRE_ALLOCATED_VUS = Math.max(20, Math.round(SUSTAINED_JPS * 4));
const MAX_VUS = Math.max(80, Math.round(SUSTAINED_JPS * 15));

const WARMUP_USER_COUNT = Math.min(1000, Math.floor(USER_POOL_SIZE * 0.2));
const userTokens = new SharedArray('sustained_journey_user_tokens', function () {
  return buildUserTokenPool(JWT_SECRET, USER_POOL_SIZE, 7200);
});

const holdExpected = http.expectedStatuses(201);
const checkoutExpected = http.expectedStatuses(202, 409);
const getOrderExpected = http.expectedStatuses(200);

const scenarios = {};
if (WARMUP_SECONDS > 0 && WARMUP_RATE > 0) {
  scenarios.warmup = {
    executor: 'constant-arrival-rate',
    exec: 'warmupFn',
    startTime: '0s',
    rate: WARMUP_RATE,
    timeUnit: '1s',
    duration: `${WARMUP_SECONDS}s`,
    preAllocatedVUs: Math.max(10, WARMUP_RATE * 4),
    maxVUs: Math.max(40, WARMUP_RATE * 12),
    gracefulStop: `${WARMUP_GAP_SEC}s`,
  };
}
scenarios.sustained_journey = {
  executor: 'ramping-arrival-rate',
  exec: 'measuredFn',
  startTime: `${MEASURED_START_OFFSET_SEC}s`,
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
};

export const options = {
  setupTimeout: '60s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: scenarios,
  thresholds: CHECKOUT_JOURNEY_THRESHOLDS,
};

export function setup() {
  initJourneyCounters();
  initWarmupCounters();
  plateau1Reqs.add(0);
  const singleUserPollRps = 1000.0 / POLL_INTERVAL_MS;
  assertPerUserRateWithinBucket(SUSTAINED_JPS * 5.0, USER_POOL_SIZE - WARMUP_USER_COUNT, 'sustained-journey', singleUserPollRps);
  const nowMs = Date.now();
  return {
    runStartMs: nowMs,
    measuredStartMs: nowMs + MEASURED_START_OFFSET_SEC * 1000,
  };
}

export function warmupFn() {
  const iter = exec.scenario.iterationInTest;
  if (iter >= manifest.meta.warmupSeats) return;
  const user = userTokens[iter % WARMUP_USER_COUNT];
  const target = seatAt(manifest, iter);
  warmupReqs.add(1, { step: 'warmup' });
  const holdRes = postHold(BASE_URL, user.token, target.eventId, target.seatId, generateUuidV4(), holdExpected, { name: 'warmup_hold', step: 'warmup' });
  const holdBody = safeJson(holdRes);
  if (isUnknownHoldOutcome(holdRes.status)) warmupHoldUnknownOutcome.add(1, { step: 'warmup' });
  if (holdRes.status !== 201 || !holdBody || !holdBody.reservationId) { warmupErrors.add(1, { step: 'warmup' }); return; }
  warmupHoldCreated.add(1, { step: 'warmup' });
  if (iter % 20 === 19) return;
  const orderId = holdBody.orderId || holdBody.reservationId;
  const token = (iter % 20 === 18) ? 'tok_decline' : 'tok_success';
  const chkIdem = generateUuidV4();
  const t0 = Date.now();
  let accepted = false;
  while (Date.now() - t0 < CHECKOUT_RETRY_TIMEOUT_MS) {
    const cr = postCheckout(BASE_URL, user.token, orderId, token, chkIdem, checkoutExpected, { name: 'warmup_checkout', step: 'warmup' });
    const cb = safeJson(cr);
    if (cr.status === 202) { accepted = true; break; }
    if (cr.status === 409 && cb && cb.errorCode === 'ORDER_NOT_READY') { sleep(parseRetryAfterSeconds(cr, 1.0)); continue; }
    warmupErrors.add(1, { step: 'warmup' });
    return;
  }
  if (!accepted) { warmupErrors.add(1, { step: 'warmup' }); return; }
  const p0 = Date.now();
  while (Date.now() - p0 < POLL_TIMEOUT_MS) {
    sleep(POLL_INTERVAL_MS / 1000.0);
    const gr = getOrder(BASE_URL, user.token, orderId, getOrderExpected, { name: 'warmup_get_order', step: 'warmup' });
    const gb = safeJson(gr);
    if (gr.status !== 200 || !gb) { warmupErrors.add(1, { step: 'warmup' }); return; }
    if (gb.status === 'CONFIRMED' || gb.status === 'CANCELLED_PAYMENT_FAILED') break;
  }
}

export function measuredFn(data) {
  const iter = exec.scenario.iterationInTest;
  const seatIdx = manifest.meta.warmupSeats + iter;
  if (seatIdx >= manifest.meta.totalSeats) {
    unexpectedStatuses.add(1);
    return;
  }

  const journeyStartMs = Date.now();
  const elapsedSec = (journeyStartMs - data.measuredStartMs) / 1000.0;
  if (elapsedSec >= RAMP_SEC && elapsedSec < RAMP_SEC + PLATEAU_SEC) {
    plateau1Reqs.add(1);
  }

  const user = userTokens[WARMUP_USER_COUNT + (iter % (USER_POOL_SIZE - WARMUP_USER_COUNT))];
  const target = seatAt(manifest, seatIdx);

  const holdRes = postHold(BASE_URL, user.token, target.eventId, target.seatId, generateUuidV4(), holdExpected);
  const holdBody = safeJson(holdRes);
  holdDuration.add(holdRes.timings.duration, { name: 'hold', outcome: classifyHoldOutcome(holdRes.status) });
  checkJourneyHold(holdRes, holdBody);

  if (isUnknownHoldOutcome(holdRes.status)) holdUnknownOutcome.add(1);
  if (holdRes.status !== 201 || !holdBody || !holdBody.reservationId) {
    unexpectedStatuses.add(1);
    return;
  }

  holdCreated.add(1);
  const holdDoneMs = Date.now();
  const orderId = holdBody.orderId || holdBody.reservationId;

  const bucket = iter % 20;
  if (bucket === 19) { journeyAbandoned.add(1); return; }

  const paymentToken = bucket === 18 ? 'tok_decline' : 'tok_success';
  const expectedFinalStatus = bucket === 18 ? 'CANCELLED_PAYMENT_FAILED' : 'CONFIRMED';

  const checkoutIdemKey = generateUuidV4();
  const checkoutStartLoopMs = Date.now();
  let checkoutAccepted = false;
  let orderVisibleMs = null;

  while (Date.now() - checkoutStartLoopMs < CHECKOUT_RETRY_TIMEOUT_MS) {
    const chkRes = postCheckout(BASE_URL, user.token, orderId, paymentToken, checkoutIdemKey, checkoutExpected);
    const chkBody = safeJson(chkRes);
    checkoutDuration.add(chkRes.timings.duration, { name: 'checkout' });
    if (chkRes.status === 202) { checkJourneyCheckout(chkRes, chkBody); orderVisibleMs = Date.now(); checkoutAccepted = true; break; }
    if (chkRes.status === 409 && chkBody && chkBody.errorCode === 'ORDER_NOT_READY') { orderNotReadyRetries.add(1); sleep(parseRetryAfterSeconds(chkRes, 1.0)); continue; }
    unexpectedStatuses.add(1);
    return;
  }

  if (!checkoutAccepted) { journeysTimedOut.add(1); return; }
  orderMaterializationLatency.add(orderVisibleMs - holdDoneMs);

  const pollStartMs = Date.now();
  let finalStatus = null;
  while (Date.now() - pollStartMs < POLL_TIMEOUT_MS) {
    sleep(POLL_INTERVAL_MS / 1000.0);
    const ordRes = getOrder(BASE_URL, user.token, orderId, getOrderExpected);
    const ordBody = safeJson(ordRes);
    if (ordRes.status !== 200 || !ordBody) { unexpectedStatuses.add(1); return; }
    if (ordBody.status === 'CONFIRMED' || ordBody.status === 'CANCELLED_PAYMENT_FAILED') { finalStatus = ordBody.status; break; }
  }

  if (!finalStatus) { journeysTimedOut.add(1); return; }
  sagaLatency.add(Date.now() - journeyStartMs);
  checkJourneyFinalStatus(finalStatus, expectedFinalStatus);
  if (finalStatus === 'CONFIRMED') journeyConfirmed.add(1);
  else if (finalStatus === 'CANCELLED_PAYMENT_FAILED') journeyPaymentFailed.add(1);
}

export function handleSummary(data) {
  const measuredStartMs = (data.setup_data && data.setup_data.measuredStartMs) || Date.now();
  const measuredStartSec = Math.floor(measuredStartMs / 1000);
  const enriched = Object.assign({}, data, {
    _meta: {
      scenario: 'sustained-journey',
      run_id: RUN_ID,
      user_pool_size: USER_POOL_SIZE,
      total_seeded_seats: manifest.meta.totalSeats,
      event_count: manifest.meta.eventCount,
      warmup_seconds: WARMUP_SECONDS,
      warmup_rate: WARMUP_RATE,
      measured_start_epoch: measuredStartSec,
      measured_end_epoch: measuredStartSec + RAMP_SEC + PLATEAU_SEC + RAMPDOWN_SEC,
      journey_rate_target: SUSTAINED_JPS,
      journey_duration_s: PLATEAU_SEC,
      ramp_s: RAMP_SEC,
      rampdown_s: RAMPDOWN_SEC,
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
