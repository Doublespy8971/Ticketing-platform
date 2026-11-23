import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';
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
const LOAD_SCALE = parseFloat(__ENV.LOAD_SCALE || '1.0');
const RUN_ID = __ENV.LOADTEST_RUN_ID || `bp-journey-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/breakpoint-journey-summary.json';

const manifest = loadSeatManifest(__ENV.SEATS_MANIFEST || '/results/seats.json');
const plan = manifest.meta.plan || {};

function envOrScaled(name, baseVal, minVal = 1) {
  if (__ENV[name] !== undefined && __ENV[name] !== '') {
    return parseInt(__ENV[name], 10);
  }
  return Math.max(minVal, Math.round(baseVal * LOAD_SCALE));
}

const WARMUP_SECONDS = __ENV.WARMUP_SECONDS !== undefined && __ENV.WARMUP_SECONDS !== ''
  ? parseInt(__ENV.WARMUP_SECONDS, 10) : 90;
const WARMUP_RATE = __ENV.WARMUP_RATE !== undefined && __ENV.WARMUP_RATE !== ''
  ? parseInt(__ENV.WARMUP_RATE, 10) : 4;
const WARMUP_GAP_SEC = WARMUP_SECONDS > 0 ? 15 : 0;
const MEASURED_START_OFFSET_SEC = WARMUP_SECONDS + WARMUP_GAP_SEC;

const START = envOrScaled('START', 2, 1);
const STEP = envOrScaled('STEP', 2, 1);
const REQUESTED_MAX = Math.max(START, envOrScaled('MAX', 40, START));
const MAX = plan.effective_max ? Math.min(REQUESTED_MAX, parseInt(plan.effective_max, 10)) : REQUESTED_MAX;
const STEP_SECONDS = __ENV.STEP_SECONDS ? parseInt(__ENV.STEP_SECONDS, 10) : 45;

const POLL_INTERVAL_MS = 250;
const POLL_TIMEOUT_MS = 30000;
const CHECKOUT_RETRY_TIMEOUT_MS = 25000;

const MAX_BOUNDED_STEPS = 40;
const stepDefs = [];
for (let rate = START, idx = 1; rate <= MAX && idx <= MAX_BOUNDED_STEPS; rate += STEP, idx++) {
  stepDefs.push({
    index: idx,
    tag: `s${String(idx).padStart(2, '0')}`,
    target_jps: rate,
    duration_s: STEP_SECONDS,
    start_offset_s: (idx - 1) * STEP_SECONDS,
    end_offset_s: idx * STEP_SECONDS,
  });
}

const PEAK_RATE = stepDefs[stepDefs.length - 1].target_jps;
const PRE_ALLOCATED_VUS = __ENV.BP_PRE_ALLOCATED_VUS
  ? parseInt(__ENV.BP_PRE_ALLOCATED_VUS, 10)
  : Math.min(150, Math.max(20, Math.round(PEAK_RATE * 3)));
const MAX_VUS = __ENV.BP_MAX_VUS
  ? parseInt(__ENV.BP_MAX_VUS, 10)
  : Math.min(400, Math.max(80, Math.round(PEAK_RATE * 10)));

const stages = [];
for (let i = 0; i < stepDefs.length; i++) {
  const s = stepDefs[i];
  if (i > 0) {
    stages.push({ duration: '0s', target: s.target_jps });
  }
  stages.push({ duration: `${STEP_SECONDS}s`, target: s.target_jps });
}

const bpStepReqs = new Counter('bp_step_reqs');
const bpStepError = new Rate('bp_step_error');
const bpStepUnknown = new Counter('bp_step_unknown');

const exportThresholds = {
  'hold_duration{outcome:created}': ['max>=0'],
  'http_req_duration{step:warmup,name:warmup_hold}': ['max>=0'],
};
for (const s of stepDefs) {
  exportThresholds[`http_req_duration{step:${s.tag},name:hold}`] = ['max>=0'];
  exportThresholds[`hold_duration{step:${s.tag},outcome:created}`] = ['max>=0'];
  exportThresholds[`http_req_duration{step:${s.tag},name:checkout}`] = ['max>=0'];
  exportThresholds[`saga_latency{step:${s.tag}}`] = ['max>=0'];
  exportThresholds[`bp_step_reqs{step:${s.tag}}`] = ['count>=0'];
  exportThresholds[`bp_step_error{step:${s.tag}}`] = ['rate>=0'];
  exportThresholds[`bp_step_unknown{step:${s.tag}}`] = ['count>=0'];
}

const WARMUP_USER_COUNT = Math.min(1000, Math.floor(USER_POOL_SIZE * 0.2));
const userTokens = new SharedArray('bp_journey_user_tokens', function () {
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
scenarios.breakpoint_journey = {
  executor: 'ramping-arrival-rate',
  exec: 'measuredFn',
  startTime: `${MEASURED_START_OFFSET_SEC}s`,
  startRate: stepDefs[0].target_jps,
  timeUnit: '1s',
  preAllocatedVUs: PRE_ALLOCATED_VUS,
  maxVUs: MAX_VUS,
  gracefulStop: '35s',
  stages: stages,
};

export const options = {
  setupTimeout: '60s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: scenarios,
  thresholds: exportThresholds,
};

export function setup() {
  initJourneyCounters();
  initWarmupCounters();
  for (const s of stepDefs) {
    bpStepReqs.add(0, { step: s.tag });
    bpStepUnknown.add(0, { step: s.tag });
  }
  const singleUserPollRps = 1000.0 / POLL_INTERVAL_MS;
  assertPerUserRateWithinBucket(PEAK_RATE * 5.0, USER_POOL_SIZE - WARMUP_USER_COUNT, 'breakpoint-journey', singleUserPollRps);

  const nowMs = Date.now();
  return {
    runStartMs: nowMs,
    measuredStartMs: nowMs + MEASURED_START_OFFSET_SEC * 1000,
  };
}

export function warmupFn() {
  const iter = exec.scenario.iterationInTest;
  const wSeats = manifest.meta.warmupSeats;
  if (iter >= wSeats) return;
  const user = userTokens[iter % WARMUP_USER_COUNT];
  const target = seatAt(manifest, iter);
  warmupReqs.add(1, { step: 'warmup' });

  const holdRes = postHold(
    BASE_URL, user.token, target.eventId, target.seatId, generateUuidV4(), holdExpected,
    { name: 'warmup_hold', step: 'warmup' }
  );
  const holdBody = safeJson(holdRes);
  if (isUnknownHoldOutcome(holdRes.status)) warmupHoldUnknownOutcome.add(1, { step: 'warmup' });
  if (holdRes.status !== 201 || !holdBody || !holdBody.reservationId) {
    warmupErrors.add(1, { step: 'warmup' });
    return;
  }
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
  const journeyStartMs = Date.now();
  const elapsedSec = Math.max(0, (journeyStartMs - data.measuredStartMs) / 1000.0);
  const stepIdx = Math.min(stepDefs.length, Math.floor(elapsedSec / STEP_SECONDS) + 1);
  const stepTag = stepDefs[stepIdx - 1].tag;
  const stepTags = { step: stepTag };

  bpStepReqs.add(1, stepTags);

  const iter = exec.scenario.iterationInTest;
  const seatIdx = manifest.meta.warmupSeats + iter;
  if (seatIdx >= manifest.meta.totalSeats) {
    unexpectedStatuses.add(1);
    bpStepError.add(true, stepTags);
    return;
  }

  const user = userTokens[WARMUP_USER_COUNT + (iter % (USER_POOL_SIZE - WARMUP_USER_COUNT))];
  const target = seatAt(manifest, seatIdx);

  const holdRes = postHold(BASE_URL, user.token, target.eventId, target.seatId, generateUuidV4(), holdExpected, stepTags);
  const holdBody = safeJson(holdRes);
  const outcome = classifyHoldOutcome(holdRes.status);
  holdDuration.add(holdRes.timings.duration, { name: 'hold', step: stepTag, outcome: outcome });
  checkJourneyHold(holdRes, holdBody);

  if (isUnknownHoldOutcome(holdRes.status)) {
    holdUnknownOutcome.add(1);
    bpStepUnknown.add(1, stepTags);
  }

  if (holdRes.status !== 201 || !holdBody || !holdBody.reservationId) {
    unexpectedStatuses.add(1);
    bpStepError.add(true, stepTags);
    return;
  }

  holdCreated.add(1);
  const holdDoneMs = Date.now();
  const orderId = holdBody.orderId || holdBody.reservationId;

  const bucket = iter % 20;
  if (bucket === 19) {
    journeyAbandoned.add(1);
    bpStepError.add(false, stepTags);
    return;
  }

  const paymentToken = bucket === 18 ? 'tok_decline' : 'tok_success';
  const expectedFinalStatus = bucket === 18 ? 'CANCELLED_PAYMENT_FAILED' : 'CONFIRMED';

  const checkoutIdemKey = generateUuidV4();
  const checkoutStartLoopMs = Date.now();
  let checkoutAccepted = false;
  let orderVisibleMs = null;

  while (Date.now() - checkoutStartLoopMs < CHECKOUT_RETRY_TIMEOUT_MS) {
    const chkRes = postCheckout(BASE_URL, user.token, orderId, paymentToken, checkoutIdemKey, checkoutExpected, stepTags);
    const chkBody = safeJson(chkRes);
    checkoutDuration.add(chkRes.timings.duration, { name: 'checkout', step: stepTag });

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
    bpStepError.add(true, stepTags);
    return;
  }

  if (!checkoutAccepted) {
    journeysTimedOut.add(1);
    bpStepError.add(true, stepTags);
    return;
  }

  orderMaterializationLatency.add(orderVisibleMs - holdDoneMs);

  const pollStartMs = Date.now();
  let finalStatus = null;

  while (Date.now() - pollStartMs < POLL_TIMEOUT_MS) {
    sleep(POLL_INTERVAL_MS / 1000.0);
    const ordRes = getOrder(BASE_URL, user.token, orderId, getOrderExpected, stepTags);
    const ordBody = safeJson(ordRes);

    if (ordRes.status !== 200 || !ordBody) {
      unexpectedStatuses.add(1);
      bpStepError.add(true, stepTags);
      return;
    }

    if (ordBody.status === 'CONFIRMED' || ordBody.status === 'CANCELLED_PAYMENT_FAILED') {
      finalStatus = ordBody.status;
      break;
    }
  }

  if (!finalStatus) {
    journeysTimedOut.add(1);
    bpStepError.add(true, stepTags);
    return;
  }

  sagaLatency.add(Date.now() - journeyStartMs, stepTags);
  checkJourneyFinalStatus(finalStatus, expectedFinalStatus);
  bpStepError.add(finalStatus !== expectedFinalStatus, stepTags);

  if (finalStatus === 'CONFIRMED') {
    journeyConfirmed.add(1);
  } else if (finalStatus === 'CANCELLED_PAYMENT_FAILED') {
    journeyPaymentFailed.add(1);
  }
}

export function handleSummary(data) {
  const runStartMs = (data.setup_data && data.setup_data.runStartMs) || Date.now();
  const measuredStartMs = (data.setup_data && data.setup_data.measuredStartMs) || (runStartMs + MEASURED_START_OFFSET_SEC * 1000);
  const measuredStartSec = Math.floor(measuredStartMs / 1000);
  const stepsWithEpoch = stepDefs.map((s) =>
    Object.assign({}, s, {
      start_epoch: measuredStartSec + s.start_offset_s,
      end_epoch: measuredStartSec + s.end_offset_s,
    })
  );
  const measuredEndSec = stepsWithEpoch.length > 0
    ? stepsWithEpoch[stepsWithEpoch.length - 1].end_epoch
    : measuredStartSec;

  const enriched = Object.assign({}, data, {
    _meta: {
      scenario: 'breakpoint-journey',
      run_id: RUN_ID,
      load_scale: LOAD_SCALE,
      user_pool_size: USER_POOL_SIZE,
      total_seeded_seats: manifest.meta.totalSeats,
      event_count: manifest.meta.eventCount,
      seed_plan: plan,
      warmup_seconds: WARMUP_SECONDS,
      warmup_rate: WARMUP_RATE,
      measured_start_epoch: measuredStartSec,
      measured_end_epoch: measuredEndSec,
      start_jps: START,
      step_jps: STEP,
      requested_max_jps: REQUESTED_MAX,
      max_jps: MAX,
      step_seconds: STEP_SECONDS,
      pre_allocated_vus: PRE_ALLOCATED_VUS,
      max_vus: MAX_VUS,
      poll_interval_ms: POLL_INTERVAL_MS,
      poll_timeout_ms: POLL_TIMEOUT_MS,
      mix: { tok_success: 0.90, tok_decline: 0.05, abandoned: 0.05 },
      steps: stepsWithEpoch,
    },
  });

  const out = {};
  out[SUMMARY_PATH] = JSON.stringify(enriched, null, 2);
  return out;
}
