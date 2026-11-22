import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';
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
const RUN_ID = __ENV.LOADTEST_RUN_ID || `bp-journey-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SEATS_PER_EVENT = parseInt(__ENV.SEATS_PER_EVENT || '5000', 10);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/breakpoint-journey-summary.json';

function envOrScaled(name, baseVal, minVal = 1) {
  if (__ENV[name] !== undefined && __ENV[name] !== '') {
    return parseInt(__ENV[name], 10);
  }
  return Math.max(minVal, Math.round(baseVal * LOAD_SCALE));
}

const START = envOrScaled('START', 2, 1);
const STEP = envOrScaled('STEP', 2, 1);
const MAX = Math.max(START, envOrScaled('MAX', 40, START));
const STEP_SECONDS = __ENV.STEP_SECONDS ? parseInt(__ENV.STEP_SECONDS, 10) : 45;

const POLL_INTERVAL_MS = 250;
const POLL_TIMEOUT_MS = 30000;
const CHECKOUT_RETRY_TIMEOUT_MS = 25000;

const MAX_BOUNDED_STEPS = 40;
const stepDefs = [];
let maxPossibleJourneys = 0;
for (let rate = START, idx = 1; rate <= MAX && idx <= MAX_BOUNDED_STEPS; rate += STEP, idx++) {
  stepDefs.push({
    index: idx,
    tag: `s${String(idx).padStart(2, '0')}`,
    target_jps: rate,
    duration_s: STEP_SECONDS,
    start_offset_s: (idx - 1) * STEP_SECONDS,
    end_offset_s: idx * STEP_SECONDS,
  });
  maxPossibleJourneys += rate * STEP_SECONDS;
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

// Non-aborting sub-metric thresholds ONLY so k6 exports per-step sub-metrics in handleSummary
const exportThresholds = {};
for (const s of stepDefs) {
  exportThresholds[`http_req_duration{step:${s.tag},name:hold}`] = ['max>=0'];
  exportThresholds[`http_req_duration{step:${s.tag},name:checkout}`] = ['max>=0'];
  exportThresholds[`saga_latency{step:${s.tag}}`] = ['max>=0'];
  exportThresholds[`bp_step_reqs{step:${s.tag}}`] = ['count>=0'];
  exportThresholds[`bp_step_error{step:${s.tag}}`] = ['rate>=0'];
  exportThresholds[`bp_step_unknown{step:${s.tag}}`] = ['count>=0'];
}

const userTokens = new SharedArray('bp_journey_user_tokens', function () {
  return buildUserTokenPool(JWT_SECRET, USER_POOL_SIZE, 7200);
});

const holdExpected = http.expectedStatuses(201);
const checkoutExpected = http.expectedStatuses(202, 409);
const getOrderExpected = http.expectedStatuses(200);

export const options = {
  setupTimeout: '300s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    breakpoint_journey: {
      executor: 'ramping-arrival-rate',
      startRate: stepDefs[0].target_jps,
      timeUnit: '1s',
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '35s',
      stages: stages,
    },
  },
  thresholds: exportThresholds,
};

export function setup() {
  initJourneyCounters();
  for (const s of stepDefs) {
    bpStepReqs.add(0, { step: s.tag });
    bpStepUnknown.add(0, { step: s.tag });
  }

  const singleUserPollRps = 1000.0 / POLL_INTERVAL_MS;
  assertPerUserRateWithinBucket(PEAK_RATE * 5.0, userTokens.length, 'breakpoint-journey', singleUserPollRps);

  const neededSeats = Math.ceil(maxPossibleJourneys * 1.05);
  const eventCount = Math.max(1, Math.ceil(neededSeats / SEATS_PER_EVENT));
  const admin = mintAdminToken(JWT_SECRET, 7200);
  const events = setupMultipleEvents(
    BASE_URL,
    admin.token,
    RUN_ID,
    'bpjourney',
    eventCount,
    SEATS_PER_EVENT
  );

  let totalSeats = 0;
  for (const ev of events) {
    totalSeats += ev.seatIds.length;
  }

  if (totalSeats < maxPossibleJourneys) {
    throw new Error(
      `[breakpoint-journey] Insufficient seats created: have ${totalSeats}, need at least ${maxPossibleJourneys}`
    );
  }

  return {
    events: events.map((ev) => ({ eventId: ev.eventId, seatIds: ev.seatIds })),
    totalSeats: totalSeats,
    seatsPerEvent: SEATS_PER_EVENT,
    scenarioStartMs: Date.now(),
  };
}

export default function (data) {
  const journeyStartMs = Date.now();
  const elapsedSec = Math.max(0, (journeyStartMs - data.scenarioStartMs) / 1000.0);
  const stepIdx = Math.min(stepDefs.length, Math.floor(elapsedSec / STEP_SECONDS) + 1);
  const stepTag = stepDefs[stepIdx - 1].tag;
  const stepTags = { step: stepTag };

  bpStepReqs.add(1, stepTags);

  const iter = exec.scenario.iterationInTest;
  if (iter >= data.totalSeats) {
    unexpectedStatuses.add(1);
    bpStepError.add(true, stepTags);
    return;
  }

  const user = userTokens[iter % userTokens.length];
  const evIdx = Math.floor(iter / data.seatsPerEvent);
  const seatIdx = iter % data.seatsPerEvent;
  const targetEvent = data.events[evIdx];
  const target = { eventId: targetEvent.eventId, seatId: targetEvent.seatIds[seatIdx] };

  // Step 1: Hold
  const holdRes = postHold(
    BASE_URL,
    user.token,
    target.eventId,
    target.seatId,
    generateUuidV4(),
    holdExpected,
    stepTags
  );
  const holdBody = safeJson(holdRes);
  holdDuration.add(holdRes.timings.duration, { name: 'hold', step: stepTag });
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

  // Deterministic 90% tok_success / 5% tok_decline / 5% abandon (mod 20 — never > 5% abandoned)
  const bucket = iter % 20;
  if (bucket === 19) {
    journeyAbandoned.add(1);
    bpStepError.add(false, stepTags);
    return;
  }

  const paymentToken = bucket === 18 ? 'tok_decline' : 'tok_success';
  const expectedFinalStatus = bucket === 18 ? 'CANCELLED_PAYMENT_FAILED' : 'CONFIRMED';

  // Step 2: Checkout with ORDER_NOT_READY retry
  const checkoutIdemKey = generateUuidV4();
  const checkoutStartLoopMs = Date.now();
  let checkoutAccepted = false;
  let orderVisibleMs = null;

  while (Date.now() - checkoutStartLoopMs < CHECKOUT_RETRY_TIMEOUT_MS) {
    const chkRes = postCheckout(
      BASE_URL,
      user.token,
      orderId,
      paymentToken,
      checkoutIdemKey,
      checkoutExpected,
      stepTags
    );
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

  // Step 3: Poll order status every 250ms up to 30s
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
  const startMs = (data.setup_data && data.setup_data.scenarioStartMs) || Date.now();
  const startSec = Math.floor(startMs / 1000);
  const stepsWithEpoch = stepDefs.map((s) =>
    Object.assign({}, s, {
      start_epoch: startSec + s.start_offset_s,
      end_epoch: startSec + s.end_offset_s,
    })
  );

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
      scenario: 'breakpoint-journey',
      run_id: RUN_ID,
      load_scale: LOAD_SCALE,
      user_pool_size: USER_POOL_SIZE,
      seats_per_event: SEATS_PER_EVENT,
      start_jps: START,
      step_jps: STEP,
      max_jps: MAX,
      step_seconds: STEP_SECONDS,
      max_possible_journeys: maxPossibleJourneys,
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
