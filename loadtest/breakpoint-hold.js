import http from 'k6/http';
import { Counter, Rate } from 'k6/metrics';
import { SharedArray } from 'k6/data';
import {
  buildUserTokenPool,
  mintAdminToken,
  generateUuidV4,
  assertPerUserRateWithinBucket,
} from './lib/jwt.js';
import { setupMultipleEvents, postHold } from './lib/api.js';
import { safeJson, checkHoldResponse } from './lib/checks.js';
import {
  holdCreated,
  holdConflict,
  holdRateLimited,
  holdRateLimitedRate,
  holdUnexpected,
  holdUnknownOutcome,
  hold5xx,
  holdDuration,
  initFlashSaleCounters,
  isUnknownHoldOutcome,
} from './lib/metrics.js';

const BASE_URL = __ENV.BASE_URL || 'http://gateway-service:8080';
const JWT_SECRET = __ENV.JWT_SECRET || '';
const LOAD_SCALE = parseFloat(__ENV.LOAD_SCALE || '1.0');
const RUN_ID = __ENV.LOADTEST_RUN_ID || `bp-hold-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SEATS_PER_EVENT = parseInt(__ENV.SEATS_PER_EVENT || '5000', 10);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/breakpoint-hold-summary.json';

function envOrScaled(name, baseVal, minVal = 1) {
  if (__ENV[name] !== undefined && __ENV[name] !== '') {
    return parseInt(__ENV[name], 10);
  }
  return Math.max(minVal, Math.round(baseVal * LOAD_SCALE));
}

const START = envOrScaled('START', 25, 5);
const STEP = envOrScaled('STEP', 25, 5);
const MAX = Math.max(START, envOrScaled('MAX', 600, START));
const STEP_SECONDS = __ENV.STEP_SECONDS ? parseInt(__ENV.STEP_SECONDS, 10) : 30;

const MAX_BOUNDED_STEPS = 40;
const stepDefs = [];
for (let rate = START, idx = 1; rate <= MAX && idx <= MAX_BOUNDED_STEPS; rate += STEP, idx++) {
  stepDefs.push({
    index: idx,
    tag: `s${String(idx).padStart(2, '0')}`,
    target_rps: rate,
    duration_s: STEP_SECONDS,
    start_offset_s: (idx - 1) * STEP_SECONDS,
    end_offset_s: idx * STEP_SECONDS,
  });
}

const PEAK_RATE = stepDefs[stepDefs.length - 1].target_rps;
const PRE_ALLOCATED_VUS = __ENV.BP_PRE_ALLOCATED_VUS
  ? parseInt(__ENV.BP_PRE_ALLOCATED_VUS, 10)
  : Math.min(200, Math.max(30, Math.round(PEAK_RATE * 0.25)));
const MAX_VUS = __ENV.BP_MAX_VUS
  ? parseInt(__ENV.BP_MAX_VUS, 10)
  : Math.min(350, Math.max(100, Math.round(PEAK_RATE * 0.6)));

const stages = [];
for (let i = 0; i < stepDefs.length; i++) {
  const s = stepDefs[i];
  if (i > 0) {
    stages.push({ duration: '0s', target: s.target_rps });
  }
  stages.push({ duration: `${STEP_SECONDS}s`, target: s.target_rps });
}

// Per-step custom metrics (bounded step tag s01..s40)
const bpStepReqs = new Counter('bp_step_reqs');
const bpStepError = new Rate('bp_step_error');
const bpStepUnknown = new Counter('bp_step_unknown');

// Non-aborting sub-metric thresholds ONLY so k6 exports per-step sub-metrics in handleSummary
const exportThresholds = {};
for (const s of stepDefs) {
  exportThresholds[`http_req_duration{step:${s.tag},name:hold}`] = ['max>=0'];
  exportThresholds[`bp_step_reqs{step:${s.tag}}`] = ['count>=0'];
  exportThresholds[`bp_step_error{step:${s.tag}}`] = ['rate>=0'];
  exportThresholds[`bp_step_unknown{step:${s.tag}}`] = ['count>=0'];
}

const userTokens = new SharedArray('bp_hold_user_tokens', function () {
  return buildUserTokenPool(JWT_SECRET, USER_POOL_SIZE, 7200);
});

const holdExpectedStatuses = http.expectedStatuses(201, 409, 429);

export const options = {
  setupTimeout: '300s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    breakpoint_hold: {
      executor: 'ramping-arrival-rate',
      startRate: stepDefs[0].target_rps,
      timeUnit: '1s',
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '15s',
      stages: stages,
    },
  },
  thresholds: exportThresholds,
};

export function setup() {
  initFlashSaleCounters();
  for (const s of stepDefs) {
    bpStepReqs.add(0, { step: s.tag });
    bpStepUnknown.add(0, { step: s.tag });
  }

  assertPerUserRateWithinBucket(PEAK_RATE, userTokens.length, 'breakpoint-hold', 0);

  // Use one 5000-seat event (matching flash-sale.js contention model; configurable via BP_HOLD_EVENTS)
  // so early/mid steps sustain real 201 writes + 409 contention while keeping k6 VU memory and outbox bounded.
  const maxEvents = parseInt(__ENV.BP_HOLD_EVENTS || '1', 10);
  const eventCount = Math.max(1, Math.min(stepDefs.length, maxEvents));
  const admin = mintAdminToken(JWT_SECRET, 7200);
  const events = setupMultipleEvents(
    BASE_URL,
    admin.token,
    RUN_ID,
    'bphold',
    eventCount,
    SEATS_PER_EVENT
  );
  if (events.length !== eventCount) {
    throw new Error(
      `[breakpoint-hold] Setup failed: expected ${eventCount} events of ${SEATS_PER_EVENT} seats, created ${events.length}`
    );
  }

  return {
    events: events.map((ev) => ({ eventId: ev.eventId, seatIds: ev.seatIds })),
    scenarioStartMs: Date.now(),
  };
}

export default function (data) {
  const elapsedSec = Math.max(0, (Date.now() - data.scenarioStartMs) / 1000.0);
  const stepIdx = Math.min(stepDefs.length, Math.floor(elapsedSec / STEP_SECONDS) + 1);
  const stepTag = stepDefs[stepIdx - 1].tag;
  const evIdx = Math.min(
    data.events.length - 1,
    Math.floor(((stepIdx - 1) * data.events.length) / stepDefs.length)
  );
  const ev = data.events[evIdx];

  const user = userTokens[Math.floor(Math.random() * userTokens.length)];
  const seatId = ev.seatIds[Math.floor(Math.random() * ev.seatIds.length)];
  const idemKey = generateUuidV4();

  const res = postHold(
    BASE_URL,
    user.token,
    ev.eventId,
    seatId,
    idemKey,
    holdExpectedStatuses,
    { step: stepTag }
  );
  const body = safeJson(res);

  holdDuration.add(res.timings.duration, { name: 'hold', step: stepTag });
  checkHoldResponse(res, body);

  const status = res.status;
  const is5xxOrTimeout = status === 0 || status >= 500;
  const isUnknown = isUnknownHoldOutcome(status);
  const isErr = is5xxOrTimeout || (status !== 201 && status !== 409 && status !== 429);

  bpStepReqs.add(1, { step: stepTag });
  bpStepError.add(isErr, { step: stepTag });
  hold5xx.add(is5xxOrTimeout);
  holdRateLimitedRate.add(status === 429);

  if (isUnknown) {
    holdUnknownOutcome.add(1);
    bpStepUnknown.add(1, { step: stepTag });
  }

  if (status === 201) {
    holdCreated.add(1);
  } else if (status === 409) {
    holdConflict.add(1);
  } else if (status === 429) {
    holdRateLimited.add(1);
  } else {
    holdUnexpected.add(1);
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
      eventCount: Array.isArray(compact.setup_data.events) ? compact.setup_data.events.length : 0,
      seatsPerEvent: SEATS_PER_EVENT,
      scenarioStartMs: startMs,
    };
  }

  const enriched = Object.assign({}, compact, {
    _meta: {
      scenario: 'breakpoint-hold',
      run_id: RUN_ID,
      load_scale: LOAD_SCALE,
      user_pool_size: USER_POOL_SIZE,
      seats_per_event: SEATS_PER_EVENT,
      start_rps: START,
      step_rps: STEP,
      max_rps: MAX,
      step_seconds: STEP_SECONDS,
      pre_allocated_vus: PRE_ALLOCATED_VUS,
      max_vus: MAX_VUS,
      steps: stepsWithEpoch,
    },
  });

  const out = {};
  out[SUMMARY_PATH] = JSON.stringify(enriched, null, 2);
  return out;
}
