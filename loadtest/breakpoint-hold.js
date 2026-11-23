import http from 'k6/http';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';
import { SharedArray } from 'k6/data';
import {
  buildUserTokenPool,
  generateUuidV4,
  assertPerUserRateWithinBucket,
} from './lib/jwt.js';
import { postHold } from './lib/api.js';
import { safeJson, checkHoldResponse } from './lib/checks.js';
import { loadSeatManifest, seatAt } from './lib/seats.js';
import {
  holdCreated,
  holdConflict,
  holdRateLimited,
  holdRateLimitedRate,
  holdUnexpected,
  holdUnknownOutcome,
  hold5xx,
  holdDuration,
  warmupReqs,
  warmupHoldCreated,
  warmupHoldUnknownOutcome,
  warmupErrors,
  initFlashSaleCounters,
  initWarmupCounters,
  isUnknownHoldOutcome,
  classifyHoldOutcome,
} from './lib/metrics.js';

const BASE_URL = __ENV.BASE_URL || 'http://gateway-service:8080';
const JWT_SECRET = __ENV.JWT_SECRET || '';
const LOAD_SCALE = parseFloat(__ENV.LOAD_SCALE || '1.0');
const RUN_ID = __ENV.LOADTEST_RUN_ID || `bp-hold-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SEAT_SUPPLY = (__ENV.SEAT_SUPPLY || 'ample').toLowerCase();
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/breakpoint-hold-summary.json';

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
  ? parseInt(__ENV.WARMUP_RATE, 10) : 50;
const WARMUP_GAP_SEC = WARMUP_SECONDS > 0 ? 5 : 0;
const MEASURED_START_OFFSET_SEC = WARMUP_SECONDS + WARMUP_GAP_SEC;

const START = envOrScaled('START', 25, 5);
const STEP = envOrScaled('STEP', 25, 5);
const REQUESTED_MAX = Math.max(START, envOrScaled('MAX', 600, START));
const MAX = plan.effective_max ? Math.min(REQUESTED_MAX, parseInt(plan.effective_max, 10)) : REQUESTED_MAX;
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
  : Math.min(400, Math.max(100, Math.round(PEAK_RATE * 0.75)));

const stages = [];
for (let i = 0; i < stepDefs.length; i++) {
  const s = stepDefs[i];
  if (i > 0) {
    stages.push({ duration: '0s', target: s.target_rps });
  }
  stages.push({ duration: `${STEP_SECONDS}s`, target: s.target_rps });
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
  exportThresholds[`bp_step_reqs{step:${s.tag}}`] = ['count>=0'];
  exportThresholds[`bp_step_error{step:${s.tag}}`] = ['rate>=0'];
  exportThresholds[`bp_step_unknown{step:${s.tag}}`] = ['count>=0'];
}

// Disjoint user pools: first 1000 users exclusively for warm-up, remaining 4000 for measured
const WARMUP_USER_COUNT = Math.min(1000, Math.floor(USER_POOL_SIZE * 0.2));
const userTokens = new SharedArray('bp_hold_user_tokens', function () {
  return buildUserTokenPool(JWT_SECRET, USER_POOL_SIZE, 7200);
});

const holdExpectedStatuses = http.expectedStatuses(201, 409, 429);

const scenarios = {};
if (WARMUP_SECONDS > 0 && WARMUP_RATE > 0) {
  scenarios.warmup = {
    executor: 'constant-arrival-rate',
    exec: 'warmupFn',
    startTime: '0s',
    rate: WARMUP_RATE,
    timeUnit: '1s',
    duration: `${WARMUP_SECONDS}s`,
    preAllocatedVUs: Math.min(80, Math.max(15, Math.round(WARMUP_RATE * 0.6))),
    maxVUs: Math.min(150, Math.max(40, WARMUP_RATE * 2)),
    gracefulStop: `${WARMUP_GAP_SEC}s`,
  };
}
scenarios.breakpoint_hold = {
  executor: 'ramping-arrival-rate',
  exec: 'measuredFn',
  startTime: `${MEASURED_START_OFFSET_SEC}s`,
  startRate: stepDefs[0].target_rps,
  timeUnit: '1s',
  preAllocatedVUs: PRE_ALLOCATED_VUS,
  maxVUs: MAX_VUS,
  gracefulStop: '15s',
  stages: stages,
};

export const options = {
  setupTimeout: '60s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: scenarios,
  thresholds: exportThresholds,
};

export function setup() {
  initFlashSaleCounters();
  initWarmupCounters();
  for (const s of stepDefs) {
    bpStepReqs.add(0, { step: s.tag });
    bpStepUnknown.add(0, { step: s.tag });
  }
  assertPerUserRateWithinBucket(PEAK_RATE, USER_POOL_SIZE - WARMUP_USER_COUNT, 'breakpoint-hold', 0);

  const nowMs = Date.now();
  return {
    runStartMs: nowMs,
    measuredStartMs: nowMs + MEASURED_START_OFFSET_SEC * 1000,
  };
}

export function warmupFn() {
  const iter = exec.scenario.iterationInTest;
  const wSeats = manifest.meta.warmupSeats;
  if (wSeats <= 0) return;
  const seatIdx = iter < wSeats ? iter : (iter % wSeats);
  const target = seatAt(manifest, seatIdx);
  const user = userTokens[iter % WARMUP_USER_COUNT];

  const res = postHold(
    BASE_URL,
    user.token,
    target.eventId,
    target.seatId,
    generateUuidV4(),
    holdExpectedStatuses,
    { name: 'warmup_hold', step: 'warmup' }
  );
  const status = res.status;
  warmupReqs.add(1, { step: 'warmup' });
  if (status === 201) {
    warmupHoldCreated.add(1, { step: 'warmup' });
  } else if (status !== 409 && status !== 429) {
    warmupErrors.add(1, { step: 'warmup' });
  }
  if (isUnknownHoldOutcome(status)) {
    warmupHoldUnknownOutcome.add(1, { step: 'warmup' });
  }
}

export function measuredFn(data) {
  const elapsedSec = Math.max(0, (Date.now() - data.measuredStartMs) / 1000.0);
  const stepIdx = Math.min(stepDefs.length, Math.floor(elapsedSec / STEP_SECONDS) + 1);
  const stepTag = stepDefs[stepIdx - 1].tag;

  const iter = exec.scenario.iterationInTest;
  const offset = manifest.meta.warmupSeats;
  const measuredCount = manifest.meta.totalSeats - offset;
  if (measuredCount <= 0) {
    holdUnexpected.add(1);
    bpStepError.add(true, { step: stepTag });
    return;
  }

  let seatIdx;
  if (SEAT_SUPPLY === 'ample') {
    seatIdx = offset + (iter % measuredCount);
  } else {
    seatIdx = offset + Math.floor(Math.random() * measuredCount);
  }

  const target = seatAt(manifest, seatIdx);
  const user = userTokens[WARMUP_USER_COUNT + (iter % (USER_POOL_SIZE - WARMUP_USER_COUNT))];

  const res = postHold(
    BASE_URL,
    user.token,
    target.eventId,
    target.seatId,
    generateUuidV4(),
    holdExpectedStatuses,
    { step: stepTag }
  );
  const body = safeJson(res);
  const status = res.status;
  const outcome = classifyHoldOutcome(status);

  holdDuration.add(res.timings.duration, { name: 'hold', step: stepTag, outcome: outcome });
  checkHoldResponse(res, body);

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
      scenario: 'breakpoint-hold',
      run_id: RUN_ID,
      load_scale: LOAD_SCALE,
      seat_supply: SEAT_SUPPLY,
      user_pool_size: USER_POOL_SIZE,
      total_seeded_seats: manifest.meta.totalSeats,
      event_count: manifest.meta.eventCount,
      seed_plan: plan,
      warmup_seconds: WARMUP_SECONDS,
      warmup_rate: WARMUP_RATE,
      measured_start_epoch: measuredStartSec,
      measured_end_epoch: measuredEndSec,
      start_rps: START,
      step_rps: STEP,
      requested_max_rps: REQUESTED_MAX,
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
