import http from 'k6/http';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import {
  buildUserTokenPool,
  generateUuidV4,
  assertPerUserRateWithinBucket,
} from './lib/jwt.js';
import { postHold } from './lib/api.js';
import { safeJson, checkHoldResponse } from './lib/checks.js';
import { FLASH_SALE_THRESHOLDS } from './lib/thresholds.js';
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
  plateau1Reqs,
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
const RUN_ID = __ENV.LOADTEST_RUN_ID || `steady-hold-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SEAT_SUPPLY = (__ENV.SEAT_SUPPLY || 'ample').toLowerCase();
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/steady-hold-summary.json';

const RATE = parseInt(__ENV.RATE || '100', 10);
const MINUTES = parseFloat(__ENV.MINUTES || '5');
const RAMP_SEC = parseInt(__ENV.STEADY_RAMP_SEC || '20', 10);
const PLATEAU_SEC = Math.max(30, Math.round(MINUTES * 60));
const RAMPDOWN_SEC = parseInt(__ENV.STEADY_RAMPDOWN_SEC || '10', 10);

const WARMUP_SECONDS = __ENV.WARMUP_SECONDS !== undefined && __ENV.WARMUP_SECONDS !== ''
  ? parseInt(__ENV.WARMUP_SECONDS, 10) : 90;
const WARMUP_RATE = __ENV.WARMUP_RATE !== undefined && __ENV.WARMUP_RATE !== ''
  ? parseInt(__ENV.WARMUP_RATE, 10) : 50;
const WARMUP_GAP_SEC = WARMUP_SECONDS > 0 ? 5 : 0;
const MEASURED_START_OFFSET_SEC = WARMUP_SECONDS + WARMUP_GAP_SEC;

const manifest = loadSeatManifest(__ENV.SEATS_MANIFEST || '/results/seats.json');
const PRE_ALLOCATED_VUS = Math.max(30, Math.round(RATE * 0.5));
const MAX_VUS = Math.max(100, Math.round(RATE * 2.0));

const WARMUP_USER_COUNT = Math.min(1000, Math.floor(USER_POOL_SIZE * 0.2));
const userTokens = new SharedArray('steady_hold_user_tokens', function () {
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
scenarios.steady_hold = {
  executor: 'ramping-arrival-rate',
  exec: 'measuredFn',
  startTime: `${MEASURED_START_OFFSET_SEC}s`,
  startRate: 1,
  timeUnit: '1s',
  preAllocatedVUs: PRE_ALLOCATED_VUS,
  maxVUs: MAX_VUS,
  gracefulStop: '15s',
  stages: [
    { duration: `${RAMP_SEC}s`, target: RATE },
    { duration: `${PLATEAU_SEC}s`, target: RATE },
    { duration: `${RAMPDOWN_SEC}s`, target: 0 },
  ],
};

// Same thresholds as sustained-hold.js (imported from shared thresholds.js) + non-aborting export for outcome=created & warmup
const combinedThresholds = Object.assign({}, FLASH_SALE_THRESHOLDS, {
  'hold_duration{outcome:created}': ['max>=0'],
  'http_req_duration{step:warmup,name:warmup_hold}': ['max>=0'],
});

export const options = {
  setupTimeout: '60s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: scenarios,
  thresholds: combinedThresholds,
};

export function setup() {
  initFlashSaleCounters();
  initWarmupCounters();
  assertPerUserRateWithinBucket(RATE, USER_POOL_SIZE - WARMUP_USER_COUNT, 'steady-hold', 0);
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
  const target = seatAt(manifest, iter < wSeats ? iter : (iter % wSeats));
  const user = userTokens[iter % WARMUP_USER_COUNT];
  const res = postHold(
    BASE_URL, user.token, target.eventId, target.seatId, generateUuidV4(), holdExpectedStatuses,
    { name: 'warmup_hold', step: 'warmup' }
  );
  const status = res.status;
  warmupReqs.add(1, { step: 'warmup' });
  if (status === 201) warmupHoldCreated.add(1, { step: 'warmup' });
  else if (status !== 409 && status !== 429) warmupErrors.add(1, { step: 'warmup' });
  if (isUnknownHoldOutcome(status)) warmupHoldUnknownOutcome.add(1, { step: 'warmup' });
}

export function measuredFn(data) {
  const iter = exec.scenario.iterationInTest;
  const offset = manifest.meta.warmupSeats;
  const measuredCount = manifest.meta.totalSeats - offset;
  const seatIdx = SEAT_SUPPLY === 'ample'
    ? offset + (iter % measuredCount)
    : offset + Math.floor(Math.random() * measuredCount);
  const target = seatAt(manifest, seatIdx);
  const user = userTokens[WARMUP_USER_COUNT + (iter % (USER_POOL_SIZE - WARMUP_USER_COUNT))];

  const elapsedSec = (Date.now() - data.measuredStartMs) / 1000.0;
  if (elapsedSec >= RAMP_SEC && elapsedSec < RAMP_SEC + PLATEAU_SEC) {
    plateau1Reqs.add(1);
  }

  const res = postHold(BASE_URL, user.token, target.eventId, target.seatId, generateUuidV4(), holdExpectedStatuses);
  const body = safeJson(res);
  const status = res.status;
  const outcome = classifyHoldOutcome(status);

  holdDuration.add(res.timings.duration, { name: 'hold', outcome: outcome });
  checkHoldResponse(res, body);

  const is5xxOrTimeout = status === 0 || status >= 500;
  hold5xx.add(is5xxOrTimeout);
  holdRateLimitedRate.add(status === 429);
  if (isUnknownHoldOutcome(status)) holdUnknownOutcome.add(1);

  if (status === 201) holdCreated.add(1);
  else if (status === 409) holdConflict.add(1);
  else if (status === 429) holdRateLimited.add(1);
  else holdUnexpected.add(1);
}

export function handleSummary(data) {
  const measuredStartMs = (data.setup_data && data.setup_data.measuredStartMs) || Date.now();
  const measuredStartSec = Math.floor(measuredStartMs / 1000);
  const measuredEndSec = measuredStartSec + RAMP_SEC + PLATEAU_SEC + RAMPDOWN_SEC;

  const enriched = Object.assign({}, data, {
    _meta: {
      scenario: 'steady-hold',
      run_id: RUN_ID,
      seat_supply: SEAT_SUPPLY,
      user_pool_size: USER_POOL_SIZE,
      total_seeded_seats: manifest.meta.totalSeats,
      event_count: manifest.meta.eventCount,
      seed_plan: manifest.meta.plan || {},
      warmup_seconds: WARMUP_SECONDS,
      warmup_rate: WARMUP_RATE,
      measured_start_epoch: measuredStartSec,
      measured_end_epoch: measuredEndSec,
      plateau1_target_rps: RATE,
      plateau1_duration_s: PLATEAU_SEC,
      ramp_s: RAMP_SEC,
      rampdown_s: RAMPDOWN_SEC,
      pre_allocated_vus: PRE_ALLOCATED_VUS,
      max_vus: MAX_VUS,
      stages: [
        { name: 'ramp', duration_s: RAMP_SEC, target_rps: RATE },
        { name: 'plateau', duration_s: PLATEAU_SEC, target_rps: RATE },
        { name: 'rampdown', duration_s: RAMPDOWN_SEC, target_rps: 0 },
      ],
    },
  });

  const out = {};
  out[SUMMARY_PATH] = JSON.stringify(enriched, null, 2);
  return out;
}
