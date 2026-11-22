import http from 'k6/http';
import { SharedArray } from 'k6/data';
import {
  buildUserTokenPool,
  mintAdminToken,
  generateUuidV4,
  assertPerUserRateWithinBucket,
} from './lib/jwt.js';
import { setupTwoEvents, postHold } from './lib/api.js';
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
  plateau1Reqs,
  plateau2Reqs,
  initFlashSaleCounters,
  isUnknownHoldOutcome,
} from './lib/metrics.js';

const BASE_URL = __ENV.BASE_URL || 'http://gateway-service:8080';
const JWT_SECRET = __ENV.JWT_SECRET || '';
const LOAD_SCALE = parseFloat(__ENV.LOAD_SCALE || '1.0');
const RUN_ID = __ENV.LOADTEST_RUN_ID || `flash-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SEATS_PER_EVENT = parseInt(__ENV.SEATS_PER_EVENT || '5000', 10);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/flash-sale-summary.json';

function scaledInt(envName, baseVal, minVal = 1) {
  if (__ENV[envName] !== undefined && __ENV[envName] !== '') {
    return parseInt(__ENV[envName], 10);
  }
  return Math.max(minVal, Math.round(baseVal * LOAD_SCALE));
}

const RAMP1_SEC = scaledInt('FLASH_RAMP1_SEC', 10, 2);
const STAGE1_RATE = scaledInt('FLASH_STAGE1_RATE', 150, 5);
const STAGE1_SEC = scaledInt('FLASH_STAGE1_SEC', 60, 10);
const RAMP2_SEC = scaledInt('FLASH_RAMP2_SEC', 15, 2);
const STAGE2_RATE = scaledInt('FLASH_STAGE2_RATE', 400, 10);
const STAGE2_SEC = scaledInt('FLASH_STAGE2_SEC', 60, 10);
const RAMPDOWN_SEC = scaledInt('FLASH_RAMPDOWN_SEC', 10, 2);

const PRE_ALLOCATED_VUS = scaledInt('FLASH_PRE_ALLOCATED_VUS', 150, 20);
const MAX_VUS = scaledInt('FLASH_MAX_VUS', 500, 60);

const PEAK_RATE = Math.max(STAGE1_RATE, STAGE2_RATE);

// Pre-generate 5000 distinct user tokens once in the init stage (shared read-only across VUs)
const userTokens = new SharedArray('flash_user_tokens', function () {
  return buildUserTokenPool(JWT_SECRET, USER_POOL_SIZE, 7200);
});

const holdExpectedStatuses = http.expectedStatuses(201, 409, 429);

export const options = {
  setupTimeout: '180s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    flash_sale: {
      executor: 'ramping-arrival-rate',
      startRate: Math.max(1, Math.round(10 * LOAD_SCALE)),
      timeUnit: '1s',
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '15s',
      stages: [
        { duration: `${RAMP1_SEC}s`, target: STAGE1_RATE },
        { duration: `${STAGE1_SEC}s`, target: STAGE1_RATE },
        { duration: `${RAMP2_SEC}s`, target: STAGE2_RATE },
        { duration: `${STAGE2_SEC}s`, target: STAGE2_RATE },
        { duration: `${RAMPDOWN_SEC}s`, target: 0 },
      ],
    },
  },
  thresholds: {
    // Documented SLO (docs/arch/09-conventions.md & user specification): hold p99 < 300ms
    'http_req_duration{name:hold}': ['p(99)<300'],
    'hold_unexpected': ['count==0'],
    'hold_5xx': ['rate<0.001'],
    'hold_rate_limited_rate': ['rate<0.01'],
  },
};

export function setup() {
  initFlashSaleCounters();
  assertPerUserRateWithinBucket(PEAK_RATE, userTokens.length, 'flash-sale', 0);

  const admin = mintAdminToken(JWT_SECRET, 7200);
  const events = setupTwoEvents(BASE_URL, admin.token, RUN_ID, SEATS_PER_EVENT);

  console.log(`LOADTEST_FLASH_EVENT_ID=${events.flashEvent.eventId}`);
  console.log(`LOADTEST_JOURNEY_EVENT_ID=${events.journeyEvent.eventId}`);

  return {
    eventId: events.flashEvent.eventId,
    seatIds: events.flashEvent.seatIds,
    journeyEventId: events.journeyEvent.eventId,
    scenarioStartMs: Date.now(),
  };
}

export default function (data) {
  const user = userTokens[Math.floor(Math.random() * userTokens.length)];
  const seatId = data.seatIds[Math.floor(Math.random() * data.seatIds.length)];
  const idemKey = generateUuidV4();

  const elapsedSec = (Date.now() - data.scenarioStartMs) / 1000.0;
  if (elapsedSec >= RAMP1_SEC && elapsedSec < RAMP1_SEC + STAGE1_SEC) {
    plateau1Reqs.add(1);
  } else if (
    elapsedSec >= RAMP1_SEC + STAGE1_SEC + RAMP2_SEC &&
    elapsedSec < RAMP1_SEC + STAGE1_SEC + RAMP2_SEC + STAGE2_SEC
  ) {
    plateau2Reqs.add(1);
  }

  const res = postHold(BASE_URL, user.token, data.eventId, seatId, idemKey, holdExpectedStatuses);
  const body = safeJson(res);

  holdDuration.add(res.timings.duration, { name: 'hold' });
  checkHoldResponse(res, body);

  const status = res.status;
  const is5xxOrTimeout = status === 0 || status >= 500;
  hold5xx.add(is5xxOrTimeout);
  holdRateLimitedRate.add(status === 429);
  if (isUnknownHoldOutcome(status)) {
    holdUnknownOutcome.add(1);
  }

  if (status === 201) {
    holdCreated.add(1);
  } else if (status === 409) {
    holdConflict.add(1);
  } else if (status === 429) {
    holdRateLimited.add(1);
  } else {
    holdUnexpected.add(1);
    if (__ITER < 10) {
      console.error(`[flash-sale] Unexpected hold status=${status} body=${res.body}`);
    }
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
      scenario: 'flash-sale',
      run_id: RUN_ID,
      load_scale: LOAD_SCALE,
      user_pool_size: USER_POOL_SIZE,
      seats_per_event: SEATS_PER_EVENT,
      pre_allocated_vus: PRE_ALLOCATED_VUS,
      max_vus: MAX_VUS,
      stages: [
        { name: 'ramp1', duration_s: RAMP1_SEC, target_rps: STAGE1_RATE },
        { name: 'plateau1', duration_s: STAGE1_SEC, target_rps: STAGE1_RATE },
        { name: 'ramp2', duration_s: RAMP2_SEC, target_rps: STAGE2_RATE },
        { name: 'plateau2', duration_s: STAGE2_SEC, target_rps: STAGE2_RATE },
        { name: 'rampdown', duration_s: RAMPDOWN_SEC, target_rps: 0 },
      ],
      plateau1_duration_s: STAGE1_SEC,
      plateau2_duration_s: STAGE2_SEC,
      plateau1_target_rps: STAGE1_RATE,
      plateau2_target_rps: STAGE2_RATE,
    },
  });

  const out = {};
  out[SUMMARY_PATH] = JSON.stringify(enriched, null, 2);
  return out;
}
