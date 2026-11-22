import http from 'k6/http';
import { SharedArray } from 'k6/data';
import {
  buildUserTokenPool,
  mintAdminToken,
  generateUuidV4,
  assertPerUserRateWithinBucket,
} from './lib/jwt.js';
import { setupMultipleEvents, postHold } from './lib/api.js';
import { safeJson, checkHoldResponse } from './lib/checks.js';
import { FLASH_SALE_THRESHOLDS } from './lib/thresholds.js';
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
  initFlashSaleCounters,
  isUnknownHoldOutcome,
} from './lib/metrics.js';

const BASE_URL = __ENV.BASE_URL || 'http://gateway-service:8080';
const JWT_SECRET = __ENV.JWT_SECRET || '';
const RUN_ID = __ENV.LOADTEST_RUN_ID || `sustained-hold-${Date.now()}`;
const USER_POOL_SIZE = parseInt(__ENV.USER_POOL_SIZE || '5000', 10);
const SEATS_PER_EVENT = parseInt(__ENV.SEATS_PER_EVENT || '5000', 10);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/results/sustained-hold-summary.json';
const PROFILE_PATH = __ENV.SUSTAINABLE_PROFILE_PATH || './sustainable-profile.json';

const profile = JSON.parse(open(PROFILE_PATH));
if (!profile || !profile.hold || !profile.hold.sustainable_rps || profile.hold.sustainable_rps <= 0) {
  throw new Error(`Invalid or missing hold.sustainable_rps in ${PROFILE_PATH}`);
}

const SUSTAINED_RPS = parseInt(profile.hold.sustainable_rps, 10);
const RAMP_SEC = parseInt(__ENV.SUSTAINED_RAMP_SEC || '20', 10);
const PLATEAU_SEC = parseInt(__ENV.SUSTAINED_PLATEAU_SEC || '300', 10);
const RAMPDOWN_SEC = parseInt(__ENV.SUSTAINED_RAMPDOWN_SEC || '10', 10);

const PRE_ALLOCATED_VUS = Math.max(30, Math.round(SUSTAINED_RPS * 0.5));
const MAX_VUS = Math.max(100, Math.round(SUSTAINED_RPS * 2.0));

const userTokens = new SharedArray('sustained_hold_user_tokens', function () {
  return buildUserTokenPool(JWT_SECRET, USER_POOL_SIZE, 7200);
});

const holdExpectedStatuses = http.expectedStatuses(201, 409, 429);

export const options = {
  setupTimeout: '240s',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    sustained_hold: {
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '15s',
      stages: [
        { duration: `${RAMP_SEC}s`, target: SUSTAINED_RPS },
        { duration: `${PLATEAU_SEC}s`, target: SUSTAINED_RPS },
        { duration: `${RAMPDOWN_SEC}s`, target: 0 },
      ],
    },
  },
  thresholds: FLASH_SALE_THRESHOLDS,
};

export function setup() {
  initFlashSaleCounters();
  assertPerUserRateWithinBucket(SUSTAINED_RPS, userTokens.length, 'sustained-hold', 0);

  // Create enough 5000-seat events to sustain real 201 writes + contention across the 5-minute plateau
  const totalExpectedReqs = Math.ceil(SUSTAINED_RPS * (PLATEAU_SEC + (RAMP_SEC + RAMPDOWN_SEC) * 0.5));
  const eventCount = Math.max(1, Math.min(6, Math.ceil(totalExpectedReqs / SEATS_PER_EVENT)));
  const admin = mintAdminToken(JWT_SECRET, 7200);
  const events = setupMultipleEvents(BASE_URL, admin.token, RUN_ID, 'susthold', eventCount, SEATS_PER_EVENT);

  return {
    events: events.map((ev) => ({ eventId: ev.eventId, seatIds: ev.seatIds })),
    totalSeats: events.reduce((acc, ev) => acc + ev.seatIds.length, 0),
    scenarioStartMs: Date.now(),
  };
}

export default function (data) {
  const user = userTokens[Math.floor(Math.random() * userTokens.length)];
  const ev = data.events[Math.floor(Math.random() * data.events.length)];
  const target = { eventId: ev.eventId, seatId: ev.seatIds[Math.floor(Math.random() * ev.seatIds.length)] };
  const idemKey = generateUuidV4();

  const elapsedSec = (Date.now() - data.scenarioStartMs) / 1000.0;
  if (elapsedSec >= RAMP_SEC && elapsedSec < RAMP_SEC + PLATEAU_SEC) {
    plateau1Reqs.add(1);
  }

  const res = postHold(BASE_URL, user.token, target.eventId, target.seatId, idemKey, holdExpectedStatuses);
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
      console.error(`[sustained-hold] Unexpected hold status=${status} body=${res.body}`);
    }
  }
}

export function handleSummary(data) {
  const compact = Object.assign({}, data);
  if (compact.setup_data) {
    compact.setup_data = {
      seatCount: compact.setup_data.totalSeats || 0,
      eventCount: Array.isArray(compact.setup_data.events) ? compact.setup_data.events.length : 0,
    };
  }
  const enriched = Object.assign({}, compact, {
    _meta: {
      scenario: 'sustained-hold',
      run_id: RUN_ID,
      user_pool_size: USER_POOL_SIZE,
      seats_per_event: SEATS_PER_EVENT,
      sustainable_rps_target: SUSTAINED_RPS,
      plateau1_target_rps: SUSTAINED_RPS,
      plateau1_duration_s: PLATEAU_SEC,
      ramp_s: RAMP_SEC,
      rampdown_s: RAMPDOWN_SEC,
      pre_allocated_vus: PRE_ALLOCATED_VUS,
      max_vus: MAX_VUS,
      profile_source: profile.hold,
      stages: [
        { name: 'ramp', duration_s: RAMP_SEC, target_rps: SUSTAINED_RPS },
        { name: 'plateau', duration_s: PLATEAU_SEC, target_rps: SUSTAINED_RPS },
        { name: 'rampdown', duration_s: RAMPDOWN_SEC, target_rps: 0 },
      ],
    },
  });

  const out = {};
  out[SUMMARY_PATH] = JSON.stringify(enriched, null, 2);
  return out;
}
