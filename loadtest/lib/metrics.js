import { Counter, Rate, Trend } from 'k6/metrics';

// Shared & Scenario A (Flash Sale) metrics
export const holdCreated = new Counter('hold_created');
export const holdConflict = new Counter('hold_conflict');
export const holdRateLimited = new Counter('hold_rate_limited');
export const holdRateLimitedRate = new Rate('hold_rate_limited_rate');
export const holdUnexpected = new Counter('hold_unexpected');
export const holdUnknownOutcome = new Counter('hold_unknown_outcome');
export const hold5xx = new Rate('hold_5xx');
export const holdDuration = new Trend('hold_duration', true);

// Dedicated warm-up counters (excluded from measured thresholds & report tables;
// reconciled in A8 since warm-up creates real reservations in inventory_db)
export const warmupReqs = new Counter('warmup_reqs');
export const warmupHoldCreated = new Counter('warmup_hold_created');
export const warmupHoldUnknownOutcome = new Counter('warmup_hold_unknown_outcome');
export const warmupErrors = new Counter('warmup_errors');

// Plateau throughput accounting counters for Scenario A (exact achieved RPS during plateaus)
export const plateau1Reqs = new Counter('plateau1_reqs');
export const plateau2Reqs = new Counter('plateau2_reqs');

// Scenario B (Checkout Journey) metrics
export const orderMaterializationLatency = new Trend('order_materialization_latency', true);
export const checkoutDuration = new Trend('checkout_duration', true);
export const sagaLatency = new Trend('saga_latency', true);
export const orderNotReadyRetries = new Counter('order_not_ready_retries');

export const journeyConfirmed = new Counter('journey_confirmed');
export const journeyPaymentFailed = new Counter('journey_payment_failed');
export const journeyAbandoned = new Counter('journey_abandoned');
export const journeysTimedOut = new Counter('journeys_timed_out');
export const unexpectedStatuses = new Counter('unexpected_statuses');

/**
 * Zero-initializes counters used in equality thresholds (e.g. count==0) so k6 always
 * emits the metric in summary JSON even when zero events occur.
 */
export function initFlashSaleCounters() {
  holdCreated.add(0);
  holdConflict.add(0);
  holdRateLimited.add(0);
  holdUnexpected.add(0);
  holdUnknownOutcome.add(0);
  plateau1Reqs.add(0);
  plateau2Reqs.add(0);
}

export function initJourneyCounters() {
  holdCreated.add(0);
  holdUnknownOutcome.add(0);
  orderNotReadyRetries.add(0);
  journeyConfirmed.add(0);
  journeyPaymentFailed.add(0);
  journeyAbandoned.add(0);
  journeysTimedOut.add(0);
  unexpectedStatuses.add(0);
}

export function initWarmupCounters() {
  warmupReqs.add(0);
  warmupHoldCreated.add(0);
  warmupHoldUnknownOutcome.add(0);
  warmupErrors.add(0);
}

/**
 * True when a POST /api/v1/reservations response leaves the client unable to know
 * whether inventory_db committed the hold (transport error/timeout status 0, or 500/502/504).
 */
export function isUnknownHoldOutcome(status) {
  return status === 0 || status === 500 || status === 502 || status === 504;
}

/**
 * Maps an HTTP hold status code to the bounded outcome tag value.
 */
export function classifyHoldOutcome(status) {
  if (status === 201) return 'created';
  if (status === 409) return 'conflict';
  if (status === 429) return 'rate_limited';
  return 'error';
}
