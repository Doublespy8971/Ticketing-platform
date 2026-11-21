import { Counter, Rate, Trend } from 'k6/metrics';

// Shared & Scenario A (Flash Sale) metrics
export const holdCreated = new Counter('hold_created');
export const holdConflict = new Counter('hold_conflict');
export const holdRateLimited = new Counter('hold_rate_limited');
export const holdRateLimitedRate = new Rate('hold_rate_limited_rate');
export const holdUnexpected = new Counter('hold_unexpected');
export const hold5xx = new Rate('hold_5xx');
export const holdDuration = new Trend('hold_duration', true);

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
  plateau1Reqs.add(0);
  plateau2Reqs.add(0);
}

export function initJourneyCounters() {
  holdCreated.add(0);
  orderNotReadyRetries.add(0);
  journeyConfirmed.add(0);
  journeyPaymentFailed.add(0);
  journeyAbandoned.add(0);
  journeysTimedOut.add(0);
  unexpectedStatuses.add(0);
}
