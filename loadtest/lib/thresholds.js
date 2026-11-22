/**
 * Shared threshold definitions for hold-only and checkout-journey scenarios.
 * Verified by scripts/test_loadtest_thresholds.py to match loadtest/flash-sale.js
 * and loadtest/checkout-journey.js byte-for-byte.
 */

export const FLASH_SALE_THRESHOLDS = {
  // Documented SLO (docs/arch/09-conventions.md & user specification): hold p99 < 300ms
  'http_req_duration{name:hold}': ['p(99)<300'],
  'hold_unexpected': ['count==0'],
  'hold_5xx': ['rate<0.001'],
  'hold_rate_limited_rate': ['rate<0.01'],
};

export const CHECKOUT_JOURNEY_THRESHOLDS = {
  // Documented SLOs (docs/arch/09-conventions.md & user specification)
  'http_req_duration{name:checkout}': ['p(99)<500'],
  'http_req_duration{name:hold}': ['p(99)<300'],
  // Chosen target (documented as chosen, not a formal SLO, in loadtest/README.md)
  'saga_latency': ['p(95)<5000'],
  'journeys_timed_out': ['count==0'],
  'unexpected_statuses': ['count==0'],
};
