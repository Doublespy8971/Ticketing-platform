import { check } from 'k6';

/**
 * Safely parses JSON body from a k6 HTTP response.
 */
export function safeJson(res) {
  if (!res || !res.body) {
    return null;
  }
  try {
    return res.json();
  } catch (_) {
    return null;
  }
}

/**
 * Checks a flash-sale hold response: expected status in {201, 409, 429},
 * and if 201, response body must contain reservationId.
 */
export function checkHoldResponse(res, body) {
  const checks = {
    'hold status is 201, 409, or 429': (r) => r.status === 201 || r.status === 409 || r.status === 429,
  };
  if (res.status === 201) {
    checks['201 hold response contains reservationId'] = () => Boolean(body && body.reservationId);
  }
  return check(res, checks);
}

/**
 * Checks a journey hold response (must be 201 with reservationId since each journey has a unique seat).
 */
export function checkJourneyHold(res, body) {
  return check(res, {
    'journey hold status is 201': (r) => r.status === 201,
    'journey hold response contains reservationId': () => Boolean(body && body.reservationId),
  });
}

/**
 * Checks a journey checkout response (202 Accepted).
 */
export function checkJourneyCheckout(res, body) {
  return check(res, {
    'journey checkout status is 202': (r) => r.status === 202,
    'journey checkout status is PAYMENT_PENDING': () => Boolean(body && body.status === 'PAYMENT_PENDING'),
  });
}

/**
 * Checks that a completed journey reached the expected terminal order status.
 */
export function checkJourneyFinalStatus(finalStatus, expectedStatus) {
  return check(finalStatus, {
    'journey reached expected terminal status': (s) => s === expectedStatus,
  });
}

/**
 * Extracts Retry-After header (case-insensitive) in seconds, clamped safely to [0.1, 5.0].
 */
export function parseRetryAfterSeconds(res, defaultSeconds = 1.0) {
  if (!res || !res.headers) {
    return defaultSeconds;
  }
  for (const key of Object.keys(res.headers)) {
    if (key.toLowerCase() === 'retry-after') {
      const parsed = parseFloat(res.headers[key]);
      if (!Number.isNaN(parsed) && parsed > 0) {
        return Math.min(Math.max(parsed, 0.1), 5.0);
      }
    }
  }
  return defaultSeconds;
}
