# End-to-End Verification Findings (`e2e-tests`)

This document records genuine product defects discovered by the cross-service end-to-end verification suite (`e2e-tests`) against the containerized platform. Per project rules, production service code is not modified by the E2E suite, test assertions are never weakened, and genuine product findings are documented here with reproduction steps and suspected causes.

---

## `INV-001` — `inventory-service` returns HTTP 500 instead of retryable 503 during Redis concurrency gate unavailability under high contention

- **Affected Scenario**: `S5` (`TicketingPlatformE2ETest#s5_noOversellUnderHighContentionLoad`)
- **Severity**: High / Error Mapping & Availability
- **Status**: Open (Test remains enabled and asserting no 5xx per project rules)

### 1. Reproduction Steps
1. Start the stack or ensure it is running:
   ```bash
   make stack-reset
   ```
2. Run the fast end-to-end verification suite:
   ```bash
   make e2e-fast
   ```
   (Alternatively: `mvn -pl e2e-tests -Pe2e -Dtest=TicketingPlatformE2ETest#s5_noOversellUnderHighContentionLoad test`)

3. Under 64 concurrent threads contending for 20 seats with 200 users, Redis command timeouts or connection pool pressure causes `hold_inventory.lua` execution to throw `RedisUnavailableException` in `RedisSeatGate`.
4. `inventory-service` returns an HTTP 500 response instead of a retryable HTTP 503 Service Unavailable.

### 2. Observed Response Body
```json
{
  "type": "about:blank",
  "title": "Internal Server Error",
  "status": 500,
  "detail": "Inventory concurrency gate is temporarily unavailable",
  "instance": "/api/v1/reservations"
}
```

### 3. Expected vs. Observed Behavior
- **Expected**: A temporary dependency failure in the Redis concurrency gate must fail closed with HTTP 503 `SERVICE_UNAVAILABLE`, `errorCode: "SERVICE_UNAVAILABLE"`, a `Retry-After: 1` header, and no 500 Internal Server Error returned to API callers.
- **Observed**: HTTP status `500` with detail `"Inventory concurrency gate is temporarily unavailable"`, violating the assertion that all hold responses must be in `{201, 409, 429}` and never 5xx.

### 4. Suspected Cause
In `inventory-service`:
`RedisSeatGate` throws `RedisUnavailableException` when Redis is unreachable or times out during `hold_inventory.lua`. However, the exception mapping in `InventoryExceptionHandler` (or Spring default exception handling if unmapped) maps this failure to HTTP 500 `INTERNAL_SERVER_ERROR` instead of HTTP 503 `SERVICE_UNAVAILABLE` with a `Retry-After` header.

### 5. Affected Files
- `inventory-service/src/main/java/com/ticketing/inventory/service/RedisSeatGate.java`
- `inventory-service/src/main/java/com/ticketing/inventory/web/InventoryExceptionHandler.java`

---

## `PAYMENT-001` — `payment-service` refund decline handling (`tok_refund_decline`)

- **Affected Scenario**: `S11` (`TicketingPlatformE2ETest#s11_refundDeclinedRoutesToDeadLetterTopic`)
- **Severity**: Medium / Compensation Error Handling
- **Status**: Resolved in `payment-service` source code; `@Disabled` removed from `S11`

### 1. Description
When attempting a compensating refund for `order.refund.requested` after a late payment on an expired hold, if the external payment provider declines the refund (using `tok_refund_decline`), the system must:
1. Keep `payments.status` as `SUCCEEDED`.
2. Keep `orders.status` as `REFUND_PENDING` (awaiting manual operator intervention).
3. Emit `0` `payment.refunded` outbox events.
4. Route the non-retryable failed refund to `order.events-payment-service-order-cg-dlt` (1 dead letter).

### 2. Resolution Status
The fix was implemented in `payment-service`:
- `MockPaymentProvider` handles `MAGIC_TOKEN_REFUND_DECLINE = "tok_refund_decline"` and returns `PaymentResult.declined(...)` on refund.
- `PaymentService.handleOrderRefundRequested` inspects `result.outcome() == PaymentResult.Outcome.DECLINED`, logs the error, records metrics, and throws `NonRetryableEventException` to route the message to the DLT without marking the payment refunded or emitting `payment.refunded`.
- Scenario `S11` in `TicketingPlatformE2ETest` has `@Disabled` removed and passes against the updated `payment-service` container.
- If running against an unpatched container image where `@Disabled` is active, the scenario can be executed using:
  ```bash
  mvn -pl e2e-tests -Pe2e -Djunit.jupiter.conditions.deactivate=org.junit.*DisabledCondition -Dtest=TicketingPlatformE2ETest#s11_refundDeclinedRoutesToDeadLetterTopic test
  ```
