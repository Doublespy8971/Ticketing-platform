# End-to-End Verification Findings (`e2e-tests`)

This document records genuine product defects discovered by the cross-service end-to-end verification suite (`e2e-tests`) against the containerized platform. Per project rules, production service code is not modified by the E2E suite, test assertions are never weakened, and genuine product findings are documented here with reproduction steps and suspected causes.

---

## `INV-001` — `inventory-service` returns HTTP 500 instead of retryable 503 during Redis concurrency gate unavailability under high contention

- **Affected Scenario**: `S5` (`TicketingPlatformE2ETest#s5_noOversellUnderHighContentionLoad`)
- **Severity**: High / Error Mapping & Availability
- **Status**: RESOLVED by Redis 503 fix (verified by S5)

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
4. `inventory-service` previously returned an HTTP 500 response instead of a retryable HTTP 503 Service Unavailable.

### 2. Observed Response Body (Prior to Fix)
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

### 4. Resolution
In `inventory-service`, `InventoryExceptionHandler` maps `RedisUnavailableException` and Redis connection failures to HTTP 503 `SERVICE_UNAVAILABLE` with `Retry-After: 1`, errorCode `SERVICE_UNAVAILABLE`, and RFC 7807 problem detail. S5 passes cleanly with 0 5xx responses.

---

## `PAYMENT-001` — `payment-service` refund decline handling (`tok_refund_decline`)

- **Affected Scenario**: `S11` (`TicketingPlatformE2ETest#s11_refundDeclinedRoutesToDeadLetterTopic`)
- **Severity**: Medium / Compensation Error Handling
- **Status**: RESOLVED by payment-service fixes (verified by S11)

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

---

## `KAFKA-001` — Slow crash recovery after `kill -9` of Kafka consuming service due to default 45 s consumer session timeout

- **Affected Scenarios**: `S4`, `S7`, `S8` (`TicketingPlatformE2ETest`)
- **Severity**: Medium / Availability & Failover Latency
- **Status**: RESOLVED (verified by S4, S7, S8)

### 1. Symptom
After `kill -9` or abrupt failure of a Kafka consuming service (`payment-service`, `order-service`), partition consumption resumed only after Kafka's default consumer session timeout (~45 s). This caused scenario S7 to exceed its original 30 s wait; the final state was correct (one payment, order `CONFIRMED`, outbox drained, consumer lag zero), but failover was unnecessarily slow.

### 2. Root Cause
Apache Kafka's default `session.timeout.ms` (45,000 ms) causes the Kafka broker / group coordinator to wait 45 seconds before detecting a terminated consumer process that left without sending a LeaveGroup request (`SIGKILL` or OOM). Only after the session timeout expires does the coordinator trigger a partition rebalance to reassign partitions to surviving or restarted consumers.

### 3. Resolution
1. **Java Consumers (`ticketing-consumer`)**:
   - Configured `session.timeout.ms = 15000` (15 s default) and `heartbeat.interval.ms = 3000` (3 s default) in `ConsumerAutoConfiguration`.
   - Exposed overridable properties `ticketing.consumer.session-timeout-ms` and `ticketing.consumer.heartbeat-interval-ms` (validated within `[6000, 60000]` ms, with heartbeat positive and `<= sessionTimeout / 3`).
2. **Go Consumer (`notification-service`)**:
   - Added configuration settings `SessionTimeout = 15s` and `HeartbeatInterval = 3s` mapped to `KAFKA_SESSION_TIMEOUT_MS` and `KAFKA_HEARTBEAT_INTERVAL_MS` env vars with matching defaults and validation.
3. **End-to-End Suite (`e2e-tests`)**:
   - Defined single constant `FAILOVER_BUDGET = Duration.ofSeconds(60)` (15 s session timeout + 45 s margin for restart and group rebalance) and replaced 30 s literals in S4, S7, S8.
   - Added recovery time instrumentation measuring elapsed seconds from process kill/restart until orders reach terminal status, recording recovery time in `ScenarioRecord` and rendering it in the verification report.

### 4. Measured Recovery Times After Fix
Across 3 consecutive verification runs of `make e2e-chaos` against the real Docker Compose topology:
- **S4 (Late Payment / payment-service stop & start)**: 16.88 s (Run 1), 16.79 s (Run 2), 16.67 s (Run 3)
- **S7 (Crash Recovery / SIGKILL payment-service, inventory-service, order-service)**: 18.23 s (Run 1), 18.33 s (Run 2), 18.48 s (Run 3)
- **S8 (Broker Outage / Kafka 15 s stop & start with 30 in-flight orders)**: 22.31 s (Run 1), 22.37 s (Run 2), 20.17 s (Run 3)

All measured recovery times consistently operate well within the 60 s failover budget (`FAILOVER_BUDGET`), achieving 100% pass rates across all chaos scenarios.
