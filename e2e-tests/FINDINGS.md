# End-to-End Verification Findings (`e2e-tests`)

This document records genuine product defects discovered by the cross-service end-to-end verification suite (`e2e-tests`) against the containerized platform. Per project rules, production service code is not modified by the E2E suite, test assertions are never weakened, and the affected test is annotated with `@Disabled` referencing the bug identifier below.

---

## `PAYMENT-001` — `payment-service` unconditionally succeeds refunds (`tok_refund_decline` unimplemented and `PaymentResult` ignored on refund)

- **Affected Scenario**: `S11` (`TicketingPlatformE2ETest#s11_refundDeclinedRoutesToDeadLetterTopic`)
- **Severity**: Medium / Compensation Error Handling
- **Status**: Open (Test marked `@Disabled("Product bug PAYMENT-001: ...")`)

### 1. Reproduction Steps
1. Start the full 10-container stack (`make stack-up` or `make stack-reset`).
2. Create an `ON_SALE` event with 1 seat and have a buyer hold the seat (`POST /api/v1/reservations` -> `201 Created`).
3. Wait for the order to materialize in `AWAITING_PAYMENT` (`GET /api/v1/orders/{orderId}`).
4. Stop `payment-service` (`docker stop payment-service`) so `order.payment.requested` remains unconsumed in Kafka.
5. Call `POST /api/v1/orders/{orderId}/checkout` with `{"paymentMethodToken": "tok_refund_decline"}` (`202 Accepted`).
6. Force-expire the reservation (`Db.forceReservationExpiry(orderId)`) and wait for the order to transition to `CANCELLED_EXPIRED`.
7. Start `payment-service` (`docker start payment-service`) so it processes the late `order.payment.requested` (emitting `payment.succeeded`), `inventory-service` rejects seat confirmation (`inventory.seat.confirmation_rejected`), `order-service` transitions the order to `REFUND_PENDING` and emits `order.refund.requested`, and `payment-service` consumes `order.refund.requested`.
8. Run:
   ```bash
   mvn -pl e2e-tests -Pe2e -Dtest=TicketingPlatformE2ETest#s11_refundDeclinedRoutesToDeadLetterTopic test
   ```

### 2. Expected vs. Observed Behavior

| Aspect | Expected (`S11` Specification & `docs/arch/02-saga.md`) | Observed |
|---|---|---|
| `payment_db.payments.status` | Remains `SUCCEEDED` | Transitions to `REFUNDED` |
| `order_db.orders.status` | Remains `REFUND_PENDING` (awaiting manual ops resolution) | Transitions to `CANCELLED_REFUNDED` |
| `payment.refunded` outbox event | `0` emitted | `1` emitted (`payment.refunded`) |
| `order.events-payment-service-order-cg-dlt` | `+1` dead-lettered `order.refund.requested` record after retries | `0` dead-lettered records |

Exact assertion failure from `TicketingPlatformE2ETest#s11_refundDeclinedRoutesToDeadLetterTopic`:
```text
org.opentest4j.AssertionFailedError: 
[Payment with tok_refund_decline must remain SUCCEEDED when refund is declined] 
expected: "SUCCEEDED"
 but was: "REFUNDED"
	at com.ticketing.e2e.TicketingPlatformE2ETest.s11_refundDeclinedRoutesToDeadLetterTopic(TicketingPlatformE2ETest.java:1360)
```

### 3. Suspected Root Cause
There are two compounding defects in `payment-service`:

1. **`MockPaymentProvider` does not implement `tok_refund_decline`**:
   - In `payment-service/src/main/java/com/ticketing/payment/service/provider/MockPaymentProvider.java` (lines 30–33 and 109–127), only `tok_success`, `tok_decline`, and `tok_error` are defined.
   - `MockPaymentProvider.refund(UUID idempotencyKey, String providerReference, long amountCents, String currency)` does not receive or inspect the payment's `paymentMethodToken` (or `providerReference` prefix) for `tok_refund_decline` and unconditionally returns `PaymentResult.succeeded(refundRef)` on line 123.
2. **`PaymentService.handleOrderRefundRequested` ignores the `PaymentResult` returned by `paymentProvider.refund(...)`**:
   - In `payment-service/src/main/java/com/ticketing/payment/service/PaymentService.java` (lines 247–276), `handleOrderRefundRequested` invokes:
     ```java
     try {
         paymentProvider.refund(orderId, existing.providerReference(), payload.amountCents(), payload.currency());
     } catch (ProviderUnavailableException ex) {
         ...
     }
     ```
   - The returned `PaymentResult` is discarded without checking `result.status()`. Even if `paymentProvider.refund(...)` returned `PaymentResult.declined(...)`, `PaymentService` would still proceed to line 269 (`paymentRepository.markRefunded(...)`) and emit `payment.refunded`, instead of throwing `NonRetryableConsumerException` (or retrying and routing to `order.events-payment-service-order-cg-dlt`).

### 4. Affected Files
- `payment-service/src/main/java/com/ticketing/payment/service/provider/MockPaymentProvider.java` (lines 30–33, 109–127)
- `payment-service/src/main/java/com/ticketing/payment/service/provider/PaymentProvider.java`
- `payment-service/src/main/java/com/ticketing/payment/service/PaymentService.java` (lines 212–290, `handleOrderRefundRequested`)
