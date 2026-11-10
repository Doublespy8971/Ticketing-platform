# order-service

Order lifecycle and saga choreography service for the Ticketing Platform (`port 8082`).

## Responsibilities

- **Order State Machine (`order_db`)**: Manages the authoritative lifecycle of customer orders (`RESERVED` -> `PAYMENT_PENDING` -> `CONFIRMED` / `CANCELLED_*` / `REFUND_PENDING` / `CANCELLED_REFUNDED`).
- **Kafka Saga Consumers (`OrderSagaConsumer` + `OrderSagaService`)**:
  - `inventory.events` listener with explicit consumer group `order-service-inventory-cg`:
    - `inventory.seat.reserved`: creates order in `RESERVED` (`orderId = reservationId`) with its `order_items`. Duplicate or already materialized orders are acknowledged as a no-op (`outcome=noop`).
    - `inventory.reservation.expired`: moves `RESERVED` or `PAYMENT_PENDING` (when `payment_id IS NULL`) to `CANCELLED_EXPIRED` and writes `order.cancelled` (`finalStatus="CANCELLED_EXPIRED"`, `cancellationReason="RESERVATION_EXPIRED"`). No-op when `PAYMENT_PENDING` already has `payment_id` recorded or in any other status.
    - `inventory.seat.confirmed`: moves `PAYMENT_PENDING` to `CONFIRMED`, backfills `payment_id` and `paid_at` if arriving before `payment.succeeded`, sets `confirmed_at = clock_timestamp()`, and writes `order.confirmed` with seat line items.
    - `inventory.seat.confirmation_rejected`: moves `PAYMENT_PENDING`, `CANCELLED_EXPIRED`, or `CANCELLED_PAYMENT_FAILED` to `REFUND_PENDING`, records `payment_id`, and writes `order.refund.requested` (`reason="LATE_PAYMENT_EXPIRED_HOLD"`).
  - `payment.events` listener with explicit consumer group `order-service-payment-cg`:
    - `payment.succeeded`: in `PAYMENT_PENDING` records `payment_id` and `paid_at` without changing status; in `CONFIRMED` backfills `payment_id` and `paid_at`; in `CANCELLED_EXPIRED` or `CANCELLED_PAYMENT_FAILED` moves to `REFUND_PENDING` and writes `order.refund.requested` (`reason="LATE_PAYMENT_EXPIRED_HOLD"`).
    - `payment.failed`: moves `PAYMENT_PENDING` to `CANCELLED_PAYMENT_FAILED` and writes `order.cancelled` (`finalStatus="CANCELLED_PAYMENT_FAILED"`, `cancellationReason="PAYMENT_FAILED"`).
    - `payment.refunded`: moves `REFUND_PENDING` to `CANCELLED_REFUNDED` and writes `order.cancelled` (`finalStatus="CANCELLED_REFUNDED"`, `cancellationReason="LATE_PAYMENT_EXPIRED_HOLD"`).
  - Unhandled event types (such as `inventory.seat.released`) are acknowledged and ignored at `DEBUG` level (`outcome=ignored`).
  - Unknown order handling: `payment.*` events for an unknown order throw `NonRetryableEventException` and route directly to `-dlt`; `inventory.reservation.expired`, `inventory.seat.confirmed`, and `inventory.seat.confirmation_rejected` for an unknown order throw a retryable `IllegalStateException` so they traverse `-retry-0` and `-retry-1` before `-dlt`.
- **Transactional Outbox Poller (`ticketing-outbox`)**:
  - Enabled by default (`ticketing.outbox.enabled=true`) with PostgreSQL session advisory lock ID `71002`.
- **Order Checkout (`POST /api/v1/orders/{orderId}/checkout`)**:
  - Validates client request: `paymentMethodToken` required, maximum 128 characters, never logged or emitted in metrics or response bodies.
  - HTTP Idempotency fencing via `idempotency_keys` table: SHA-256 canonical request fingerprint (`POST:/api/v1/orders/{orderId}/checkout:{json}`), atomic claim with microsecond-truncated timestamp, 30s stale reclaim, 202 response replay with `X-Idempotent-Replay: true`, 422 mismatch, and 409 in-progress with `Retry-After: 1`. Business rejections delete the `IN_PROGRESS` claim so clients can retry.
  - Transactional checkout owned by `CheckoutService` (`TransactionTemplate`):
    - Locks order row with `SELECT ... FOR UPDATE` (`WHERE order_id = ? AND user_id = ?`).
    - Handles early-checkout race and mismatched users by returning `409 ORDER_NOT_READY` with `Retry-After: 1` without leaking whether the order exists for another caller.
    - Verifies 15-second grace buffer against the database clock (`expires_at - clock_timestamp() >= INTERVAL '15 seconds'`); returns `409 RESERVATION_EXPIRED` otherwise.
    - Transitions order from `RESERVED` to `PAYMENT_PENDING`, records `checkout_at = clock_timestamp()` and `payment_method_token`, and increments `version`.
    - Writes `order.payment.requested` to `outbox_events` (`ticketing-outbox`) inside the same ACID transaction.
    - Marks idempotency key `COMPLETED` and returns `202 Accepted`.
    - If the order is already in `PAYMENT_PENDING` on retry with a new key, returns `409 INVALID_ORDER_STATE` without writing a second outbox event.
- **Order Query (`GET /api/v1/orders/{orderId}`)**:
  - Returns `200 OK` with order details, timestamps, and seat line items from `order_items`.
  - Rejects unauthorized callers with `401 Unauthorized` (`UNAUTHORIZED`).
  - Rejects callers who do not own the order with `403 Forbidden` (`FORBIDDEN`).
  - Returns `404 Not Found` (`RESOURCE_NOT_FOUND`) if the order does not exist.

## Metrics

- `ticketing_orders_checkout_total{outcome}`: Total checkout requests tagged by `outcome` (`ACCEPTED`, `ORDER_NOT_READY`, `RESERVATION_EXPIRED`, `INVALID_ORDER_STATE`, `REPLAY`).
- `order_checkout_duration_seconds`: Duration of order checkout processing.
- `order_saga_events_handled_total{event_type, outcome}`: Total Kafka events handled by `order-service` saga tagged by `event_type` (whitelist of `EventTypes` or `"unknown"`) and `outcome` (`applied`, `noop`, `ignored`, `refund_requested`).
- `ticketing_orders_materialization_lag_seconds`: Lag between `inventory.seat.reserved` (`reservedAt`) and `orders` row insertion (`created_at`).
- `ticketing_orders_terminal_total{final_status}`: Orders reaching a terminal status (`CONFIRMED`, `CANCELLED_EXPIRED`, `CANCELLED_PAYMENT_FAILED`, `CANCELLED_REFUNDED`).

## Assumptions

1. **Early-checkout and mismatched user response equivalence**: To prevent leaking order ownership across callers, requests to `POST /api/v1/orders/{orderId}/checkout` return the exact same `409 ORDER_NOT_READY` response with `Retry-After: 1` whether the order does not exist or belongs to a different user.
2. **Already PAYMENT_PENDING retry semantics**: If checkout is retried with the same `Idempotency-Key` and matching payload, the stored `202 Accepted` response is replayed with `X-Idempotent-Replay: true`. If retried with a different `Idempotency-Key`, `409 INVALID_ORDER_STATE` is returned without a second outbox event, and the new key's `IN_PROGRESS` claim is released.
3. **Grace buffer evaluation**: The 15-second grace buffer is evaluated directly against the database engine clock (`expires_at - clock_timestamp() >= INTERVAL '15 seconds'`) under row lock `SELECT ... FOR UPDATE` to avoid clock drift between application instances.
4. **Idempotency endpoint path**: The endpoint string in `idempotency_keys.endpoint` and in the SHA-256 hash input is canonicalized to `/api/v1/orders/{orderId}/checkout` incorporating the path variable `orderId` so an idempotency key cannot be reused across different orders.
5. **Refund request reason and sibling deduplication**: Both `payment.succeeded` and `inventory.seat.confirmation_rejected` arriving against a cancelled order (`CANCELLED_EXPIRED` or `CANCELLED_PAYMENT_FAILED`), as well as `inventory.seat.confirmation_rejected` arriving in `PAYMENT_PENDING`, transition the order to `REFUND_PENDING` and emit `order.refund.requested` with `reason="LATE_PAYMENT_EXPIRED_HOLD"`. The second sibling event arriving when the order is already in `REFUND_PENDING` is treated as a no-op (`outcome=noop`) so at most one `order.refund.requested` is ever written per order.
6. **Outbox poller and Kafka listener defaults**: `ticketing.outbox.enabled` defaults to `true` (advisory lock ID `71002`), and Kafka listener auto-startup defaults to `true` (`spring.kafka.listener.auto-startup:true`), which can be disabled in isolated database/HTTP tests.
