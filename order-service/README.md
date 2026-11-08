# order-service

Order lifecycle and saga choreography service for the Ticketing Platform (`port 8082`).

## Responsibilities

- **Order State Machine (`order_db`)**: Manages the authoritative lifecycle of customer orders (`RESERVED` -> `PAYMENT_PENDING` -> `CONFIRMED` / `CANCELLED_*` / `REFUND_PENDING` / `CANCELLED_REFUNDED`).
- **Order Checkout (`POST /api/v1/orders/{orderId}/checkout`)**:
  - Validates client request: `paymentMethodToken` required, maximum 128 characters, never logged or emitted in metrics or response bodies.
  - HTTP Idempotency fencing via `idempotency_keys` table: SHA-256 canonical request fingerprint (`POST:/api/v1/orders/{orderId}/checkout:{json}`), atomic claim with microsecond-truncated timestamp, 30s stale reclaim, 202 response replay with `X-Idempotent-Replay: true`, 422 mismatch, and 409 in-progress with `Retry-After: 1`. Business rejections delete the `IN_PROGRESS` claim so clients can retry.
  - Transactional checkout owned by `CheckoutService` (`TransactionTemplate`):
    - Locks order row with `SELECT ... FOR UPDATE`.
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

- `order_checkout_requests_total{outcome}`: Total checkout requests tagged by `outcome` (`accepted`, `not_ready`, `expired`, `conflict`, `replay`).
- `order_checkout_duration_seconds`: Histogram of checkout processing duration.

## Assumptions

1. **Early-checkout and mismatched user response equivalence**: To prevent leaking order ownership across callers, requests to `POST /api/v1/orders/{orderId}/checkout` return the exact same `409 ORDER_NOT_READY` response with `Retry-After: 1` whether the order does not exist or belongs to a different user.
2. **Already PAYMENT_PENDING retry semantics**: If checkout is retried with the same `Idempotency-Key` and matching payload, the stored `202 Accepted` response is replayed with `X-Idempotent-Replay: true`. If retried with a different `Idempotency-Key`, `409 INVALID_ORDER_STATE` is returned without a second outbox event, and the new key's `IN_PROGRESS` claim is released.
3. **Grace buffer evaluation**: The 15-second grace buffer is evaluated directly against the database engine clock (`expires_at - clock_timestamp() >= INTERVAL '15 seconds'`) under row lock `SELECT ... FOR UPDATE` to avoid clock drift between application instances.
4. **Idempotency endpoint path**: The endpoint string in `idempotency_keys.endpoint` and in the SHA-256 hash input is canonicalized to `/api/v1/orders/{orderId}/checkout` incorporating the path variable `orderId` so an idempotency key cannot be reused across different orders.
5. **Outbox poller and Kafka listener defaults**: `ticketing.outbox.enabled` defaults to `false` in this service until the saga event consumers are implemented, and Kafka listener auto-startup can be disabled via `spring.kafka.listener.auto-startup:false` for isolated database tests.

