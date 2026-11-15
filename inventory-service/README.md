# inventory-service

Authoritative seat inventory and reservation hold service for the Ticketing Platform (`port 8081`).

## Responsibilities

- **Admin Event & Seat Management**: Create events (`POST /api/v1/admin/events`), transition event status (`PATCH /api/v1/admin/events/{eventId}/status`), and bulk-provision seats (`POST /api/v1/admin/events/{eventId}/seats/bulk`).
- **Public Seat Availability (`GET /api/v1/events/{eventId}/seats`)**:
  - Ingress read endpoint routed via `gateway-service` without requiring authentication or identity headers. Strictly never exposes reservation or user data.
  - Keyset pagination with `limit` (default 200, maximum 1000; clamped to 1000 if exceeded) and `cursor` (seat ID UUID). Returns `items` (and alias `seats`) with `nextCursor`.
  - Seats strictly ordered by `(section_code ASC, row_label ASC, seat_number ASC, seat_id ASC)`.
  - Backed by Flyway migration `V4__seats_keyset_index.sql` composite indexes: `idx_seats_event_keyset` on `(event_id, section_code, row_label, seat_number, seat_id)` and `idx_seats_event_status_keyset` on `(event_id, status, section_code, row_label, seat_number, seat_id)` to ensure index scans with no sequential scans on 10,000+ seats.
  - Returns `Cache-Control: max-age=1` header. Unknown `eventId` returns RFC 7807 problem details with HTTP 404 `RESOURCE_NOT_FOUND`.
  - **Source of Truth**: PostgreSQL is the authoritative source of truth for seat status; a `HELD` seat in PostgreSQL may become `AVAILABLE` again when its hold expires or is cancelled.
- **Two-Layer Seat Hold (`POST /api/v1/reservations`)**:
  1. Fast atomic multi-seat gate in Redis (`hold_inventory.lua`, 660-second safety-net TTL).
  2. Authoritative PostgreSQL transaction locking seats (`FOR UPDATE` in ascending `seat_id` order), inserting `reservations` (`HELD`, 600-second TTL from DB clock) and `reservation_items`, and appending `inventory.seat.reserved` to `outbox_events`.
- **Hold Expiry Sweeper**: Background scheduled job (`ExpirySweeper`) that selects overdue `HELD` reservations in batches using `FOR UPDATE SKIP LOCKED`, transitions each reservation and its items to `EXPIRED` (`FOR UPDATE SKIP LOCKED` on the individual row), returns seats to `AVAILABLE` with `version` incremented, writes `inventory.reservation.expired` to `outbox_events`, and releases Redis seat locks after commit.
- **Transactional Outbox Publisher**: Enabled via `ticketing-outbox` (`ticketing.outbox.enabled=true`, `advisory-lock-id=71001`) to publish `inventory.events` records with `acks=all`, `enable.idempotence=true`, `delivery.timeout.ms=4000`, and `request.timeout.ms=1500`.
- **Payment Event Consumer (`payment.events`, group `inventory-service-payment-cg`)**:
  - Uses `IdempotentEventProcessor` (`ticketing-consumer`) so deduplication (`processed_events`), domain state transitions, and outbox writes execute in one PostgreSQL transaction.
  - `payment.succeeded`: Locks the reservation row with blocking `FOR UPDATE`.
    - `HELD`: Confirms reservation (`CONFIRMED`, `confirmed_at` from DB clock), items (`CONFIRMED`), and seats (`SOLD`, `version` incremented), writes `inventory.seat.confirmed` to `outbox_events`, and after commit calls `RedisSeatGate.confirm` (with up to 3 retries before logging and incrementing `inventory_redis_confirm_failures_total`).
    - `EXPIRED` / `RELEASED`: Leaves domain rows unchanged and writes `inventory.seat.confirmation_rejected` (`reason = "RESERVATION_ALREADY_EXPIRED"` or `"RESERVATION_ALREADY_RELEASED"`, `rejectedAt` from DB clock) to `outbox_events`.
    - `CONFIRMED`: No-op.
    - Non-existent reservation: Throws `NonRetryableEventException` (routes directly to `payment.events.dlt`).
  - `payment.failed`: Locks the reservation row with blocking `FOR UPDATE`.
    - `HELD`: Transitions reservation to `RELEASED` (`released_at` from DB clock), items to `RELEASED`, and seats to `AVAILABLE` (`version` incremented), writes `inventory.seat.released` (`reason = "PAYMENT_FAILED"`) to `outbox_events`, and after commit calls `RedisSeatGate.release` with the `reservationId` token.
    - `CONFIRMED` / `EXPIRED` / `RELEASED`: No-op.
    - Non-existent reservation: Throws `NonRetryableEventException`.
  - Any other event type on `payment.events`: Logged at `DEBUG` level, recorded in `processed_events`, and counted with `outcome="ignored"`.

## Metrics

- `inventory_payment_events_handled_total{event_type, outcome}` (`outcome` in `confirmed`, `rejected`, `released`, `noop`, `ignored`)
- `inventory_redis_confirm_failures_total`

## Assumptions

1. **Post-commit Redis `confirm` retry backoff**: When `RedisSeatGate.confirm` fails after the PostgreSQL transaction commits on `payment.succeeded`, `PaymentEventService` performs up to 3 retries (4 attempts total) with a default 50 ms pause between retries (`setRedisConfirmRetryPauseMs`), then logs at `ERROR` with `reservationId` and increments `inventory_redis_confirm_failures_total` without throwing.
2. **Envelope `correlationId` propagation to outbox**: When handling `payment.succeeded` or `payment.failed`, if `MDC` does not already contain `correlationId` (for example during direct service invocation in tests), `PaymentEventService` populates `MDC` from the incoming `EventEnvelope.correlationId()` for the duration of the call so `OutboxWriter` writes the incoming `correlationId` onto the resulting `inventory.events` outbox row.
3. **Fallback `orderId` on payment payloads**: Per `docs/arch/02-saga.md` (`orderId == reservationId` in V1), if an incoming payment payload has a null `orderId`, `reservationId` is used as the fallback `orderId` when constructing `SeatConfirmedPayload`, `SeatConfirmationRejectedPayload`, or `SeatReleasedPayload`.
4. **Unrelated event types on `payment.events`**: Envelope validation in `PaymentEventService` verifies the structural envelope fields (`schemaVersion == 1`, `eventId`, `eventType`, `occurredAt`, `correlationId`, non-null `payload`) before delegating to `IdempotentEventProcessor.process(UUID, String, String, Runnable)` so that any event type other than `payment.succeeded` and `payment.failed` (such as `payment.refunded` or any future payment event type) is deduplicated, logged at `DEBUG`, and counted with `outcome="ignored"` (using `ConsumerMetrics.sanitizeEventType` to keep metric tags bounded) without being retried or dead-lettered.
5. **Kafka listener auto-startup property**: `PaymentEventConsumer` binds `autoStartup = "${spring.kafka.listener.auto-startup:true}"` (default `true`) so existing non-Kafka integration tests can disable the Kafka listener container alongside `ticketing.outbox.enabled=false`.
6. **Redis confirmed seat representation and DLT topic naming**: Per `docs/arch/08-redis.md` (Section 9.1 and 9.2.3) and `confirm_inventory.lua`, `RedisSeatGate.confirm` marks confirmed seats in Redis by overwriting `seat:{eventId}:{seatId}` with `"SOLD"` and no TTL (`-1`) rather than using a separate `confirmed:seats:{eventId}` set. Per `docs/arch/03-kafka.md` and `infra/kafka/create-topics.sh`, retry and dead-letter topics for `payment.events` are qualified with the consumer group (`payment.events-inventory-service-payment-cg-retry-0`, `payment.events-inventory-service-payment-cg-retry-1`, `payment.events-inventory-service-payment-cg-dlt`).
7. **Public Seat Availability Keyset Pagination & Filtering**:
   - `GET /api/v1/events/{eventId}/seats` supports optional filters `section` (also accepting `sectionCode`) and `status` (`AVAILABLE`, `HELD`, `SOLD`). Invalid status values return HTTP 400 `VALIDATION_ERROR`.
   - `limit` defaults to 200 and is clamped to a maximum of 1000 (values > 1000 clamp to 1000; non-positive values default to 200).
   - `cursor` is based on the seat ID (UUID string or URL-safe base64 encoded UUID). When provided, the cursor seat's location `(section_code, row_label, seat_number, seat_id)` is resolved in PostgreSQL and used in the keyset condition `(section_code, row_label, seat_number, seat_id) > (...)` to ensure deterministic paging without duplicates or gaps.
   - `Cache-Control: max-age=1` header is set on all 200 responses.
   - PostgreSQL is the source of truth for seat status; a seat marked `HELD` in PostgreSQL transitions back to `AVAILABLE` upon hold expiry. Sequential scans are prevented by `V4__seats_keyset_index.sql`.

