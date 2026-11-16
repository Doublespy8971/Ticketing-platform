# Payment Service

`payment-service` is the payment processor and compensating refund service for the Ticketing Platform.

## Architecture & Integration Scope

- **HTTP Port**: `8083` (health, readiness, and metrics endpoints under `/actuator/*`).
- **Database**: PostgreSQL 16 `payment_db` (default host port `5435`, database `payment_db`, user `ticketing`).
- **Transactional Outbox**: Single-leader polling outbox (`200 ms`) with session advisory lock `71003` publishing to `payment.events`.
- **Kafka Consumer Group**: `payment-service-order-cg` consuming from `order.events` with `@RetryableTopic` (2 retry levels: `2s`, `10s` + DLT `order.events-payment-service-order-cg-dlt`).

## Events Consumed & Produced

- **Consumed from `order.events`**:
  - `order.payment.requested`: Triggers charge through `PaymentProvider`. Inserts payment row into `payments` table (`SUCCEEDED` or `FAILED`) and emits `payment.succeeded` or `payment.failed` to `outbox_events`.
  - `order.refund.requested`: Triggers refund through `PaymentProvider` for a `SUCCEEDED` payment. Transitions payment to `REFUNDED` and emits `payment.refunded` to `outbox_events`.
  - All other event types on `order.events` (`order.confirmed`, `order.cancelled`) are acknowledged and ignored with debug logging.
- **Produced to `payment.events`**:
  - `payment.succeeded` (partition key: `orderId`)
  - `payment.failed` (partition key: `orderId`)
  - `payment.refunded` (partition key: `orderId`)

## Mock Payment Provider & Deterministic Tokens

`MockPaymentProvider` implements `PaymentProvider` and simulates external gateway behavior:
- **Idempotency Guarantee**: Real gateways (Stripe, Adyen) deduplicate requests natively by idempotency key. `MockPaymentProvider` maintains in-memory maps keyed by `idempotencyKey` returning cached results on repeat invocations without counting as duplicate charges or refunds.
- **Magic Tokens Table**:

| Magic Token | Operation | Behavior | Failure Code / Outcome |
| :--- | :--- | :--- | :--- |
| `tok_success` | Charge | Always succeeds immediately regardless of configured decline rate or Random seed. | Outcome `SUCCEEDED`, provider reference `mock_ch_*` |
| `tok_decline` | Charge | Deterministically declined by card issuer regardless of decline rate. | Outcome `DECLINED`, failure code `CARD_DECLINED`, message `Payment declined by card issuer` |
| `tok_error` | Charge | Throws `ProviderUnavailableException` on the first delivery attempt per idempotency key; succeeds on subsequent retry attempts. | Transient exception, routed to Kafka retry topics (`-retry-0`, `-retry-1`) |
| `tok_refund_decline` | Charge + Refund | Charge succeeds immediately (`SUCCEEDED`); subsequent refund is deterministically declined by the provider. | Charge: outcome `SUCCEEDED`, provider ref `mock_ch_*`; Refund: outcome `DECLINED`, failure code `REFUND_DECLINED`, message `Refund declined by payment provider` |

Non-magic tokens (any arbitrary token string such as `"pm_tok_visa"`) succeed or decline based on the configured random decline rate (`ticketing.payment.provider.mock.decline-rate`, default `0.05`).

- **Latency & Failure Configuration**:
  - `ticketing.payment.provider.mock.decline-rate`: default `0.05`
  - `ticketing.payment.provider.mock.min-latency-ms`: default `50`
  - `ticketing.payment.provider.mock.max-latency-ms`: default `300`

## Assumptions

1. **Provider Invocation Outside Database Transactions**: External gateway calls (`charge`, `refund`) always execute before the local PostgreSQL transaction begins, preventing remote network latency from holding database row locks.
2. **One Payment per Order Constraint**: `payments.order_id` is unique. If concurrent consumer threads race to insert a payment for the same order, `ON CONFLICT (order_id) DO NOTHING` ensures only the winning thread writes the row and publishes the outbox event.
3. **Refund Reason Default**: When `order.refund.requested` carries an empty or null reason, `"LATE_PAYMENT_EXPIRED_HOLD"` is used as defined in `docs/arch/04-events.md`.
4. **Dead-Letter Routing for Unrecoverable Events**: `NonRetryableEventException` is thrown when an order payment cannot be found for a refund, or when refund currency/amount does not match the payment. Spring Kafka routes these directly to `order.events-payment-service-order-cg-dlt` without retry backoff.
5. **Database Clock Authority**: All payment timestamps (`paid_at`, `failed_at`, `refunded_at`, `created_at`, `updated_at`) are derived from PostgreSQL's `clock_timestamp()`.
6. **Token Security**: Payment method tokens are never logged, never included in metrics, never placed in exception messages, and stored only in the `payments` table as specified by the database schema.

## Failure runbook

This runbook outlines operational procedures and recovery steps for common failure scenarios encountered by `payment-service`:

### 1. Technical Provider Error Exhausted Retries
- **Symptom**: External payment gateway experiences prolonged downtime or connectivity failure. Consumer retries across `order.events-payment-service-order-cg-retry-0` and `order.events-payment-service-order-cg-retry-1` are exhausted, and the `order.payment.requested` event lands in `order.events-payment-service-order-cg-dlt`.
- **System State**: The order remains in `PAYMENT_PENDING` with no row present in the `payments` table. Eventually, the reservation hold in `inventory-service` expires, triggering `inventory.reservation.expired` and transitioning the order to `CANCELLED_EXPIRED`.
- **Operator Action**:
  1. Investigate provider outage or network egress issues.
  2. Inspect the DLT record in `order.events-payment-service-order-cg-dlt`. Verify whether a charge actually executed at the external provider by checking gateway logs or API dashboard using `orderId` as the idempotency key.
  3. Reconcile from DLT: If the gateway never executed the charge, or once connectivity is restored, replay the event from the DLT back to `order.events`. Because `paymentProvider.charge` is idempotent per `orderId` and `payments.order_id` has a unique constraint with `ON CONFLICT DO NOTHING`, replay is safe and idempotent.

### 2. Declined Refund
- **Symptom**: When attempting a compensating refund for `order.refund.requested` (e.g., following a late payment after hold expiration), the external provider declines the refund (e.g., card cancelled, account closed, or mock token `tok_refund_decline`).
- **System State**:
  - The payment row in `payments` remains in `SUCCEEDED` status (it is NOT marked `REFUNDED`).
  - The order remains in `REFUND_PENDING` in `order-service`.
  - No `payment.refunded` event is published to `outbox_events` or Kafka.
  - An error log is emitted with `paymentId`, `failureCode`, and `failureMessage` (tokens are never logged).
  - Metric `payment_refund_declined_total` is incremented with tag `failure_code`.
  - The message is immediately routed to `order.events-payment-service-order-cg-dlt` without retrying.
- **Operator Action**:
  1. Inspect the DLT record and alert on `payment_refund_declined_total > 0`.
  2. Identify the root cause for the refund decline (e.g., contact customer for alternate refund method or resolve gateway merchant configuration).
  3. Once the root cause is resolved or manual credit is arranged, replay the message from the DLT. Replay is idempotent per `paymentId` via `MockPaymentProvider`'s idempotency key and database row lock on `payments.payment_id`.

### 3. Invalid Events
- **Symptom**: An incoming `order.payment.requested` or `order.refund.requested` event fails pre-validation against database schema constraints (e.g., missing required fields, `reservationId != orderId`, non-positive `amountCents`, non-3-letter `currency`, blank or `> 128` char `paymentMethodToken`, or non-UUID `correlationId`).
- **System State**: The event is rejected immediately before calling the payment provider and before modifying the database. `NonRetryableEventException` is thrown, forwarding the event directly to `order.events-payment-service-order-cg-dlt` without retrying. No payment row is created, no provider call is made, and no outbox event is appended.
- **Operator Action**:
  1. Inspect the malformed payload in the DLT and review producer service logs (`order-service` or upstream caller) to identify schema contract mismatches.
  2. Because the payload violates core schema constraints, invalid messages should not be replayed without manual payload correction or producer bug fixes.

