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
- **Magic Tokens**:
  - `tok_decline`: Deterministically declines with failure code `CARD_DECLINED`.
  - `tok_error`: Throws `ProviderUnavailableException` on the first delivery attempt for an idempotency key, allowing Spring Kafka retry topics to redeliver and succeed on the next attempt.
  - Any other token succeeds (subject to configurable random decline rate).
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

