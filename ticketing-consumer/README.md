# ticketing-consumer

Shared Kafka consumer library (`com.ticketing.consumer`) providing transactional `processed_events` deduplication, non-blocking retry topics (`2s` and `10s` backoff), dead-letter topic routing, Micrometer metrics, and scheduled retention cleanup.

## Single Configuration Import

Spring Boot services automatically discover `ConsumerAutoConfiguration` via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`, or can import it explicitly with a single `@Import`:

```java
@Import(ConsumerAutoConfiguration.class)
```

## Public Classes

- `com.ticketing.consumer.IdempotentEventProcessor` — Executes `INSERT INTO processed_events ... ON CONFLICT (event_id, consumer_group) DO NOTHING` and caller business logic atomically in a single PostgreSQL transaction, failing fast if a transaction is already active.
- `com.ticketing.consumer.ConsumerEnvelopeParser` — Parses raw `String` Kafka values into `EventEnvelope<JsonNode>` or typed `EventEnvelope<T>` using `ObjectMapperFactory`, validating `schemaVersion == 1`, required fields, and `EventTypes` whitelist membership.
- `com.ticketing.consumer.NonRetryableEventException` — Runtime exception indicating a poison or schema-invalid record that must bypass `-retry-0` and `-retry-1` and route directly to `-dlt`.
- `com.ticketing.consumer.TicketingRetryTopicNamesProviderFactory` — Spring Kafka `RetryTopicNamesProviderFactory` generating `<topic>-<consumer-group>-retry-0`, `<topic>-<consumer-group>-retry-1`, and `<topic>-<consumer-group>-dlt` topic and group names matching `infra/kafka/create-topics.sh`.
- `com.ticketing.consumer.TicketingDeadLetterPublishingRecoverer` — Custom `DeadLetterPublishingRecoverer` that preserves first-failure `kafka_dlt-original-*` headers across retry hops, writes unwrapped latest `kafka_dlt-exception-*` headers, and increments retry/DLT counters.
- `com.ticketing.consumer.ConsumerMetrics` — Registers and records `ticketing_kafka_consumer_dlt_total`, `consumer_events_processed_total`, `consumer_events_duplicate_total`, `consumer_events_retried_total`, and `consumer_event_processing_seconds` with whitelisted `event_type` tags.
- `com.ticketing.consumer.TicketingConsumerProperties` — Enforces mandatory consumer settings (`StringDeserializer`, `enable.auto.commit=false`, `isolation.level=read_committed`, `auto.offset.reset=earliest`, `max.poll.records=100`, `max.poll.interval.ms=300000`).
- `com.ticketing.consumer.ConsumerCleanupProperties` — `@ConfigurationProperties(prefix = "ticketing.consumer.cleanup")` validating `retention-hours >= 192` and `batch-size` in `1..10000` at startup.
- `com.ticketing.consumer.ProcessedEventsRetentionCleaner` — Scheduled batch cleaner running on `ticketingConsumerCleanupScheduler` (`0 0 * * * *`) that purges `processed_events` rows older than 192 hours using `FOR UPDATE SKIP LOCKED`.
- `com.ticketing.consumer.autoconfigure.ConsumerAutoConfiguration` — Single Spring Boot auto-configuration wiring consumer factories, listener containers (`AckMode.RECORD` + `CorrelationRecordInterceptor`), retry topics, metrics, and retention cleanup.

## Why Non-Blocking Retry Topics Can Reorder Events of the Same Aggregate

When event $E_1$ for aggregate key $K$ (for example `orderId`) fails with a transient error on the main topic (e.g., `order.events`), `DeadLetterPublishingRecoverer` publishes $E_1$ to `<topic>-<group>-retry-0` (2,000 ms delay) and immediately acknowledges $E_1$'s offset on the main partition so that other aggregates sharing that partition are not blocked.

Because the main partition continues advancing while $E_1$ waits on `-retry-0`, a subsequent event $E_2$ for the **same** aggregate key $K$ arriving on the main topic can be consumed and committed **before** $E_1$ finishes its 2,000 ms (or 10,000 ms) retry delay.

Consequently, **every domain event handler must be written as an explicit state-machine guard** (`docs/arch/02-saga.md`, `docs/arch/05-outbox-idempotency.md`):
1. Lock the target domain row (`SELECT ... FOR UPDATE`) inside `IdempotentEventProcessor.process(...)`.
2. Check whether the current domain status permits the transition (for example, `RESERVED -> PAYMENT_PENDING`, or `HELD -> CONFIRMED`).
3. Ignore or compensate out-of-order transitions deterministically rather than assuming strict arrival order across retry hops.

## Failure Windows & Recovery Guarantees

| Failure Window | Database State (`processed_events` & Domain Tables) | Kafka Offset / Retry State | Recovery Outcome |
| :--- | :--- | :--- | :--- |
| **1. Crash before `IdempotentEventProcessor.process` starts** | No `processed_events` row; no domain state changes. | Main topic offset is uncommitted (`enable.auto.commit=false`, `AckMode.RECORD`). | After rebalance/restart, Kafka redelivers the record from the last committed offset and `process` executes normally. |
| **2. Exception or crash inside `process` before PostgreSQL commit** | `TransactionTemplate` rolls back **both** the `processed_events` insert and all domain/outbox SQL statements together. | Main topic offset is not acknowledged; `DeadLetterPublishingRecoverer` forwards the record to `-retry-0` (`2s`), then `-retry-1` (`10s`). | Retry listener re-runs `process`; because `processed_events` rolled back, the retry inserts the deduplication row and executes business logic cleanly. |
| **3. Crash after PostgreSQL commit but before `AckMode.RECORD` offset commit** | Both `processed_events` and domain/outbox mutations are durably committed in PostgreSQL. | Offset was not committed to Kafka before the crash, so Kafka redelivers the record on restart. | On redelivery, `INSERT INTO processed_events ... ON CONFLICT (event_id, consumer_group) DO NOTHING` returns `0` rows; `process` skips `businessLogic`, increments `consumer_events_duplicate_total`, and returns `false` so the offset is committed. |
| **4. Concurrent duplicate delivery across two consumer instances** | Instance A's transaction holds the row lock on `(event_id, consumer_group)` in `processed_events`; Instance B blocks on the unique index until A commits or rolls back. | Both instances hold their respective record deliveries. | Once A commits, B's `ON CONFLICT DO NOTHING` returns `0` rows, skips `businessLogic`, and increments `consumer_events_duplicate_total`. Business logic executes at most once. |
| **5. Poison record (`NonRetryableEventException`) or all 3 attempts exhausted** | No `processed_events` row or domain state change is committed. | Record is published to `<topic>-<group>-dlt` with original payload bytes and all 7 `kafka_dlt-*` headers; source offset is committed. | Partition continues processing subsequent valid records immediately; `ticketing_kafka_consumer_dlt_total` is incremented for alerting and replay inspection. |

## Assumptions

1. `IdempotentEventProcessor.process(UUID, String, String, Runnable)` returns `boolean` (`true` when executed, `false` when skipped as a duplicate), which is binary- and source-compatible with void-style statement invocations while allowing callers and tests to inspect whether the event was deduplicated.
2. `ConsumerMetrics` registers a fallback `MeterFilter` adding `service` (defaulting to `spring.application.name` or `unknown-service`) only when the `MeterRegistry` does not already supply a `service` common tag, ensuring standalone unit tests with `SimpleMeterRegistry` and full Spring Boot contexts produce identical tag sets.
3. `TicketingDeadLetterPublishingRecoverer` unwraps Spring Kafka's internal wrapper exceptions (`TimestampedException` and `ListenerExecutionFailedException`) when populating `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`, and `kafka_dlt-exception-stacktrace` so dead-letter headers report the actual exception thrown by the listener or parser.

