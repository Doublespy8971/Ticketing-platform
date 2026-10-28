# `ticketing-outbox` — Transactional Outbox Module

`ticketing-outbox` implements the PostgreSQL-backed transactional outbox (`outbox_events`) and single-leader sequential polling publisher defined in [docs/arch/05-outbox-idempotency.md](../docs/arch/05-outbox-idempotency.md) and [docs/arch/07-database.md](../docs/arch/07-database.md).

## Why Ordering Is Preserved

1. **Single Active Poller per Service Database (`pg_try_advisory_lock`) on an Unpooled Connection**:
   Each poller instance acquires a PostgreSQL session-level advisory lock (`SELECT pg_try_advisory_lock(:fixedLockId)`) on a dedicated, unpooled JDBC connection (`DriverManager`) that is never borrowed from or returned to the application's `HikariDataSource` pool. All poller database operations—selecting the unpublished batch, running a `SELECT 1` liveness check immediately before every Kafka send, and executing `UPDATE outbox_events SET published_at = NOW() WHERE event_id = ? AND published_at IS NULL`—run in autocommit mode directly on that dedicated leader connection. On shutdown or leadership release, `SELECT pg_advisory_unlock(:fixedLockId)` is executed before closing (or aborting on error) the connection.
2. **Database-Clock Ordering (`clock_timestamp()`) + Sequential Send-and-Ack + Stop-on-First-Failure**:
   `OutboxWriter` inserts rows inside the caller's ACID transaction (`Propagation.MANDATORY`) using PostgreSQL's `clock_timestamp()` (`RETURNING created_at`) rather than the application pod's wall clock, eliminating cross-pod NTP clock-skew inversions and truncating `occurredAt` to microsecond precision to match `TIMESTAMPTZ`. The active leader selects `WHERE published_at IS NULL ORDER BY created_at ASC, event_id ASC LIMIT ?`, verifies the leader connection with `SELECT 1` before each send, sends one `ProducerRecord` at a time keyed by `aggregate_id` (`acks=all`, `enable.idempotence=true`, `delivery.timeout.ms=4000`, `request.timeout.ms=1500`, `linger.ms=5`), awaits the future for up to `5000 ms` (so the producer future always completes before the poller stops waiting and never abandons an in-flight record), verifies that `markPublished` updates exactly `1` row on the leader connection, and **stops the batch immediately at the first failure**.

## Honest Failure Windows & Why Consumers Must Be Idempotent

| Failure Window | What Happens | Remaining Risk & System Guarantee |
| :--- | :--- | :--- |
| **1. Caller transaction fails or rolls back after `OutboxWriter.append(...)`** | Because `OutboxWriter.append` runs with `Propagation.MANDATORY` inside the caller's local PostgreSQL transaction, the `outbox_events` insert rolls back atomically with the domain mutation. | **Zero phantom events**: uncommitted state changes never reach Kafka. |
| **2. Kafka broker send fails or times out on row $k$ of a batch** | Because `delivery.timeout.ms` (`4000 ms`) is shorter than the poller's `sendTimeout` (`5000 ms`), the `KafkaProducer` finishes failing the batch before `.get(...)` returns (never leaving an abandoned in-flight retry in `RecordAccumulator`). `OutboxPoller` increments `outbox_publish_failures_total` and **breaks out of the batch loop immediately**. Rows $k \dots N$ remain `published_at IS NULL`. | **Zero skipped or reordered rows**: the next poll cycle re-selects row $k$ first and resumes in exact `created_at ASC, event_id ASC` order. |
| **3. Crash or DB failure *after* Kafka broker ack for row $k$, *before* `markPublished` commits** | Row $k$ was durably written to Kafka (`acks=all`), but `published_at` remains `NULL` in PostgreSQL. On the next poll cycle (or after leader failover), row $k$ is selected again and **republished to Kafka as a duplicate record** with the same `eventId` (and a new Kafka producer sequence number). | **Duplicates on republish (at-least-once delivery, zero lost rows)**: Kafka's idempotent producer only deduplicates network retries of the *same* `send()` call, not a subsequent `send()` call on the next poll cycle. **All downstream consumers must be idempotent by `eventId`.** |
| **4. Small window between losing the advisory lock and noticing it** | Before every single Kafka send, `OutboxPoller` executes `SELECT 1` on `leaderConnection`. If the database backend was terminated *before* `SELECT 1`, the stale leader notices immediately, drops leadership, and sends nothing. However, PostgreSQL advisory locks do not provide a fencing token to Kafka: if `leaderConnection` drops *after* `SELECT 1` succeeds for row $k$ (for example, during a JVM GC pause or slow network hop while `kafkaTemplate.send(record).get(...)` is already in flight), PostgreSQL releases the session lock and a standby poller can acquire the lock and also read/send row $k$ before the first poller's `markPublished` fails on its dead `leaderConnection` (`updated != 1` / `SQLException`) and halts its batch. | **Bounded 1-row overlap / duplicate delivery**: the stale leader is guaranteed to halt as soon as `markPublished` (or the next row's `SELECT 1`) executes on the dead `leaderConnection`, so it cannot advance to row $k+1$, but row $k$ itself can be sent by both pollers. |
| **5. Consumer-side idempotency requirement** | Because both Window 3 (crash before `markPublished`) and Window 4 (lock loss after pre-send `SELECT 1`) can produce duplicate Kafka messages with the same `eventId`, exactly-once processing requires idempotent consumers. | Stateful Java services deduplicate inside their local PostgreSQL transaction via `INSERT INTO processed_events (event_id, consumer_group, event_type, processed_at) VALUES (?, ?, ?, NOW()) ON CONFLICT DO NOTHING`; `notification-service` deduplicates in Redis via `SET notif:dedup:{eventId} "1" NX EX 604800`. |

## Usage

1. Copy `src/main/resources/db/outbox/outbox_events.sql` into the service's Flyway migrations.
2. Enable the poller in `application.yml`:
   ```yaml
   ticketing:
     outbox:
       enabled: true
       advisory-lock-id: 71001 # 71001 inventory, 71002 order, 71003 payment
       poll-interval-ms: ${OUTBOX_POLL_INTERVAL_MS:200}
       batch-size: ${OUTBOX_BATCH_SIZE:100}
   ```
