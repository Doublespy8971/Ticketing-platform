## 5. Transactional Outbox

Every state-mutating service (`inventory-service`, `order-service`, `payment-service`) persists domain events into a local `outbox_events` table within the **exact same ACID PostgreSQL transaction** that mutates its domain tables. No service ever calls `KafkaTemplate.send(...)` inside an HTTP request thread or Kafka listener transaction. Instead, a single elected polling publisher instance per service (`pg_try_advisory_lock` on a fixed lock ID held on a dedicated connection) drains unpublished rows every `200 ms` sequentially.

### 5.1 Outbox Table Schema (`inventory_db`, `order_db`, `payment_db`)

```sql
CREATE TABLE outbox_events (
    event_id        UUID            PRIMARY KEY,
    aggregate_type  VARCHAR(64)     NOT NULL,
    aggregate_id    UUID            NOT NULL,
    topic           VARCHAR(128)    NOT NULL,
    event_type      VARCHAR(128)    NOT NULL,
    correlation_id  UUID            NOT NULL,
    schema_version  INTEGER         NOT NULL DEFAULT 1,
    payload         JSONB           NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ     NULL,
    CONSTRAINT chk_outbox_schema_version CHECK (schema_version >= 1)
);

-- Partial index scanned by the 200ms polling publisher (only indexes unpublished rows)
CREATE INDEX idx_outbox_events_unpublished
    ON outbox_events (created_at ASC, event_id ASC)
    WHERE published_at IS NULL;

-- Partial index used by the hourly retention cleanup job
CREATE INDEX idx_outbox_events_published_retention
    ON outbox_events (published_at ASC)
    WHERE published_at IS NOT NULL;
```

---

### 5.2 Polling Publisher Execution Protocol (`200 ms` Interval)

Only **one poller instance is active per service**, elected using a session-level PostgreSQL advisory lock (`SELECT pg_try_advisory_lock(:fixedLockId)`) acquired and held on a dedicated database connection (`71001` for `inventory-service`, `71002` for `order-service`, `71003` for `payment-service`). Non-leader replicas check `pg_try_advisory_lock(:fixedLockId)` periodically and take over automatically if the active leader's dedicated connection drops. The elected leader runs a Spring `@Scheduled(fixedDelayString = "${OUTBOX_POLL_INTERVAL_MS:200}")` loop:

1. **Select Unpublished Batch**:
   ```sql
   SELECT event_id,
          aggregate_type,
          aggregate_id,
          topic,
          event_type,
          correlation_id,
          schema_version,
          payload,
          created_at
   FROM outbox_events
   WHERE published_at IS NULL
   ORDER BY created_at ASC, event_id ASC
   LIMIT 100;
   ```
2. **Send Sequentially, Await Each Broker Ack, and Mark Published Immediately**:
   - The active poller iterates through the selected rows **sequentially** in `created_at ASC, event_id ASC` order.
   - For each row, the poller serializes the Common Event Envelope JSON (`eventId = event_id`, `eventType = event_type`, `correlationId = correlation_id`, `occurredAt = created_at`, `schemaVersion = schema_version`, `payload = payload`) and sends a `ProducerRecord<String, String>` to `topic` with **key = `aggregate_id.toString()`** (which is always `reservationId` / `orderId`) and headers `X-Correlation-Id`, `event-type`, `event-id`, and `schema-version`.
   - The poller waits synchronously for the Kafka broker acknowledgment (`send(...).get(5, TimeUnit.SECONDS)`) for that single row before proceeding to the next row.
   - Right after receiving the broker ack for that row, the poller immediately marks that row as published in PostgreSQL:
     ```sql
     UPDATE outbox_events
     SET published_at = NOW()
     WHERE event_id = :eventId;
     ```
   - **Stop on First Failure**: If any Kafka send fails or times out, the poller logs the error and **immediately stops processing the current batch** at the first failed send without attempting subsequent rows, preserving strict ordering for retry on the next `200 ms` poll cycle.

---

### 5.3 Ordering Guarantees & Retention Cleanup

- **Strict Causal Ordering per `reservationId` / `orderId` (Two-Sentence Proof)**:
  Because `pg_try_advisory_lock` on a fixed lock ID held on a dedicated connection guarantees that only a single poller instance is active per service at any time, there is zero concurrent batch overlapping or out-of-order row skipping across replicas. That single active poller selects unpublished rows in `ORDER BY created_at ASC, event_id ASC` order, sends each record sequentially while waiting for its broker ack before sending the next, and stops the batch immediately at the first failed send, guaranteeing that events for every `aggregate_id` arrive at their Kafka partition in exact creation order.
- **Retention Cleanup**:
  - A scheduled cleanup job (`@Scheduled(cron = "0 0 * * * *")`) runs hourly in each stateful service to purge published outbox records older than 72 hours without long table locks:
    ```sql
    DELETE FROM outbox_events
    WHERE event_id IN (
        SELECT event_id
        FROM outbox_events
        WHERE published_at IS NOT NULL
          AND published_at < NOW() - INTERVAL '72 hours'
        ORDER BY published_at ASC
        LIMIT 5000
        FOR UPDATE SKIP LOCKED
    );
    ```

---

## 6. Idempotency

### 6.1 HTTP Request Idempotency (`Idempotency-Key` Header)

All client-initiated state-mutating customer endpoints (`POST /api/v1/reservations` on `inventory-service` and `POST /api/v1/orders/{orderId}/checkout` on `order-service`) require an `Idempotency-Key` HTTP header containing a valid UUID (`36` characters). Requests missing a valid UUID `Idempotency-Key` header are rejected immediately with `400 Bad Request` (`MISSING_IDEMPOTENCY_KEY`).

#### `idempotency_keys` Table Schema (`inventory_db` and `order_db`)

```sql
CREATE TABLE idempotency_keys (
    user_id         UUID            NOT NULL,
    idempotency_key UUID            NOT NULL,
    endpoint        VARCHAR(128)    NOT NULL,
    request_hash    CHAR(64)        NOT NULL,
    status          VARCHAR(32)     NOT NULL,
    response_status INTEGER         NULL,
    response_body   JSONB           NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ     NULL,
    expires_at      TIMESTAMPTZ     NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
    PRIMARY KEY (user_id, idempotency_key),
    CONSTRAINT chk_idempotency_status CHECK (status IN ('IN_PROGRESS', 'COMPLETED'))
);

CREATE INDEX idx_idempotency_keys_expires_at
    ON idempotency_keys (expires_at ASC);
```

#### HTTP Idempotency Execution Rules
1. **Compute Request Fingerprint**: Compute `request_hash` as the lowercase hex-encoded SHA-256 digest of `HTTP_METHOD + ":" + REQUEST_PATH + ":" + CANONICAL_JSON_BODY`.
2. **Claim Key**:
   ```sql
   INSERT INTO idempotency_keys (user_id, idempotency_key, endpoint, request_hash, status)
   VALUES (:userId, :idempotencyKey, :endpoint, :requestHash, 'IN_PROGRESS')
   ON CONFLICT (user_id, idempotency_key) DO NOTHING;
   ```
3. **Handle Conflict (`0` rows inserted)**:
   - Query the existing row for `(user_id, idempotency_key)`:
   - If `request_hash != :requestHash` or `endpoint != :endpoint`, return `422 Unprocessable Entity` (`IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`).
   - If `status = 'IN_PROGRESS'` (and `created_at > NOW() - INTERVAL '30 seconds'`), return `409 Conflict` (`IDEMPOTENCY_REQUEST_IN_PROGRESS`) with header `Retry-After: 1`.
   - If `status = 'COMPLETED'`, return the stored `response_status` and `response_body` with header `X-Idempotent-Replay: true`.
4. **Commit Outcome (`1` row inserted)**:
   - For deterministic business outcomes (`201 Created`, `202 Accepted`, `409 SEAT_ALREADY_TAKEN`, `409 RESERVATION_EXPIRED`, `409 EVENT_NOT_OPEN`), update the `idempotency_keys` row inside the same PostgreSQL transaction:
     ```sql
     UPDATE idempotency_keys
     SET status = 'COMPLETED',
         response_status = :httpStatus,
         response_body = :responseJson::jsonb,
         completed_at = NOW()
     WHERE user_id = :userId AND idempotency_key = :idempotencyKey;
     ```
   - **Special Rule for Transient `409 ORDER_NOT_READY` and `5xx` Errors**: When `POST /api/v1/orders/{orderId}/checkout` returns `409 ORDER_NOT_READY` (because `inventory.seat.reserved` has not yet been consumed) or encounters an unhandled `5xx` database/infrastructure failure, the transaction rolls back (deleting the `IN_PROGRESS` row) so the client can retry after `Retry-After: 1` using the **same** `Idempotency-Key`.

---

### 6.2 Kafka Consumer Idempotency (`processed_events` Table & Redis Deduplication)

#### Stateful Java Services (`inventory_db`, `order_db`, `payment_db`)
Every Kafka consumer in `inventory-service`, `order-service`, and `payment-service` deduplicates incoming events using the `processed_events` table inside the **same local PostgreSQL transaction** as the domain mutation and outbox insert:

```sql
CREATE TABLE processed_events (
    event_id        UUID            NOT NULL,
    consumer_group  VARCHAR(128)    NOT NULL,
    event_type      VARCHAR(128)    NOT NULL,
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, consumer_group)
);

CREATE INDEX idx_processed_events_processed_at
    ON processed_events (processed_at ASC);
```

- **Consumer Transaction Sequence**:
  1. Begin local PostgreSQL transaction (`@Transactional`).
  2. Execute:
     ```sql
     INSERT INTO processed_events (event_id, consumer_group, event_type, processed_at)
     VALUES (:eventId, :consumerGroup, :eventType, NOW())
     ON CONFLICT (event_id, consumer_group) DO NOTHING;
     ```
  3. If affected row count is `0`, commit transaction and acknowledge the Kafka offset immediately (idempotent no-op).
  4. If affected row count is `1`, execute the domain table mutation and `INSERT INTO outbox_events`, commit the PostgreSQL transaction, and acknowledge the Kafka offset.
  5. If any exception occurs during step 4, the PostgreSQL transaction rolls back (removing the `processed_events` insert) and the message is routed to `<topic>-<consumer-group>-retry-0`.

#### Stateless Go Service (`notification-service`)
Because `notification-service` has no PostgreSQL database, it enforces idempotency using Redis 7:
1. On receiving `order.confirmed` or `order.cancelled` with `eventId`, execute:
   ```text
   SET notif:dedup:{eventId} "1" NX EX 604800
   ```
2. If Redis returns `nil` (key already exists), log duplicate event at `DEBUG` level, increment `ticketing_notifications_sent_total{status="DEDUPLICATED"}`, and commit the Kafka offset.
3. If Redis returns `"OK"`, dispatch the notification. If notification dispatch fails, log the error at `ERROR` level, increment `ticketing_notifications_sent_total{status="FAILED"}`, and commit the Kafka offset without forwarding to any retry or dead-letter topic.

