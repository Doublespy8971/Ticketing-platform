# Notification Service

`notification-service` is a stateless Go service (HTTP port `8084`) responsible for customer notification delivery in the Ticketing Platform. It consumes order domain events from Apache Kafka and formats and dispatches asynchronous notifications (e.g., email confirmation and cancellation receipts).

---

## 1. Architecture & Responsibilities

- **Stateless Design**: The service has no relational database, schema migrations, or transactional outbox.
- **Kafka Consumer**:
  - Subscribes to topic `order.events`.
  - Consumer group: `notification-service-order-cg`.
  - Isolation level: `read_committed` (`kafka.ReadCommitted`), guaranteeing only committed transactional events are processed.
  - Manual offset commit per message with up to 5 retries.
  - **No Retry / Dead-Letter Topics**: In accordance with `docs/arch/03-kafka.md`, the notification consumer does not produce to `-retry-0`, `-retry-1`, or `-dlt` topics.
- **Two-Phase Deduplication with Ownership**:
  - Every consumer process generates a unique random `workerId` (UUIDv4) at startup.
  - **Phase 1 (Claim)**: `SET notif:dedup:{consumerGroup}:{eventId} <workerId>:IN_PROGRESS NX EX 60`.
  - **Phase 2 (Promotion)**: Upon successful send, promoted to `"SENT"` with full 7-day TTL (`604800s`) via compare-and-set Lua script verifying token ownership.
  - **Release on Failure / Exit**: If send fails all retries, or on cancellation, panic, or commit error, the claim is released via compare-and-delete Lua script on a fresh 2-second context.

---

## 2. Event Processing Lifecycle

```
[Kafka Message on order.events]
              │
              ▼
   [Envelope Parsing & Validation]
        ├── Malformed/Invalid? ──► Log WARN (topic/partition/offset, no payload) ──► outcome="invalid" ──► Commit Offset
        └── Unhandled event_type? ───────────────────────────────────────────────► outcome="ignored" ──► Commit Offset
              │
              ▼ (order.confirmed or order.cancelled)
    [Two-Phase Redis Deduplication Claim]
        ├── Claim Succeeds (acquired <workerId>:IN_PROGRESS) ───────────────────┐
        └── Claim Fails (Key already exists)                                    │
              ├── Value is "SENT"? ──────────────────────────────────────────┐  │
              └── Value is "<otherWorker>:IN_PROGRESS"?                      │  │
                    │                                                        │  │
                    ▼                                                        │  │
             [Poll every 500ms up to claimTTL + 5s]                          │  │
                    ├── Becomes "SENT"? ─────────────────────────────────────┤  │
                    ├── Key disappears (expired/released)? ──► Re-claim ─────┼──┘
                    └── Timeout exceeded? ──► Log warning & retry claim      │
                                                                             │
        ┌────────────────────────────────────────────────────────────────────┘
        ▼
   [Duplicate Event] ──► outcome="duplicate" ──► Commit Offset
        │
        ▼ (when claim is owned by this worker)
   [Notification Dispatch (up to 3 attempts with exponential backoff)]
        ├── Success ──► CAS Promotion to "SENT" (7-day TTL) ──► outcome="sent" ──► Commit Offset
        ├── Exit / Interrupted / Cancelled / Panic ──► CAD Release (fresh 2s context)
        └── All 3 Attempts Exhausted
              │
              ▼
   [At-Most-Once Exhaustion Policy]
   1. CAD Release claim from Redis (allows optional manual replay later)
   2. Log ERROR (eventId, eventType, orderId, attempts - NO tokens or PII)
   3. Metric: outcome="failed", reason="failure"
   4. Commit Kafka offset with up to 5 retries
```

---

## 3. Two-Phase Deduplication Strategy & Ownership Scripts

### 3.1 Key Format & Values
- **Key Format**: `notif:dedup:{consumerGroup}:{eventId}`
  - Example: `notif:dedup:notification-service-order-cg:550e8400-e29b-41d4-a716-446655440000`
- **Values & TTLs**:
  - `IN_PROGRESS`: `<workerId>:IN_PROGRESS` (TTL `60s`, configurable via `NOTIFICATION_CLAIM_TTL_SECONDS`).
  - `SENT`: `"SENT"` (TTL 7 days / `604800s`, configurable via `NOTIFICATION_DEDUP_TTL_SECONDS`).

### 3.2 Atomic Lua Scripts
1. **CAS Promotion (`promote_notification.lua`)**:
   Promotes `<workerId>:IN_PROGRESS` to `"SENT"` with 7-day TTL only if the current value matches this worker's token:
   ```lua
   local val = redis.call('GET', KEYS[1])
   if val == ARGV[1] then
       redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
       return 1
   else
       return 0
   end
   ```
2. **CAD Release (`release_notification.lua`)**:
   Deletes `<workerId>:IN_PROGRESS` only if the current value matches this worker's token, preventing accidental deletion of another worker's in-progress claim:
   ```lua
   local val = redis.call('GET', KEYS[1])
   if val == ARGV[1] then
       return redis.call('DEL', KEYS[1])
   else
       return 0
   end
   ```

---

## 4. Delivery Guarantees & Residual Risks

- **At-Least-Once Delivery Before Exhaustion**: If a message delivery fails before reaching the maximum retry count, or if processing is cancelled or interrupted by shutdown, the in-progress claim is immediately deleted via compare-and-delete on an uncancelled context (2s timeout), guaranteeing that subsequent redeliveries will re-claim and re-try delivery.
- **At-Most-Once Delivery After Exhaustion**: When all 3 delivery attempts are exhausted without success, the in-progress claim is deleted (permitting manual replay), an `ERROR` log is emitted, and the offset is committed to prevent blocking the consumer partition.
- **Residual Crash Risk**: If the consumer process suffers an abrupt, uncatchable crash (`SIGKILL`, host power loss, OOM) after a notification has been successfully transmitted over the network but before the CAS promotion to `"SENT"` completes, the Kafka offset remains uncommitted. When the partition is assigned to another worker, the message will be redelivered. Once the 60-second `IN_PROGRESS` TTL expires, the new worker will re-claim the key and transmit a duplicate notification (at most once).

---

## 5. Fault Tolerance & Operational Hardening

1. **Panic Recovery**: Every message processing cycle is wrapped in a `recover()` block. If a panic occurs, topic, partition, offset, and panic type are logged (never the payload), `outcome="invalid"` is recorded, any held claim is released on a fresh 2-second context with `reason="panic"`, the offset is committed to bypass the poison pill, and the process continues running.
2. **Transient Kafka Errors**:
   - `FetchMessage` errors other than context cancellation do not terminate the loop: the consumer logs a `WARN`, backs off exponentially (500ms to 10s), and resumes fetching.
   - `CommitMessages` retries up to 5 times with exponential backoff (500ms to 10s). If all retries fail, it is safe because the `"SENT"` Redis key prevents duplicate dispatches on redelivery.
3. **Graceful Shutdown**:
   - On `SIGTERM` or `SIGINT`:
     1. Immediately marks `/ready` as not ready (HTTP 503 `DOWN`).
     2. Stops fetching new Kafka messages (`StopFetching()`).
     3. Allows in-flight message processing and offset commit up to the 10-second drain period.
     4. Closes Kafka reader, Redis client, and HTTP server, in that exact order.
     5. If the 10-second drain period is exceeded, releases the in-flight claim on a fresh context (`reason="cancelled"`) and exits non-zero (`os.Exit(1)`).

---

## 6. Metrics & Observability

Exposed at `GET /metrics` in Prometheus exposition format:

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `notification_events_total` | Counter | `service`, `event_type`, `outcome` (`sent`, `duplicate`, `ignored`, `invalid`, `failed`) | Events processed partitioned by canonical event type and outcome. |
| `notification_send_duration_seconds` | Histogram | `service` | Duration of external notification send attempts. |
| `notification_dedup_redis_errors_total` | Counter | `service` | Total Redis errors encountered during deduplication claims or releases. |
| `notification_claim_wait_total` | Counter | `service` | Total times a worker waited on an in-progress deduplication claim held by another worker. |
| `notification_claim_release_total` | Counter | `service`, `reason` (`failure`, `cancelled`, `panic`, `commit_error`) | Total deduplication claims released before promotion partitioned by reason. |
| `ticketing_notifications_sent_total` | Counter | `service`, `event_type`, `status` (`SENT`, `DEDUPLICATED`, `FAILED`) | Cross-service notification count partitioned by event type and status. |

### Health Endpoints

- **Liveness**: `GET /health` and `GET /health/liveness`
  - Returns HTTP 200 `{"status":"UP"}`.
- **Readiness**: `GET /ready` and `GET /health/readiness`
  - Probes live Kafka and Redis connectivity.
  - Returns HTTP 200 `{"status":"UP"}` when both respond and not shutting down.
  - Returns HTTP 503 `{"status":"DOWN"}` during shutdown or when either dependency fails.

---

## 7. Configuration Reference

All configuration is provided via environment variables with defaults for local development:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `SERVER_PORT` | `8084` | HTTP listening port for health and metrics endpoints. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker bootstrap list (comma-separated). |
| `KAFKA_TOPIC` | `order.events` | Source topic for order domain events. |
| `KAFKA_CONSUMER_GROUP` | `notification-service-order-cg` | Kafka consumer group name. |
| `REDIS_HOST` | `localhost` | Redis server hostname. |
| `REDIS_PORT` | `6379` | Redis server port. |
| `REDIS_PASSWORD` | *(empty)* | Optional Redis password (never logged). |
| `REDIS_DB` | `0` | Redis logical database index (integer >= 0). |
| `REDIS_TLS_ENABLED` | `false` | Enable TLS encryption on Redis connections. |
| `NOTIFICATION_CLAIM_TTL_SECONDS` | `60` | In-progress deduplication claim TTL in seconds (must exceed worst-case send duration). |
| `NOTIFICATION_DEDUP_TTL_SECONDS` | `604800` | Full retention TTL (7 days) for `"SENT"` deduplication keys. |
| `NOTIFICATION_SENDER_MAX_ATTEMPTS` | `3` | Maximum notification dispatch attempts per event. |
| `NOTIFICATION_SENDER_INITIAL_BACKOFF_MS` | `200` | Initial backoff delay between dispatch attempts in ms. |
| `NOTIFICATION_SENDER_MAX_BACKOFF_MS` | `800` | Maximum backoff delay between dispatch attempts in ms. |
| `NOTIFICATION_SENDER_TIMEOUT_MS` | `5000` | Timeout per send attempt in ms. |
| `REDIS_MAX_POLL_INTERVAL_MS` | `300000` | Maximum polling duration when Redis is unavailable (5 minutes). |

---

## 8. Build & Verification

### Running Locally
```bash
make run-notification
```

### Running Tests
```bash
cd notification-service
go test -v ./...
```

### Live Smoke Test
With the supporting infrastructure running (`make infra-up`):
```bash
make smoke-notification
```
