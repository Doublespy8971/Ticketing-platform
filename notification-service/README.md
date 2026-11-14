# Notification Service

`notification-service` is a stateless Go service (HTTP port `8084`) responsible for customer notification delivery in the Ticketing Platform. It consumes order domain events from Apache Kafka and formats and dispatches asynchronous notifications (e.g., email confirmation and cancellation receipts).

---

## 1. Architecture & Responsibilities

- **Stateless Design**: The service has no relational database, schema migrations, or transactional outbox.
- **Kafka Consumer**:
  - Subscribes to topic `order.events`.
  - Consumer group: `notification-service-order-cg`.
  - Isolation level: `read_committed` (`kafka.ReadCommitted`), guaranteeing only committed transactional events are processed.
  - Manual offset commit after processing.
  - **No Retry / Dead-Letter Topics**: In accordance with `docs/arch/03-kafka.md`, the notification consumer does not produce to `-retry-0`, `-retry-1`, or `-dlt` topics.
- **Deduplication**: Uses Redis 7 with atomic `SET key value NX EX` operations to prevent duplicate notifications during Kafka at-least-once deliveries or rebalances.

---

## 2. Event Processing Lifecycle

```
[Kafka Message on order.events]
              │
              ▼
   [Envelope Parsing & Validation]
        ├── Malformed/Invalid? ──► Log WARN (topic/partition/offset, no payload) ──► outcome="invalid" ──► Commit Offset
        └── Unknown event_type? ──────────────────────────────────────────────────► outcome="ignored" ──► Commit Offset
              │
              ▼ (order.confirmed or order.cancelled)
    [Redis Deduplication Claim]
        ├── Claim Failed (Key exists) ───────────────────────────────────────────► outcome="duplicate" ──► Commit Offset
        ├── Redis Error/Unavailable ─────────────────────────────────────────────► Retry with backoff; halt consumer if persistent (No Commit)
        └── Claim Succeeded (Key created)
              │
              ▼
    [Notification Dispatch (up to 3 attempts)]
        ├── Attempt 1 (0ms)
        ├── Attempt 2 (backoff 200ms)
        └── Attempt 3 (backoff 400ms / 800ms)
              │
              ├── Delivery Success ──────────────────────────────────────────────► outcome="sent" ──► Commit Offset
              └── All 3 Attempts Failed
                    │
                    ▼
          [At-Most-Once Exhaustion Policy]
          1. Delete Redis dedup key (enables manual replay/re-delivery later)
          2. Log ERROR (eventId, eventType, error - NO tokens or PII)
          3. Metric: outcome="failed"
          4. Commit Kafka Offset (at-most-once after exhaustion)
```

---

## 3. Deduplication Strategy

- **Key Format**: `notif:dedup:{consumerGroup}:{eventId}`
  - Example: `notif:dedup:notification-service-order-cg:550e8400-e29b-41d4-a716-446655440000`
  - Includes `consumerGroup` to prevent key collisions across distinct consumer groups.
- **TTL**: 7 days (`604800` seconds), configurable via `NOTIFICATION_DEDUP_TTL_SECONDS`.
- **Atomic Claim**: `SET key 1 NX EX {ttl}`
- **Redis Outage Safety**: If Redis is unreachable during the dedup claim, the consumer does **not** commit the offset and does **not** dispatch the notification. It retries with backoff up to `max.poll.interval.ms`. If Redis remains down, the consumer halts so no messages are dropped or acknowledged prematurely.

---

## 4. At-Most-Once After Exhaustion Policy

Because customer notifications (emails, SMS) are informational and non-transactional, notification delivery must never block consumer progress on `order.events` indefinitely when an external notification gateway suffers an outage.
- When all 3 delivery attempts are exhausted:
  1. The Redis deduplication key is deleted via `DEL key`, so that if the event is re-injected or re-delivered manually, it will not be rejected as a duplicate.
  2. The failure is recorded in Prometheus metrics (`notification_events_total{outcome="failed"}`).
  3. A structured error log is emitted with `eventId` and `eventType` (excluding all customer PII and credentials).
  4. The Kafka offset is committed, allowing the consumer group to advance.

---

## 5. Metrics & Observability

Exposed at `GET /metrics` in Prometheus exposition format:

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `notification_events_total` | Counter | `outcome` (`sent`, `duplicate`, `ignored`, `invalid`, `failed`) | Kafka events processed by outcome. |
| `notification_send_duration_seconds` | Histogram | (none) | Duration of external notification send attempts. |
| `notification_dedup_redis_errors_total` | Counter | (none) | Total Redis errors encountered during dedup operations. |
| `ticketing_notifications_sent_total` | Counter | `service`, `event_type`, `status` (`SENT`, `DEDUPLICATED`, `FAILED`) | Cross-service notification count partitioned by event type and status. |

### Health Endpoints

- **Liveness**: `GET /health` and `GET /health/liveness`
  - Returns HTTP 200 `{"status":"UP"}`.
- **Readiness**: `GET /ready` and `GET /health/readiness`
  - Performs live connectivity probes against Redis (`PING`) and Kafka bootstrap brokers.
  - Returns HTTP 200 `{"status":"UP"}` when both dependencies respond.
  - Returns HTTP 503 `{"status":"DOWN"}` without leaking internal addresses or credentials.

---

## 6. Configuration

All configuration is provided via environment variables with defaults for local development:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `SERVER_PORT` | `8084` | HTTP listening port for health and metrics endpoints. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker bootstrap list (comma-separated). |
| `KAFKA_TOPIC` | `order.events` | Source topic for order lifecycle events. |
| `KAFKA_CONSUMER_GROUP` | `notification-service-order-cg` | Kafka consumer group name. |
| `REDIS_HOST` | `localhost` | Redis server hostname. |
| `REDIS_PORT` | `6379` | Redis server port. |
| `REDIS_PASSWORD` | *(empty)* | Optional Redis password. |
| `NOTIFICATION_DEDUP_TTL_SECONDS` | `604800` | Redis key TTL (7 days) for event deduplication. |
| `NOTIFICATION_SENDER_TYPE` | `log` | Notification sender implementation (`log`, `failing`, `slow`). |
| `LOG_LEVEL` | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`). |

---

## 7. Assumptions & Design Decisions

Where architecture specifications were silent, the following engineering choices were made:
1. **Deduplication Key Namespacing**: The Redis key format includes the consumer group (`notif:dedup:{consumerGroup}:{eventId}`) rather than only `notif:dedup:{eventId}` to avoid collisions if multiple services share the Redis cluster.
2. **Payload Size Guard**: Envelope payload parsing enforces a strict 1MB JSON limit to prevent denial-of-service via memory exhaustion from rogue messages.
3. **Structured Log Masking**: `LogSender` outputs structured JSON with customer email and message summary while strictly omitting tokens, payment keys, or unnecessary PII.
4. **Graceful Shutdown**: Listens for `SIGINT` and `SIGTERM`, stopping the consumer loop and providing up to 10 seconds to finish in-flight delivery attempts before closing network listeners.

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

### Docker Build
```bash
docker build -t ticketing/notification-service:latest -f notification-service/Dockerfile notification-service
```

### Live Smoke Test
With the supporting infrastructure running (`make infra-up`):
```bash
make smoke-notification
```
