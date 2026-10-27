## 10. Cross-Cutting Conventions

### 10.1 Monorepo Structure & Package Naming

```text
ticketing-platform/
├── pom.xml                                  # Root Maven aggregator (com.ticketing:ticketing-platform:1.0.0-SNAPSHOT)
├── ARCHITECTURE.md                          # Single source of truth architecture contract
├── DECISIONS.md                             # Architectural Decision Records
├── docker-compose.yml                       # Local stack: Kafka KRaft, PG16 (x3 DBs), Redis 7, 5 services
├── common-contracts/                        # Shared Java DTOs, EventEnvelope, RFC 7807 ProblemDetail models
│   └── src/main/java/com/ticketing/common/
├── gateway-service/                         # Spring Cloud Gateway (:8080)
│   └── src/main/java/com/ticketing/gateway/
├── inventory-service/                       # Seat inventory & hold reservation service (:8081)
│   └── src/main/java/com/ticketing/inventory/
├── order-service/                           # Order lifecycle & checkout saga service (:8082)
│   └── src/main/java/com/ticketing/order/
├── payment-service/                         # Mock payment processor & refund service (:8083)
│   └── src/main/java/com/ticketing/payment/
└── notification-service/                    # Stateless Go notification consumer (:8084)
    ├── go.mod                               # module github.com/ticketing-platform/notification-service
    ├── cmd/server/main.go
    └── internal/
        ├── config/
        ├── consumer/
        ├── dedup/
        ├── notifier/
        └── telemetry/
```

#### Java Sub-Package Layout Convention (`inventory-service`, `order-service`, `payment-service`)
Every Spring Boot service under `com.ticketing.<service>` follows this exact package hierarchy:
- `com.ticketing.<service>.api` — REST controllers, request/response records, `@RestControllerAdvice` RFC 7807 exception handler.
- `com.ticketing.<service>.domain` — Entities, state enums, domain exceptions, repository interfaces.
- `com.ticketing.<service>.service` — Transactional command/query services, expiry handlers.
- `com.ticketing.<service>.messaging` — Kafka `@KafkaListener` consumers, `@RetryableTopic` configuration.
- `com.ticketing.<service>.outbox` — `OutboxEvent` entity, `OutboxPoller` (`200 ms` `FOR UPDATE SKIP LOCKED`), retention cleaner.
- `com.ticketing.<service>.idempotency` — `IdempotencyKeyFilter`, `ProcessedEventRepository`.
- `com.ticketing.<service>.config` — Redis Lua `DefaultRedisScript` beans, Kafka producer/consumer factories, Jackson `ObjectMapper`, Micrometer meters.

---

### 10.2 Correlation-ID & Identity Propagation

1. **Ingress (`gateway-service`)**:
   - A reactive `GlobalFilter` (`Ordered.HIGHEST_PRECEDENCE`) inspects incoming HTTP requests for header `X-Correlation-Id`.
   - If missing or not a valid UUID, `gateway-service` generates a new UUIDv4 string.
   - `gateway-service` attaches `X-Correlation-Id` to the downstream proxied request and to the outgoing HTTP response.
2. **Synchronous HTTP Processing (`inventory-service`, `order-service`)**:
   - A `OncePerRequestFilter` extracts `X-Correlation-Id` and `X-User-Id`, places them into SLF4J `MDC.put("correlationId", ...)` and `MDC.put("userId", ...)`, and echoes `X-Correlation-Id` on the HTTP response.
3. **Outbox Persistence & Asynchronous Kafka Propagation**:
   - Whenever a service writes a row to `outbox_events`, it copies `MDC.get("correlationId")` into `outbox_events.correlation_id`. For background jobs (`expireReservation`), a fresh UUIDv4 `correlationId` is generated per expired reservation.
   - The `200 ms` outbox poller populates `correlationId` inside the JSON event envelope and sets the Kafka record header `X-Correlation-Id`.
   - Every Kafka consumer (Java and Go) extracts `X-Correlation-Id` from the Kafka record header and binds it to SLF4J MDC (or Go `context.Context`) before logging or writing subsequent outbox events.

---

### 10.3 Structured JSON Logging Format

All five services emit single-line UTF-8 JSON logs to `stdout` (`logstash-logback-encoder` in Java; `log/slog` `JSONHandler` in Go). Every log line contains these exact top-level keys:

```json
{
  "timestamp": "2026-10-27T22:00:00.123456Z",
  "level": "INFO",
  "service": "inventory-service",
  "correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "spanId": "00f067aa0ba902b7",
  "thread": "http-nio-8081-exec-4",
  "logger": "com.ticketing.inventory.service.ReservationService",
  "message": "Seat hold created in Redis and PostgreSQL",
  "reservationId": "01926b54-1111-7000-8000-000000000001",
  "orderId": "01926b54-1111-7000-8000-000000000001",
  "eventId": "f0e1d2c3-b4a5-4968-8796-a5b4c3d2e1f0",
  "userId": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d"
}
```

---

### 10.4 Metric Naming Contract (Micrometer & Prometheus)

All custom metrics use the `ticketing_` prefix, lowercase `snake_case`, and base unit suffixes (`_seconds`, `_total`):

| Metric Name | Meter Type | Labels | Emitting Service | Description |
| :--- | :--- | :--- | :--- | :--- |
| `http_server_requests_seconds` | `Timer` (Histogram) | `service`, `method`, `uri`, `status` | All services | Standard HTTP request latency histogram with SLO buckets enabled. |
| `ticketing_reservations_created_total` | `Counter` | `service`, `event_id` | `inventory-service` | Total seat holds committed to Redis and `inventory_db`. |
| `ticketing_reservations_rejected_total` | `Counter` | `service`, `reason` (`EVENT_NOT_OPEN`, `SEAT_ALREADY_TAKEN`) | `inventory-service` | Total seat hold requests rejected by the Redis Lua gate or DB constraint. |
| `ticketing_reservations_expired_total` | `Counter` | `service`, `trigger` (`postgres_sweeper`) | `inventory-service` | Total held reservations transitioned to `EXPIRED` in `inventory_db` by the `1000 ms` PostgreSQL sweeper. |
| `ticketing_redis_lua_duration_seconds` | `Timer` | `service`, `script`, `result` | `gateway-service`, `inventory-service` | Execution duration of Redis Lua scripts. |
| `ticketing_outbox_unpublished_records` | `Gauge` | `service` | `inventory-service`, `order-service`, `payment-service` | Count of rows in `outbox_events` where `published_at IS NULL`. |
| `ticketing_outbox_publish_latency_seconds` | `Timer` | `service`, `topic` | `inventory-service`, `order-service`, `payment-service` | Elapsed time between `outbox_events.created_at` and `published_at`. |
| `ticketing_orders_checkout_total` | `Counter` | `service`, `outcome` (`ACCEPTED`, `ORDER_NOT_READY`, `RESERVATION_EXPIRED`, `INVALID_ORDER_STATE`) | `order-service` | Total checkout attempts partitioned by outcome. |
| `ticketing_orders_materialization_lag_seconds` | `Timer` | `service` | `order-service` | Elapsed time between `inventory.seat.reserved.occurredAt` and `orders` insert in `order_db`. |
| `ticketing_orders_terminal_total` | `Counter` | `service`, `final_status` | `order-service` | Total orders reaching `CONFIRMED`, `CANCELLED_PAYMENT_FAILED`, `CANCELLED_EXPIRED`, or `CANCELLED_REFUNDED`. |
| `ticketing_payments_processed_total` | `Counter` | `service`, `status` (`SUCCEEDED`, `FAILED`, `REFUNDED`), `failure_code` | `payment-service` | Total payment charges and compensating refunds processed. |
| `ticketing_kafka_consumer_dlt_total` | `Counter` | `service`, `topic`, `consumer_group` | All Kafka consumers | Total messages routed to `*-dlt` dead-letter topics (triggers immediate page alert when `> 0`). |
| `ticketing_notifications_sent_total` | `Counter` | `service`, `event_type`, `status` (`SENT`, `DEDUPLICATED`, `FAILED`) | `notification-service` | Total notifications dispatched or deduplicated via Redis. |

---

### 10.5 Health & Observability Endpoints

| Service | Liveness Endpoint | Readiness Endpoint (Checks DB / Redis / Kafka) | Prometheus Metrics Endpoint |
| :--- | :--- | :--- | :--- |
| `gateway-service` (`:8080`) | `GET /actuator/health/liveness` | `GET /actuator/health/readiness` (Redis 7) | `GET /actuator/prometheus` |
| `inventory-service` (`:8081`) | `GET /actuator/health/liveness` | `GET /actuator/health/readiness` (`inventory_db`, Redis 7, Kafka) | `GET /actuator/prometheus` |
| `order-service` (`:8082`) | `GET /actuator/health/liveness` | `GET /actuator/health/readiness` (`order_db`, Kafka) | `GET /actuator/prometheus` |
| `payment-service` (`:8083`) | `GET /actuator/health/liveness` | `GET /actuator/health/readiness` (`payment_db`, Kafka) | `GET /actuator/prometheus` |
| `notification-service` (`:8084`) | `GET /health/liveness` | `GET /health/readiness` (Redis 7, Kafka) | `GET /metrics` |

---

### 10.6 Configuration via Environment Variables

| Environment Variable | Consuming Services | Default Value (Local Dev) | Description |
| :--- | :--- | :--- | :--- |
| `SPRING_PROFILES_ACTIVE` | All Java services | `dev` | Active Spring profile (`dev`, `local`, or `prod`). Controls `/api/v1/auth/token` registration. |
| `SERVER_PORT` | All services | `8080`..`8084` | HTTP listening port per service. |
| `JWT_SECRET` | `gateway-service` | `dev-only-hs256-secret-key-minimum-32-bytes-long!` | 256-bit secret key used to sign and verify HS256 JWT tokens. |
| `REDIS_HOST` | `gateway-service`, `inventory-service`, `notification-service` | `localhost` | Redis 7 hostname. |
| `REDIS_PORT` | `gateway-service`, `inventory-service`, `notification-service` | `6379` | Redis 7 port. |
| `KAFKA_BOOTSTRAP_SERVERS` | `inventory-service`, `order-service`, `payment-service`, `notification-service` | `localhost:9092` | Apache Kafka KRaft bootstrap servers list. |
| `POSTGRES_URL` | `inventory-service`, `order-service`, `payment-service` | `jdbc:postgresql://localhost:5432/<service>_db` | JDBC connection URL for `inventory_db`, `order_db`, or `payment_db`. |
| `POSTGRES_USER` | `inventory-service`, `order-service`, `payment-service` | `ticketing` | PostgreSQL username. |
| `POSTGRES_PASSWORD` | `inventory-service`, `order-service`, `payment-service` | `ticketing_secret` | PostgreSQL password. |
| `OUTBOX_POLL_INTERVAL_MS` | `inventory-service`, `order-service`, `payment-service` | `200` | Fixed delay in milliseconds between single-leader (`pg_try_advisory_lock`) outbox polling runs. |
| `OUTBOX_BATCH_SIZE` | `inventory-service`, `order-service`, `payment-service` | `100` | Maximum unpublished outbox rows selected per poll cycle. |
| `POSTGRES_HOLD_TTL_SECONDS` | `inventory-service` | `600` | Authoritative PostgreSQL reservation hold duration (`expires_at`). |
| `REDIS_SEAT_HOLD_TTL_SECONDS` | `inventory-service` | `660` | Redis `seat:{eventId}:{seatId}` safety-net TTL during a hold (60s longer than `POSTGRES_HOLD_TTL_SECONDS`). |
| `HOLD_EXPIRY_SWEEP_INTERVAL_MS` | `inventory-service` | `1000` | Interval in milliseconds for the PostgreSQL expired-hold sweeper (`FOR UPDATE SKIP LOCKED`). |
| `CHECKOUT_EXPIRY_GRACE_SECONDS` | `order-service` | `15` | Minimum remaining seconds before `expires_at` required to accept a checkout request. |
| `MOCK_PAYMENT_FAILURE_RATE` | `payment-service` | `0.05` | Probability (`0.0` to `1.0`) of random charge failure when using a non-deterministic payment token. |
| `MOCK_PAYMENT_MIN_LATENCY_MS` | `payment-service` | `50` | Minimum simulated payment processing latency in milliseconds. |
| `MOCK_PAYMENT_MAX_LATENCY_MS` | `payment-service` | `300` | Maximum simulated payment processing latency in milliseconds. |
| `NOTIFICATION_DEDUP_TTL_SECONDS` | `notification-service` | `604800` | TTL in seconds (7 days) for `notif:dedup:{eventId}` keys in Redis. |

---

### 10.7 Pinned Dependency & Infrastructure Versions

> **Spring Boot BOM Dependency Management Note**: Spring Boot BOM (`org.springframework.boot:spring-boot-starter-parent:3.3.5`) managed versions are used for `spring-kafka`, `lettuce`, `micrometer`, and `testcontainers` (no explicit `<version>` tags in Maven POMs for BOM-managed libraries). Only dependencies not managed by the Spring Boot BOM (such as `spring-cloud-dependencies`, `jjwt`, `java-uuid-generator`, and `logstash-logback-encoder`) are pinned explicitly in `pom.xml`.

| Component / Library | Version / BOM Source | Scope / Artifact |
| :--- | :--- | :--- |
| **Java Development Kit (JDK)** | `21.0.5` (LTS) | Eclipse Temurin `21.0.5+11` |
| **Maven** | `3.9.9` | Build system via Maven Wrapper (`./mvnw`) |
| **Spring Boot (BOM)** | `3.3.5` (Explicit Parent) | `org.springframework.boot:spring-boot-starter-parent:3.3.5` |
| **Spring Cloud** | `2023.0.3` (Explicit BOM) | `org.springframework.cloud:spring-cloud-dependencies:2023.0.3` (Spring Cloud Gateway `4.1.5`) |
| **Spring Kafka** | Managed by Spring Boot BOM (`3.2.4`) | `org.springframework.kafka:spring-kafka` |
| **Apache Kafka (KRaft Mode)** | `3.8.0` (Infrastructure Image) | Docker image `apache/kafka:3.8.0` |
| **PostgreSQL** | `16.4` (Image) / BOM-managed JDBC | Docker image `postgres:16.4-alpine`, JDBC driver `org.postgresql:postgresql` (managed by Spring Boot BOM) |
| **Flyway** | Managed by Spring Boot BOM | `org.flywaydb:flyway-core`, `org.flywaydb:flyway-database-postgresql` |
| **Redis & Lettuce Client** | `7.4.1` (Image) / Managed by Spring Boot BOM (`6.3.2.RELEASE`) | Docker image `redis:7.4.1-alpine`, `io.lettuce:lettuce-core` |
| **JJWT (HS256 JWT)** | `0.12.6` (Explicitly Pinned) | `io.jsonwebtoken:jjwt-api:0.12.6`, `jjwt-impl:0.12.6`, `jjwt-jackson:0.12.6` |
| **UUIDv7 Generator** | `5.1.0` (Explicitly Pinned) | `com.fasterxml.uuid:java-uuid-generator:5.1.0` |
| **Logstash Logback Encoder** | `8.0` (Explicitly Pinned) | `net.logstash.logback:logstash-logback-encoder:8.0` |
| **Micrometer Prometheus** | Managed by Spring Boot BOM (`1.13.6`) | `io.micrometer:micrometer-registry-prometheus` |
| **Testcontainers** | Managed by Spring Boot BOM (`1.19.8` / BOM default) | `org.testcontainers:postgresql`, `org.testcontainers:kafka`, `org.testcontainers:toxiproxy`, `org.testcontainers:junit-jupiter` |
| **Go Toolchain** | `1.23.2` | Docker image `golang:1.23.2-alpine` |
| **Go Kafka Client** | `v0.4.47` | `github.com/segmentio/kafka-go v0.4.47` |
| **Go Redis Client** | `v9.7.0` | `github.com/redis/go-redis/v9 v9.7.0` |
| **Go Prometheus Client** | `v1.20.5` | `github.com/prometheus/client_golang v1.20.5` |

---

## 11. Initial SLOs (Service Level Objectives)

Deterministic client business rejections (`400`, `401`, `403`, `404`, `409 Conflict` for `SEAT_ALREADY_TAKEN` or `ORDER_NOT_READY`, `422`, and `429`) are expected flash-sale outcomes and are **not** counted as availability failures; only `5xx` responses and gateway timeouts count against availability error budgets.

### 11.1 SLO Summary Table (Rolling 28-Day Window)

| User Journey / Operation | SLI Type | Target Objective | Error Budget (28 Days) | Measuring Metric(s) |
| :--- | :--- | :--- | :--- | :--- |
| **Seat Hold Availability** (`POST /api/v1/reservations`) | Availability (Non-`5xx` ratio) | **`>= 99.95%`** | `20.16 minutes` | `http_server_requests_seconds_count{service="inventory-service",uri="/api/v1/reservations",method="POST"}` |
| **Seat Hold Latency — Accepted (`201 Created`)** | Latency (`p95` / `p99`) | **`p95 <= 45 ms`**<br>**`p99 <= 120 ms`** | `<= 5%` above `45 ms`; `<= 1%` above `120 ms` | `http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",status="201"}` |
| **Seat Hold Fast Reject (`409 SEAT_ALREADY_TAKEN`)** | Latency (`p95` / `p99`) | **`p95 <= 8 ms`**<br>**`p99 <= 20 ms`** | `<= 5%` above `8 ms`; `<= 1%` above `20 ms` | `http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",status="409"}` |
| **Order Checkout Availability** (`POST /api/v1/orders/{orderId}/checkout`) | Availability (Non-`5xx` ratio) | **`>= 99.95%`** | `20.16 minutes` | `http_server_requests_seconds_count{service="order-service",uri="/api/v1/orders/{orderId}/checkout",method="POST"}` |
| **Order Checkout Latency (`202 Accepted`)** | Latency (`p95` / `p99`) | **`p95 <= 50 ms`**<br>**`p99 <= 150 ms`** | `<= 5%` above `50 ms`; `<= 1%` above `150 ms` | `http_server_requests_seconds_bucket{service="order-service",uri="/api/v1/orders/{orderId}/checkout",status="202"}` |
| **Order Materialization Lag** (`inventory.seat.reserved` -> `order_db`) | End-to-End Event Lag (`p95` / `p99`) | **`p95 <= 350 ms`**<br>**`p99 <= 800 ms`** | `<= 1%` above `800 ms` | `ticketing_orders_materialization_lag_seconds_bucket{service="order-service"}` |
| **Oversold Seat Invariant** | Data Integrity | **`0` oversold seats** (`100.000%`) | `0` events | PostgreSQL unique index `idx_reservation_items_active_seat` + `ticketing_kafka_consumer_dlt_total == 0` |

---

### 11.2 Exact PromQL Queries for SLI Evaluation

```promql
# 1. Seat Reservation Availability SLI (5m rate)
1 - (
  sum(rate(http_server_requests_seconds_count{service="inventory-service",uri="/api/v1/reservations",method="POST",status=~"5.."}[5m]))
  /
  sum(rate(http_server_requests_seconds_count{service="inventory-service",uri="/api/v1/reservations",method="POST"}[5m]))
)

# 2. Seat Reservation p99 Latency SLI on 201 Created (seconds)
histogram_quantile(0.99,
  sum by (le) (rate(http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",method="POST",status="201"}[5m]))
)

# 3. Seat Reservation p99 Fast-Reject Latency SLI on 409 Conflict (seconds)
histogram_quantile(0.99,
  sum by (le) (rate(http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",method="POST",status="409"}[5m]))
)

# 4. Order Checkout Availability SLI (5m rate)
1 - (
  sum(rate(http_server_requests_seconds_count{service="order-service",uri="/api/v1/orders/{orderId}/checkout",method="POST",status=~"5.."}[5m]))
  /
  sum(rate(http_server_requests_seconds_count{service="order-service",uri="/api/v1/orders/{orderId}/checkout",method="POST"}[5m]))
)

# 5. Order Checkout p99 Latency SLI on 202 Accepted (seconds)
histogram_quantile(0.99,
  sum by (le) (rate(http_server_requests_seconds_bucket{service="order-service",uri="/api/v1/orders/{orderId}/checkout",method="POST",status="202"}[5m]))
)

# 6. Order Materialization p99 Lag (seconds)
histogram_quantile(0.99,
  sum by (le) (rate(ticketing_orders_materialization_lag_seconds_bucket{service="order-service"}[5m]))
)
```

