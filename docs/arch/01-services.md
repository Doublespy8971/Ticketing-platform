## 1. Service Responsibilities

### 1.1 System Topology Overview

| Service Name | Runtime & Framework | HTTP Port | Database Instance | Stateful / Stateless |
| :--- | :--- | :--- | :--- | :--- |
| `gateway-service` | Java 21, Spring Boot 3.3.5, Spring Cloud Gateway (WebFlux / Project Reactor) | `8080` | None (uses Redis 7 for rate limiting) | Stateless |
| `inventory-service` | Java 21, Spring Boot 3.3.5, Spring MVC, Spring Data JDBC / JPA, Spring Kafka | `8081` | PostgreSQL 16 (`inventory_db`) + Redis 7 | Stateful |
| `order-service` | Java 21, Spring Boot 3.3.5, Spring MVC, Spring Data JDBC / JPA, Spring Kafka | `8082` | PostgreSQL 16 (`order_db`) | Stateful |
| `payment-service` | Java 21, Spring Boot 3.3.5, Spring MVC, Spring Data JDBC / JPA, Spring Kafka | `8083` | PostgreSQL 16 (`payment_db`) | Stateful |
| `notification-service` | Go 1.23, standard `net/http`, `segmentio/kafka-go`, `redis/go-redis/v9` | `8084` | None (uses Redis 7 for event deduplication) | Stateless |

```mermaid
graph LR
  Client[Client] -->|JWT + Idempotency-Key| GW[gateway-service :8080]
  GW -->|POST /api/v1/auth/token| GW
  GW -->|POST /api/v1/admin/events| Inv[inventory-service :8081]
  GW -->|POST /api/v1/reservations| Inv
  GW -->|POST /api/v1/orders/{orderId}/checkout| Ord[order-service :8082]
  GW -->|Rate Limit Lua| Redis[(Redis 7 :6379)]
  Inv -->|Lua Gate 660s Seat TTL| Redis
  Inv -->|Sync TX + Outbox| InvDB[(inventory_db PG16)]
  Ord -->|Sync TX + Outbox| OrdDB[(order_db PG16)]
  Pay[payment-service :8083] -->|Sync TX + Outbox| PayDB[(payment_db PG16)]
  Inv -->|200ms Advisory-Locked Poller| Kafka[(Apache Kafka KRaft :9092)]
  Ord -->|200ms Advisory-Locked Poller| Kafka
  Pay -->|200ms Advisory-Locked Poller| Kafka
  Kafka -->|inventory.events| Ord
  Kafka -->|order.events| Pay
  Kafka -->|payment.events| Inv
  Kafka -->|payment.events| Ord
  Kafka -->|order.events| Notif[notification-service :8084 Go Stateless]
  Notif -->|SET notif:dedup NX EX| Redis
```

---

### 1.2 Per-Service Ownership & Forbidden Actions

#### `gateway-service` (Port `8080`)
- **What It Owns**:
  - Edge HTTP routing to `inventory-service` (`:8081`) and `order-service` (`:8082`).
  - Development-only token issuance via `POST /api/v1/auth/token` (active only when `SPRING_PROFILES_ACTIVE` includes `dev` or `local`), minting HS256 JWTs signed with `JWT_SECRET` containing `sub` (UUID string) and `roles` (JSON string array).
  - Stateless HS256 Bearer JWT signature and expiration verification on all protected `/api/v1/**` routes.
  - Role-based access control (RBAC) enforcement for `/api/v1/admin/**` routes, requiring the `ADMIN` role in the verified JWT `roles` claim.
  - Propagation of verified identity headers (`X-User-Id` from `sub`, `X-User-Roles` from `roles`) and stripping of any client-supplied `X-User-Id` or `X-User-Roles` headers to prevent header spoofing.
  - Generation or propagation of `X-Correlation-Id` (UUIDv4) on every incoming HTTP request and response.
  - Per-user (for authenticated routes) and per-IP (for public routes) token-bucket rate limiting backed by Redis 7.
- **What It Must Never Do**:
  - Must never connect to `inventory_db`, `order_db`, or `payment_db`.
  - Must never produce to or consume from Apache Kafka.
  - Must never execute business state validation, seat availability checks, or price calculations.
  - Must never expose `/api/v1/auth/token` when running with `SPRING_PROFILES_ACTIVE=prod`.

#### `inventory-service` (Port `8081`)
- **What It Owns**:
  - System of record for events (`events`), discrete numbered seats (`seats`, scoped strictly to `ASSIGNED` inventory), seat reservations (`reservations`), and reserved seat line items (`reservation_items`) in `inventory_db`.
  - Admin endpoints (`POST /api/v1/admin/events` and `POST /api/v1/admin/events/{eventId}/seats/bulk`) restricted to callers with the `ADMIN` role.
  - Public seat map availability queries (`GET /api/v1/events/{eventId}/seats`) and customer seat reservation creation (`POST /api/v1/reservations`).
  - High-concurrency flash-sale gatekeeper in Redis 7 via atomic Lua scripts (`hold_inventory.lua`, `release_inventory.lua`, `confirm_inventory.lua`).
  - Authoritative hold expiration in PostgreSQL (`expires_at = created_at + INTERVAL '600 seconds'`), triggered exclusively by the `1000 ms` PostgreSQL sweeper (`FOR UPDATE SKIP LOCKED`), always committing the PostgreSQL state change (`HELD -> EXPIRED`) before invoking `release_inventory.lua` in Redis (where seat lock keys `seat:{eventId}:{seatId}` carry a 660-second TTL as a safety net).
  - Publishing `inventory.seat.reserved`, `inventory.seat.released`, `inventory.reservation.expired`, `inventory.seat.confirmed`, and `inventory.seat.confirmation_rejected` via its local `outbox_events` table and 200 ms single-leader (`pg_try_advisory_lock`) polling publisher to topic `inventory.events`.
- **What It Must Never Do**:
  - Must never create orders, calculate payment totals for billing, or call `payment-service` or `order-service` over synchronous HTTP.
  - Must never allow Redis to release a held seat (`seat:{eventId}:{seatId}`) before PostgreSQL has committed the transition of that reservation to `EXPIRED` or `RELEASED`.
  - Must never read from or write to `order_db` or `payment_db`.

#### `order-service` (Port `8082`)
- **What It Owns**:
  - System of record for customer orders (`orders`, where `order_id` is always identical to `reservation_id`) and order line items (`order_items`) in `order_db`.
  - Consuming `inventory.seat.reserved` from `inventory.events` to materialize orders in the `RESERVED` state with deterministic `order_id = reservation_id`.
  - Handling customer checkout requests (`POST /api/v1/orders/{orderId}/checkout`) and order status queries (`GET /api/v1/orders/{orderId}`).
  - Returning `409 Conflict` with error code `ORDER_NOT_READY` and HTTP header `Retry-After: 1` whenever a client calls `POST /api/v1/orders/{orderId}/checkout` before `inventory.seat.reserved` has been consumed and persisted in `order_db`.
  - Driving compensation when a late payment collides with an expired reservation by transitioning the order to `REFUND_PENDING` and emitting `order.refund.requested`.
  - Publishing `order.payment.requested`, `order.refund.requested`, `order.confirmed`, and `order.cancelled` via its local `outbox_events` table and 200 ms single-leader (`pg_try_advisory_lock`) polling publisher to topic `order.events`.
- **What It Must Never Do**:
  - Must never mutate seat inventory in Redis or `inventory_db`.
  - Must never call `inventory-service` or `payment-service` over synchronous HTTP; all cross-service coordination occurs exclusively via Kafka events.
  - Must never accept a checkout request if the order's `expires_at` is less than 15 seconds in the future (`expires_at - NOW() < INTERVAL '15 seconds'`).

#### `payment-service` (Port `8083`)
- **What It Owns**:
  - System of record for payment transactions and refunds (`payments`) in `payment_db`.
  - Consuming `order.payment.requested` and `order.refund.requested` from `order.events`.
  - Executing the configurable mock payment provider (supporting deterministic simulation via payment method tokens as well as configurable random latency and failure rate via environment variables).
  - Enforcing strict one-charge-per-order idempotency (`UNIQUE (order_id)` on charge records in `payments`).
  - Publishing `payment.succeeded`, `payment.failed`, and `payment.refunded` via its local `outbox_events` table and 200 ms single-leader (`pg_try_advisory_lock`) polling publisher to topic `payment.events`.
- **What It Must Never Do**:
  - Must never expose public customer-facing mutation endpoints through `gateway-service` (all payment executions are triggered asynchronously by `order.payment.requested` and `order.refund.requested`).
  - Must never directly access `order_db` or `inventory_db` or manipulate Redis seat locks.

#### `notification-service` (Port `8084`, Go)
- **What It Owns**:
  - Consuming terminal order lifecycle events (`order.confirmed` and `order.cancelled`) from `order.events`.
  - Stateless event deduplication using Redis 7 (`SET notif:dedup:{eventId} "1" NX EX 604800`).
  - Formatting and dispatching user notifications (structured JSON email/push simulation output and Prometheus metrics); on failure, logging at `ERROR` level and incrementing `ticketing_notifications_sent_total{status="FAILED"}` without forwarding to any retry or dead-letter topic.
- **What It Must Never Do**:
  - Must never own or connect to any PostgreSQL database (`notification-service` is strictly stateless).
  - Must never publish domain events that participate in or block the order/inventory/payment saga, nor forward failed records to any retry or dead-letter Kafka topic.
  - Must never mutate Redis keys outside the `notif:dedup:*` namespace.

