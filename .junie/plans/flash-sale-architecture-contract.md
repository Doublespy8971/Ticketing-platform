---
sessionId: session-261027-215919-qmjy
---

# Requirements

### Overview & Goals
Produce a single, comprehensive, production-grade architecture contract in `ARCHITECTURE.md` for a high-concurrency flash-sale ticketing platform. No application code will be written; `ARCHITECTURE.md` will serve as the single source of truth for engineers and AI coding agents building the system.

### Scope
#### In Scope
- Complete specification of all 12 mandatory architecture sections plus the `DECISIONS.md` seed (10 ADRs) and explicit assumptions/open questions in a single `ARCHITECTURE.md` file at the repository root.
- Support for **assigned seat inventory (`ASSIGNED`) only**: discrete numbered seats with strict database-level uniqueness constraints (`GENERAL_ADMISSION` is scoped out to future work).
- Support for a **two-step hold-then-pay user journey**: Step 1 acquires a time-bounded hold via `inventory-service`; Step 2 initiates checkout and payment via `order-service` using `orderId = reservationId` and `/api/v1/orders/{orderId}/checkout`.
- Support for **stateless HS256 JWT validation + Redis token-bucket rate limiting** at `gateway-service`, including a dev-only `POST /api/v1/auth/token` endpoint issuing HS256 JWTs (`sub`, `roles` claims) signed with an environment variable secret, and `ADMIN`-restricted endpoints on `inventory-service` to create events and bulk-create seats.
- Concrete SQL schemas (PostgreSQL 16 for `inventory_db`, `order_db`, and `payment_db`; `notification-service` is stateless with no database), Redis 7 Lua scripts, `FOR UPDATE SKIP LOCKED` polling outbox publishers (every 200 ms), Kafka topic topologies (2 retry levels at 2s and 10s plus DLT using Spring Kafka `RetryableTopic` naming), JSON event schemas, REST API contracts (with RFC 7807 errors), SLOs, and failure testing strategies.

#### Out of Scope
- Writing Java, Go, SQL migration files, Dockerfiles, or Maven `pom.xml` application code.
- General admission (`GENERAL_ADMISSION`) capacity pools and Debezium CDC outbox streaming (both documented under future work / `DECISIONS.md`).
- Binary serialization formats (Avro/Protobuf) — all Kafka events use strict JSON schemas with an explicit `schemaVersion`.

### Functional Requirements
1. **Zero Overselling Guarantee**: The architecture must make overselling mathematically impossible at both the fast concurrency gate (Redis 7 atomic Lua scripts) and the system of record (PostgreSQL 16 row-level constraints, `UNIQUE` indexes, and partial unique indexes) for assigned seats.
2. **Two-Step Choreography Saga**:
   - **Step 1 (Hold)**: Client requests a seat hold on `inventory-service` (`POST /api/v1/reservations`), which executes the Redis Lua gate + synchronous Postgres transaction and writes `inventory.seat.reserved` to its outbox. `order-service` consumes `inventory.seat.reserved` to materialize an order in `RESERVED` state with `orderId = reservationId` and `expiresAt`.
   - **Step 2 (Checkout)**: Client submits checkout on `order-service` (`POST /api/v1/orders/{orderId}/checkout`), transitioning the order to `PAYMENT_PENDING` and emitting `order.payment.requested`. `payment-service` processes payment and emits `payment.succeeded` or `payment.failed`.
3. **Race Condition & Crash Resilience**:
   - Explicitly resolve the **early-checkout race** (user calls `/api/v1/orders/{orderId}/checkout` before `order-service` has consumed `inventory.seat.reserved`) using only this behavior: `order-service` returns `409 Conflict` with error code `ORDER_NOT_READY` and `Retry-After: 1`.
   - Explicitly make **PostgreSQL the authority for hold expiry**: Postgres `expires_at` is 600 seconds while Redis seat key TTL (`seat:{eventId}:{seatId}`) is 660 seconds so Redis never releases a seat before Postgres. The expiry handler updates Postgres first (`HELD -> EXPIRED`, seat `HELD -> AVAILABLE`, outbox `inventory.reservation.expired`), then calls `release_inventory.lua`.
   - Explicitly resolve the **late-payment vs. TTL-expiry race** (`payment.succeeded` arrives after `inventory.reservation.expired` has already released the seat) via automatic compensating refund (`inventory.seat.confirmation_rejected` -> `order.refund.requested` -> `payment.refunded`).
   - Explicitly resolve **Redis keyspace pub/sub drop risk** by pairing `__keyevent@0__:expired` notifications on a 600-second shadow timer key (`reservation:ttl:{reservationId}`) with a deterministic PostgreSQL fallback sweeper (`FOR UPDATE SKIP LOCKED`).
4. **Precision Rule**: Every table, column, type, index, constraint, topic, partition count, retention value, HTTP status code, JSON field, metric name, and dependency version must be explicitly named and pinned with zero vague placeholders.

# Technical Design

### Current Implementation
The repository (`/Users/macbookpro/Desktop/Ticketing-platform`) is currently an empty greenfield directory. `ARCHITECTURE.md` will establish the foundational contract for the Maven multi-module monorepo and the Go notification service.

### Key Decisions
1. **Two-Step Saga Partitioning (`Inventory-First Hold -> Order Checkout`)**:
   - Step 1 targets `inventory-service` directly (`POST /api/v1/reservations`) so flash-sale traffic hits the Redis Lua concurrency gate without an extra Kafka hop. `inventory-service` commits the hold + outbox row (`inventory.seat.reserved`) and returns `201 Created` with `reservationId` (`orderId = reservationId`) and `expiresAt`.
   - `order-service` consumes `inventory.seat.reserved` (partitioned by `reservationId`) to create the `orders` row in `RESERVED` status with `order_id = reservation_id`.
   - If the client calls `POST /api/v1/orders/{orderId}/checkout` before `order-service` has consumed `inventory.seat.reserved`, `order-service` returns `409 Conflict` (`ORDER_NOT_READY` with HTTP header `Retry-After: 1`).
2. **Assigned-Seat Anti-Overselling Model (`ASSIGNED` Only)**:
   - Stored in `seats` (`UNIQUE (event_id, section_code, row_label, seat_number)`) with `status IN ('AVAILABLE', 'HELD', 'SOLD')` and `version BIGINT`, reinforced by a partial unique index `idx_reservation_items_active_seat ON reservation_items (event_id, seat_id) WHERE status IN ('HELD', 'CONFIRMED')`.
   - `GENERAL_ADMISSION` is excluded from DDL, Lua scripts, and REST APIs and documented under future work.
3. **Redis Lua Gate + Synchronous PostgreSQL Transaction + Postgres-First Hold Expiry**:
   - `inventory-service` executes an atomic Lua script (`hold_inventory.lua`) that validates event drop time, verifies all requested seats are available, sets `SET seat:{eventId}:{seatId} {reservationId} NX EX 660` and shadow timer key `SET reservation:ttl:{reservationId} 1 EX 600`, and only then opens the Postgres transaction (`expires_at = NOW() + INTERVAL '600 seconds'`).
   - Because Redis seat key TTL is 660 seconds and Postgres `expires_at` is 600 seconds, Redis never expires a seat key before Postgres.
   - If the Postgres transaction fails or rolls back during hold creation, `inventory-service` immediately invokes `release_inventory.lua` in a `finally` block (matching `reservationId` token).
   - On hold expiration (triggered by `__keyevent@0__:expired` on `reservation:ttl:{reservationId}` at 600s or the 5-second Postgres sweeper), the expiry handler updates Postgres first (`reservations.status = 'EXPIRED'`, `reservation_items.status = 'EXPIRED'`, `seats.status = 'AVAILABLE'`, outbox `inventory.reservation.expired`), and after commit calls `release_inventory.lua` to delete `seat:{eventId}:{seatId}` keys.
4. **Polling Transactional Outbox (`FOR UPDATE SKIP LOCKED` at 200 ms) & Stateless Notification Service**:
   - All state-mutating Java services (`inventory-service`, `order-service`, `payment-service`) write domain events to `outbox_events` in the same local PostgreSQL transaction. A scheduled outbox poller runs every `200 ms`, selects unpublished rows (`WHERE published_at IS NULL ORDER BY created_at ASC LIMIT 100 FOR UPDATE SKIP LOCKED`), publishes them to Kafka using `aggregate_id` (`reservationId`) as the partition key, and updates `published_at = NOW()`. Debezium CDC is documented only as a future upgrade in `DECISIONS.md`.
   - `notification-service` (Go, 8084) is stateless with no PostgreSQL database; it deduplicates consumed Kafka events using Redis `SET notif:dedup:{eventId} 1 NX EX 604800`.
5. **Authentication & Admin Provisioning Endpoints**:
   - `gateway-service` exposes a dev-only `POST /api/v1/auth/token` endpoint that issues HS256 JWTs signed with `JWT_SECRET` containing `sub` and `roles` (`USER`, `ADMIN`) claims.
   - `inventory-service` exposes `POST /api/v1/admin/events` and `POST /api/v1/admin/events/{eventId}/seats/bulk`, restricted to the `ADMIN` role.

### Architecture Diagram
```mermaid
graph LR
  Client[Client] -->|JWT + Idempotency-Key| GW[gateway-service :8080]
  GW -->|POST /api/v1/reservations| Inv[inventory-service :8081]
  GW -->|POST /api/v1/orders/{orderId}/checkout| Ord[order-service :8082]
  Inv -->|Lua Gate 660s Seat TTL| Redis[(Redis 7)]
  Inv -->|Sync TX + Outbox| InvDB[(inventory_db PG16)]
  Ord -->|Sync TX + Outbox| OrdDB[(order_db PG16)]
  Pay[payment-service :8083] -->|Sync TX + Outbox| PayDB[(payment_db PG16)]
  Inv -->|200ms SKIP LOCKED Poller| Kafka[(Apache Kafka KRaft)]
  Ord -->|200ms SKIP LOCKED Poller| Kafka
  Pay -->|200ms SKIP LOCKED Poller| Kafka
  Kafka -->|seat.reserved / reservation.expired| Ord
  Kafka -->|payment.requested / refund.requested| Pay
  Kafka -->|payment.succeeded / payment.failed| Inv
  Kafka -->|payment.succeeded / payment.failed| Ord
  Kafka -->|order.confirmed / order.cancelled| Notif[notification-service :8084 Go Stateless]
  Notif -->|Dedup Key NX EX| Redis
```

### Data Models / Contracts
- **Common Event Envelope**:
  ```json
  {
    "eventId": "01926b54-7c8a-7000-8000-1a2b3c4d5e6f",
    "eventType": "inventory.seat.reserved",
    "occurredAt": "2025-05-01T16:00:00.123456Z",
    "correlationId": "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e",
    "schemaVersion": 1,
    "payload": {}
  }
  ```
- **Kafka Topics & Partitioning**:
  All core saga topics (`inventory.events`, `order.events`, `payment.events`) use `reservationId` (`orderId = reservationId`) as the partition key (12 partitions in Docker Compose / 24 in K8s) to guarantee strict causal ordering for all events belonging to a single reservation/order lifecycle. Each consumer group has 2 retry topic levels (`2s` and `10s`) plus a dead-letter topic using Spring Kafka `@RetryableTopic` naming (`<topic>-<consumer-group>-retry-0`, `<topic>-<consumer-group>-retry-1`, and `<topic>-<consumer-group>-dlt`).
- **Order Saga States (`order_db.orders.status`)**:
  `RESERVED` -> `PAYMENT_PENDING` -> `CONFIRMED` | `CANCELLED_PAYMENT_FAILED` | `CANCELLED_EXPIRED` | `REFUND_PENDING` -> `CANCELLED_REFUNDED`.
- **Inventory Reservation States (`inventory_db.reservations.status`)**:
  `HELD` -> `CONFIRMED` | `RELEASED` | `EXPIRED`.

### File Structure
- `ARCHITECTURE.md` (created at repository root `/Users/macbookpro/Desktop/Ticketing-platform/ARCHITECTURE.md` containing all 12 contract sections, the `DECISIONS.md` seed section, and the Open Questions & Assumptions section).

### Risks
- **Late Payment vs. Hold Expiration Race**: A user initiates checkout at `t = 598s`; the hold expires in `inventory-service` at `t = 600s` (`EXPIRED`, seat released to another buyer), and `payment-service` succeeds at `t = 601s`.
  - *Mitigation in Design*: First, `order-service` rejects checkout requests if `expires_at - now() < 15s` (grace buffer). Second, if `payment.succeeded` still arrives after `inventory-service` transitioned the reservation to `EXPIRED`, `inventory-service` refuses to transition `EXPIRED -> CONFIRMED` and emits `inventory.seat.confirmation_rejected`; `order-service` transitions to `REFUND_PENDING` and emits `order.refund.requested` so `payment-service` automatically refunds the charge and emits `payment.refunded`.
- **Outbox Poller At-Least-Once Duplicate Delivery**: If an application instance crashes after publishing to Kafka but before committing `published_at = NOW()`, the next poll cycle re-emits the outbox row.
  - *Mitigation in Design*: Every stateful Java Kafka consumer inserts `event_id` into `processed_events (event_id UUID, consumer_group VARCHAR(100), processed_at TIMESTAMPTZ, PRIMARY KEY (event_id, consumer_group))` inside the same local database transaction as its business state update, and stateless `notification-service` deduplicates via Redis `SET notif:dedup:{eventId} 1 NX EX 604800`.

# Verification

### Validation Approach
Since this task produces the architecture specification (`ARCHITECTURE.md`) without application code, validation focuses on structural completeness, schema consistency, and race-condition soundness across all sections of the generated Markdown document.

### Key Scenarios
1. **14-Section Structural Completeness Check**: Verify that `ARCHITECTURE.md` contains all 12 mandatory architecture sections, the `DECISIONS.md` seed (10 ADRs), and the Open Questions & Assumptions section.
2. **Cross-Section Contract Alignment**:
   - Every event emitted in Section 2 (Order Saga State Machine) has a corresponding topic in Section 3 (Kafka Design) and an exact JSON payload schema in Section 4 (Event Contracts).
   - Every database table referenced in Section 5 (Outbox), Section 6 (Idempotency), and Section 8 (Database Schema) uses valid PostgreSQL 16 types, constraints, and indexes across `inventory_db`, `order_db`, and `payment_db` (with `notification-service` stateless).
   - `ASSIGNED` seat flows, Postgres-first hold expiry (600s Postgres vs. 660s Redis seat TTL), `/api/v1/orders/{orderId}/checkout` (`orderId = reservationId`), dev auth token endpoint, and admin event/seat bulk endpoints are consistently represented across REST APIs (Section 7), Postgres DDL (Section 8), and Redis Lua scripts (Section 9).
3. **Strict Rule Compliance**: Verify via regex search (`grep_search`) on `ARCHITECTURE.md` that forbidden vague phrases do not appear anywhere in the document.

# Delivery Steps

### ✓ Step 1: Author Service Boundaries, Saga State Machine, Kafka Topology, and Event Contracts
`ARCHITECTURE.md` is created with complete specifications for service boundaries, the two-step choreography state machine, Kafka topology, and JSON event schemas (Sections 1–4).

- Define **Section 1 (Service Responsibilities)** covering `gateway-service` (8080), `inventory-service` (8081), `order-service` (8082), `payment-service` (8083), and stateless `notification-service` (8084, Go, no database), detailing exact ownership vs. forbidden cross-boundary actions.
- Define **Section 2 (Order Saga Choreography)** with the full state machine across `inventory-service` and `order-service`, covering `SeatReserved`, `PaymentSucceeded`, `PaymentFailed`, `ReservationExpired`, duplicate events, mid-saga process crashes, the `Checkout`-before-`SeatReserved` ordering race (returning `409 ORDER_NOT_READY` with `Retry-After: 1` on `/api/v1/orders/{orderId}/checkout` where `orderId = reservationId`), and the `PaymentSucceeded`-vs-`ReservationExpired` late-payment race (automatic refund compensation).
- Define **Section 3 (Kafka Design)** with the complete topic table (names, partitions, replication factors for Docker Compose vs. Kubernetes, partition keys, retention policies, producers, and consumers), plus 2 retry topic levels (`2s` and `10s`) and dead-letter topics using Spring Kafka `@RetryableTopic` naming (`<topic>-<consumer-group>-retry-0`, `<topic>-<consumer-group>-retry-1`, `<topic>-<consumer-group>-dlt`).
- Define **Section 4 (Event Contracts)** specifying the canonical JSON event envelope (`eventId`, `eventType`, `occurredAt`, `correlationId`, `schemaVersion`, `payload`) and concrete field-by-field JSON schemas for all domain events (using `ASSIGNED` seats only and `orderId = reservationId`).

### ✓ Step 2: Author Outbox, Idempotency, REST APIs, Database Schemas, and Redis Lua Design
`ARCHITECTURE.md` is extended with exact SQL and JSON specifications for the polling transactional outbox, HTTP and Kafka idempotency, REST API endpoints (including dev auth and admin endpoints), PostgreSQL 16 schemas, and Redis 7 Lua scripts (Sections 5–9).

- Define **Section 5 (Transactional Outbox)** with the `outbox_events` DDL, the 200 ms `FOR UPDATE SKIP LOCKED` scheduled poller query and publisher logic, partition-key ordering guarantees, and retention cleanup.
- Define **Section 6 (Idempotency)** with exact PostgreSQL DDL and state transition rules for `idempotency_keys` (HTTP `Idempotency-Key` header + SHA-256 request hash), `processed_events` (transactional Kafka consumer deduplication for Java services), and Redis `SET NX EX` deduplication for stateless `notification-service`.
- Define **Section 7 (REST API Contracts)** detailing authentication (`POST /api/v1/auth/token` dev-only HS256 JWT issuer on `gateway-service` with `sub` and `roles` claims), `ADMIN`-restricted endpoints on `inventory-service` (`POST /api/v1/admin/events`, `POST /api/v1/admin/events/{eventId}/seats/bulk`), customer endpoints (`POST /api/v1/reservations`, `POST /api/v1/orders/{orderId}/checkout`), status codes, and RFC 7807 Problem Details error payloads.
- Define **Section 8 (Database Schema per Service)** with full PostgreSQL 16 DDL, indexes, and anti-overselling constraints for `inventory_db`, `order_db`, and `payment_db` (`ASSIGNED` seats only with `UNIQUE (event_id, section_code, row_label, seat_number)` + partial unique index on active `reservation_items`; no database for `notification-service`).
- Define **Section 9 (Redis Design)** specifying key naming conventions, TTLs (660s on `seat:{eventId}:{seatId}`, 600s on `reservation:ttl:{reservationId}` and Postgres `expires_at`), atomic Lua scripts (`hold_inventory.lua`, `release_inventory.lua`, `confirm_inventory.lua`), and Postgres-authoritative hold expiry (updating Postgres first, then calling `release_inventory.lua`).

### ✓ Step 3: Author Cross-Cutting Conventions, SLOs, Failure Matrix, and DECISIONS.md Seed
`ARCHITECTURE.md` is completed with operational conventions, pinned dependency versions, SLO definitions, failure-testing matrix, ADR seeds, and assumptions (Sections 10–12 and Appendices).

- Define **Section 10 (Cross-Cutting Conventions)** covering Maven multi-module layout, Java package structure (`com.ticketing.<service>`), Go module structure, `X-Correlation-Id` and JWT header propagation, structured JSON logging, Micrometer/Prometheus metric names, health probes, environment variables (including `JWT_SECRET`), and pinned dependency versions.
- Define **Section 11 (Initial SLOs)** defining availability and p95/p99 latency targets for seat reservation and checkout, error budgets, and exact PromQL queries.
- Define **Section 12 (Failure Scenarios Table)** mapping 12 concrete distributed failure modes to expected system behaviors and automated verification methods (Testcontainers, Toxiproxy, k6).
- Author the **DECISIONS.md Seed** section with 10 interview-defensible architecture decision records (including polling outbox with Debezium CDC as a future upgrade, and assigned-seat-only scope with general admission as future work) and the **Open Questions and Assumptions** section.

### ✓ Step 4: Verify Contract Completeness, Schema Consistency, and Race-Condition Mitigations
`ARCHITECTURE.md` is verified end-to-end for internal consistency, race-condition closure, and strict adherence to all prompt rules.

- Cross-check all event names, topic names, partition keys, column names, and status enums across Sections 2, 3, 4, 7, 8, and 9 to guarantee zero schema drift.
- Audit all concurrency and failure paths (Postgres-first hold expiry before Redis release, 660s Redis seat TTL vs 600s Postgres expiry, `409 ORDER_NOT_READY` on early checkout, late payment vs. TTL expiry race, 200ms outbox poller duplicate delivery) to ensure every race condition has an explicit deterministic resolution in the document.
- Verify that zero vague placeholders exist anywhere in `ARCHITECTURE.md`.