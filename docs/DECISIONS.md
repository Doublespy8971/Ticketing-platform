## DECISIONS.md Seed

The following 10 Architecture Decision Records (ADRs) form the seed content for `DECISIONS.md`.

### ADR-01: Two-Step Inventory-First Hold (`POST /api/v1/reservations`) Then Order Checkout (`POST /api/v1/orders/{orderId}/checkout`) with `orderId = reservationId`
- **Context**: During a flash sale, 95%+ of incoming traffic consists of contending seat hold attempts that must be rejected in milliseconds, while only successful seat holders proceed to payment checkout over the next 10 minutes.
- **Alternatives Considered**:
  1. *Single-Step Order-First Saga*: Client posts to `order-service`, which creates a `PENDING` order and requests a hold from `inventory-service` via Kafka. Rejected because it forces PostgreSQL writes and Kafka round-trips for 100% of rejected flash-sale requests and requires client polling just to learn if a seat was held.
  2. *Two-Step Saga with Separate `reservationId` and `orderId`*: Requires the client to poll a lookup endpoint (`GET /api/v1/orders?reservationId=...`) before calling checkout.
  3. *Two-Step Inventory-First Hold with Deterministic `orderId = reservationId` (Chosen)*.
- **Why This Option Won**: Hitting `inventory-service` directly in Step 1 lets the Redis Lua gate reject 99% of contending traffic in `< 10 ms` before touching PostgreSQL or Kafka. Setting `orderId = reservationId` deterministically allows the client to call `POST /api/v1/orders/{orderId}/checkout` immediately in Step 2 using the ID returned in Step 1.

---

### ADR-02: Redis 7 Atomic Lua Concurrency Gate Paired with Synchronous PostgreSQL 16 ACID Transactions
- **Context**: We must survive extreme flash-sale concurrency spikes at drop time while guaranteeing that every `201 Created` hold response is durably persisted in the system of record.
- **Alternatives Considered**:
  1. *PostgreSQL `SELECT ... FOR UPDATE NOWAIT` Only*: Accurate, but 10,000 concurrent transactions contending on hot rows exhaust HikariCP connection pools and saturate PostgreSQL CPU/locks.
  2. *Redis Hold with Asynchronous Write-Behind to PostgreSQL*: Lowest latency, but if Redis crashes after returning `201 Created` and before the Kafka consumer flushes to PostgreSQL, confirmed holds are lost or double-assigned.
  3. *Redis Lua Gate (`hold_inventory.lua`) Followed by Synchronous PostgreSQL Transaction in the Same Request (Chosen)*.
- **Why This Option Won**: `hold_inventory.lua` executes atomically on Redis's single thread in sub-millisecond time, filtering out all losing requests so that **at most one winner per seat** ever opens a PostgreSQL transaction. If the PostgreSQL transaction fails, `inventory-service` runs `release_inventory.lua` in its `finally` block.

---

### ADR-03: PostgreSQL as the Sole Authority for Hold Expiry (`1000 ms` PostgreSQL Sweeper with `600s` Postgres `expires_at` and `660s` Redis Seat Safety-Net TTL)
- **Context**: Held seats must expire after 10 minutes (`600 seconds`) if unpaid. Both Redis and PostgreSQL track hold state, creating a risk of split-brain if Redis releases a seat before PostgreSQL marks the reservation `EXPIRED` or if hold expiration depends on lossy Redis keyspace pub/sub notifications.
- **Alternatives Considered**:
  1. *Equal 600s TTL in Redis and PostgreSQL*: Unsafe because Redis evicts `seat:{eventId}:{seatId}` at `600.000s` before the expiry handler updates PostgreSQL, allowing a new buyer to pass Redis and collide with the un-expired PostgreSQL row (or race with a late payment).
  2. *Redis Keyspace Expiry Listener (`__keyevent@0__:expired`) with a `reservation:ttl` Shadow Key*: Adds an extra Redis key per reservation and relies on at-most-once Pub/Sub notifications that still require a PostgreSQL sweeper anyway.
  3. *PostgreSQL-Only `1000 ms` Sweeper (`FOR UPDATE SKIP LOCKED`) with `600s` Postgres `expires_at` and `660s` Redis Seat Safety-Net TTL (Chosen)*.
- **Why This Option Won**: Removing the Redis keyspace listener and `reservation:ttl` shadow key eliminates dual-trigger complexity while keeping a single deterministic expiry path. Giving `seat:{eventId}:{seatId}` a `660s` safety-net TTL while PostgreSQL `expires_at` is `600s` guarantees Redis never expires a seat on its own before the `1000 ms` PostgreSQL sweeper (`FOR UPDATE SKIP LOCKED`) commits `EXPIRED` and `seats.status = 'AVAILABLE'` in PostgreSQL **first**, and only then invokes `release_inventory.lua` in Redis.

---

### ADR-04: Single-Leader Polling Transactional Outbox (`pg_try_advisory_lock` Every 200 ms) Instead of Debezium CDC
- **Context**: Services must atomically update their PostgreSQL database and publish Kafka events in strict causal order without dual-write bugs.
- **Alternatives Considered**:
  1. *Debezium Server / Kafka Connect WAL CDC*: Low latency and no polling queries, but requires operating Kafka Connect clusters, managing PostgreSQL logical replication slots (`wal_level=logical`), and preventing WAL disk exhaustion if a connector stalls.
  2. *Single-Leader In-Process Scheduled Polling Publisher Elected via `pg_try_advisory_lock` on a Fixed Lock ID Every 200 ms (Chosen)*.
- **Why This Option Won**: Electing a single active poller instance per service via `pg_try_advisory_lock` on a fixed lock ID held on a dedicated connection—selecting `LIMIT 100` unpublished rows ordered by `created_at ASC, event_id ASC`, sending them sequentially, waiting for each broker ack, marking each row published right after its ack, and stopping the batch at the first failed send—guarantees strict causal ordering with zero external CDC containers. **Future Upgrade Path**: If outbox write throughput exceeds single-leader sequential polling capacity in production, the `outbox_events` table schema is 100% compatible with swapping the in-process poller for Debezium `PostgresConnector` (`pgoutput` + `EventRouter` SMT) without changing any domain code.

---

### ADR-05: Scoping Inventory to Discrete `ASSIGNED` Seats Only (General Admission Deferred)
- **Context**: Event ticketing can involve discrete numbered seats (`ASSIGNED`) or unnumbered section capacity counters (`GENERAL_ADMISSION`).
- **Alternatives Considered**:
  1. *Hybrid Schema Supporting Both `ASSIGNED` and `GENERAL_ADMISSION` in v1*: Doubles the complexity of Lua scripts, REST APIs, and PostgreSQL constraints.
  2. *`ASSIGNED` Seats Only in v1 (Chosen)*.
- **Why This Option Won**: Numbered seat selection is the hardest concurrency problem (each seat is a contended singleton resource) and allows a clean, uniform data model enforced by `UNIQUE (event_id, section_code, row_label, seat_number)` and the partial unique index `idx_reservation_items_active_seat`. `GENERAL_ADMISSION` capacity pools are documented under Future Work.

---

### ADR-06: Resolving the Early-Checkout Race with `409 ORDER_NOT_READY` (`Retry-After: 1`)
- **Context**: Because Step 1 (`POST /api/v1/reservations`) writes to `inventory_db` and asynchronously materializes the order in `order_db` via `inventory.seat.reserved`, a client calling `POST /api/v1/orders/{orderId}/checkout` within ~200 ms of Step 1 may hit `order-service` before the order row exists.
- **Alternatives Considered**:
  1. *Return `404 Not Found`*: Ambiguous to clients (indistinguishable from a bogus `orderId`).
  2. *Hold HTTP Thread Open in `order-service` Polling `order_db`*: Blocks servlet threads and risks connection pool starvation during traffic spikes.
  3. *Return `409 Conflict` with `errorCode: "ORDER_NOT_READY"` and `Retry-After: 1` (Chosen)*.
- **Why This Option Won**: Returning `409 ORDER_NOT_READY` with `Retry-After: 1` (while rolling back the `IN_PROGRESS` idempotency key so the client can retry with the same `Idempotency-Key`) is non-blocking, explicit, and preserves strict database isolation between `inventory-service` and `order-service`.

---

### ADR-07: Choreography Saga with Authoritative Inventory Confirmation and Automatic Late-Payment Refund
- **Context**: A payment charge in `payment-service` can succeed fractions of a second after `inventory-service` has already expired the 600s hold and released the seat to another buyer.
- **Alternatives Considered**:
  1. *Allow `payment.succeeded` to Force-Transition `EXPIRED -> CONFIRMED`*: Causes seat overselling if another buyer held the seat after expiry.
  2. *Centralized Saga Orchestrator Service*: Adds an extra service and extra Kafka hops on every transition.
  3. *Choreography Saga Where `inventory-service` Validates Hold Status on `payment.succeeded` and Emits `inventory.seat.confirmation_rejected` to Trigger Automatic Refund (Chosen)*.
- **Why This Option Won**: `inventory-service` is the sole authority on seat state. If `reservations.status` is already `EXPIRED` when `payment.succeeded` arrives, `inventory-service` rejects confirmation (`inventory.seat.confirmation_rejected`), and `order-service` automatically drives a compensating refund (`order.refund.requested` -> `payment.refunded` -> `CANCELLED_REFUNDED`), guaranteeing both zero overselling and zero unrefunded charges.

---

### ADR-08: Two Retry Topic Levels (`2s` and `10s`) Plus Dead-Letter Topic for Stateful Java Consumers Using Spring Kafka `@RetryableTopic` Naming
- **Context**: Transient database or network failures in stateful Kafka consumers (`inventory-service`, `order-service`, `payment-service`) must not block partition processing for other reservations or silently drop saga events.
- **Alternatives Considered**:
  1. *In-Consumer Blocking Retries (`DefaultErrorHandler` with long backoff)*: Stalls the entire partition for all other orders sharing that partition.
  2. *3+ Retry Levels (`2s`, `10s`, `60s`, `300s`)*: Multiplies topic and partition counts across the cluster with diminishing returns during a 600-second hold window.
  3. *2 Retry Levels (`2s` and `10s`) + DLT with Spring Kafka `@RetryableTopic` Naming (`<topic>-<group>-retry-0`, `-retry-1`, `-dlt`) for Stateful Consumers (Chosen)*.
- **Why This Option Won**: Two retry tiers (`2s` and `10s`) resolve transient DB failovers well within the reservation TTL window while keeping total topic count compact and leveraging Spring Kafka's built-in `@RetryableTopic` infrastructure.

---

### ADR-09: Stateless Go `notification-service` with Redis `SET NX EX` Deduplication, No Database, and Log-plus-Metric Failure Handling (No Retry or DLT Topics)
- **Context**: `notification-service` consumes terminal `order.confirmed` and `order.cancelled` events to send customer notifications under at-least-once Kafka delivery, where notification delivery is non-critical to saga state convergence.
- **Alternatives Considered**:
  1. *Provision a Dedicated `notification_db` PostgreSQL Database and Retry/DLT Topics*: Adds schema migration, connection pooling, and extra Kafka retry/DLT topics for a side-effect service that does not own core domain state.
  2. *Stateless Go Service Using Redis `SET notif:dedup:{eventId} "1" NX EX 604800` and Logging + Metric Increment on Failure Without Retry/DLT Topics (Chosen)*.
- **Why This Option Won**: Go's low memory footprint (`< 25 MiB` RSS) combined with atomic Redis `SET NX EX` deduplication (with a 7-day TTL matching Kafka retention) keeps `notification-service` completely stateless and horizontally scalable without a fourth PostgreSQL database. Logging notification failures at `ERROR` level and incrementing `ticketing_notifications_sent_total{status="FAILED"}` without forwarding to retry or DLT topics avoids unnecessary topic proliferation and broker write amplification.

---

### ADR-10: Stateless HS256 JWT Authentication at `gateway-service` with Dev-Only `/api/v1/auth/token` Endpoint
- **Context**: Downstream services need verified user identity (`X-User-Id`) and roles (`X-User-Roles` for `USER` and `ADMIN` endpoints), and developers/automated tests need a frictionless way to mint tokens locally without an external OAuth2 IdP container.
- **Alternatives Considered**:
  1. *Trust Client-Supplied `X-User-Id` Header*: Insecure and fails to model real gateway authentication or `ADMIN` RBAC.
  2. *Full Keycloak / OAuth2 Server in Docker Compose*: Adds heavy container startup time and memory overhead for local development.
  3. *HS256 JWT Validation at `gateway-service` Using `JWT_SECRET` + `@Profile({"dev", "local"})` `POST /api/v1/auth/token` Issuer (Chosen)*.
- **Why This Option Won**: Verifying HS256 JWTs at `gateway-service` is stateless and fast (`< 0.1 ms`), enables role checks (`ADMIN` for `/api/v1/admin/**`), and the profile-gated `POST /api/v1/auth/token` endpoint makes local testing and k6 load generation completely self-contained.

---

## Open Questions, Assumptions, and Future Work

### Explicit Assumptions Made
1. **Maximum Seats per Reservation (`6` Seats)**: A single `POST /api/v1/reservations` request is capped at `6` assigned seats to prevent bulk-scalping holds and bound the number of keys locked in a single `hold_inventory.lua` execution.
2. **Single Currency per Event (`USD`)**: All seats within an event are priced in integer minor units (`price_cents` in `USD`) to eliminate floating-point rounding and currency conversion during checkout.
3. **Deterministic Order-to-Reservation Identity (`orderId = reservationId`)**: Each reservation maps 1:1 to at most one order, allowing `orderId` to equal `reservationId` across all APIs, events, and database tables.
4. **Synchronized Clocks (NTP)**: Container and node clocks across `inventory-service`, `order-service`, PostgreSQL, and Redis are synchronized via NTP within `<= 100 ms` drift; the 60-second safety margin between PostgreSQL `expires_at` (`600s`) and Redis `seat:{eventId}:{seatId}` TTL (`660s`) absorbs any clock skew.

### Future Work (Out of Scope for v1)
1. **General Admission (`GENERAL_ADMISSION`) Capacity Pools**: Support for unnumbered standing-room / lawn sections using atomic counter decrements in Redis (`DECRBY ga:{eventId}:{sectionCode}:avail {qty}`) and capacity invariant `CHECK` constraints in PostgreSQL (`available_capacity + held_capacity + sold_capacity = total_capacity`).
2. **Debezium WAL Change Data Capture (CDC) Outbox Publisher**: Upgrading the in-process `200 ms` `FOR UPDATE SKIP LOCKED` outbox poller to Debezium PostgreSQL `pgoutput` logical replication with the `EventRouter` SMT when cluster write throughput exceeds polling capacity.
3. **Virtual Waiting Room / Fair Queueing at Edge**: Adding a pre-drop virtual queue token admission layer ahead of `gateway-service` to assign randomized queue positions before `drop_time`.
