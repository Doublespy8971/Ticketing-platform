# Cross-Service End-to-End Verification Suite (`e2e-tests`)

This module exercises the real, containerized 10-container Ticketing Platform topology through `gateway-service` (`http://localhost:8080`) and proves the platform's global distributed systems guarantees (`I1`–`I8`):
- **`I1` — Oversell**: Every `(event, seat)` has at most 1 active (`HELD` or `CONFIRMED`) `reservation_item` in `inventory_db` and at most 1 `CONFIRMED` `order_item` in `order_db`; `SOLD` seats equal `CONFIRMED` reservation seats and `CONFIRMED` order seats, and never exceed the event's total seats.
- **`I2` — Seat Consistency**: Every `HELD`, `SOLD`, and `AVAILABLE` seat matches its exact active `reservation_item` and parent `reservation` state, and no `HELD` reservation is older than `expires_at + 90s` at quiescence.
- **`I3` — Order and Payment Consistency**: Bijective alignment across `reservations`, `orders`, and `payments` in both directions (every `CONFIRMED` order has a `CONFIRMED` reservation and `SUCCEEDED` payment; every `CONFIRMED` reservation has a `CONFIRMED` order; `REFUNDED` payment iff `CANCELLED_REFUNDED` order; matching `order_items` and `reservation_items` seat sets; no order left in `PAYMENT_PENDING` or undeclared `REFUND_PENDING` at quiescence).
- **`I4` — Duplicate Effects**: At most 1 payment row per order in `payment_db` (with unique `provider_reference` and `refund_id`), matching payment outbox counts, and at most 1 outbox row per `(aggregateId, eventType)` across `inventory_db`, `order_db`, and `payment_db`.
- **`I5` — Money**: All parent rows exist (never skipped), all outbox JSON payloads parse cleanly, every `REFUNDED` payment has a `payment.refunded` outbox row, and monetary amounts (`amountCents` / `totalAmountCents`) and `currency` match across seats, reservation items, reservations, order items, orders, payments, and refund events.
- **`I6` — Outbox Drained**: Every `outbox_events` row across `inventory_db`, `order_db`, and `payment_db` has `published_at IS NOT NULL` at quiescence.
- **`I7` — Dead Letters**: Dead-letter topic (`*-dlt`) offset growth equals the exact scenario-declared expected dead-letter count.
- **`I8` — Notifications**: Expected notifications are derived both from `order_db.orders` (`1` `order.confirmed` per `CONFIRMED` order; `1..2` `order.cancelled` per cancelled order) and from distinct `eventId`s on `order.events`, and `notification-service` counter diffs must equal those counts with `0` failed notifications, `0` unexpected duplicates, and `0` counter resets.

---

## 1. Running the Suite

`e2e-tests` is declared inside the opt-in Maven profile `e2e` (`-Pe2e`) so that standard unit/integration builds (`mvn verify`) never require a running Docker Compose stack.

### Make Targets
```bash
# Run all scenarios (S1–S13 + InvariantAuditor unit tests) against the running stack
make e2e

# Run @Tag("fast") functional scenarios (S1, S2, S3, S5, S6, S9, S10, S12, S13 + InvariantAuditorTest)
make e2e-fast

# Run @Tag("chaos") fault-injection scenarios and global audit (S4, S7, S8, S11, S13)
make e2e-chaos

# Reset the full containerized stack cleanly, wait until healthy, and run the complete E2E suite
make e2e-full

# Kubernetes local cluster targets (requires k3d and make k8s-forward)
make e2e-k8s-fast    # Run fast scenarios on Kubernetes (E2E_BACKEND=k8s, budget=180s)
make e2e-k8s-chaos   # Run chaos scenarios on Kubernetes (E2E_BACKEND=k8s, budget=180s)
make e2e-k8s         # Run fast suite -> reset k8s -> chaos suite
```

### Direct Maven Commands
```bash
# Run the full E2E suite and write e2e-tests/target/e2e-report.md
mvn -pl e2e-tests -Pe2e verify

# Run the full E2E suite and also copy the report to docs/e2e-report.md (only copied when 0 failures occur)
mvn -pl e2e-tests -Pe2e -De2e.publishReport=true verify

# Run only InvariantAuditor unit tests (no Docker stack required)
mvn -pl e2e-tests -Pe2e -Dtest=InvariantAuditorTest test

# Force-run any @Disabled test (if any scenario is annotated with @Disabled)
mvn -pl e2e-tests -Pe2e -Djunit.jupiter.conditions.deactivate=org.junit.*DisabledCondition test
```

---

## 2. Architecture & Support Package (`com.ticketing.e2e.support`)

- **`PreflightExtension`**: JUnit 5 `BeforeAllCallback` that fails fast if Docker CLI is unavailable, any container is unhealthy, any HTTP health endpoint (`/actuator/health`, `:8084/health/readiness`) is not `UP`, any PostgreSQL database (`:5433`, `:5434`, `:5435`) or Redis (`:6379`) is unreachable, or any required Kafka topic is missing on `localhost:29092`.
- **`Stack`**: High-level topology control façade that delegates to either `ComposeBackend` (default) or `K8sBackend` based on `E2EConfig.backend()`. Provides uniform lifecycle methods (`stop`, `start`, `kill`, `restart`, `pause`, `unpause`, `waitUntilHealthy`, `restoreAndWaitAllHealthy`).
- **`PlatformBackend` (`ComposeBackend` / `K8sBackend`)**: Abstracts runtime topology interactions. `ComposeBackend` executes `docker` CLI commands identically to the original harness. `K8sBackend` dynamically queries workload kinds and selectors (`kubectl`), deletes pods with `--grace-period=0 --force` for `kill`, scales to 0 while persisting replica counts for `stop`, restores replicas and awaits readiness for `start`, treats `pause`/`unpause` as `stop`/`start` (intentional equivalence), and checks supervised port-forward connectivity for infrastructure restarts.
- **`Api`**: Pure `java.net.http.HttpClient` wrapper targeting `gateway-service` (`http://localhost:8080`). Never throws on non-2xx responses and never fabricates HTTP 502/504 for client transport errors (returns `status=0, transportError=true`, which is accepted only during declared crash/outage windows in `S7` and `S8`).
- **`Db`**: Read-only JDBC queries (`PreparedStatement`) against `inventory_db` (`:5433`), `order_db` (`:5434`), and `payment_db` (`:5435`), plus the single documented write helper `forceReservationExpiry(UUID reservationId)` which backdates `created_at` and `expires_at` in `inventory_db.reservations` so hold-expiry tests do not wait 10 minutes. Also provides `Db.InMemoryStore` for fast unit-testing of `InvariantAuditor` across `I1`–`I8`.
- **`KafkaProbe`**: Implements `InvariantAuditor.KafkaAccess`. Uses `AdminClient`, `KafkaConsumer`, and `KafkaProducer` against `localhost:29092` to inspect topic end offsets, read message ranges (throwing if any active partition fails to reach target end offsets), verify consumer group `Stable` state and lag across all 5 main and 8 retry consumer groups, diff dead-letter topics (`*-dlt`), and inject duplicate/replayed envelopes for idempotency verification.
- **`Notifications`**: Implements `InvariantAuditor.NotificationAccess`. Scrapes `:8084/metrics`, parses `notification_events_total{event_type,outcome}` counters into snapshots and signed diffs, and detects counter resets (`after < before`).
- **`Fixtures`**: Creates isolated `ON_SALE` events, bulk seats, and distinct users with random UUIDs per scenario (`newUser` via gateway token endpoint and `newUserLocal` for bulk load tests so the gateway's token rate limit is not consumed by test setup).
- **`Quiescence`**: Uses Awaitility (never `Thread.sleep`) to await zero unpublished outbox rows across all 3 PostgreSQL databases, zero consumer group lag across all main and retry topics, and terminal/resting order and reservation states (including declared `REFUND_PENDING` orders from `S11`), emitting a full multi-service diagnostic dump on timeout.
- **`InvariantAuditor`**: Evaluates invariants `I1` through `I8` after every scenario and across all rows of all three databases in `S13`.
- **`ScenarioReportRecorder`**: Writes `e2e-tests/target/e2e-report.md` (and copies to `docs/e2e-report.md` only when `-De2e.publishReport=true` AND zero scenario failures occurred) with separate reconciled tracking of HTTP holds accepted (`201`), database reservations committed (`inventory_db`), and orders created (`order_db`).

---

## 3. Documented Assumptions & Findings

1. **Clean Baseline Enforcement**: `@BeforeAll` inspects `inventory_db.reservations` and `order_db.orders` before running any scenario and fails fast with instructions to run `make stack-reset` if any active `HELD` reservations or non-terminal orders exist from an earlier aborted run.
2. **Dev Token Rate Limit Protection (`S5`)**: `POST /api/v1/auth/token` on `gateway-service` enforces an IP-based rate limit of 10 burst / 1 req/s. `S5` mints at most 5 tokens through the gateway token endpoint (1 admin + 4 buyers) and signs the remaining tokens locally with `JWT_SECRET`.
3. **Multiple `order.cancelled` Events in Late-Payment Compensation (`S4`)**: When a hold expires while payment is pending (`S4`), `order-service` emits an `order.cancelled` event on transition to `CANCELLED_EXPIRED` and a second `order.cancelled` event with a new `eventId` when the late payment refund completes (`REFUND_PENDING -> CANCELLED_REFUNDED`). Invariant `I8` verifies that every cancelled order has between 1 and 2 `order.cancelled` outbox rows and that `notification-service` sent counters match the exact distinct `eventId` count on `order.events`.
4. **Active Product Finding in `S5` (`INV-001`)**: Under 64-thread contention (200 concurrent hold requests for 20 seats), `inventory-service` experiences Redis command timeouts and returns HTTP `500 INTERNAL_SERVER_ERROR` with `"Inventory concurrency gate is temporarily unavailable."`. Per project rules, `s5_noOversellUnderHighContentionLoad` keeps asserting that no hold returns `5xx` and remains enabled and failing until `INV-001` is fixed in the deployed container; see [FINDINGS.md](file:///Users/macbookpro/Desktop/Ticketing-platform/e2e-tests/FINDINGS.md).
5. **Kubernetes Failover Budget (`180s`) & Recovery Time Non-Comparability**: On Kubernetes, `E2E_FAILOVER_BUDGET_SECONDS` is set to `180s` (via `make e2e-k8s-*`) because pod crash recovery encompasses container termination, kubelet scheduling, init container readiness checks, Spring Boot application startup, Actuator health readiness probing (`failureThreshold=30, period=5s`), and Kafka consumer group rejoining. As a consequence, measured recovery times on Kubernetes are not directly comparable to Docker Compose restart metrics.
6. **Kubernetes Pause Equivalence**: Because Kubernetes pods cannot be paused without direct cgroup freezer manipulation on node hosts, `pause` is mapped to scaling replicas to `0`, and `unpause` scales back to the remembered replica count and awaits readiness. This fulfills the scenario requirement of ensuring the consumer is absent while events are produced.
7. **Supervised Host Port Forwarding**: Host-side database (`5433..5435`), Redis (`6379`), and Kafka (`29092`) access over Kubernetes relies on the background supervisor in `scripts/k8s-forward.sh`. When infrastructure pods recycle during chaos testing, the supervisor auto-reconnects and `K8sBackend.waitUntilHealthy` polls the local TCP port before proceeding.
