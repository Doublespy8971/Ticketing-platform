# Ticketing Platform k6 Load-Test Harness

Reproducible, containerized k6 load-test suite for the flash-sale ticketing platform with independent post-run database/Kafka/container auditing and automated Markdown reporting.

## 1. Structure

```text
loadtest/
├── lib/
│   ├── jwt.js                 # Local HS256 JWT minting (matches gateway JwtTokenService) + 5000-user SharedArray pool
│   ├── api.js                 # Admin setup (2 events x 5000 seats) and customer HTTP helpers
│   ├── checks.js              # Response validation checks and Retry-After parser
│   └── metrics.js             # Custom k6 Counters, Rates, and Trends
├── flash-sale.js              # Scenario A: ramping-arrival-rate flash-sale contention on POST /api/v1/reservations
├── checkout-journey.js        # Scenario B: constant-arrival-rate end-to-end hold -> checkout -> poll saga
├── results/
│   └── .gitkeep               # Per-run artifacts stored in loadtest/results/<UTC timestamp>/ (git-ignored)
└── README.md
```

Supporting scripts in `scripts/`:

- `scripts/loadtest-run.sh`: Orchestrates pinned `grafana/k6` Docker container on `ticketing-net`, reads `JWT_SECRET` from `infra/.env` at runtime without logging/writing it, manages monitoring, audit, and reporting.
- `scripts/loadtest-monitor.sh`: Captures container restart/OOM baselines, Kafka DLT offset baselines, and 5-second `docker stats --no-stream` CSV time series.
- `scripts/loadtest-audit.sh`: Waits up to 120s for quiescence (outbox published + Kafka lag zero), then independently verifies invariants `A1..A9` against all 3 PostgreSQL databases, Kafka broker tools, container inspect snapshots, and k6 summary JSONs.
- `scripts/test-loadtest-audit.sh`: Self-test using isolated scratch databases proving clean state passes and corrupted state (duplicate active seat item) fails `A1`.
- `scripts/loadtest-report.py`: Standard-library-only Markdown report generator (`results/<ts>/report.md`) with Prometheus run-window queries and publish gating.
- `scripts/test_loadtest_report.py`: Unit tests (`python3 -m unittest scripts/test_loadtest_report.py`).

---

## 2. How to Run

### Prerequisites

- Docker Desktop / Docker Engine running.
- No host installation of `k6` is required; k6 runs from the pinned Docker image:
  `grafana/k6:0.54.0@sha256:1f40432b1cbe7234e977f96c362c9bc550a2d2b583d014dd8669fe40d3e9e755`.

### Common Commands

```bash
# Quick smoke load test (20% scale) on a fresh stack + observability
LOAD_SCALE=0.2 make load-fresh

# Full-scale run on a fresh stack (stack-reset -> obs-up -> flash -> journey -> audit -> report)
make load-fresh

# Run full suite against an already-running stack
make load-all

# Run individual scenarios
make load-flash
make load-journey

# Re-run audit or regenerate report for latest (or explicit RESULTS_DIR) run
make load-audit
make load-report
RESULTS_DIR=loadtest/results/<ts> make load-report

# Publish docs/load-test-report.md ONLY if every threshold and every audit item A1..A9 passed
LOAD_PUBLISH=true RESULTS_DIR=loadtest/results/<ts> make load-report

# Self-tests (audit script, report generator & comparison tool)
bash scripts/test-loadtest-audit.sh
python3 -m unittest scripts/test_loadtest_report.py
python3 -m unittest scripts/test_loadtest_compare.py

# Verify G1GC and Docker VM memory headroom across all Java services
make jvm-check

# Compare baseline vs after result directories (median and range)
python3 scripts/loadtest-compare.py --baseline loadtest/results/20261121T190134Z --after loadtest/results/<ts1> loadtest/results/<ts2> loadtest/results/<ts3>
```

### Environment Variables

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `LOAD_SCALE` | `1.0` | Scales scenario rates, durations, and VU allocations (e.g. `0.2` for a quick smoke run). |
| `USER_POOL_SIZE` | `5000` | Number of distinct pre-minted USER JWTs in `SharedArray`. |
| `SEATS_PER_EVENT` | `5000` | Unique seats bulk-created per event during `setup()`. |
| `FLASH_STAGE1_RATE` / `FLASH_STAGE2_RATE` | `150` / `400` | Plateau target arrival rates (req/s) for Scenario A (scaled by `LOAD_SCALE` if unset). |
| `FLASH_PRE_ALLOCATED_VUS` / `FLASH_MAX_VUS` | `150` / `500` | VU pool sizing for Scenario A. |
| `JOURNEY_RATE` / `JOURNEY_DURATION_SEC` | `25` / `180` | Constant arrival rate (journeys/s) and duration for Scenario B. |
| `JOURNEY_PRE_ALLOCATED_VUS` / `JOURNEY_MAX_VUS` | `100` / `300` | VU pool sizing for Scenario B. |
| `LOAD_PUBLISH` | `false` | When `true`, copies `report.md` to `docs/load-test-report.md` iff all thresholds and all audit items pass. |
| `RESULTS_DIR` | `loadtest/results/<UTC ts>` | Override results output directory. |

---

## 3. How It Works

### Local HS256 JWT Minting

`POST /api/v1/auth/token` is IP rate-limited (10 burst, 1 req/s refill). To avoid bottlenecking setup or runtime on token issuance, `loadtest/lib/jwt.js` mints HS256 JWTs locally using `k6/crypto` and `k6/encoding` with `JWT_SECRET` passed securely via environment variable from `infra/.env` (never written to disk or printed). Claims match `gateway-service`'s `JwtTokenService`: canonical UUID `sub`, allowlisted `roles`, integral epoch-second `iat`/`exp` (7200s TTL, compatible with the 30s clock skew).

A pool of 5,000 distinct user tokens is pre-generated once in the init stage via `SharedArray`. In `setup()`, the harness computes expected per-user rate against the gateway's documented token bucket (`5 req/s` sustained, `10` burst per user in `docs/arch/08-redis.md`) and aborts if the configuration would breach it:

- **Scenario A**: peak `400 req/s / 5000 users = 0.08 req/s` per user (`<< 5 req/s`).
- **Scenario B**: unique user per journey, polling every `250 ms` (`4.0 req/s` active `< 5 req/s` sustained).

### Setup

`setupTwoEvents` creates two events (`loadtest-<run_id>-flash` and `loadtest-<run_id>-journey`) with 5,000 unique seats each via `POST /api/v1/admin/events/{eventId}/seats/bulk`, past `dropTime`, transitions both to `ON_SALE`, and waits until seats are visible on `GET /api/v1/events/{eventId}/seats`. In `make load-all`, Scenario A creates both events and Scenario B reuses the second event via `REUSE_EVENT_ID`.

### Scenario A — Flash Sale (`flash-sale.js`)

- `ramping-arrival-rate`: `10s -> 150 req/s`, `60s @ 150`, `15s -> 400 req/s`, `60s @ 400`, `10s -> 0`.
- Random user from 5,000 pool + uniform random seat over 5,000 seats -> real seat contention.
- Expected statuses: `201`, `409`, `429`. Any other status (including any `5xx` or timeout) increments `hold_unexpected`.
- Thresholds:
  - `http_req_duration{name:hold} p(99)<300` (documented SLO)
  - `hold_unexpected count==0`
  - `hold_5xx rate<0.001` (99.9% availability SLO)
  - `hold_rate_limited_rate rate<0.01` (<1% rate-limited ratio; tracked alongside `hold_rate_limited` counter because k6 Counter `rate` is events/sec rather than ratio)

### Scenario B — Checkout Journey (`checkout-journey.js`)

- `constant-arrival-rate`: `25 journeys/s` for `180s` (`4,500` journeys total, each assigned a unique seat index `exec.scenario.iterationInTest` and unique user).
- Mix (deterministic mod 20): 90% `tok_success`, 5% `tok_decline`, 5% abandoned holds (no checkout, no forced expiry, no polling).
- Handles the documented early-checkout race (`409 ORDER_NOT_READY` with `Retry-After`) by sleeping `Retry-After` seconds, counting `order_not_ready_retries`, and retrying checkout with the same `Idempotency-Key`.
- Polls `GET /api/v1/orders/{id}` every `250 ms` up to `30 s` until `CONFIRMED` or `CANCELLED_PAYMENT_FAILED`.
- Thresholds:
  - `http_req_duration{name:checkout} p(99)<500` (documented SLO)
  - `http_req_duration{name:hold} p(99)<300` (documented SLO)
  - `saga_latency p(95)<5000` (**chosen target**, not a formal SLO in `docs/arch/09-conventions.md`; accounts for `MockPaymentProvider` 50–300ms latency, outbox 200ms polling hops, and 250ms client poll interval)
  - `journeys_timed_out count==0`
  - `unexpected_statuses count==0`

### Observability Integration

- Prometheus runs with `--web.enable-remote-write-receiver` in `infra/docker-compose.observability.yml`.
- k6 pushes live metrics via `-o experimental-prometheus-rw` (`K6_PROMETHEUS_RW_SERVER_URL=http://prometheus:9090/api/v1/write`, `K6_PROMETHEUS_RW_TREND_STATS=p(50),p(95),p(99),avg,max`).

---

## 4. How to Read the Report

Every figure in `loadtest/results/<ts>/report.md` is computed programmatically from k6 JSON summaries, Prometheus HTTP API queries, `docker-stats.csv`, container snapshots, and `audit.json`:

1. **Overall & Thresholds Verdicts**: Computed strictly from k6 threshold `ok` booleans and `audit.json`. A single failing threshold or audit item turns the overall verdict to `FAIL` and blocks publishing `docs/load-test-report.md`.
2. **Throughput Achieved**: Computed from actual requests counted during each plateau stage divided by plateau duration (Scenario A) and completed iterations divided by target duration (Scenario B) — never copied from target rates.
3. **Audit (`A1..A9`)**: Independently verified after quiescence (all outbox published & all Kafka consumer group lags zero):
   - `A1`: Zero seats with >1 active (`HELD`/`CONFIRMED`) reservation item.
   - `A2`: `SOLD` seats == `CONFIRMED` reservation items == `CONFIRMED` order items <= created seats.
   - `A3`: Zero unpublished outbox rows across `inventory_db`, `order_db`, `payment_db`.
   - `A4`: Zero Kafka consumer group lag across all groups.
   - `A5`: Zero growth in `*-dlt` end offsets compared to pre-run snapshot.
   - `A6`: At most one payment per order and zero `SUCCEEDED` payments on `CANCELLED_*` orders.
   - `A7`: Zero orders remaining in `PAYMENT_PENDING` or `REFUND_PENDING` after quiescence (`RESERVED` holds from Scenario A and the 5% abandoned holds in Scenario B are valid resting states awaiting their 10-minute TTL).
   - `A8`: Sum of k6 `201` holds (`hold_created`) equals reservations inserted in `inventory_db` for the run's events.
   - `A9`: Zero container restarts and zero OOM kills during the run.

---

## 5. Assumptions & Limits

1. **Single-node Docker on a laptop**: All services, infrastructure (Kafka, Redis, 3 PostgreSQL instances), observability stack, and k6 share one Docker VM and host CPU/RAM. Results are **not** production capacity estimates and are only meaningful relative to each other across identical local runs.
2. **Threshold metric naming (`hold_rate_limited_rate`)**: Because k6 `Counter` metrics report `rate` as count/second rather than ratio `[0, 1]`, Scenario A defines both `hold_rate_limited` (`Counter`) for exact 429 counts and `hold_rate_limited_rate` (`Rate`) for the `< 0.01` (1%) ratio threshold.
3. **`saga_latency p(95)<5000` is a chosen target**, not an architecture SLO; formal SLOs in `docs/arch/09-conventions.md` cover hold and checkout HTTP latency/availability and order materialization lag.
4. **Resting `RESERVED` orders**: Un-checked-out holds from Scenario A (up to 5,000) and the 5% abandoned holds from Scenario B remain in `RESERVED` state until their 10-minute `expires_at` TTL is swept by `ExpirySweeper`; `A7` treats `RESERVED` as a documented valid resting state and strictly forbids `PAYMENT_PENDING` / `REFUND_PENDING`.
5. **Bulk seat creation chunking**: `setupTwoEvents` chunks bulk inserts at 2,500 seats per request (2 requests per 5,000-seat event) with a 250ms pause to remain well under both the gateway's 2MB payload limit and 10s response timeout.
6. **JVM G1GC & Memory Limit Trade-off (`512m` -> `768m`)**: Raising the 4 Java service containers (`gateway-service`, `inventory-service`, `order-service`, `payment-service`) from `512m` to `768m` with `-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError` (overridable via `JAVA_OPTS_EXTRA`) eliminates OpenJDK 21's automatic Serial GC fallback and reduces GC pause duration/frequency, at the cost of `+1,024 MiB` (`+1 GiB`) higher configured memory commitment on the local Docker VM (`4 × 768 MiB + 256 MiB notification-service = 3,328 MiB`, or `41.9%` of a `7,937 MiB` Docker Desktop VM, leaving ~4.5 GiB for Kafka, PostgreSQL x3, Redis, Prometheus, Grafana, exporters, and k6).
7. **Consumer Concurrency & Per-Key Ordering**: `ticketing.consumer.concurrency` defaults to `1` (validated `1..12` matching the 12 topic partitions) and is configured to `6` for `payment-service`, `3` for `order-service`, and `3` for `inventory-service`. Per-key ordering is preserved because Kafka never assigns a partition to more than one consumer thread within a consumer group and all events for an order share key `orderId`; parallel consumption is safe because `payment-service` passes deterministic idempotency keys (`orderId` / `paymentId`) to the payment provider and guards persistence with `processed_events` + `payments(order_id)` unique constraints.
8. **PostgreSQL `max_connections` Arithmetic**: Compose PostgreSQL 16 containers run with default `max_connections = 100`. With `HIKARI_MAX_POOL_SIZE` defaults (`20` inventory, `15` order, `15` payment) and unchanged HikariCP connection timeout (`30000ms`), worst-case connection usage per database is:
   - `postgres-inventory` (`inventory_db`): `20` (Hikari pool) + `1` (outbox unpooled advisory-lock leader) + `2` (`postgres-inventory-exporter`) + `3` (PostgreSQL `superuser_reserved_connections`) = **`26 / 100`** (`74` headroom).
   - `postgres-order` (`order_db`): `15` (Hikari pool) + `1` (outbox leader) + `2` (`postgres-order-exporter`) + `3` (superuser reserved) = **`21 / 100`** (`79` headroom).
   - `postgres-payment` (`payment_db`): `15` (Hikari pool) + `1` (outbox leader) + `2` (`postgres-payment-exporter`) + `3` (superuser reserved) = **`21 / 100`** (`79` headroom).
9. **macOS Idle Sleep Prevention**: Unattended full-scale runs (~7 minutes each) on macOS should be wrapped with `caffeinate -is` so the host does not enter idle sleep mid-run (which causes multi-minute clock jumps in `docker-stats.csv`, k6 latencies, and GC wall-clock counters). Two earlier unattended runs (`20261122T121906Z` with a `487s` sleep gap and `20261122T123747Z` with an `85s` sleep gap) were retained in `loadtest/results/` for transparency and replaced by clean `caffeinate -is` runs (`20261122T125331Z` and `20261122T130512Z`) alongside `20261122T120819Z` for the 3-run median/range comparison.
