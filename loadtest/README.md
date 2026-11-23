# Ticketing Platform k6 Load-Test Harness

Reproducible, containerized k6 load-test suite for the flash-sale ticketing platform with quiet-host preflight gating, external seat seeding (`SharedArray`), dedicated JVM warm-up, breakpoint + throughput knee discovery, steady-state & sustained capacity verification, 20-minute soak drift detection, 10-second windowed tail-latency correlation analysis, host-noise invalidation, independent post-run database/Kafka/container auditing, and automated Markdown reporting.

## 1. Structure

```text
loadtest/
├── lib/
│   ├── jwt.js                 # Local HS256 JWT minting + 5000-user SharedArray pool
│   ├── api.js                 # Admin setup and customer HTTP helpers
│   ├── checks.js              # Response validation checks and Retry-After parser
│   ├── metrics.js             # Custom k6 Counters (incl. hold_unknown_outcome, warmup_*), Rates, Trends & outcome tagger
│   ├── seats.js               # SharedArray loader for external results/<ts>/seats.json manifest
│   └── thresholds.js          # Shared threshold definitions (verified identical to overload scenarios)
├── flash-sale.js              # Overload profile A: 150 -> 400 req/s flash-sale contention (untouched)
├── checkout-journey.js        # Overload profile B: 25 journeys/s end-to-end saga (untouched)
├── breakpoint-hold.js         # Warm-up + staircase hold breakpoint (25..600 req/s, 30s/step, ample/scarce, outcome tag)
├── breakpoint-journey.js      # Warm-up + staircase journey breakpoint (2..40 j/s, 45s/step, unique seats)
├── steady-hold.js             # Warm-up + single steady-state hold plateau (RATE & MINUTES configurable, ample default)
├── sustained-hold.js          # Warm-up + 5-minute sustained hold plateau at derived sustainable_rps
├── sustained-journey.js       # Warm-up + 5-minute sustained journey plateau at derived sustainable_jps
├── soak.js                    # Warm-up + 20-minute soak at derived sustainable_jps (SOAK_MINUTES configurable)
├── sustainable-profile.json   # Script-generated sustainable rates + derivation (never hand-edited)
├── results/
│   └── .gitkeep               # Per-run artifacts in loadtest/results/<UTC timestamp>/ (git-ignored)
├── FINDINGS.md                # Honest record of measured bottlenecks, mitigations, and outcomes
└── README.md
```

Supporting scripts in `scripts/`:

- `scripts/loadtest-run.sh`: Orchestrates quiet-host preflight (`preflight`), external seat seeding (`loadtest-seed.py`), pinned `grafana/k6` Docker container on `ticketing-net`, monitoring, audit, breakpoint + knee analysis, tail-latency analysis, soak drift analysis, and reporting.
- `scripts/loadtest-seed.py` (Python stdlib): Mints a local HS256 `ADMIN` JWT from `infra/.env` (never printed), creates `ON_SALE` events via the gateway admin API, bulk-creates seats in 2,500-seat chunks (`<= 5,000` documented bulk limit), enforces the `150,000`-seat hard cap (automatically lowering staircase `MAX` when exceeded and recording the adjustment), and writes `results/<ts>/seats.json` + `seed-meta.json`.
- `scripts/loadtest-monitor.sh`: Captures container restart/OOM baselines, Kafka DLT offset baselines, 4-second `docker stats --no-stream` CSV time series (including k6 peak memory), and flags any timestamp gap `> 15s` as `results/<ts>/INVALID`.
- `scripts/loadtest-audit.sh`: Waits up to 120s for quiescence (outbox published + Kafka lag zero), then independently verifies invariants `A1..A9` against all 3 PostgreSQL databases, Kafka broker tools, container snapshots, and k6 summary JSONs — including the 60s 1-second-polling `A3` stuck-row check and bounded `A8` hold reconciliation (`C <= R <= C + U`, summing both measured and warm-up holds).
- `scripts/loadtest-breakpoint.py`: Computes per-step metrics (after warm-up), overall & per-step `outcome=created` hold p99, breakpoint (highest step where this step and all lower steps pass SLO/error/unknown/drop criteria), derived sustainable rate (`70%` rounded down to step multiple, `>= 1 step`), and **throughput knee** (first step where achieved rate `< 95%` of target or dropped iterations `> 1%` of planned step requests).
- `scripts/loadtest-tail-analysis.py` (Python stdlib): Queries Prometheus range API (`step=10s`) over the measured window (excluding warm-up) and aligns `docker-stats.csv` CPU%, lists the 10 worst hold p99 windows side-by-side with GC pause, CPU%, HikariCP pending, Redis Lua p99, outbox oldest age, and Kafka lag, ranks signals by Pearson `r` (with paired window count `n`), and prints a data-supported coincidence summary plus a correlation-is-not-causation reminder.
- `scripts/loadtest-soak-report.py`: Computes 2-minute windowed Prometheus + `docker-stats.csv` time series over a soak run and detects latency (`>1.3x`), old-gen heap (`>20%` positive OLS growth), and monotonic queue/lag/Hikari drift over the last 10 minutes.
- `scripts/loadtest-report.py`: Generates per-run `results/<ts>/report.md` (embedding `host-state.txt`, warm-up info, created-only p99, `breakpoint.md`, and `tail-analysis.md`) and enforces the publish gate for `docs/load-test-report.md`.
- `scripts/loadtest-compare.py`: Compares baseline vs valid after runs (median & min..max range), automatically excluding `INVALID` runs.

---

## 2. How to Run

### Prerequisites

- Docker Desktop / Docker Engine running. No host `k6` installation required (`grafana/k6:0.54.0@sha256:1f40432b1cbe7234e977f96c362c9bc550a2d2b583d014dd8669fe40d3e9e755`).
- Host plugged into **AC Power** with 1-minute load average `<= 3.0` (enforced by the quiet-host preflight gate).
- All long-running load targets automatically wrap execution in `caffeinate -is` when available on macOS to prevent idle sleep.

### Commands

```bash
# 0. Quiet-host preflight check (power source, 60s load avg <= 3.0, Docker VM idle memory)
make load-preflight

# 1. Breakpoint discovery (preflight -> seed -> warm-up -> staircase -> audit -> breakpoint/knee -> tail analysis -> report)
SEAT_SUPPLY=ample make load-breakpoint-hold
make load-breakpoint-journey
make load-breakpoint            # both staircases (stack-reset between) & derive sustainable-profile.json

# 2. Steady-state hold experiment (preflight -> seed -> warm-up -> plateau -> audit -> tail analysis -> report)
RATE=100 MINUTES=5 SEAT_SUPPLY=ample make load-steady

# 3. Sustained profile verification (reads loadtest/sustainable-profile.json)
make load-sustained             # single run against running stack
make load-sustained-x3          # 3 VALID runs (stack-reset + obs-up between, up to 6 attempts if INVALID)

# 4. 20-minute soak stability & drift check (SOAK_MINUTES configurable)
make load-soak
SOAK_MINUTES=30 make load-soak

# 5. Overload profile (original scenarios — intentionally beyond single-node laptop capacity)
LOAD_SCALE=0.2 make load-fresh  # 20% smoke run
make load-fresh                 # full-scale overload run (400 req/s flash + 25 j/s journey)
make load-all / make load-flash / make load-journey

# 6. Audit, reporting, JVM check & self-tests
make load-audit
make load-report
make jvm-check
bash scripts/test-loadtest-audit.sh
python3 -m unittest discover -s scripts -p 'test_loadtest_*.py' -v
```

### Environment Variables

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `SEAT_SUPPLY` | `ample` (hold breakpoint / steady / sustained) or `scarce` | `ample`: each iteration targets a distinct unused seat from `seats.json` (real holds). `scarce`: uniform random over the pool (contention / `409`s). |
| `WARMUP_SECONDS` / `WARMUP_RATE` | Hold `90` / `50`; Journey `90` / `4` | Dedicated `step=warmup` scenario run before measured traffic (set `0` to disable). Excluded from thresholds, breakpoint/knee, and measured tables. |
| `RATE` / `MINUTES` | `100` / `5` | Target req/s and plateau duration (minutes) for `steady-hold.js` (`make load-steady`). |
| `START` / `STEP` / `MAX` / `STEP_SECONDS` | Hold `25/25/600/30`; Journey `2/2/40/45` | Override staircase parameters (bounded to `<= 40` steps; in ample mode `MAX` is auto-lowered if planned seats exceed `150,000`, e.g., `600 -> 450`). |
| `ALLOW_BATTERY` | `0` | Must be `0` for publishable runs. Setting `ALLOW_BATTERY=1` bypasses battery refusal and writes `results/<ts>/NOT_PUBLISHABLE`. |
| `PREFLIGHT_SETTLE_TIMEOUT_SECONDS` / `PREFLIGHT_MAX_LOAD` | `360` / `3.0` | Maximum duration (s) to wait for 1-minute host load average to settle and maximum allowed idle 1-min load average. |
| `LOAD_SCALE` | `1.0` | Scales overload scenario rates (`flash-sale.js`, `checkout-journey.js`). |
| `SOAK_MINUTES` | `20` | Plateau duration (minutes) for `soak.js`. |
| `USER_POOL_SIZE` / `SEATS_PER_EVENT` | `5000` / `5000` | Pre-minted user JWT pool size and seats per event (bulk-created in 2,500 chunks). |
| `LOAD_PUBLISH` | `false` | Enables copying `report.md` to `docs/load-test-report.md` when the publish gate is satisfied. |
| `RESULTS_DIR` | `loadtest/results/<UTC ts>` | Override results output directory. |

---

## 3. Reading Results

### Quiet-Host Preflight Gate (`scripts/loadtest-run.sh preflight`)

Before any measured run (`breakpoint-*`, `steady`, `sustained`, `soak`), the runner inspects the host (macOS & Linux aware) and writes `results/<ts>/host-state.txt` (embedded in `report.md` Section 1):

1. **Power source**: Refuses to start (exit `2`) on battery power unless `ALLOW_BATTERY=1` is set (which creates `results/<ts>/NOT_PUBLISHABLE` and blocks publishing).
2. **Host idle load settle phase**: Samples 1-minute load average every 15 seconds for up to 6 minutes after the stack is up; passes as soon as three consecutive samples are `<= 3.0`; refuses to start (exit `2`) if the limit is not reached within 6 minutes, printing the load trend and top 5 CPU processes by name.
3. **Docker VM capacity**: Prints Docker VM CPUs and memory, sum of configured container memory limits, and sum of actual idle container memory (warns if actual idle memory exceeds `70%` of VM memory).

### External Seat Seeding, Warm-up & `outcome` Tag

- **External seeding (`scripts/loadtest-seed.py`)**: Creates events and seats outside k6 before k6 starts and writes `results/<ts>/seats.json`. All new scenarios load `seats.json` via k6 `SharedArray` in the init context (keeping k6 RAM well below `1 GiB`). Total seeded seats are hard-capped at **`150,000`**; if an `ample` breakpoint staircase plus warm-up plus `10%` margin exceeds `150,000` (e.g., default `25..600` = `252,450` seats), the seeder automatically lowers `MAX` to the highest step that fits (`450` req/s = `146,025` seats across `30` events) and records the lowering in `seats.json` and the report.
- **Warm-up (`step=warmup`)**: Runs `90s @ 50 req/s` (hold) or `90s @ 4 j/s` (journey) using disjoint seats and users before measured traffic so JVM JIT and pools warm up. Excluded from all thresholds, breakpoint/knee calculations, tail-latency windows, and measured summary tables; printed separately as informational context while `A8` reconciles its real database reservations.
- **Bounded `outcome` tag**: Hold requests in the new scenarios carry `outcome` in `{created, conflict, rate_limited, error}`. The hold SLO (`p(99) < 300 ms`) is evaluated on the whole endpoint (`name=hold`), and `outcome=created` p99 (`hold_duration{outcome:created}`) is printed separately alongside it.

### Breakpoint & Throughput Knee (`scripts/loadtest-breakpoint.py`)

- **Breakpoint**: Highest measured step (after warm-up) where this step and every lower step satisfy hold `p99 < 300 ms` (journey also checkout `p99 < 500 ms`, saga `p95 < 5000 ms`), error rate `< 0.1%`, `hold_unknown_outcome == 0`, and `dropped_iterations == 0`. Sustainable rate = `max(step_size, floor((0.70 * breakpoint) / step_size) * step_size)`. `NO BREAKPOINT FOUND` remains valid if `s01` fails.
- **Knee**: First measured step where `achieved_rate < 0.95 * target_rate` OR `dropped_iterations > 0.01 * planned_step_requests`, printed with its exact derivation. For `make load-steady`, the knee-derived rate is `max(25, floor((0.70 * knee_rate) / 25) * 25)` (or `100 req/s` if no knee was found).

### Tail-Latency Correlation Analysis (`scripts/loadtest-tail-analysis.py`)

Over the measured window (warm-up excluded), queries Prometheus at `step=10s` for server-side `inventory-service` and `gateway-service` hold `p99`, JVM GC pause max and sum rate, HikariCP pending connections, `ticketing_redis_lua_duration_seconds` p99, outbox oldest pending age, and Kafka consumer lag, joined with `docker-stats.csv` CPU% for `inventory-service`, `gateway-service`, and `kafka`. Prints the 10 worst p99 windows side-by-side, ranks signals by Pearson `r` (with paired sample count `n`), and outputs a plain data-supported coincidence summary with a reminder that correlation is not causation.

### Refined Audit Rules `A3` (Stuck-Row Check) & `A8` (Unknown-Outcome Holds)

- **`A3` (resolves `LOAD-008`)**: Polls `outbox_events` across `inventory_db`, `order_db`, and `payment_db` every 1 second for up to 60 seconds. **PASS** iff zero unpublished rows are observed on at least one poll within 60 s **AND** no unpublished row older than 30 seconds exists at any poll. **FAIL** otherwise, printing the condition and every offending `(database, event_type, age)`. This eliminates false failures from benign 10-minute `ExpirySweeper` rows while reliably catching stuck outbox publishers.
- **`A8` (resolves `LOAD-006`)**: Reconciles `R` (`inventory_db` reservations for the run's events) against `C` (`hold_created + warmup_hold_created`) and `U` (`hold_unknown_outcome + warmup_hold_unknown_outcome`): **PASS** iff `C <= R <= C + U` (exact `R == C` when `U == 0`); **FAIL** on phantom holds (`R < C`) or unexplained holds (`R > C + U`).

### Publishing Gate (`docs/load-test-report.md`)

- With `LOAD_PUBLISH=true`, `scripts/loadtest-report.py` publishes `docs/load-test-report.md` when:
  1. The run is **VALID** (no `INVALID` gap marker),
  2. The run is **publishable** (no `NOT_PUBLISHABLE` marker from `ALLOW_BATTERY=1`), and
  3. The post-run audit (`A1..A9` + quiescence) **PASSED completely** — **even if some k6 SLO thresholds failed**.
- The top lines of every report state the computed **SLO Verdict** (`SLO MET` or `SLO NOT MET (failed: ...)`) and **Post-Run Audit Verdict**, and every threshold is rendered with its measured value and `PASS` or `FAIL`. No verdict is ever hardcoded.
- Any failed audit, missing audit, `INVALID` run, or `NOT_PUBLISHABLE` run **never** publishes.

### Overload Profile (`flash-sale.js` & `checkout-journey.js`)

The original scenarios (`400 req/s` flash sale and `25 journeys/s` checkout) remain completely unchanged (thresholds, stages, and rates untouched) as the **overload profile** to prove system integrity beyond single-node laptop capacity.

---

## 4. Assumptions & Limits

1. **Single-node Docker on a laptop**: All application services, Kafka, Redis, 3 PostgreSQL instances, Prometheus, Grafana, exporters, and k6 share one Docker VM (`8` CPUs, `~9.7 GiB` RAM on current host). Results are reproducible local-host baselines for regression tracking, **not** production capacity estimates.
2. **150,000-seat external seeding hard cap**: Bulk creation uses `2,500`-seat chunks (`<= 5,000` API bulk limit) across `5,000`-seat `ON_SALE` events. For default `breakpoint-hold` in `ample` mode (`25..600` req/s, 30 s/step + 90 s @ 50 req/s warm-up + 10% margin = `252,450` seats), `scripts/loadtest-seed.py` automatically caps staircase `MAX` at `450` req/s (`146,025` seats across `30` events) so seeding time, PostgreSQL size, and k6 `SharedArray` memory stay bounded.
3. **Gateway `uri` label fallback in tail-latency analysis**: Spring Cloud Gateway records proxied routes under `uri="UNKNOWN"` in `http_server_requests_seconds_bucket`; `scripts/loadtest-tail-analysis.py` queries `uri="/api/v1/reservations"` first and falls back to `service="gateway-service",method="POST"`, using `inventory-service` `uri="/api/v1/reservations"` as primary `p99` target.
4. **Threshold metric naming (`hold_rate_limited_rate`)**: Because k6 `Counter` reports `rate` as count/second rather than ratio `[0, 1]`, hold scenarios track both `hold_rate_limited` (`Counter`) and `hold_rate_limited_rate` (`Rate`, threshold `< 0.01`). Non-aborting sub-metric export thresholds (`max>=0`, `count>=0`, `rate>=0`) are excluded from SLO threshold pass/fail counts in `scripts/loadtest-report.py`.
5. **`saga_latency p(95)<5000` is a chosen target**, not a formal architecture SLO in `docs/arch/09-conventions.md`.
6. **JVM G1GC & Memory Limits (`768m`)**: Java services run G1GC (`-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError`, overridable via `JAVA_OPTS_EXTRA`) at `768 MiB` each (`3,328 MiB` total configured limits across the 5 application containers).
7. **Consumer Concurrency & PostgreSQL `max_connections`**: `ticketing.consumer.concurrency` (`1..12`, default `1`) is configured to `6` (`payment`), `3` (`order`), `3` (`inventory`) preserving per-key partition ordering. `HIKARI_MAX_POOL_SIZE` defaults (`20/15/15`) stay within `max_connections=100`.
8. **macOS Idle Load & Sleep Prevention**: Long-running targets wrap execution in `caffeinate -is` when installed. On macOS Virtualization.framework, running all 17 stack + observability containers plus desktop apps can keep host 1-minute load average above `3.0`; the preflight gate honestly refuses to run under that condition rather than generating noisy measurements.
