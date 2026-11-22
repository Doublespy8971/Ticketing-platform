# Ticketing Platform k6 Load-Test Harness

Reproducible, containerized k6 load-test suite for the flash-sale ticketing platform with breakpoint discovery, derived sustained capacity verification, 20-minute soak drift detection, host-noise invalidation, independent post-run database/Kafka/container auditing, and automated Markdown reporting.

## 1. Structure

```text
loadtest/
├── lib/
│   ├── jwt.js                 # Local HS256 JWT minting + 5000-user SharedArray pool
│   ├── api.js                 # Admin setup (chunked 5000-seat events) and customer HTTP helpers
│   ├── checks.js              # Response validation checks and Retry-After parser
│   ├── metrics.js             # Custom k6 Counters (incl. hold_unknown_outcome), Rates, and Trends
│   └── thresholds.js          # Shared threshold definitions (verified identical to overload scenarios)
├── flash-sale.js              # Overload profile A: 150 -> 400 req/s flash-sale contention (untouched)
├── checkout-journey.js        # Overload profile B: 25 journeys/s end-to-end saga (untouched)
├── breakpoint-hold.js         # Staircase hold breakpoint (25..600 req/s, 30s/step, bounded step tags)
├── breakpoint-journey.js      # Staircase journey breakpoint (2..40 j/s, 45s/step, unique seats)
├── sustained-hold.js          # 5-minute sustained hold plateau at derived sustainable_rps
├── sustained-journey.js       # 5-minute sustained journey plateau at derived sustainable_jps
├── soak.js                    # 20-minute soak at derived sustainable_jps (SOAK_MINUTES configurable)
├── sustainable-profile.json   # Script-generated sustainable rates + derivation (never hand-edited)
├── results/
│   └── .gitkeep               # Per-run artifacts in loadtest/results/<UTC timestamp>/ (git-ignored)
├── FINDINGS.md                # Honest record of measured bottlenecks, mitigations, and outcomes
└── README.md
```

Supporting scripts in `scripts/`:

- `scripts/loadtest-run.sh`: Orchestrates pinned `grafana/k6` Docker container on `ticketing-net`, prints host preflight state (power source + battery warning, Docker VM CPU/RAM, running containers, load average), manages monitoring, audit, breakpoint analysis, soak drift analysis, and reporting.
- `scripts/loadtest-monitor.sh`: Captures container restart/OOM baselines, Kafka DLT offset baselines, 4-second `docker stats --no-stream` CSV time series, and flags any timestamp gap `> 15s` as `results/<ts>/INVALID`.
- `scripts/loadtest-audit.sh`: Waits up to 120s for quiescence (outbox published + Kafka lag zero), then independently verifies invariants `A1..A9` against all 3 PostgreSQL databases, Kafka broker tools, container snapshots, and k6 summary JSONs.
- `scripts/loadtest-breakpoint.py`: Computes per-step metrics, breakpoint (highest step where this step and all lower steps pass SLO/error/unknown/drop criteria), and derived sustainable rate (`70%` rounded down to step multiple, `>= 1 step`), writing `results/<ts>/breakpoint.json` and `loadtest/sustainable-profile.json`.
- `scripts/loadtest-soak-report.py`: Computes 2-minute windowed Prometheus + `docker-stats.csv` time series over a soak run and detects latency (`>1.3x`), old-gen heap (`>20%` positive OLS growth), and monotonic queue/lag/Hikari drift over the last 10 minutes.
- `scripts/loadtest-report.py`: Generates per-run `results/<ts>/report.md` and enforces the strict publish gate for `docs/load-test-report.md` (only from 3 valid, fully passing sustained runs).
- `scripts/loadtest-compare.py`: Compares baseline vs valid after runs (median & min..max range), automatically excluding `INVALID` runs.

---

## 2. How to Run

### Prerequisites

- Docker Desktop / Docker Engine running. No host `k6` installation required (`grafana/k6:0.54.0@sha256:1f40432b1cbe7234e977f96c362c9bc550a2d2b583d014dd8669fe40d3e9e755`).
- All long-running load targets automatically wrap execution in `caffeinate -is` when available on macOS to prevent idle sleep.

### Commands

```bash
# 1. Breakpoint discovery (resets stack between hold and journey, derives loadtest/sustainable-profile.json)
make load-breakpoint
make load-breakpoint-hold       # single hold staircase against running stack
make load-breakpoint-journey    # single journey staircase against running stack

# 2. Sustained profile verification (reads loadtest/sustainable-profile.json)
make load-sustained             # single run against running stack
make load-sustained-x3          # 3 VALID runs (stack-reset + obs-up between, up to 6 attempts if INVALID)
LOAD_PUBLISH=true make load-sustained-x3  # publishes docs/load-test-report.md iff all 3 valid runs pass 100%

# 3. 20-minute soak stability & drift check (SOAK_MINUTES configurable)
make load-soak
SOAK_MINUTES=30 make load-soak

# 4. Overload profile (original scenarios — intentionally beyond single-node laptop capacity)
LOAD_SCALE=0.2 make load-fresh  # 20% smoke run
make load-fresh                 # full-scale overload run (400 req/s flash + 25 j/s journey)
make load-all / make load-flash / make load-journey

# 5. Audit, reporting, JVM check & self-tests
make load-audit
make load-report
make jvm-check
bash scripts/test-loadtest-audit.sh
python3 -m unittest -v scripts/test_loadtest_report.py scripts/test_loadtest_compare.py scripts/test_loadtest_thresholds.py scripts/test_loadtest_breakpoint.py scripts/test_loadtest_soak_report.py
```

### Environment Variables

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `LOAD_SCALE` | `1.0` | Scales overload and breakpoint scenario rates (`START`/`STEP`/`MAX` when unset). |
| `START` / `STEP` / `MAX` / `STEP_SECONDS` | Hold `25/25/600/30`; Journey `2/2/40/45` | Override staircase parameters for `breakpoint-hold.js` / `breakpoint-journey.js` (bounded to `<= 40` steps). |
| `SOAK_MINUTES` | `20` | Plateau duration (minutes) for `soak.js`. |
| `USER_POOL_SIZE` / `SEATS_PER_EVENT` | `5000` / `5000` | Pre-minted user JWT pool size and seats per event (chunked at `2500 <= 5000` bulk limit). |
| `LOAD_PUBLISH` | `false` | Enables publishing `docs/load-test-report.md` from `load-sustained-x3` iff all 3 valid runs pass every threshold and audit item. |
| `RESULTS_DIR` | `loadtest/results/<UTC ts>` | Override results output directory. |

---

## 3. Reading Results

### Overload Profile (`flash-sale.js` & `checkout-journey.js`)

The original scenarios (`400 req/s` flash sale and `25 journeys/s` checkout) intentionally exceed single-node 8-logical-core laptop capacity. They remain completely unchanged as the **overload profile** to prove data integrity under saturation (`A1` zero oversell, `A5` zero DLT, `A6` payment integrity, `A8` bounded holds, `A9` zero OOM/restarts) and are summarized separately under **"Behavior beyond capacity"** — never presented as sustainable capacity.

### Breakpoint & Sustainable Profile Derivation (`scripts/loadtest-breakpoint.py`)

- **Per-step metrics**: Every request carries a bounded `step` tag (`s01..s40`) computed from elapsed time, exported via non-aborting k6 sub-metric thresholds (`max>=0`, `count>=0`, `rate>=0`).
- **Breakpoint criterion**: The highest step such that **this step and every lower step** satisfy:
  - Hold `p99 < 300 ms` (journey also requires checkout `p99 < 500 ms` and saga `p95 < 5000 ms`),
  - Error rate `< 0.1%` (`< 0.001`),
  - `hold_unknown_outcome == 0`,
  - `dropped_iterations == 0` (per-step delta from Prometheus `k6_dropped_iterations_total` when reachable, or `0` when global `dropped_iterations == 0`).
- **Sustainable rate formula**: `max(step_size, floor((0.70 * breakpoint) / step_size) * step_size)`. If step `s01` fails, the script prints `NO BREAKPOINT FOUND` and does not create `loadtest/sustainable-profile.json`.

### Sustained Capacity (`sustained-hold.js` & `sustained-journey.js`)

- Runs `20s ramp -> 5m plateau -> 10s ramp down` at the derived sustainable rates from `loadtest/sustainable-profile.json` using the exact same thresholds as the overload scenarios (shared in `loadtest/lib/thresholds.js` and verified byte-identical by `scripts/test_loadtest_thresholds.py`).
- `make load-sustained-x3` executes up to 6 attempts with `stack-reset` + `obs-up` between attempts until 3 **VALID** runs are collected. Valid failing runs are **never** retried silently — they count as one of the 3 valid runs and block publishing.

### Soak Stability & Drift (`soak.js` & `scripts/loadtest-soak-report.py`)

- Runs the derived sustainable journey rate for `SOAK_MINUTES` (default 20 min) and slices the plateau into 2-minute windows from Prometheus + `docker-stats.csv`.
- Uses `min_over_time(jvm_memory_used_bytes{area="heap",id="G1 Old Gen"}[2m:10s])` as the post-GC old-generation heap proxy (falling back to total heap if the pool label is absent).
- Flags **`DRIFT`** if:
  1. Last window hold or checkout `p99 > 1.3 × first full window p99`,
  2. Linear regression slope of old-gen heap is positive with fitted total growth `> 20%`, or
  3. Consumer lag, outbox oldest pending age, or HikariCP pending grows strictly monotonically over the last 10 minutes (last 5 consecutive 2-minute windows).
- Soak verdict is **`PASS`** only with no `INVALID` marker, no `DRIFT`, all k6 thresholds passing, and audit `A1..A9` passing.

### Refined Audit Rule `A8` & Unknown-Outcome Holds

- Let `R` = reservations created in `inventory_db` for the run's events, `C` = k6 count of `201` responses (`hold_created`), and `U` = k6 count of hold responses with unknown client outcome (`hold_unknown_outcome`: status `0`/transport error, `500`, `502`, `504`).
- **PASS** iff `C <= R <= C + U` (exact `R == C` when `U == 0`).
- **FAIL** if `R < C` (`FAIL_PHANTOM_HOLD`: client told a hold exists that does not) or `R > C + U` (`FAIL_UNEXPLAINED_HOLDS`: holds nobody could have requested).
- Holds committed on unknown-outcome requests are **not leaked**: `inventory-service`'s `ExpirySweeper` expires them normally after their 10-minute `expires_at` TTL, releasing both PostgreSQL and Redis locks.

### Handling `INVALID` Runs (Host Noise Protection)

- `scripts/loadtest-monitor.sh` inspects `docker-stats.csv` at the end of every run. Any consecutive timestamp gap `> 15s` (indicating host sleep or Docker VM pause) writes `results/<ts>/INVALID` with the exact gap timestamps.
- `scripts/loadtest-report.py` renders `INVALID` runs prominently as **`RUN INVALID — NOT COUNTED`**.
- `scripts/loadtest-compare.py` excludes `INVALID` runs from medians and ranges (listing them explicitly as excluded).
- The publish gate (`docs/load-test-report.md`) and `load-sustained-x3` never count or publish an `INVALID` run.

---

## 4. Assumptions & Limits

1. **Single-node Docker on a laptop**: All application services, Kafka, Redis, 3 PostgreSQL instances, Prometheus, Grafana, exporters, and k6 share one Docker VM (8 logical CPUs, 7.75 GiB RAM). Results are reproducible local-host baselines for regression tracking, **not** production capacity estimates.
2. **Hold breakpoint event sizing**: `breakpoint-hold.js` uses a 5,000-seat event (matching `flash-sale.js`; configurable via `BP_HOLD_EVENTS`, chunked at 2,500 seats within the 5,000 bulk limit) so early/mid steps sustain real `201 Created` Postgres+Redis+Outbox writes plus `409` contention while keeping k6 VU memory and outbox quiescence bounded. `breakpoint-journey.js`, `sustained-journey.js`, and `soak.js` create `ceil(max_possible_journeys * 1.05..1.15 / 5000)` 5,000-seat events so every journey has a unique seat.
3. **Threshold metric naming (`hold_rate_limited_rate`)**: Because k6 `Counter` reports `rate` as count/second rather than ratio `[0, 1]`, hold scenarios track both `hold_rate_limited` (`Counter`) and `hold_rate_limited_rate` (`Rate`, threshold `< 0.01`).
4. **`saga_latency p(95)<5000` is a chosen target**, not a formal architecture SLO in `docs/arch/09-conventions.md`.
5. **JVM G1GC & Memory Limits (`768m`)**: Java services run G1GC (`-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError`, overridable via `JAVA_OPTS_EXTRA`) at `768 MiB` each (`3,328 MiB` / `41.9%` of Docker VM across all 5 limited app containers).
6. **Consumer Concurrency & PostgreSQL `max_connections`**: `ticketing.consumer.concurrency` (`1..12`, default `1`) is configured to `6` (`payment`), `3` (`order`), `3` (`inventory`) preserving per-key partition ordering. `HIKARI_MAX_POOL_SIZE` defaults (`20/15/15`) stay within `max_connections=100` (`26/100` inventory, `21/100` order, `21/100` payment worst-case).
7. **macOS Idle Sleep Prevention**: Long-running targets wrap execution in `caffeinate -is` when installed. Two earlier unattended overload runs (`20261122T121906Z` and `20261122T123747Z`) hit macOS sleep before this safeguard and remain archived in `loadtest/results/`.
