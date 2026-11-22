# Load Test Findings

This document records product and capacity bottlenecks observed during acceptance runs of the k6 load-test harness, together with honest before-vs-after measurements after applying the configuration and concurrency mitigations (`LOAD-001`, `LOAD-003`, `LOAD-004`). Per harness rules, no k6 thresholds, scenario stages, or audit rules were modified, and every number below comes directly from k6 summaries, Prometheus, or `docker-stats.csv`.

## Runs Analyzed

| Phase | Run ID | Scale | Flash Sale Plateau 2 (Achieved / Target) | Flash Hold p99 (SLO <300ms) | Flash 5xx / Unexpected | Journey Hold p99 (<300ms) | Journey Checkout p99 (<500ms) | Saga p95 (<5000ms) | Journey Timeouts | Quiescence | Post-Run Audit |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Baseline | `20261121T183045Z` | `0.2` | `80.00 / 80 req/s` | **`446.60 ms`** (FAIL) | `0` / `0` | `163.85 ms` (PASS) | `293.04 ms` (PASS) | `2484.40 ms` (PASS) | `0` | `5 s` (PASS) | **PASS (9/9)** |
| Baseline | `20261121T190134Z` | `1.0` | `297.00 / 400 req/s` | **`4215.51 ms`** (FAIL) | `126` (`0.383%`) / `126` | **`1397.08 ms`** (FAIL) | **`1953.69 ms`** (FAIL) | **`31285.40 ms`** (FAIL) | **`1638`** | `125 s` (FAIL) | **FAIL (5/9)** |
| After | `20261122T120131Z` | `0.2` | `77.33 / 80 req/s` | **`830.40 ms`** (FAIL) | `0` / `0` | **`509.17 ms`** (FAIL) | `431.22 ms` (PASS) | **`10413.60 ms`** (FAIL) | `0` | `5 s` (PASS) | **PASS (9/9)** |
| After (Run 1) | `20261122T120819Z` | `1.0` | `258.77 / 400 req/s` | **`7738.16 ms`** (FAIL) | `115` (`0.379%`) / `115` | **`5465.57 ms`** (FAIL) | **`4090.90 ms`** (FAIL) | **`37304.45 ms`** (FAIL) | `0` | `5 s` (PASS) | **PASS (9/9)** |
| After (Run 2) | `20261122T125331Z` | `1.0` | `101.95 / 400 req/s` | **`15556.24 ms`** (FAIL) | `4614` (`37.576%`) / `4614` | **`4379.64 ms`** (FAIL) | **`4038.30 ms`** (FAIL) | **`40825.05 ms`** (FAIL) | **`1353`** | `7 s` (PASS) | **FAIL (8/9, A8)** |
| After (Run 3) | `20261122T130512Z` | `1.0` | `333.23 / 400 req/s` | **`3697.17 ms`** (FAIL) | `75` (`0.212%`) / `75` | **`1230.62 ms`** (FAIL) | **`1282.21 ms`** (FAIL) | **`31323.00 ms`** (FAIL) | `0` | `5 s` (PASS) | **PASS (9/9)** |

> **Invalidated Sleep-Interrupted Runs**: Two unattended runs executed before wrapping `make load-fresh` with `caffeinate -is` (`20261122T121906Z` with a `487 s` macOS idle-sleep gap in `docker-stats.csv`, and `20261122T123747Z` with an `85 s` gap) remain in `loadtest/results/` for auditability and are excluded from the 3-run median/range below. All three after runs in the comparison table (`20261122T120819Z`, `20261122T125331Z`, `20261122T130512Z`) completed with zero clock/sleep gaps.

**Host Environment** (all runs): macOS `15.8.3 (24H241) x86_64`, `Intel(R) Core(TM) i7-8569U CPU @ 2.80GHz` (4 physical / 8 logical cores), `16.0 GiB` RAM, Docker VM `8 CPUs / 7.75 GiB`, 17 monitored containers + k6 (`grafana/k6:0.54.0`).

**Publish Gate**: Because k6 latency/availability thresholds failed in all runs (and audit `A8` failed in Run 2 under heavy gateway `504` timeouts), `docs/load-test-report.md` was **not** published.

**Data Integrity Note**: Across all baseline and after runs — including the saturated full-scale runs — **zero oversells occurred (`A1 = 0`)**, **zero dead-letter messages were produced (`A5 = 0`)**, **zero duplicate payments or succeeded payments on cancelled orders occurred (`A6 = 0`)**, and **zero containers restarted or were OOM killed (`A9 = 0`)**.

---

## Before vs After Comparison (`scripts/loadtest-compare.py`)

Generated directly by:
```bash
python3 scripts/loadtest-compare.py \
  --baseline loadtest/results/20261121T190134Z \
  --after loadtest/results/20261122T120819Z loadtest/results/20261122T125331Z loadtest/results/20261122T130512Z
```

### Full Scale (`LOAD_SCALE=1.0`)

- **Baseline**: `20261121T190134Z`
- **After Runs (3)**: `20261122T120819Z`, `20261122T125331Z`, `20261122T130512Z`

| Metric | Baseline | After (Median) | After (Range) |
| :--- | ---: | ---: | ---: |
| Flash Sale Plateau 1 Throughput | 142.88 req/s | 145.70 req/s | 64.20 req/s .. 148.05 req/s |
| Flash Sale Plateau 2 Throughput | 297.00 req/s | 258.77 req/s | 101.95 req/s .. 333.23 req/s |
| Checkout Journey Throughput | 10.56 j/s | 14.68 j/s | 11.26 j/s .. 16.71 j/s |
| Flash Sale Hold p99 | 4215.51 ms | 7738.16 ms | 3697.17 ms .. 15556.24 ms |
| Journey Hold p99 | 1397.08 ms | 4379.64 ms | 1230.62 ms .. 5465.57 ms |
| Journey Checkout p99 | 1953.69 ms | 4038.30 ms | 1282.21 ms .. 4090.90 ms |
| Journey Saga p95 | 31285.40 ms | 37304.45 ms | 31323.00 ms .. 40825.05 ms |
| Flash Sale 429 Count | 0 | 0 | 0 .. 0 |
| Flash Sale Unexpected Count | 126 | 115 | 75 .. 4614 |
| Journey Unexpected Count | 9 | 2 | 0 .. 2 |
| Journey Timed Out Count | 1638 | 0 | 0 .. 1353 |
| 5xx Count (`gateway-service`) | 130.3 | 111.9 | 45.4 .. 4204.1 |
| 5xx Count (`inventory-service`) | n/a | 163.0 | 69.3 .. 256.7 |
| 5xx Count (`order-service`) | n/a | 0.0 | 0.0 .. 0.0 |
| 5xx Count (`payment-service`) | n/a | 0.0 | 0.0 .. 0.0 |
| Peak Consumer Lag (`inventory-service-payment-cg`) | 0.0 | 0.0 | 0.0 .. 0.0 |
| Peak Consumer Lag (`notification-service-order-cg`) | 3.0 | 1.0 | 0.0 .. 8.0 |
| Peak Consumer Lag (`order-service-inventory-cg`) | 12.0 | 11.0 | 9.0 .. 24.0 |
| Peak Consumer Lag (`order-service-payment-cg`) | 0.0 | 1.0 | 0.0 .. 2.0 |
| Peak Consumer Lag (`payment-service-order-cg`) | 1478.0 | 10.0 | 6.0 .. 13.0 |
| Peak Outbox Oldest Age (`inventory-service`) | 66.188 s | 111.646 s | 109.880 s .. 175.465 s |
| Peak Outbox Oldest Age (`order-service`) | 4.008 s | 23.396 s | 18.073 s .. 27.872 s |
| Peak Outbox Oldest Age (`payment-service`) | 1.324 s | 2.470 s | 1.043 s .. 2.841 s |
| Peak HikariCP Pending (`inventory-service`) | n/a | 152.0 | 50.0 .. 174.0 |
| Peak HikariCP Pending (`order-service`) | n/a | 92.0 | 85.0 .. 110.0 |
| Peak HikariCP Pending (`payment-service`) | n/a | 0.0 | 0.0 .. 0.0 |
| Total JVM GC Pause (`gateway-service`) | n/a | 16.918 s | 7.045 s .. 34.663 s |
| Total JVM GC Pause (`inventory-service`) | n/a | 4.694 s | 4.662 s .. 16.770 s |
| Total JVM GC Pause (`order-service`) | n/a | 8.062 s | 5.255 s .. 8.473 s |
| Total JVM GC Pause (`payment-service`) | n/a | 3.153 s | 1.968 s .. 3.987 s |
| Peak CPU (Top 3 Containers) | inventory-service: 315.2%; kafka: 313.6%; order-service: 301.0% | inventory-service: 371.4%; order-service: 320.0%; gateway-service: 275.4% | inventory-service: 344.5..756.7%; order-service: 311.6..331.3%; gateway-service: 267.2..785.4% |
| Time to Quiescence | 125 s (FAIL) | 5 s | 5 s .. 7 s |
| Audit Result | **FAIL** | 2/3 PASS | PASS, FAIL, PASS |

> Note: Baseline `20261121T190134Z` predated `prometheus-metrics.json` persistence, so `loadtest-compare.py` reports `n/a` for metrics not emitted into the baseline `report.md`; baseline values recorded from live Prometheus queries during `20261121T190134Z` are cited directly inside `LOAD-002`, `LOAD-003`, and `LOAD-004` below (`inventory-service` 503 count = `10`, HikariCP pending = `179` inventory / `71` order / `0` payment, total GC pause = `44.36 s` gateway / `6.92 s` order).

### Smoke Scale (`LOAD_SCALE=0.2`)

- **Baseline**: `20261121T183045Z`
- **After Runs (1)**: `20261122T120131Z`

| Metric | Baseline | After (Median) | After (Range) |
| :--- | ---: | ---: | ---: |
| Flash Sale Plateau 1 Throughput | 30.00 req/s | 30.00 req/s | 30.00 req/s .. 30.00 req/s |
| Flash Sale Plateau 2 Throughput | 80.00 req/s | 77.33 req/s | 77.33 req/s .. 77.33 req/s |
| Checkout Journey Throughput | 5.03 j/s | 4.61 j/s | 4.61 j/s .. 4.61 j/s |
| Flash Sale Hold p99 | 446.60 ms | 830.40 ms | 830.40 ms .. 830.40 ms |
| Journey Hold p99 | 163.85 ms | 509.17 ms | 509.17 ms .. 509.17 ms |
| Journey Checkout p99 | 293.04 ms | 431.22 ms | 431.22 ms .. 431.22 ms |
| Journey Saga p95 | 2484.40 ms | 10413.60 ms | 10413.60 ms .. 10413.60 ms |
| Flash Sale 429 Count | 0 | 0 | 0 .. 0 |
| Flash Sale Unexpected Count | 0 | 0 | 0 .. 0 |
| Journey Unexpected Count | 0 | 0 | 0 .. 0 |
| Journey Timed Out Count | 0 | 0 | 0 .. 0 |
| 5xx Count (`gateway-service`) | 0.0 | 0.0 | 0.0 .. 0.0 |
| 5xx Count (`inventory-service`) | n/a | 0.0 | 0.0 .. 0.0 |
| Peak Consumer Lag (`payment-service-order-cg`) | 1.0 | 0.0 | 0.0 .. 0.0 |
| Peak Outbox Oldest Age (`inventory-service`) | 3.529 s | 10.462 s | 10.462 s .. 10.462 s |
| Peak Outbox Oldest Age (`order-service`) | 0.110 s | 0.172 s | 0.172 s .. 0.172 s |
| Peak Outbox Oldest Age (`payment-service`) | 0.150 s | 1.922 s | 1.922 s .. 1.922 s |
| Peak HikariCP Pending (`inventory-service`) | n/a | 1.0 | 1.0 .. 1.0 |
| Peak HikariCP Pending (`order-service`) | n/a | 0.0 | 0.0 .. 0.0 |
| Peak HikariCP Pending (`payment-service`) | n/a | 0.0 | 0.0 .. 0.0 |
| Total JVM GC Pause (`gateway-service`) | n/a | 0.458 s | 0.458 s .. 0.458 s |
| Total JVM GC Pause (`inventory-service`) | n/a | 1.158 s | 1.158 s .. 1.158 s |
| Total JVM GC Pause (`order-service`) | n/a | 0.687 s | 0.687 s .. 0.687 s |
| Total JVM GC Pause (`payment-service`) | n/a | 0.836 s | 0.836 s .. 0.836 s |
| Peak CPU (Top 3 Containers) | inventory-service: 191.8%; gateway-service: 169.0%; kafka: 146.6% | order-service: 320.3%; kafka: 240.5%; gateway-service: 225.7% | order-service: 320.3..320.3%; kafka: 240.5..240.5%; gateway-service: 225.7..225.7% |
| Time to Quiescence | 5 s (PASS) | 5 s | 5 s .. 5 s |
| Audit Result | **PASS** | 1/1 PASS | PASS |

---

## LOAD-001: `payment-service` Single-Threaded Kafka Consumer Caps Saga Throughput at ~5.6 Payments/s

- **Status**: **`MITIGATED`** (consumer lag eliminated and quiescence restored; downstream outbox backlog now limits `saga_latency p95`)
- **Severity**: High (baseline caused 1,638 checkout journey timeouts and 125 s quiescence audit failure at 25 journeys/s)
- **Change Applied**: Added configurable `ticketing.consumer.concurrency` (default `1`, validated `1..12` matching topic partitions) in `ticketing-consumer`, configured via `KAFKA_CONSUMER_CONCURRENCY` to `6` in `payment-service`, `3` in `order-service`, and `3` in `inventory-service`.
- **Measured Before vs After (`LOAD_SCALE=1.0`, 3 uninterrupted runs)**:
  - `payment-service-order-cg` peak consumer lag: **`1,478.0` -> `10.0` median (`6.0..13.0`)** (-99.3%).
  - Time to quiescence: **`125 s (FAIL)` -> `5 s (PASS)` median (`5..7 s`)**; `A3` (outbox drained) and `A4` (all consumer lag zero) passed in **3/3** runs.
  - `Journey Timed Out Count`: **`1,638` -> `0` median** (`0` in Runs `20261122T120819Z` and `20261122T130512Z`; `1,353` in Run `20261122T125331Z` where gateway/inventory hit 100% container memory and `inventory-service` outbox aged `175.5 s`).
  - Completed checkout journeys: **`10.56 j/s` (`149` confirmed) -> `14.68 j/s` median (`11.26..16.71 j/s`; `2,378` and `2,706` confirmed in Runs 1 & 3)**.
  - Remaining limitation: `Journey Saga p95` stayed above 5,000 ms (`31,285.40 ms -> 37,304.45 ms` median, `31,323.00..40,825.05 ms`), no longer due to Kafka consumer lag (`<= 13` msgs), but because `order_materialization_latency` (`p95 = 17.4s..21.5s`) and `inventory-service`/`order-service` outbox backlog (`111.6 s` / `23.4 s` median) now dominate end-to-end saga time (see `LOAD-004`).

---

## LOAD-002: Gateway `504 GATEWAY_TIMEOUT` (`PoolAcquireTimeoutException`) and Inventory `503 SERVICE_UNAVAILABLE` (`RedisCommandTimeoutException`) Under 400 req/s

- **Status**: **`OPEN`** (not targeted by this configuration/concurrency pass)
- **Severity**: High (violates 99.9% availability SLO; persists across all full-scale runs)
- **Baseline Evidence (`20261121T190134Z`)**:
  - Scenario A: `hold_unexpected = 126`, `hold_5xx = 0.003829` (`< 0.001` threshold). Scenario B: `unexpected_statuses = 9`.
  - Gateway `inventory-reservations` 5xx = `130`, `order-get` 5xx = `5`; `inventory-service` 503 (`RedisCommandTimeoutException`) = `10` (`hold/timeout=10`, `release/timeout=2`); `gateway_redis_failures_total = 9,309`.
- **After Measurements (`20261122T120819Z`, `20261122T125331Z`, `20261122T130512Z`)**:
  - Flash Sale `hold_unexpected`: **`115` median (`75..4,614`)**; Journey `unexpected_statuses`: **`2` median (`0..2`)**.
  - Gateway 5xx count: **`111.9` median (`45.4..4,204.1`)**; Inventory 5xx count: **`163.0` median (`69.3..256.7`)**.
  - In Run 3 (`20261122T130512Z`), Prometheus recorded `gateway_requests_total{route="inventory-reservations",status_class="5xx"} = 75` matching `inventory_redis_unavailable_total{operation="hold",cause="timeout"} = 75` (`release/timeout = 8`), showing that `inventory-service` Redis 500 ms command timeouts under host CPU saturation are still a direct source of hold 503s even when gateway connection-pool timeouts drop.

---

## LOAD-003: Serial GC (`MarkSweepCompact` / `Copy`) Auto-Selected in 512 MiB Java 21 Containers (Up to 1.244 s STW Pauses)

- **Status**: **`PARTIAL`** (G1GC verified active and gateway total GC pause time cut by ~62%, but HTTP tail latencies did **not** improve on the 8-vCPU laptop host)
- **Severity**: High
- **Change Applied**: Set explicit `-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError -Xlog:gc+init=info` (overridable via `JAVA_OPTS_EXTRA`) in all four Java Dockerfiles, raised memory limits in `infra/docker-compose.full.yml` from `512m` to `768m`, and verified with `make jvm-check` (`4/4` Java services on G1GC; `3,328 MiB / 7,937 MiB = 41.9%` of Docker VM).
- **Measured Before vs After**:
  - **Improved**: `gateway-service` total JVM GC pause time over the full-scale run dropped from **`44.36 s`** (baseline SerialGC: `10.48 s` `MarkSweepCompact` + `33.88 s` `Copy`) to **`16.918 s` median (`7.045..34.663 s`)** under G1GC (-62%). In Run 3 (`20261122T130512Z`), max single G1 pause on `gateway-service` was `0.299 s` (down from `1.244 s`) and `payment-service` was `0.158 s`.
  - **Did NOT improve**: End-to-end HTTP tail latencies worsened in median runs at both `LOAD_SCALE=0.2` (Flash Hold p99 `446.60 ms -> 830.40 ms`, Journey Hold p99 `163.85 ms -> 509.17 ms`) and `LOAD_SCALE=1.0` (Flash Hold p99 `4,215.51 ms -> 7,738.16 ms` median [`3,697.17..15,556.24 ms`], Journey Hold p99 `1,397.08 ms -> 4,379.64 ms` median [`1,230.62..5,465.57 ms`]).
  - **Why**: On the 8-logical-core Docker VM (`LOAD-005`), G1GC's concurrent/parallel GC threads (combined with the additional Kafka consumer threads) increased host CPU contention (`inventory-service` peak CPU `315.2% -> 371.4%` median, `order-service` `301.0% -> 320.0%` median, `gateway-service` `261.5% -> 275.4%` median [`785.4%` in Run 2]), causing `order-service` to still suffer a `2.293 s` G1 Young GC pause under CPU starvation in Run 3 (`20261122T130512Z`). In addition, `gateway-service` reached `768.0 MiB` (`100.0%` of its 768 MiB container cap) in Runs 1 & 2 (`75.3%` in Run 3), and `inventory-service` hit `767.9 MiB` (`100.0%`) in Run 2.

---

## LOAD-004: `inventory-service` & `order-service` HikariCP Pool Exhaustion (`maximum-pool-size: 10`) and Outbox Backlog

- **Status**: **`NO IMPROVEMENT`** (enlarging HikariCP pools did not eliminate connection queueing, latency, or outbox lag; outbox publisher throughput re-measured and confirmed as the primary remaining saga bottleneck)
- **Severity**: High
- **Change Applied**: Made `spring.datasource.hikari.maximum-pool-size` configurable via `HIKARI_MAX_POOL_SIZE` with defaults `20` (`inventory-service`), `15` (`order-service`), and `15` (`payment-service`), keeping connection timeout (`30000 ms`) unchanged. Verified `SHOW max_connections` = `100` on all three PostgreSQL containers (`<= 26/100` worst-case connections per DB). **The outbox publisher was intentionally not modified in this pass.**
- **Measured Before vs After (`LOAD_SCALE=1.0`, 3 uninterrupted runs)**:
  - Active pool usage confirmed: `hikaricp_connections_active` peaked at `20/20` on `inventory-service`, `15/15` on `order-service`, and `1/15` on `payment-service` (Run 3 Prometheus).
  - **HikariCP queueing did not resolve**: `inventory-service` peak pending threads moved only slightly from `179` (baseline) to **`152.0` median (`50.0..174.0`)**, while `order-service` peak pending threads worsened from `71` (baseline) to **`92.0` median (`85.0..110.0`)** as higher checkout throughput and `6,216..26,491` `409 ORDER_NOT_READY` retries hit `order_db`. `payment-service` stayed at `0.0` pending.
  - **Throughput and latency did not improve in median**: Flash Sale Plateau 2 moved from `297.00 req/s` to **`258.77 req/s` median (`101.95..333.23 req/s`)**; Journey Checkout p99 moved from `1,953.69 ms` to **`4,038.30 ms` median (`1,282.21..4,090.90 ms`)**.
  - **Outbox Throughput Re-Measurement (Before Any Outbox Design Change)**:
    - `inventory-service` peak `outbox_oldest_pending_age_seconds`: **`66.188 s` -> `111.646 s` median (`109.880..175.465 s`)** (and at `LOAD_SCALE=0.2`: `3.529 s -> 10.462 s`).
    - `order-service` peak `outbox_oldest_pending_age_seconds`: **`4.008 s` -> `23.396 s` median (`18.073..27.872 s`)**.
    - `payment-service` peak `outbox_oldest_pending_age_seconds`: **`1.324 s` -> `2.470 s` median (`1.043..2.841 s`)**.
    - With `payment-service` consumer lag mitigated (`LOAD-001`), the unmodified single-poller outbox publisher (100 rows / 200 ms poll interval competing for DB/CPU) is now confirmed by direct measurement to be the primary driver of `order_materialization_latency` (`p95 = 17.4s..21.5s`) and `saga_latency` (`p95 = 31.3s..40.8s`), justifying an outbox throughput design change (e.g., drain-while-full polling loop or larger batch size) as the next step.

---

## LOAD-005: Single-Node Laptop CPU Saturation Across 17 Containers + k6

- **Status**: **`OPEN`** (environmental limit — compounded by additional G1GC and Kafka consumer threads on an 8-logical-core VM)
- **Evidence (`docker-stats.csv`)**:
  - Even at `LOAD_SCALE=0.2` (`80 req/s` peak), after-run peak CPU rose to `order-service 320.3%`, `kafka 240.5%`, `gateway-service 225.7%`, `inventory-service 214.8%`, `payment-service 196.3%`, `k6-flash-sale 136.2%` (>1,300% aggregate peak demand on 4 physical / 8 logical cores), explaining why smoke-scale hold p99 regressed from `446.60 ms` to `830.40 ms` despite zero 5xx and zero consumer lag.
  - At `LOAD_SCALE=1.0`, top-3 median peak CPUs across the 3 after runs were `inventory-service 371.4%` (`344.5..756.7%`), `order-service 320.0%` (`311.6..331.3%`), and `gateway-service 275.4%` (`267.2..785.4%`), with `k6-flash-sale` peaking at `685.1%` in Run 3.

---

## LOAD-006: Client-Visible `504 GATEWAY_TIMEOUT` After Downstream Commit Causes `A8` Hold-Count Divergence

- **Status**: **`OPEN`** (new finding revealed in Run 2 `20261122T125331Z`)
- **Severity**: Medium (no oversell or data corruption, but client receives `504` while a valid 10-minute `RESERVED` hold was committed in `inventory_db`)
- **Reproduction**: `make load-fresh` (Run `20261122T125331Z`, where `gateway-service` hit `768 MiB` / `100%` memory and `785.4%` CPU, returning `4,204` `5xx` responses and `hold p99 = 15.56 s`).
- **Observed Symptoms**:
  - Audit `A8` failed: `k6_201_holds=5783` vs `inventory_db_reservations=6083` (`+300` reservations committed in `inventory_db` whose HTTP `201` responses never reached k6 before the gateway timed out).
  - All consistency invariants (`A1=0` oversells, `A2: 542==542==542`, `A3=0` outbox, `A4=0` lag, `A5=0` DLT, `A6=0` duplicate payments, `A7=0` stuck pending, `A9=0` restarts/OOM) still passed; the 300 orphaned holds remained safely in `RESERVED` state awaiting their 10-minute TTL expiry.
- **Root Cause**:
  - Under severe gateway/inventory latency spikes exceeding the gateway's upstream response timeout (`10 s`), `gateway-service` returns `504 GATEWAY_TIMEOUT` to the caller while `inventory-service` finishes committing the PostgreSQL transaction. Without client-side retry with the same `Idempotency-Key` (the k6 flash-sale scenario does not retry 504s), the reservation exists in `inventory_db` without the caller observing `201`.
