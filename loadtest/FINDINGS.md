# Load Test Findings

This document records product and capacity bottlenecks observed during acceptance runs of the k6 load-test harness. Per harness rules, no thresholds were loosened and no production code was modified during load-test authoring.

## Runs Analyzed

| Run ID | Scale | Flash Sale Plateau 2 (Achieved / Target) | Flash Hold p99 (SLO <300ms) | Flash 5xx / Unexpected | Journey Hold p99 (<300ms) | Journey Checkout p99 (<500ms) | Saga p95 (<5000ms) | Journey Timeouts | Post-Run Audit |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `20261121T183045Z` | `0.2` | `80.00 / 80 req/s` | **`446.60 ms`** (FAIL) | `0` / `0` | `163.85 ms` (PASS) | `293.04 ms` (PASS) | `2484.40 ms` (PASS) | `0` | **PASS (9/9)** |
| `20261121T190134Z` | `1.0` | `297.00 / 400 req/s` | **`4215.51 ms`** (FAIL) | `126` (`0.383%`) / `126` | **`1397.08 ms`** (FAIL) | **`1953.69 ms`** (FAIL) | **`31285.40 ms`** (FAIL) | **`1638`** | **FAIL** (quiescence timeout at 125s; converged after ~4 min) |

**Host Environment** (both runs): macOS `15.8.3 (24H241) x86_64`, `Intel(R) Core(TM) i7-8569U CPU @ 2.80GHz` (4 physical / 8 logical cores), `16.0 GiB` RAM, Docker VM `8 CPUs / 7.75 GiB`, 17 monitored containers + k6 (`grafana/k6:0.54.0`).

**Data Integrity Note**: Across both runs — including the heavily saturated full-scale run (`6,893` holds created, `32,909` flash-sale requests, `1,802` checkouts) — **zero oversells occurred (`A1 = 0`)**, **zero dead-letter messages were produced (`A5 = 0`)**, **zero phantom or lost holds occurred (`A8: 6893 == 6893`)**, and **zero containers restarted or were OOM killed (`A9 = 0`)**. Once `payment-service`'s backlog drained ~2 minutes after the 120s audit timeout, all three databases reconciled identically (`SOLD=1708 == CONFIRMED res_items=1708 == CONFIRMED orders=1708 == SUCCEEDED payments=1708`, `CANCELLED_PAYMENT_FAILED=94 == RELEASED=94 == FAILED=94`, `mismatched_held_seats=0`).

---

## LOAD-001: `payment-service` Single-Threaded Kafka Consumer Caps Saga Throughput at ~5.6 Payments/s

- **Severity**: High (causes 1,638 checkout journey timeouts and 125s quiescence audit failure at 25 journeys/s)
- **Reproduction**: `make load-fresh` (Run `20261121T190134Z`, Scenario B `checkout-journey.js` at 25 journeys/s for 180s)
- **Observed Symptoms**:
  - `journeys_timed_out = 1638` out of `1802` checked-out journeys (`saga_latency p95 = 31,285.40 ms`, threshold `< 5000 ms`).
  - `loadtest-audit.sh` timed out waiting for quiescence after `125s` with `payment-service-order-cg:order.events` still lagging by `1,074` messages (down from `1,381` at t=32s, draining at ~5–6 msg/s).
  - Because the saga was still actively draining at t=125s, `A2` (`sold_seats=1574` vs `confirmed_res_items=1576`), `A3` (`unpublished inv=1, pay=1`), `A4` (`lag=1076`), and `A7` (`stuck_pending_orders=16`) failed at the 125s cutoff. Two minutes later, after lag hit `0`, all counts reconciled cleanly (`1708/1708/1708`, `0` pending).
- **Evidence (Prometheus & Docker Stats)**:
  - `sum(payment_provider_duration_seconds_sum) / sum(payment_provider_duration_seconds_count)` = **`0.1789 s`** (178.9 ms mean simulated latency from `MockPaymentProvider` 50–300 ms sleep).
  - `max_over_time(hikaricp_connections_active{service="payment-service"}[15m])` = **`1`** (pending = `0`).
  - `max_over_time((sum by (consumergroup) (clamp_min(kafka_consumergroup_lag, 0)))[425s:15s])` -> `payment-service-order-cg` = **`1478.0`**.
  - `max_inflight_checkouts_saga` = **`972.0`**.
  - `docker-stats.csv`: `payment-service` averaged only `29.0%` CPU (peak `129.8%`) while lagging by 1,478 messages because its single listener thread spent most of its time sleeping inside `MockPaymentProvider`.
- **Root Cause**:
  - `order.events` has 12 partitions, but `payment-service`'s `@KafkaListener` runs with default Spring Kafka `concurrency = 1`. With 179 ms average blocking provider latency per charge, theoretical maximum throughput is `1 / 0.179 ≈ 5.6 payments/s` — far below Scenario B's `23.75 checkouts/s` (95% of 25 journeys/s).
- **Recommended Fix**:
  - Increase Kafka listener concurrency for `payment-service-order-cg` (e.g., `concurrency = 6..12` matching the 12 partitions of `order.events` and HikariCP pool sizing) and/or scale `payment-service` replicas.

---

## LOAD-002: Gateway `504 GATEWAY_TIMEOUT` (`PoolAcquireTimeoutException`) and Inventory `503 SERVICE_UNAVAILABLE` (`RedisCommandTimeoutException`) Under 400 req/s

- **Severity**: High (violates 99.9% availability SLO; causes 135 total 5xx responses at full scale)
- **Reproduction**: `make load-fresh` (Run `20261121T190134Z`)
- **Observed Symptoms**:
  - Scenario A (`flash-sale.js`): `hold_unexpected = 126`, `hold_5xx = 0.003829` (threshold `< 0.001`).
  - Scenario B (`checkout-journey.js`): `unexpected_statuses = 9` (4 on hold, 5 on `GET /api/v1/orders/{id}`).
- **Evidence (Prometheus & Container Logs)**:
  - `sum by (route, status_class) (gateway_requests_total)`:
    - `inventory-reservations` `5xx` = **`130`** (`2xx = 6893`, `4xx = 27787`)
    - `order-get` `5xx` = **`5`** (`2xx = 110599`)
  - `sum by (status) (http_server_requests_seconds_count{service="inventory-service",uri="/api/v1/reservations",method="POST"})`:
    - `201` = `6893`, `409` = `27787`, **`503` = `10`**
  - `inventory_redis_unavailable_total`:
    - `operation="hold", cause="timeout"` = **`10`**
    - `operation="release", cause="timeout"` = **`2`**
  - `gateway_redis_failures_total` = **`9309`** (gateway rate limiter hit its 200 ms timeout and failed open).
  - Container logs (`gateway-service`):
    ```text
    2026-11-21T19:06:20.323Z ERROR 1 --- [gateway-service] [parallel-3] c.t.g.e.GlobalErrorWebExceptionHandler : Handling gateway error for instance '/api/v1/orders/01a1ff80-56a1-700c-8875-099049bdbf87' [504 GATEWAY_TIMEOUT]: PoolAcquireTimeoutException
    2026-11-21T19:06:22.155Z  WARN 1 --- [gateway-service] [parallel-1] c.t.g.ratelimit.RedisRateLimiterService : Redis rate limiter unavailable for key 'ratelimit:user:...': Did not observe any item or terminal signal within 200ms in 'next' (and no fallback has been configured). Failing open.
    ```
  - Container logs (`inventory-service`):
    ```text
    2026-11-21T19:02:19.441Z ERROR 1 --- [inventory-service] [o-8081-exec-136] c.t.i.api.InventoryExceptionHandler : Redis unavailable while handling /api/v1/reservations [correlationId=e2e89a3e-a649-43c4-80f7-637456c7ad78]: io.lettuce.core.RedisCommandTimeoutException: Command timed out after 500 millisecond(s)
    2026-11-21T19:06:19.349Z  WARN 1 --- [inventory-service] [o-8081-exec-153] c.t.inventory.redis.RedisSeatGate : Best-effort release compensation timed out (300 ms): ...; relying on 660s safety-net TTL
    ```
- **Root Cause**:
  - Approximately 120 of the 130 hold 5xx responses and all 5 `order-get` 5xx responses were gateway `504 GATEWAY_TIMEOUT` from Reactor Netty `PoolAcquireTimeoutException` (`max-connections: 200`, `acquire-timeout: 2000ms` in `gateway-service/src/main/resources/application.yml`) when slow downstream responses (`p99 = 4.2s`, `max = 7.97s`) saturated the 200-connection HTTP client pool.
  - The remaining 10 hold 5xx responses were `inventory-service` `503 SERVICE_UNAVAILABLE` fail-closed rejections caused by Lettuce hitting the `500 ms` Redis command timeout during JVM GC pauses and CPU saturation.

---

## LOAD-003: Serial GC (`MarkSweepCompact` / `Copy`) Auto-Selected in 512 MiB Java 21 Containers (Up to 1.244s STW Pauses)

- **Severity**: High (directly violates 300 ms hold p99 SLO even at `LOAD_SCALE=0.2` / 80 req/s)
- **Reproduction**: Both `LOAD_SCALE=0.2 make load-fresh` (`20261121T183045Z`) and `make load-fresh` (`20261121T190134Z`)
- **Evidence (Prometheus)**:
  - `max_over_time(jvm_gc_pause_seconds_max[5m])` during `LOAD_SCALE=0.2` (`20261121T183045Z`):
    - `inventory-service` (`MarkSweepCompact`, `CodeCache GC Threshold`): **`0.287 s`** (287 ms STW pause)
    - `order-service` (`MarkSweepCompact`, `Allocation Failure`): **`0.451 s`** (451 ms STW pause)
    - `payment-service` (`MarkSweepCompact`, `Metadata GC Threshold`): **`0.194 s`**
    - `gateway-service` (`MarkSweepCompact`, `Metadata GC Threshold`): **`0.099 s`**
  - `max_over_time(jvm_gc_pause_seconds_max[15m])` and `sum by (service, gc) (jvm_gc_pause_seconds_sum)` during `LOAD_SCALE=1.0` (`20261121T190134Z`):
    - `gateway-service`: max major GC **`1.244 s`** (`MarkSweepCompact`, total `10.48 s`), max minor GC **`0.634 s`** (`Copy`, total `33.88 s`) -> **44.36 s total GC pause time**!
    - `inventory-service`: max major GC **`0.541 s`** (`MarkSweepCompact`, total `0.82 s`), max minor GC `0.130 s` (`Copy`)
    - `order-service`: max minor GC `0.215 s` (`Copy`, total `5.90 s`), total major GC `1.02 s`
- **Root Cause**:
  - `infra/docker-compose.full.yml` caps each Java container at `memory: 512m` with `JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError` and no explicit GC flag. OpenJDK 21's ergonomic defaults pick **Serial GC** when container RAM is `< 1792 MiB`, causing multi-hundred-millisecond to >1-second Stop-The-World pauses that directly break the 300 ms hold SLO and trigger 200 ms gateway Redis timeouts.
- **Recommended Fix**:
  - Explicitly configure `-XX:+UseG1GC -XX:MaxGCPauseMillis=50` (or ZGC with larger heap) in the service Dockerfiles / `JAVA_TOOL_OPTIONS` and increase memory limits above 512 MiB for load environments.

---

## LOAD-004: `inventory-service` & `order-service` HikariCP Pool Exhaustion (`maximum-pool-size: 10`) and Outbox Backlog

- **Severity**: High (drives hold p50 to 988 ms, p99 to 4,215 ms, and outbox age to 66.2 s at 400 req/s)
- **Reproduction**: `make load-fresh` (`20261121T190134Z`)
- **Evidence (Prometheus)**:
  - `max_over_time(hikaricp_connections_active[15m])`: `inventory-service` = **`10/10`**, `order-service` = **`10/10`**.
  - `max_over_time(hikaricp_connections_pending[15m])`: `inventory-service` = **`179` waiting threads**, `order-service` = **`71` waiting threads**.
  - `max_over_time(outbox_oldest_pending_age_seconds[425s:5s])`: `inventory-service` = **`66.188 s`**, `order-service` = **`4.008 s`**.
  - `histogram_quantile(0.99, ... inventory hold 201)` = **`1.050 s`**, `inventory hold 409` = **`1.836 s`**, `order checkout 202` = **`0.750 s`**.
  - Scenario A k6 `dropped_iterations` = **`7,015`** (achieved Plateau 2 throughput `297.00 req/s` vs `400 req/s` target).
- **Root Cause**:
  - Both `inventory-service` and `order-service` configure `hikari.maximum-pool-size: 10`, which becomes heavily queued when hundreds of concurrent HTTP requests + `OutboxPoller` + Kafka consumers contend for connections simultaneously.
  - Because `inventory-service` emits 1 outbox row per 201 hold and polls 100 rows per 200 ms (`max 500/s` ideal, slower under DB/CPU contention), creating 4,996 holds during the flash sale produced a 66.2 s outbox backlog, which in turn drove `1,726` `409 ORDER_NOT_READY` checkout retries and `order_materialization_latency p99 = 5,202.86 ms` in Scenario B.

---

## LOAD-005: Single-Node Laptop CPU Saturation Across 17 Containers + k6

- **Severity**: Environmental limit (compounds LOAD-003 and LOAD-004)
- **Evidence (`docker-stats.csv`)**:
  - Even at `LOAD_SCALE=0.2` (`80 req/s` peak), top containers reached `inventory-service 191.8%`, `gateway-service 169.0%`, `kafka 146.6%`, `order-service 140.0%`, `payment-service 139.2%`, `k6 98.5%`, `postgres-inventory 73.6%` (>800% aggregate demand across 4 physical / 8 logical cores), contributing to `hold p99 = 446.60 ms` (`> 300 ms`).
  - At `LOAD_SCALE=1.0`, top containers reached `inventory-service 315.2%`, `kafka 313.6%`, `order-service 301.0%`, `k6-flash-sale 277.5%`, `gateway-service 261.5%`, `postgres-inventory 149.2%`, `postgres-order 135.1%` (>1,500% aggregate demand on an 8-thread VM).
  - Note: During Scenario B (`LOAD_SCALE=1.0`), `1,638` timed-out journeys polling `GET /api/v1/orders/{id}` every 250 ms for 30 s generated `110,599` polling requests (`544.78 req/s` total HTTP load), further amplifying CPU pressure on `gateway-service` and `order-service`.
