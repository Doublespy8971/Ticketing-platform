# Platform Observability Guide

## 1. Architecture

The ticketing platform observability stack runs alongside the 10-container application topology on the shared `ticketing-net` bridge network:

```text
  [gateway-service :8080/actuator/prometheus]        (5s)  ──┐
  [inventory-service :8081/actuator/prometheus]      (5s)  ──┤
  [order-service :8082/actuator/prometheus]          (5s)  ──┼──▶ [Prometheus :9090 (127.0.0.1)] ──▶ [Grafana :3000 (127.0.0.1)]
  [payment-service :8083/actuator/prometheus]        (5s)  ──┤          │
  [notification-service :8084/metrics]               (5s)  ──┤          ├─ slo.rules.yml (5m/30m/1h/6h/30d SLIs, burn rates)
                                                                        └─ alerts.rules.yml (20 alerting rules + runbooks)
  [kafka-exporter :9308/metrics] ◀── Kafka (:9092)   (15s) ──┤
  [redis-exporter :9121/metrics] ◀── Redis (:6379)   (15s) ──┤
  [postgres-*-exporter :9187/metrics] ◀── PG16 (x3)  (15s) ──┘
```

### Design Choices & Assumptions
1. **Prometheus Retention & 30-Day Budget Approximation**: TSDB retention is configured to `3d` (`--storage.tsdb.retention.time=3d`) for local/dev footprint efficiency. Recording rules targeting 30-day error budgets (`rate(...[30d])`) evaluate accurately across all available samples inside the retention window.
2. **Localhost-Only Publishing**: Only Prometheus (`127.0.0.1:9090`) and Grafana (`127.0.0.1:3000`) publish host ports; all exporters (`kafka-exporter`, `redis-exporter`, and the 3 `postgres-exporter` containers) are scraped strictly over the internal `ticketing-net` Docker network.
3. **Gateway Production Actuator Port**: In `prod` profile (`application-prod.yml`), `gateway-service` binds Actuator (`health,info,prometheus`) to internal port `8085` with `show-details: never`. Local Docker Compose uses `SPRING_PROFILES_ACTIVE=local` (`8080`).
4. **OpenMetrics `_created` Normalization**: `ReservationService` registers `Counter.builder("ticketing_reservations_created_total")` per `docs/arch/09-conventions.md`; Micrometer 1.13's `PrometheusMeterRegistry` (`prometheus-metrics-core`) treats `_created` as an OpenMetrics timestamp suffix and exports the series as `ticketing_reservations_total`. Both are documented and accounted for in `scripts/metrics-optional.txt` and `scripts/check-metrics.sh`.
5. **Prometheus Histogram Buckets**: `management.metrics.distribution.percentiles-histogram.http.server.requests: true` and explicit SLO buckets (`25ms,50ms,100ms,250ms,500ms,1s,2500ms,5s`) are enabled across all four Java services (`2500ms` is used instead of `2.5s` for Spring Boot `DurationStyle` compatibility). `order-service` additionally enables percentile histograms on `ticketing_orders_materialization_lag_seconds`.
6. **Uninitialized Consumer Offsets (`kafka_consumergroup_lag`)**: `kafka-exporter` emits `-1` on partitions with no committed offset yet; alert rules and `scripts/obs-queries.sh` clamp or filter `> 0` before aggregating lag.
7. **Grafana Provisioning & Auth**: Anonymous access is disabled (`GF_AUTH_ANONYMOUS_ENABLED=false`). Provisioning directories under `infra/observability/grafana/provisioning/` are mounted read-only and intentionally empty until dashboards are added. Development-only credentials are sourced from `infra/.env` (`infra/.env.example`).
8. **Optional / Lazy Metrics**: Metrics that require fault injection, dead letters, duplicate redelivery, or Go non-HTTP server design are allowlisted with one-line justifications in `scripts/metrics-optional.txt`.

---

## 2. How to Start & Verify

```bash
# 1. Start (or reset) the 10-container application stack
make stack-reset

# 2. Start Prometheus, Grafana, and exporters alongside the stack and wait until healthy
make obs-up

# 3. Print local URLs
make obs-urls

# 4. Validate Prometheus configuration & rule syntax via promtool
make obs-rules-check

# 5. Verify all 10 Prometheus scrape targets are UP
make obs-targets-check

# 6. Run traffic probe, verify metrics catalog, common tags, JVM, SLO buckets & URI hygiene
make metrics-check

# 7. Run 5 sample PromQL verification queries
bash scripts/obs-queries.sh
```

Other lifecycle targets: `make obs-down` (stops observability containers), `make obs-reset` (wipes observability volumes and restarts cleanly).

---

## 3. Metric Catalog by Service

All application and HTTP metrics include common tag `service="<service-name>"`. Unmatched HTTP paths collapse cleanly to `uri="/**"` or `uri="NOT_FOUND"` (never raw paths).

### `gateway-service` (`:8080/actuator/prometheus`, prod `:8085`)
| Metric | Type | Tags | Source |
|---|---|---|---|
| `http_server_requests_seconds` | Timer (Histogram) | `service`, `method`, `uri`, `status`, `outcome` | Spring WebFlux + Actuator |
| `jvm_*` | Gauge/Counter | `service`, ... | Micrometer JVM Metrics |
| `ticketing_redis_lua_duration_seconds` | Timer | `service`, `script`, `result` | `RedisRateLimiterService` |
| `gateway_requests_total` | Counter | `service`, `route`, `status_class` | `GatewayMetrics` / `MetricsWebFilter` |
| `gateway_rate_limited_total` | Counter | `service`, `route` | `GatewayMetrics` |
| `gateway_auth_failures_total` | Counter | `service`, `reason` | `GatewayMetrics` |
| `gateway_redis_failures_total` | Counter | `service` | `RedisRateLimiterService` |

### `inventory-service` (`:8081/actuator/prometheus`)
| Metric | Type | Tags | Source |
|---|---|---|---|
| `http_server_requests_seconds` | Timer (Histogram) | `service`, `method`, `uri`, `status`, `outcome` | Spring WebMVC + Actuator |
| `jvm_*` | Gauge/Counter | `service`, ... | Micrometer JVM Metrics |
| `ticketing_reservations_total` (`ticketing_reservations_created_total`) | Counter | `service` | `ReservationService` |
| `ticketing_reservations_rejected_total` | Counter | `service`, `reason` | `ReservationService` |
| `ticketing_reservations_expired_total` | Counter | `service`, `trigger` | `ExpirySweeper` |
| `inventory_hold_requests_total` / `inventory_hold_duration_seconds` | Counter / Timer | `service`, `outcome` | `ReservationService` |
| `inventory_oldest_overdue_hold_seconds` | Gauge | `service` | `ExpirySweeper` |
| `inventory_expiry_sweep_duration_seconds` / `failures_total` | Timer / Counter | `service` | `ExpirySweeper` |
| `ticketing_redis_lua_duration_seconds` | Timer | `service`, `script`, `result` | `RedisSeatGate` |
| `inventory_redis_unavailable_total` | Counter | `service`, `operation`, `cause` | `RedisSeatGate` / `InventoryExceptionHandler` |
| `inventory_redis_confirm_failures_total` / `release_failures_total` / `compensations_total` | Counter | `service` | `PaymentEventService` / `ReservationService` |
| `inventory_payment_events_handled_total` | Counter | `service`, `event_type`, `outcome` | `PaymentEventService` |
| `ticketing_outbox_unpublished_records` / `outbox_pending_rows` | Gauge | `service` | `OutboxMetrics` (`ticketing-outbox`) |
| `outbox_oldest_pending_age_seconds` | Gauge | `service` | `OutboxMetrics` (`ticketing-outbox`) |
| `outbox_published_total` / `outbox_publish_failures_total` | Counter | `service` | `OutboxMetrics` (`ticketing-outbox`) |
| `ticketing_outbox_publish_latency_seconds` | Timer | `service`, `topic` | `OutboxMetrics` (`ticketing-outbox`) |
| `consumer_events_processed_total` / `duplicate_total` / `retried_total` | Counter | `service`, `consumer_group`, `event_type` | `ConsumerMetrics` (`ticketing-consumer`) |
| `consumer_event_processing_seconds` | Timer | `service`, `consumer_group`, `event_type` | `ConsumerMetrics` (`ticketing-consumer`) |
| `ticketing_kafka_consumer_dlt_total` | Counter | `service`, `topic`, `consumer_group` | `ConsumerMetrics` (`ticketing-consumer`) |

### `order-service` (`:8082/actuator/prometheus`)
| Metric | Type | Tags | Source |
|---|---|---|---|
| `http_server_requests_seconds` | Timer (Histogram) | `service`, `method`, `uri`, `status`, `outcome` | Spring WebMVC + Actuator |
| `jvm_*` | Gauge/Counter | `service`, ... | Micrometer JVM Metrics |
| `ticketing_orders_checkout_total` / `order_checkout_duration_seconds` | Counter / Timer | `service`, `outcome` | `CheckoutService` |
| `ticketing_orders_materialization_lag_seconds` | Timer (Histogram) | `service` | `OrderSagaService` |
| `ticketing_orders_terminal_total` | Counter | `service`, `final_status` | `OrderSagaService` |
| `order_saga_events_handled_total` | Counter | `service`, `event_type`, `outcome` | `OrderSagaService` |
| `ticketing_outbox_*` & `outbox_*` | Gauge/Counter/Timer | `service`, (`topic`) | `OutboxMetrics` (`ticketing-outbox`) |
| `consumer_*` & `ticketing_kafka_consumer_dlt_total` | Counter/Timer | `service`, `consumer_group`, ... | `ConsumerMetrics` (`ticketing-consumer`) |

### `payment-service` (`:8083/actuator/prometheus`)
| Metric | Type | Tags | Source |
|---|---|---|---|
| `http_server_requests_seconds` | Timer (Histogram) | `service`, `method`, `uri`, `status`, `outcome` | Spring WebMVC + Actuator |
| `jvm_*` | Gauge/Counter | `service`, ... | Micrometer JVM Metrics |
| `ticketing_payments_processed_total` | Counter | `service`, `status`, `failure_code` | `PaymentMetrics` |
| `payment_provider_calls_total` / `payment_provider_duration_seconds` | Counter / Timer | `service`, `operation`, (`outcome`) | `PaymentMetrics` |
| `payment_events_handled_total` | Counter | `service`, `event_type`, `outcome` | `PaymentMetrics` |
| `payment_refund_declined_total` | Counter | `service`, `failure_code` | `PaymentMetrics` |
| `ticketing_outbox_*` & `outbox_*` | Gauge/Counter/Timer | `service`, (`topic`) | `OutboxMetrics` (`ticketing-outbox`) |
| `consumer_*` & `ticketing_kafka_consumer_dlt_total` | Counter/Timer | `service`, `consumer_group`, ... | `ConsumerMetrics` (`ticketing-consumer`) |

### `notification-service` (`:8084/metrics`)
| Metric | Type | Tags | Source |
|---|---|---|---|
| `ticketing_notifications_sent_total` | Counter | `service`, `event_type`, `status` | `internal/metrics/metrics.go` |
| `notification_events_total` | Counter | `service`, `event_type`, `outcome` | `internal/metrics/metrics.go` |
| `notification_send_duration_seconds` | Histogram | `service` | `internal/metrics/metrics.go` |
| `notification_dedup_redis_errors_total` | Counter | `service` | `internal/metrics/metrics.go` |
| `notification_claim_wait_total` | Counter | `service` | `internal/metrics/metrics.go` |
| `notification_claim_release_total` | Counter | `service`, `reason` | `internal/metrics/metrics.go` |

### Infrastructure Exporters
- **`kafka-exporter` (`:9308/metrics`)**: `kafka_brokers`, `kafka_topic_partition_current_offset`, `kafka_consumergroup_lag`.
- **`redis_exporter` (`:9121/metrics`)**: `redis_up`, memory, commands, client stats.
- **`postgres-*-exporter` (`:9187/metrics`)**: `pg_up`, `pg_stat_*`, connections, locks.

---

## 4. SLOs & Recording Rules (`infra/observability/prometheus/rules/slo.rules.yml`)

Implementing the SLOs from `docs/arch/09-conventions.md` (`99.95%` availability target -> error budget `B = 0.0005`; fast burn `14.4x` over `5m` & `1h`; slow burn `6x` over `30m` & `6h`):

| SLO | Target | Recording Rules | Exact PromQL Basis |
|---|---|---|---|
| **Seat Hold Availability** | `>= 99.95%` non-5xx (`B = 0.0005`) | `sli:hold_availability:ratio{5m,30m,1h,6h}`<br>`slo:hold_availability:error_budget_remaining_ratio30d`<br>`slo:hold_availability:burnrate_{fast,slow}` | `1 - (sum(rate(http_server_requests_seconds_count{service="inventory-service",uri="/api/v1/reservations",method="POST",status=~"5.."}[w])) / sum(rate(http_server_requests_seconds_count{service="inventory-service",uri="/api/v1/reservations",method="POST"}[w])))` |
| **Seat Hold Latency (`201`)** | `p95 <= 45ms`, `p99 <= 120ms` | `sli:hold_201_latency_seconds:p95_5m`, `:p99_{5m,30m,1h,6h}` | `histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",method="POST",status="201"}[w])))` |
| **Seat Hold Fast Reject (`409`)** | `p95 <= 8ms`, `p99 <= 20ms` | `sli:hold_409_latency_seconds:p95_5m`, `:p99_{5m,30m,1h,6h}` | `histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",method="POST",status="409"}[w])))` |
| **Order Checkout Availability** | `>= 99.95%` non-5xx (`B = 0.0005`) | `sli:checkout_availability:ratio{5m,30m,1h,6h}`<br>`slo:checkout_availability:error_budget_remaining_ratio30d`<br>`slo:checkout_availability:burnrate_{fast,slow}` | `1 - (sum(rate(http_server_requests_seconds_count{service="order-service",uri="/api/v1/orders/{orderId}/checkout",method="POST",status=~"5.."}[w])) / sum(rate(http_server_requests_seconds_count{service="order-service",uri="/api/v1/orders/{orderId}/checkout",method="POST"}[w])))` |
| **Order Checkout Latency (`202`)** | `p95 <= 50ms`, `p99 <= 150ms` | `sli:checkout_202_latency_seconds:p95_5m`, `:p99_{5m,30m,1h,6h}` | `histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="order-service",uri="/api/v1/orders/{orderId}/checkout",method="POST",status="202"}[w])))` |
| **Order Materialization Lag** | `p95 <= 350ms`, `p99 <= 800ms` | `sli:order_materialization_lag_seconds:p95_5m`, `:p99_{5m,30m,1h,6h}` | `histogram_quantile(0.99, sum by (le) (rate(ticketing_orders_materialization_lag_seconds_bucket{service="order-service"}[w])))` |

---

## 5. Alert Catalog (`infra/observability/prometheus/rules/alerts.rules.yml`) & Runbook Index

Every alert below uses verified live metrics and links directly to its <=25-line runbook under `docs/runbooks/`:

| Alert Name | Severity | Verified Metric(s) Used | Where Defined | `for` | Runbook |
|---|---|---|---|---|---|
| `ServiceDown` | `critical` | `up` | Prometheus built-in | `1m` | [ServiceDown.md](runbooks/ServiceDown.md) |
| `HoldSLOFastBurn` | `critical` | `slo:hold_availability:burnrate_fast` (`http_server_requests_seconds_count`) | `slo.rules.yml` / `inventory-service` | `2m` | [HoldSLOFastBurn.md](runbooks/HoldSLOFastBurn.md) |
| `HoldSLOSlowBurn` | `warning` | `slo:hold_availability:burnrate_slow` (`http_server_requests_seconds_count`) | `slo.rules.yml` / `inventory-service` | `5m` | [HoldSLOSlowBurn.md](runbooks/HoldSLOSlowBurn.md) |
| `CheckoutSLOFastBurn` | `critical` | `slo:checkout_availability:burnrate_fast` (`http_server_requests_seconds_count`) | `slo.rules.yml` / `order-service` | `2m` | [CheckoutSLOFastBurn.md](runbooks/CheckoutSLOFastBurn.md) |
| `Gateway5xxRatioHigh` | `critical` | `gateway_requests_total` | `GatewayMetrics.java` | `2m` | [Gateway5xxRatioHigh.md](runbooks/Gateway5xxRatioHigh.md) |
| `Gateway429Spike` | `warning` | `gateway_rate_limited_total` | `GatewayMetrics.java` | `2m` | [Gateway429Spike.md](runbooks/Gateway429Spike.md) |
| `DeadLetterGrowth` | `critical` | `kafka_topic_partition_current_offset` | `kafka-exporter` | `0m` | [DeadLetterGrowth.md](runbooks/DeadLetterGrowth.md) |
| `RetryTopicBacklog` | `warning` | `kafka_consumergroup_lag` | `kafka-exporter` | `2m` | [RetryTopicBacklog.md](runbooks/RetryTopicBacklog.md) |
| `ConsumerLagHigh` | `warning` | `kafka_consumergroup_lag` | `kafka-exporter` | `2m` | [ConsumerLagHigh.md](runbooks/ConsumerLagHigh.md) |
| `OutboxStuck` | `critical` | `outbox_oldest_pending_age_seconds` | `OutboxMetrics.java` (`ticketing-outbox`) | `1m` | [OutboxStuck.md](runbooks/OutboxStuck.md) |
| `OutboxPendingGrowing` | `warning` | `ticketing_outbox_unpublished_records` | `OutboxMetrics.java` (`ticketing-outbox`) | `2m` | [OutboxPendingGrowing.md](runbooks/OutboxPendingGrowing.md) |
| `ExpirySweeperBehind` | `warning` | `inventory_oldest_overdue_hold_seconds` | `ExpirySweeper.java` (`inventory-service`) | `1m` | [ExpirySweeperBehind.md](runbooks/ExpirySweeperBehind.md) |
| `RedisGateUnavailable` | `critical` | `inventory_redis_unavailable_total` | `RedisSeatGate.java` / `InventoryExceptionHandler.java` | `1m` | [RedisGateUnavailable.md](runbooks/RedisGateUnavailable.md) |
| `RedisDown` | `critical` | `redis_up` | `redis_exporter` | `30s` | [RedisDown.md](runbooks/RedisDown.md) |
| `PaymentProviderErrors` | `warning` | `payment_provider_calls_total` | `PaymentMetrics.java` (`payment-service`) | `2m` | [PaymentProviderErrors.md](runbooks/PaymentProviderErrors.md) |
| `PaymentRefundDeclined` | `critical` | `payment_refund_declined_total` | `PaymentMetrics.java` (`payment-service`) | `0m` | [PaymentRefundDeclined.md](runbooks/PaymentRefundDeclined.md) |
| `NotificationFailures` | `warning` | `notification_events_total` | `metrics.go` (`notification-service`) | `1m` | [NotificationFailures.md](runbooks/NotificationFailures.md) |
| `NotificationServiceStalled` | `critical` | `kafka_consumergroup_lag`, `notification_events_total` | `kafka-exporter`, `metrics.go` | `1m` | [NotificationServiceStalled.md](runbooks/NotificationServiceStalled.md) |
| `PostgresDown` | `critical` | `pg_up` | `postgres-exporter` (x3) | `30s` | [PostgresDown.md](runbooks/PostgresDown.md) |
| `KafkaBrokerDown` | `critical` | `kafka_brokers`, `up` | `kafka-exporter` | `30s` | [KafkaBrokerDown.md](runbooks/KafkaBrokerDown.md) |
