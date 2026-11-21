#!/usr/bin/env python3
"""
Dashboard Generator for Antigravity Ticketing Platform.
Generates Grafana dashboard JSON files from compact definitions into infra/observability/grafana/dashboards.
Standard library only.
"""

import json
import os
import sys

DASHBOARDS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "infra", "observability", "grafana", "dashboards"))

DATASOURCE = {"type": "prometheus", "uid": "prometheus"}

def make_panel(panel_id, title, description, p_type, targets, unit="short", grid_pos=None, thresholds=None):
    if grid_pos is None:
        grid_pos = {"h": 8, "w": 12, "x": 0, "y": 0}

    panel_targets = []
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for idx, t in enumerate(targets):
        if isinstance(t, str):
            expr = t
            legend = ""
        else:
            expr = t.get("expr", "")
            legend = t.get("legend", "")
        panel_targets.append({
            "datasource": DATASOURCE,
            "expr": expr,
            "legendFormat": legend,
            "refId": letters[idx % len(letters)],
        })

    field_config = {
        "defaults": {
            "unit": unit,
        },
        "overrides": []
    }

    if thresholds:
        field_config["defaults"]["thresholds"] = thresholds
        # If thresholds include line rendering
        field_config["defaults"]["custom"] = {
            "thresholdsStyle": {"mode": "line"}
        }

    panel = {
        "id": panel_id,
        "title": title,
        "description": description,
        "type": p_type,
        "datasource": DATASOURCE,
        "gridPos": grid_pos,
        "targets": panel_targets,
        "fieldConfig": field_config,
    }

    if p_type == "stat":
        panel["options"] = {
            "reduceOptions": {
                "values": False,
                "calcs": ["lastNotNull"],
                "fields": ""
            },
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "area"
        }
    elif p_type == "timeseries":
        panel["options"] = {
            "tooltip": {"mode": "single", "sort": "none"},
            "legend": {"displayMode": "list", "placement": "bottom", "calcs": []}
        }

    return panel

def layout_panels(panels_def):
    """Assigns unique IDs and grid positions to panels in a 24-column layout."""
    laid_out = []
    current_y = 0
    current_x = 0
    row_max_h = 0

    for idx, p in enumerate(panels_def, start=1):
        w = p.get("w", 12)
        h = p.get("h", 8)
        if current_x + w > 24:
            current_x = 0
            current_y += row_max_h
            row_max_h = 0

        grid_pos = {"h": h, "w": w, "x": current_x, "y": current_y}
        current_x += w
        if h > row_max_h:
            row_max_h = h

        panel = make_panel(
            panel_id=idx,
            title=p["title"],
            description=p["description"],
            p_type=p["type"],
            targets=p["targets"],
            unit=p.get("unit", "short"),
            grid_pos=grid_pos,
            thresholds=p.get("thresholds")
        )
        laid_out.append(panel)
    return laid_out

def build_dashboard(uid, title, panels_def, templating_list=None):
    panels = layout_panels(panels_def)
    dashboard = {
        "annotations": {"list": []},
        "editable": False,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": [],
        "liveNow": False,
        "panels": panels,
        "refresh": "5s",
        "schemaVersion": 39,
        "tags": ["ticketing-platform"],
        "templating": {"list": templating_list or []},
        "time": {
            "from": "now-30m",
            "to": "now"
        },
        "timepicker": {},
        "timezone": "browser",
        "title": title,
        "uid": uid,
        "version": 1
    }
    return dashboard

# =============================================================================
# Dashboard Definitions
# =============================================================================

def get_platform_overview_def():
    templating = [
        {
            "name": "service",
            "label": "Service",
            "type": "custom",
            "current": {"selected": True, "text": "All", "value": ".*"},
            "options": [
                {"selected": True, "text": "All", "value": ".*"},
                {"selected": False, "text": "gateway-service", "value": "gateway-service"},
                {"selected": False, "text": "inventory-service", "value": "inventory-service"},
                {"selected": False, "text": "order-service", "value": "order-service"},
                {"selected": False, "text": "payment-service", "value": "payment-service"},
                {"selected": False, "text": "notification-service", "value": "notification-service"},
            ],
            "query": ".*, gateway-service, inventory-service, order-service, payment-service, notification-service",
            "includeAll": False,
            "multi": False
        }
    ]

    panels = [
        {
            "title": "Up Status per Service",
            "description": "All service instances report up=1 indicating healthy Prometheus scrape status.",
            "type": "stat",
            "w": 12, "h": 5,
            "unit": "short",
            "targets": [
                {"expr": 'up{job=~".*service"}', "legend": "{{job}}"}
            ]
        },
        {
            "title": "Firing Alerts Count",
            "description": "Zero firing alerts indicates nominal platform operational status.",
            "type": "stat",
            "w": 12, "h": 5,
            "unit": "short",
            "targets": [
                {"expr": 'count(ALERTS{alertstate="firing"}) or vector(0)', "legend": "Firing Alerts"}
            ]
        },
        {
            "title": "Per Service Request Rate",
            "description": "Steady request throughput without sudden unpredicted drops indicates normal incoming traffic.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "reqps",
            "targets": [
                {"expr": 'sum by (service) (rate(http_server_requests_seconds_count{service=~"$service"}[5m]))', "legend": "{{service}}"}
            ]
        },
        {
            "title": "Error Ratio (5xx)",
            "description": "HTTP 5xx error ratio below 0.05% (0.0005) meets the platform availability SLO.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "percentunit",
            "targets": [
                {
                    "expr": '(sum by (service) (rate(http_server_requests_seconds_count{service=~"$service",status=~"5.."}[5m])) or (sum by (service) (rate(http_server_requests_seconds_count{service=~"$service"}[5m])) * 0)) / (sum by (service) (rate(http_server_requests_seconds_count{service=~"$service"}[5m])) > 0)',
                    "legend": "{{service}}"
                }
            ]
        },
        {
            "title": "Request Latency (p50, p95, p99)",
            "description": "HTTP server request latency percentiles: p50 under 25ms, p95 under 50ms, and p99 under 150ms reflect nominal responsiveness.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "s",
            "targets": [
                {"expr": 'histogram_quantile(0.50, sum by (le, service) (rate(http_server_requests_seconds_bucket{service=~"$service"}[5m])))', "legend": "p50 {{service}}"},
                {"expr": 'histogram_quantile(0.95, sum by (le, service) (rate(http_server_requests_seconds_bucket{service=~"$service"}[5m])))', "legend": "p95 {{service}}"},
                {"expr": 'histogram_quantile(0.99, sum by (le, service) (rate(http_server_requests_seconds_bucket{service=~"$service"}[5m])))', "legend": "p99 {{service}}"}
            ]
        },
        {
            "title": "JVM Heap Used Ratio",
            "description": "JVM heap utilization below 80% (0.80) indicates sufficient memory headroom without GC thrashing.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "percentunit",
            "targets": [
                {"expr": 'sum by (service) (jvm_memory_used_bytes{service=~"$service",area="heap"}) / sum by (service) (jvm_memory_max_bytes{service=~"$service",area="heap"})', "legend": "{{service}}"}
            ]
        },
        {
            "title": "JVM Live Threads",
            "description": "Live thread count should remain stable and bounded without unbounded thread leaks.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "short",
            "targets": [
                {"expr": 'jvm_threads_live_threads{service=~"$service"}', "legend": "{{service}}"}
            ]
        },
        {
            "title": "Hikari Active Connections",
            "description": "Active database connection pool usage below pool maximum (default 10) prevents connection exhaustion.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "short",
            "targets": [
                {"expr": 'hikaricp_connections_active{service=~"$service"}', "legend": "{{service}}"}
            ]
        },
        {
            "title": "Process CPU Usage",
            "description": "Process CPU utilization below 70% (0.70) indicates healthy CPU headroom across containers.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "percentunit",
            "targets": [
                {"expr": 'process_cpu_usage{service=~"$service"}', "legend": "{{service}}"}
            ]
        }
    ]
    return build_dashboard("platform-overview", "Platform Overview", panels, templating)

def get_business_flow_def():
    templating = [
        {
            "name": "service",
            "label": "Service",
            "type": "custom",
            "current": {"selected": True, "text": "All", "value": ".*"},
            "options": [
                {"selected": True, "text": "All", "value": ".*"},
                {"selected": False, "text": "inventory-service", "value": "inventory-service"},
                {"selected": False, "text": "order-service", "value": "order-service"},
                {"selected": False, "text": "payment-service", "value": "payment-service"},
                {"selected": False, "text": "notification-service", "value": "notification-service"},
            ],
            "query": ".*, inventory-service, order-service, payment-service, notification-service",
            "includeAll": False,
            "multi": False
        }
    ]

    panels = [
        {
            "title": "Holds Created and Rejected by Outcome",
            "description": "Holds created steadily with zero or low business rejection rate (under contention) indicates smooth inventory reservation.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "reqps",
            "targets": [
                {"expr": 'sum by (outcome) (rate(inventory_hold_requests_total[5m]))', "legend": "Hold: {{outcome}}"},
                {"expr": 'sum by (reason) (rate(ticketing_reservations_rejected_total[5m]))', "legend": "Rejected: {{reason}}"}
            ]
        },
        {
            "title": "Checkouts by Outcome",
            "description": "Checkout rate dominated by ACCEPTED with minimal non-accepted outcomes indicates healthy checkout saga processing.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "reqps",
            "targets": [
                {"expr": 'sum by (outcome) (rate(ticketing_orders_checkout_total[5m]))', "legend": "Checkout: {{outcome}}"}
            ]
        },
        {
            "title": "Orders Materialization Lag p95",
            "description": "p95 order materialization lag below 350ms ensures orders are ready for payment immediately after reservation.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "s",
            "targets": [
                {"expr": 'histogram_quantile(0.95, sum by (le) (rate(ticketing_orders_materialization_lag_seconds_bucket[5m])))', "legend": "p95 materialization lag"}
            ]
        },
        {
            "title": "Payments by Status and Failure Code",
            "description": "Payments dominated by SUCCEEDED status with failure rate matching expected decline rate indicates normal payment processor integration.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "reqps",
            "targets": [
                {"expr": 'sum by (status, failure_code) (rate(ticketing_payments_processed_total[5m]))', "legend": "Payment: {{status}} ({{failure_code}})"}
            ]
        },
        {
            "title": "Refunds Rate & Declines",
            "description": "Refund call rate should track compensating transaction volume and decline count should remain zero.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "reqps",
            "targets": [
                {"expr": 'sum by (outcome) (rate(payment_provider_calls_total{operation="refund"}[5m]))', "legend": "Refund: {{outcome}}"},
                {"expr": 'sum by (failure_code) (rate(payment_refund_declined_total[5m]))', "legend": "Declined: {{failure_code}}"}
            ]
        },
        {
            "title": "Notifications by Event Type and Outcome",
            "description": "Notification events processed with outcome 'sent' and zero failed outcomes reflects reliable async buyer notifications.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "reqps",
            "targets": [
                {"expr": 'sum by (event_type, outcome) (rate(notification_events_total[5m]))', "legend": "{{event_type}} ({{outcome}})"}
            ]
        },
        {
            "title": "Reservations Expired Per Minute",
            "description": "Expired reservations rate matching abandoned hold volume confirms the 1000ms sweeper is actively releasing overdue seats.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "opm",
            "targets": [
                {"expr": 'sum by (trigger) (rate(ticketing_reservations_expired_total[5m])) * 60', "legend": "Expired ({{trigger}})"}
            ]
        }
    ]
    return build_dashboard("business-flow", "Business Flow", panels, templating)

def get_messaging_def():
    templating = [
        {
            "name": "service",
            "label": "Service",
            "type": "custom",
            "current": {"selected": True, "text": "All", "value": ".*"},
            "options": [
                {"selected": True, "text": "All", "value": ".*"},
                {"selected": False, "text": "inventory-service", "value": "inventory-service"},
                {"selected": False, "text": "order-service", "value": "order-service"},
                {"selected": False, "text": "payment-service", "value": "payment-service"},
                {"selected": False, "text": "notification-service", "value": "notification-service"},
            ],
            "query": ".*, inventory-service, order-service, payment-service, notification-service",
            "includeAll": False,
            "multi": False
        }
    ]

    panels = [
        {
            "title": "Consumer Group Lag per Group and Topic",
            "description": "Consumer group lag near zero across all topics indicates consumers are keeping up with Kafka event streams.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "short",
            "targets": [
                {"expr": 'sum by (consumergroup, topic) (clamp_min(kafka_consumergroup_lag, 0))', "legend": "{{consumergroup}} -> {{topic}}"}
            ]
        },
        {
            "title": "Retry and Dead-Letter Topic End Offsets",
            "description": "Dead-letter topic (-dlt) offsets should remain flat at zero; retry topic offsets should only increase during transient errors.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "short",
            "targets": [
                {"expr": 'sum by (topic) (kafka_topic_partition_current_offset{topic=~".*-(retry-[01]|dlt)"})', "legend": "Offset: {{topic}}"}
            ]
        },
        {
            "title": "Dead-Letter and Retry Topic 1h Increase",
            "description": "Zero increase over 1 hour on dead-letter topics indicates zero poison messages routed to DLT.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "short",
            "targets": [
                {"expr": 'sum by (topic) (increase(kafka_topic_partition_current_offset{topic=~".*-(retry-[01]|dlt)"}[1h]))', "legend": "1h Increase: {{topic}}"}
            ]
        },
        {
            "title": "Outbox Unpublished Rows per Service",
            "description": "Outbox unpublished queue depth below 50 records confirms outbox pollers are draining committed events within 200ms.",
            "type": "timeseries",
            "w": 12, "h": 8,
            "unit": "short",
            "targets": [
                {"expr": 'ticketing_outbox_unpublished_records{service=~"$service"}', "legend": "Unpublished: {{service}}"}
            ]
        },
        {
            "title": "Outbox Oldest Pending Age per Service",
            "description": "Oldest unpublished outbox row age below 1 second indicates real-time event publishing without pipeline stalls.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "s",
            "targets": [
                {"expr": 'outbox_oldest_pending_age_seconds{service=~"$service"}', "legend": "Oldest Age: {{service}}"}
            ]
        },
        {
            "title": "Consumer Duplicate and Retried Counters",
            "description": "Duplicate delivery rate should remain low and retried events should remain near zero during steady-state processing.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "events/s",
            "targets": [
                {"expr": 'sum by (service, consumer_group) (rate(consumer_events_duplicate_total{service=~"$service"}[5m]))', "legend": "Duplicate: {{service}} ({{consumer_group}})"},
                {"expr": 'sum by (service, consumer_group) (rate(consumer_events_retried_total{service=~"$service"}[5m]))', "legend": "Retried: {{service}} ({{consumer_group}})"}
            ]
        },
        {
            "title": "Notification Claim Wait and Release Counters",
            "description": "Zero claim releases and minimal claim waits indicate healthy Redis deduplication lock handoffs between notification workers.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "ops/s",
            "targets": [
                {"expr": 'rate(notification_claim_wait_total[5m])', "legend": "Claim Wait"},
                {"expr": 'sum by (reason) (rate(notification_claim_release_total[5m]))', "legend": "Claim Release: {{reason}}"}
            ]
        }
    ]
    return build_dashboard("messaging", "Messaging", panels, templating)

def get_slo_def():
    panels = [
        {
            "title": "Seat Hold Availability SLI Ratio",
            "description": "Hold availability SLI ratio at or above 99.95% (0.9995) non-5xx preserves the error budget.",
            "type": "timeseries",
            "w": 12, "h": 7,
            "unit": "percentunit",
            "targets": [
                {"expr": 'sli:hold_availability:ratio5m', "legend": "Hold Availability (5m)"}
            ]
        },
        {
            "title": "Order Checkout Availability SLI Ratio",
            "description": "Checkout availability SLI ratio at or above 99.95% (0.9995) non-5xx preserves the error budget.",
            "type": "timeseries",
            "w": 12, "h": 7,
            "unit": "percentunit",
            "targets": [
                {"expr": 'sli:checkout_availability:ratio5m', "legend": "Checkout Availability (5m)"}
            ]
        },
        {
            "title": "Seat Hold Latency SLI (p95 & p99)",
            "description": "Seat hold latency p95 below 45ms and p99 below 120ms meets the hold response time SLO.",
            "type": "timeseries",
            "w": 12, "h": 7,
            "unit": "s",
            "targets": [
                {"expr": 'sli:hold_201_latency_seconds:p95_5m', "legend": "p95 (201 Created)"},
                {"expr": 'sli:hold_201_latency_seconds:p99_5m', "legend": "p99 (201 Created)"}
            ]
        },
        {
            "title": "Order Checkout Latency SLI (p95 & p99)",
            "description": "Order checkout latency p95 below 50ms and p99 below 150ms meets the checkout response time SLO.",
            "type": "timeseries",
            "w": 12, "h": 7,
            "unit": "s",
            "targets": [
                {"expr": 'sli:checkout_202_latency_seconds:p95_5m', "legend": "p95 (202 Accepted)"},
                {"expr": 'sli:checkout_202_latency_seconds:p99_5m', "legend": "p99 (202 Accepted)"}
            ]
        },
        {
            "title": "Order Materialization Lag SLI (p95 & p99)",
            "description": "Order materialization lag p95 below 350ms and p99 below 800ms meets the end-to-end event materialization SLO.",
            "type": "timeseries",
            "w": 12, "h": 7,
            "unit": "s",
            "targets": [
                {"expr": 'sli:order_materialization_lag_seconds:p95_5m', "legend": "p95 lag"},
                {"expr": 'sli:order_materialization_lag_seconds:p99_5m', "legend": "p99 lag"}
            ]
        },
        {
            "title": "Error Budget Remaining Ratio (30d)",
            "description": "Remaining error budget ratio above 0.0 (positive) indicates availability target is satisfied over the 30-day window.",
            "type": "timeseries",
            "w": 12, "h": 7,
            "unit": "percentunit",
            "targets": [
                {"expr": 'slo:hold_availability:error_budget_remaining_ratio30d', "legend": "Hold Availability Budget"},
                {"expr": 'slo:checkout_availability:error_budget_remaining_ratio30d', "legend": "Checkout Availability Budget"}
            ]
        },
        {
            "title": "Hold Availability Burn Rates (Fast & Slow)",
            "description": "Burn rates equal to 0 indicate consumption within budget; burn rate of 1 triggers alerting (fast: 14.4x, slow: 6x).",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "short",
            "thresholds": {
                "mode": "absolute",
                "steps": [
                    {"color": "green", "value": None},
                    {"color": "red", "value": 1.0}
                ]
            },
            "targets": [
                {"expr": 'slo:hold_availability:burnrate_fast', "legend": "Fast Burn (14.4x)"},
                {"expr": 'slo:hold_availability:burnrate_slow', "legend": "Slow Burn (6.0x)"}
            ]
        },
        {
            "title": "Checkout Availability Burn Rates (Fast & Slow)",
            "description": "Burn rates equal to 0 indicate consumption within budget; burn rate of 1 triggers alerting (fast: 14.4x, slow: 6x).",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "short",
            "thresholds": {
                "mode": "absolute",
                "steps": [
                    {"color": "green", "value": None},
                    {"color": "red", "value": 1.0}
                ]
            },
            "targets": [
                {"expr": 'slo:checkout_availability:burnrate_fast', "legend": "Fast Burn (14.4x)"},
                {"expr": 'slo:checkout_availability:burnrate_slow', "legend": "Slow Burn (6.0x)"}
            ]
        },
        {
            "title": "Active Platform Alerts",
            "description": "No firing or pending alerts indicates complete operational health across all SLO and platform monitors.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "short",
            "targets": [
                {"expr": 'sum by (alertname, severity) (ALERTS)', "legend": "{{alertname}} ({{severity}})"}
            ]
        }
    ]
    return build_dashboard("slo", "SLO", panels)

def get_dependencies_def():
    templating = [
        {
            "name": "database",
            "label": "Database",
            "type": "custom",
            "current": {"selected": True, "text": "All", "value": ".*"},
            "options": [
                {"selected": True, "text": "All", "value": ".*"},
                {"selected": False, "text": "inventory_db", "value": "inventory_db"},
                {"selected": False, "text": "order_db", "value": "order_db"},
                {"selected": False, "text": "payment_db", "value": "payment_db"},
            ],
            "query": ".*, inventory_db, order_db, payment_db",
            "includeAll": False,
            "multi": False
        }
    ]

    panels = [
        {
            "title": "Redis Up Status",
            "description": "redis_up equal to 1 confirms Redis 7 is online and responding to ping.",
            "type": "stat",
            "w": 4, "h": 5,
            "unit": "short",
            "targets": [
                {"expr": "redis_up", "legend": "Redis Up"}
            ]
        },
        {
            "title": "Redis Memory Used",
            "description": "Redis memory usage should stay bounded well below available system memory.",
            "type": "timeseries",
            "w": 10, "h": 5,
            "unit": "bytes",
            "targets": [
                {"expr": "redis_memory_used_bytes", "legend": "Memory Used"}
            ]
        },
        {
            "title": "Redis Connected Clients",
            "description": "Connected clients count bounded within connection pool limits prevents client exhaustion.",
            "type": "timeseries",
            "w": 10, "h": 5,
            "unit": "short",
            "targets": [
                {"expr": "redis_connected_clients", "legend": "Connected Clients"}
            ]
        },
        {
            "title": "Redis Commands Rate",
            "description": "Command rate tracking platform transaction volume reflects active seat gating and rate limiting.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "ops/s",
            "targets": [
                {"expr": "rate(redis_commands_total[5m])", "legend": "Commands/sec"}
            ]
        },
        {
            "title": "Redis Keyspace Hits and Misses",
            "description": "Keyspace hits and misses rates reflecting active key lookups and TTL expirations.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "ops/s",
            "targets": [
                {"expr": "rate(redis_keyspace_hits_total[5m])", "legend": "Hits/sec"},
                {"expr": "rate(redis_keyspace_misses_total[5m])", "legend": "Misses/sec"}
            ]
        },
        {
            "title": "Redis Rejected Connections",
            "description": "Zero rejected connections confirms Redis maxclients limit has not been breached.",
            "type": "timeseries",
            "w": 8, "h": 7,
            "unit": "ops/s",
            "targets": [
                {"expr": "rate(redis_rejected_connections_total[5m])", "legend": "Rejected Connections/sec"}
            ]
        },
        {
            "title": "PostgreSQL Up Status per Database",
            "description": "pg_up equal to 1 across all 3 databases confirms PostgreSQL instances are online.",
            "type": "stat",
            "w": 6, "h": 5,
            "unit": "short",
            "targets": [
                {"expr": "pg_up", "legend": "{{job}}"}
            ]
        },
        {
            "title": "PostgreSQL Active Connections per Database",
            "description": "Active connection count below max_connections (100) ensures database connection headroom.",
            "type": "timeseries",
            "w": 9, "h": 5,
            "unit": "short",
            "targets": [
                {"expr": 'sum by (datname) (pg_stat_database_numbackends{datname=~"$database"})', "legend": "{{datname}}"}
            ]
        },
        {
            "title": "PostgreSQL Transaction Rate per Database",
            "description": "Committed transactions rate reflecting write throughput across inventory, order, and payment databases.",
            "type": "timeseries",
            "w": 9, "h": 5,
            "unit": "tps",
            "targets": [
                {"expr": 'sum by (datname) (rate(pg_stat_database_xact_commit{datname=~"$database"}[5m]))', "legend": "Commit Rate: {{datname}}"}
            ]
        },
        {
            "title": "PostgreSQL Locks and Deadlocks",
            "description": "Zero deadlocks and stable lock counts confirm transactional concurrency without lock contention.",
            "type": "timeseries",
            "w": 12, "h": 7,
            "unit": "short",
            "targets": [
                {"expr": 'sum by (datname) (pg_locks_count{datname=~"$database"})', "legend": "Locks: {{datname}}"},
                {"expr": 'sum by (datname) (rate(pg_stat_database_deadlocks{datname=~"$database"}[5m]))', "legend": "Deadlocks: {{datname}}"}
            ]
        },
        {
            "title": "PostgreSQL Cache Hit Ratio per Database",
            "description": "Buffer cache hit ratio above 99% (0.99) ensures efficient in-memory query execution without disk reads.",
            "type": "timeseries",
            "w": 12, "h": 7,
            "unit": "percentunit",
            "targets": [
                {
                    "expr": 'sum by (datname) (pg_stat_database_blks_hit{datname=~"$database"}) / (sum by (datname) (pg_stat_database_blks_hit{datname=~"$database"}) + sum by (datname) (pg_stat_database_blks_read{datname=~"$database"}) > 0)',
                    "legend": "Cache Hit Ratio: {{datname}}"
                }
            ]
        },
        {
            "title": "Kafka Active Brokers",
            "description": "Active broker count equal to 1 confirms KRaft broker is registered and serving cluster metadata.",
            "type": "stat",
            "w": 6, "h": 5,
            "unit": "short",
            "targets": [
                {"expr": "kafka_brokers", "legend": "Brokers"}
            ]
        },
        {
            "title": "Kafka Topic Partition Offsets Rate",
            "description": "Topic offset growth rate reflects active event publishing across platform event streams.",
            "type": "timeseries",
            "w": 10, "h": 5,
            "unit": "events/s",
            "targets": [
                {"expr": 'sum by (topic) (rate(kafka_topic_partition_current_offset[5m]))', "legend": "Offset Rate: {{topic}}"}
            ]
        },
        {
            "title": "Kafka Under-Replicated Partitions",
            "description": "Zero under-replicated partitions indicates healthy Kafka partition replication.",
            "type": "timeseries",
            "w": 8, "h": 5,
            "unit": "short",
            "targets": [
                {"expr": 'kafka_topic_partitions{state="under_replicated"}', "legend": "Under Replicated: {{topic}}"}
            ]
        }
    ]
    return build_dashboard("dependencies", "Dependencies", panels, templating)

def main():
    os.makedirs(DASHBOARDS_DIR, exist_ok=True)
    generators = [
        ("platform-overview.json", get_platform_overview_def),
        ("business-flow.json", get_business_flow_def),
        ("messaging.json", get_messaging_def),
        ("slo.json", get_slo_def),
        ("dependencies.json", get_dependencies_def),
    ]

    print(f"Generating {len(generators)} Grafana dashboards into: {DASHBOARDS_DIR}")
    for filename, fn in generators:
        dash = fn()
        out_path = os.path.join(DASHBOARDS_DIR, filename)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(dash, f, indent=2, sort_keys=True)
        print(f"  [OK] Generated {filename} (uid={dash['uid']}, {len(dash['panels'])} panels)")

    print("[SUCCESS] All dashboards generated successfully.")

if __name__ == "__main__":
    main()
