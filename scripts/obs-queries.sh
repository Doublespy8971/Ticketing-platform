#!/usr/bin/env bash
set -euo pipefail

PROM_URL="${PROM_URL:-http://127.0.0.1:9090}"

python3 - "$PROM_URL" << 'PYEOF'
import json, sys, urllib.parse, urllib.request

prom_url = sys.argv[1].rstrip("/")

queries = [
    ("1. Hold request rate by outcome (5m)",
     'sum by (outcome) (rate(inventory_hold_requests_total[5m]))'),
    ("2. Gateway request rate by status_class (5m)",
     'sum by (status_class) (rate(gateway_requests_total[5m]))'),
    ("3. p99 of http_server_requests_seconds for POST /api/v1/reservations (5m)",
     'histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",method="POST"}[5m])))'),
    ("4. Outbox oldest pending age (seconds) per service",
     'max by (service) (outbox_oldest_pending_age_seconds)'),
    ("5. Kafka consumer group lag per group and topic",
     'sum by (consumergroup, topic) (clamp_min(kafka_consumergroup_lag, 0))'),
]

for title, expr in queries:
    url = f"{prom_url}/api/v1/query?query={urllib.parse.quote(expr)}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    print(f"=== {title} ===")
    print(f"Query: {expr}")
    if data.get("status") != "success":
        print(f"ERROR: {data}")
        sys.exit(1)
    results = data.get("data", {}).get("result", [])
    if not results:
        print("  (empty result)")
    else:
        for r in results:
            labels = r.get("metric", {})
            val = r.get("value", [None, None])[1]
            print(f"  {labels} => {val}")
    print()
PYEOF
