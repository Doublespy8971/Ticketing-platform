#!/usr/bin/env bash
# =============================================================================
# scripts/loadtest-audit.sh — Independent post-run data & messaging invariant audit
#
# Waits up to 120s for quiescence (all outbox rows published & all consumer group
# lags zero), then evaluates invariants A1..A9 independently against the three
# PostgreSQL databases, Kafka broker tools, container state snapshots, and k6
# summary JSONs. Writes audit.json and audit.txt to <results_dir>.
# =============================================================================
set -euo pipefail

RESULTS_DIR="${1:-}"
if [ -z "$RESULTS_DIR" ] || [ ! -d "$RESULTS_DIR" ]; then
    echo "Usage: $0 <results_dir>" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load POSTGRES_USER from infra/.env if not already provided
if [ -z "${POSTGRES_USER:-}" ] && [ -f "${REPO_ROOT}/infra/.env" ]; then
    POSTGRES_USER=$(grep -E '^POSTGRES_USER=' "${REPO_ROOT}/infra/.env" | head -n 1 | cut -d= -f2- | tr -d '"' | tr -d "'")
fi
POSTGRES_USER="${POSTGRES_USER:-ticketing}"

INVENTORY_CONTAINER="${INVENTORY_CONTAINER:-postgres-inventory}"
INVENTORY_DB="${INVENTORY_DB:-inventory_db}"
ORDER_CONTAINER="${ORDER_CONTAINER:-postgres-order}"
ORDER_DB="${ORDER_DB:-order_db}"
PAYMENT_CONTAINER="${PAYMENT_CONTAINER:-postgres-payment}"
PAYMENT_DB="${PAYMENT_DB:-payment_db}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://127.0.0.1:9090}"
QUIESCENCE_TIMEOUT_SEC="${QUIESCENCE_TIMEOUT_SEC:-120}"

psql_query() {
    local container="$1"
    local db="$2"
    local sql="$3"
    docker exec "$container" psql -U "$POSTGRES_USER" -d "$db" -t -A -c "$sql" | tr -d '\r'
}

# Computes total effective consumer group lag across all Kafka consumer groups.
# Prints: "<total_lag>|<non_zero_details>"
compute_kafka_lag() {
    local raw
    raw=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:9092 --describe --all-groups 2>/dev/null || true)
    python3 - "$raw" <<'PYEOF'
import sys
raw = sys.argv[1]
total_lag = 0
non_zero = {}
for line in raw.splitlines():
    parts = line.split()
    if len(parts) < 6:
        continue
    if parts[0] == "GROUP" or parts[0].startswith("Consumer"):
        continue
    group, topic, part, curr_off, end_off, lag_str = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
    try:
        int(part)
    except ValueError:
        continue
    eff_lag = 0
    if lag_str.isdigit():
        eff_lag = int(lag_str)
    elif lag_str == "-":
        # If CURRENT-OFFSET is '-' and LOG-END-OFFSET > 0, those messages are unconsumed!
        if end_off.isdigit() and int(end_off) > 0:
            eff_lag = int(end_off)
    if eff_lag > 0:
        total_lag += eff_lag
        key = f"{group}:{topic}"
        non_zero[key] = non_zero.get(key, 0) + eff_lag

details = ", ".join(f"{k}={v}" for k, v in sorted(non_zero.items())) if non_zero else "all zero"
print(f"{total_lag}|{details}")
PYEOF
}

capture_dlt_after() {
    local outfile="$1"
    : > "$outfile"
    local dlt_topics
    dlt_topics=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:9092 --list 2>/dev/null | grep -- '-dlt$' | sort || true)
    while IFS= read -r topic; do
        [ -z "$topic" ] && continue
        docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
            --bootstrap-server kafka:9092 --topic "$topic" --time -1 2>/dev/null | sort >> "$outfile"
    done <<< "$dlt_topics"
}

echo "=== Load Test Post-Run Audit (${RESULTS_DIR}) ==="
echo "Waiting up to ${QUIESCENCE_TIMEOUT_SEC}s for quiescence (outbox published + consumer lag == 0)..."

QUIESCENCE_OK=0
QUIESCENCE_START=$(date +%s)
QUIESCENCE_DETAIL=""

while true; do
    INV_UNPUB=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" "SELECT count(*) FROM outbox_events WHERE published_at IS NULL;")
    ORD_UNPUB=$(psql_query "$ORDER_CONTAINER" "$ORDER_DB" "SELECT count(*) FROM outbox_events WHERE published_at IS NULL;")
    PAY_UNPUB=$(psql_query "$PAYMENT_CONTAINER" "$PAYMENT_DB" "SELECT count(*) FROM outbox_events WHERE published_at IS NULL;")
    LAG_INFO=$(compute_kafka_lag)
    TOTAL_LAG="${LAG_INFO%%|*}"
    LAG_DETAIL="${LAG_INFO#*|}"

    ELAPSED=$(( $(date +%s) - QUIESCENCE_START ))
    if [ "$INV_UNPUB" = "0" ] && [ "$ORD_UNPUB" = "0" ] && [ "$PAY_UNPUB" = "0" ] && [ "$TOTAL_LAG" = "0" ]; then
        QUIESCENCE_OK=1
        QUIESCENCE_DETAIL="Quiesced in ${ELAPSED}s (outbox unpublished: 0/0/0, kafka lag: 0)"
        echo "[OK] ${QUIESCENCE_DETAIL}"
        break
    fi

    if [ "$ELAPSED" -ge "$QUIESCENCE_TIMEOUT_SEC" ]; then
        QUIESCENCE_OK=0
        QUIESCENCE_DETAIL="Timed out after ${ELAPSED}s (outbox unpublished inv=${INV_UNPUB} ord=${ORD_UNPUB} pay=${PAY_UNPUB}; kafka lag=${TOTAL_LAG} [${LAG_DETAIL}])"
        echo "[FAIL] Quiescence: ${QUIESCENCE_DETAIL}"
        break
    fi

    echo "  [${ELAPSED}s/${QUIESCENCE_TIMEOUT_SEC}s] waiting: outbox unpub inv=${INV_UNPUB} ord=${ORD_UNPUB} pay=${PAY_UNPUB}, kafka_lag=${TOTAL_LAG} (${LAG_DETAIL})"
    sleep 2
done

# Collect raw audit data from DBs, Kafka, Prometheus, and result snapshots
A1_DUP_ACTIVE=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" \
    "SELECT count(*) FROM (SELECT seat_id FROM reservation_items WHERE status IN ('HELD', 'CONFIRMED') GROUP BY seat_id HAVING count(*) > 1) t;")

A2_SOLD_SEATS=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" "SELECT count(*) FROM seats WHERE status = 'SOLD';")
A2_TOTAL_SEATS=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" "SELECT count(*) FROM seats;")
A2_CONFIRMED_RES_ITEMS=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" "SELECT count(*) FROM reservation_items WHERE status = 'CONFIRMED';")
A2_CONFIRMED_ORD_ITEMS=$(psql_query "$ORDER_CONTAINER" "$ORDER_DB" \
    "SELECT count(*) FROM order_items oi JOIN orders o ON o.order_id = oi.order_id WHERE o.status = 'CONFIRMED';")

A3_INV_UNPUB=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" "SELECT count(*) FROM outbox_events WHERE published_at IS NULL;")
A3_ORD_UNPUB=$(psql_query "$ORDER_CONTAINER" "$ORDER_DB" "SELECT count(*) FROM outbox_events WHERE published_at IS NULL;")
A3_PAY_UNPUB=$(psql_query "$PAYMENT_CONTAINER" "$PAYMENT_DB" "SELECT count(*) FROM outbox_events WHERE published_at IS NULL;")

A4_LAG_INFO=$(compute_kafka_lag)
A4_TOTAL_LAG="${A4_LAG_INFO%%|*}"
A4_LAG_DETAIL="${A4_LAG_INFO#*|}"

# Cross-check Prometheus consumer lag if reachable
PROM_LAG_NOTE=""
PROM_LAG_RAW=$(curl -sS --max-time 3 "${PROMETHEUS_URL}/api/v1/query?query=sum(clamp_min(kafka_consumergroup_lag,0))" 2>/dev/null || true)
if [ -n "$PROM_LAG_RAW" ]; then
    PROM_LAG_VAL=$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); r=d.get("data",{}).get("result",[]); print(r[0]["value"][1] if r else "n/a")' "$PROM_LAG_RAW" 2>/dev/null || echo "n/a")
    PROM_LAG_NOTE=" (prometheus_lag=${PROM_LAG_VAL})"
fi

capture_dlt_after "${RESULTS_DIR}/dlt-after.txt"

A6_DUP_PAYMENTS=$(psql_query "$PAYMENT_CONTAINER" "$PAYMENT_DB" \
    "SELECT count(*) FROM (SELECT order_id FROM payments GROUP BY order_id HAVING count(*) > 1) t;")

# Fetch SUCCEEDED payment order_ids and CANCELLED_% order_ids to intersect safely in Python (handles 5000+ rows cleanly)
TMP_PAY_SUCC=$(mktemp)
TMP_ORD_CANC=$(mktemp)
trap 'rm -f "$TMP_PAY_SUCC" "$TMP_ORD_CANC"' EXIT
psql_query "$PAYMENT_CONTAINER" "$PAYMENT_DB" "SELECT order_id FROM payments WHERE status = 'SUCCEEDED';" > "$TMP_PAY_SUCC"
psql_query "$ORDER_CONTAINER" "$ORDER_DB" "SELECT order_id FROM orders WHERE status LIKE 'CANCELLED_%';" > "$TMP_ORD_CANC"

A7_STUCK_ORDERS=$(psql_query "$ORDER_CONTAINER" "$ORDER_DB" \
    "SELECT count(*) FROM orders WHERE status IN ('PAYMENT_PENDING', 'REFUND_PENDING');")
A7_RESERVED_ORDERS=$(psql_query "$ORDER_CONTAINER" "$ORDER_DB" \
    "SELECT count(*) FROM orders WHERE status = 'RESERVED';")

# Determine run_id from run-meta.json or k6 summaries for A8
RUN_ID=$(python3 - "$RESULTS_DIR" <<'PYEOF'
import json, os, sys
rd = sys.argv[1]
for fname in ("run-meta.json", "flash-sale-summary.json", "checkout-journey-summary.json"):
    p = os.path.join(rd, fname)
    if os.path.isfile(p):
        with open(p) as f:
            d = json.load(f)
        rid = d.get("run_id") or d.get("_meta", {}).get("run_id")
        if rid:
            print(rid)
            sys.exit(0)
print("")
PYEOF
)

if [ -n "$RUN_ID" ]; then
    A8_DB_RESERVATIONS=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" \
        "SELECT count(*) FROM reservations r JOIN events e ON e.event_id = r.event_id WHERE e.name LIKE 'loadtest-${RUN_ID}-%';")
    A8_DB_EVENTS=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" \
        "SELECT count(*) FROM events WHERE name LIKE 'loadtest-${RUN_ID}-%';")
else
    A8_DB_RESERVATIONS=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" "SELECT count(*) FROM reservations;")
    A8_DB_EVENTS=$(psql_query "$INVENTORY_CONTAINER" "$INVENTORY_DB" "SELECT count(*) FROM events;")
fi

# Evaluate all A1..A9 in Python, emit audit.json and audit.txt, and exit non-zero on any FAIL
python3 - "$RESULTS_DIR" \
    "$QUIESCENCE_OK" "$QUIESCENCE_DETAIL" \
    "$A1_DUP_ACTIVE" \
    "$A2_SOLD_SEATS" "$A2_TOTAL_SEATS" "$A2_CONFIRMED_RES_ITEMS" "$A2_CONFIRMED_ORD_ITEMS" \
    "$A3_INV_UNPUB" "$A3_ORD_UNPUB" "$A3_PAY_UNPUB" \
    "$A4_TOTAL_LAG" "$A4_LAG_DETAIL" "$PROM_LAG_NOTE" \
    "$A6_DUP_PAYMENTS" "$TMP_PAY_SUCC" "$TMP_ORD_CANC" \
    "$A7_STUCK_ORDERS" "$A7_RESERVED_ORDERS" \
    "$RUN_ID" "$A8_DB_RESERVATIONS" "$A8_DB_EVENTS" <<'PYEOF'
import csv
import json
import os
import sys

(
    results_dir,
    quiescence_ok_s, quiescence_detail,
    a1_dup_s,
    a2_sold_s, a2_total_s, a2_conf_res_s, a2_conf_ord_s,
    a3_inv_s, a3_ord_s, a3_pay_s,
    a4_lag_s, a4_lag_detail, prom_lag_note,
    a6_dup_pay_s, tmp_pay_succ, tmp_ord_canc,
    a7_stuck_s, a7_reserved_s,
    run_id, a8_db_res_s, a8_db_events_s,
) = sys.argv[1:]

quiescence_ok = quiescence_ok_s == "1"
items = []

def add_item(code, name, passed, detail):
    items.append({
        "id": code,
        "name": name,
        "status": "PASS" if passed else "FAIL",
        "detail": detail,
    })

# A1
a1_dup = int(a1_dup_s)
add_item(
    "A1",
    "No duplicate active reservation item per seat",
    a1_dup == 0,
    f"seats_with_multiple_active_items={a1_dup}",
)

# A2
a2_sold = int(a2_sold_s)
a2_total = int(a2_total_s)
a2_conf_res = int(a2_conf_res_s)
a2_conf_ord = int(a2_conf_ord_s)
a2_pass = (a2_sold == a2_conf_res == a2_conf_ord) and (a2_sold <= a2_total)
add_item(
    "A2",
    "SOLD seats == CONFIRMED res items == CONFIRMED order items <= created seats",
    a2_pass,
    f"sold_seats={a2_sold}, confirmed_res_items={a2_conf_res}, confirmed_order_items={a2_conf_ord}, total_seats={a2_total}",
)

# A3
a3_inv, a3_ord, a3_pay = int(a3_inv_s), int(a3_ord_s), int(a3_pay_s)
add_item(
    "A3",
    "All outbox rows published across all 3 databases",
    a3_inv == 0 and a3_ord == 0 and a3_pay == 0,
    f"unpublished: inventory_db={a3_inv}, order_db={a3_ord}, payment_db={a3_pay}",
)

# A4
a4_lag = int(a4_lag_s)
add_item(
    "A4",
    "Every Kafka consumer group lag is zero",
    a4_lag == 0,
    f"total_lag={a4_lag} ({a4_lag_detail}){prom_lag_note}",
)

# A5: DLT offset comparison
def parse_dlt_file(path):
    res = {}
    if not os.path.isfile(path):
        return None
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                continue
            parts = line.rsplit(":", 2)
            if len(parts) == 3:
                res[f"{parts[0]}:{parts[1]}"] = int(parts[2])
    return res

dlt_before = parse_dlt_file(os.path.join(results_dir, "dlt-before.txt"))
dlt_after = parse_dlt_file(os.path.join(results_dir, "dlt-after.txt"))
if dlt_before is None:
    add_item("A5", "Dead-letter topic end offsets did not grow", False, "Missing dlt-before.txt snapshot")
elif dlt_after is None:
    add_item("A5", "Dead-letter topic end offsets did not grow", False, "Missing dlt-after.txt snapshot")
else:
    grown = []
    for k, after_v in sorted(dlt_after.items()):
        before_v = dlt_before.get(k, 0)
        if after_v > before_v:
            grown.append(f"{k}:{before_v}->{after_v}")
    add_item(
        "A5",
        "Dead-letter topic end offsets did not grow",
        len(grown) == 0,
        "0 growth across all DLT partitions" if not grown else f"DLT growth detected: {', '.join(grown)}",
    )

# A6
a6_dup_pay = int(a6_dup_pay_s)
with open(tmp_pay_succ) as f:
    succ_orders = {line.strip() for line in f if line.strip()}
with open(tmp_ord_canc) as f:
    canc_orders = {line.strip() for line in f if line.strip()}
bad_succ_canc = sorted(succ_orders & canc_orders)
add_item(
    "A6",
    "At most one payment per order and no SUCCEEDED payment on CANCELLED_* order",
    a6_dup_pay == 0 and len(bad_succ_canc) == 0,
    f"duplicate_order_payments={a6_dup_pay}, succeeded_payment_on_cancelled_order={len(bad_succ_canc)}",
)

# A7
a7_stuck = int(a7_stuck_s)
a7_reserved = int(a7_reserved_s)
add_item(
    "A7",
    "No order remains PAYMENT_PENDING or REFUND_PENDING after quiescence",
    a7_stuck == 0,
    f"stuck_pending_orders={a7_stuck} (resting RESERVED holds awaiting 10m TTL={a7_reserved})",
)

# A8: k6 201 hold responses vs DB reservations for run's events
k6_201_total = 0
k6_files_found = []
for fname in ("flash-sale-summary.json", "checkout-journey-summary.json"):
    p = os.path.join(results_dir, fname)
    if os.path.isfile(p):
        k6_files_found.append(fname)
        with open(p) as f:
            d = json.load(f)
        hc = d.get("metrics", {}).get("hold_created", {}).get("values", {}).get("count", 0)
        k6_201_total += int(hc or 0)

a8_db_res = int(a8_db_res_s)
a8_db_events = int(a8_db_events_s)
if not k6_files_found:
    add_item("A8", "k6 201 hold count equals reservations created in inventory_db", False, "No k6 summary JSON files found")
else:
    a8_pass = (k6_201_total == a8_db_res) and (a8_db_events > 0)
    add_item(
        "A8",
        "k6 201 hold count equals reservations created in inventory_db",
        a8_pass,
        f"k6_201_holds={k6_201_total}, inventory_db_reservations={a8_db_res} (run_id={run_id or 'all'}, events={a8_db_events}, summaries={','.join(k6_files_found)})",
    )

# A9: No container restarted or OOMKilled
def load_containers(path):
    if not os.path.isfile(path):
        return None
    out = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            out[row["container"]] = {
                "restart_count": int(row.get("restart_count", "0") or 0),
                "oom_killed": str(row.get("oom_killed", "false")).lower() == "true",
                "status": row.get("status", ""),
            }
    return out

c_start = load_containers(os.path.join(results_dir, "containers-start.csv"))
c_end = load_containers(os.path.join(results_dir, "containers-end.csv"))
if c_start is None or c_end is None:
    add_item("A9", "No container restarted or OOM killed during the run", False, "Missing containers-start.csv or containers-end.csv")
else:
    c_issues = []
    for cname, end_info in sorted(c_end.items()):
        start_restarts = c_start.get(cname, {}).get("restart_count", 0)
        if end_info["restart_count"] > start_restarts:
            c_issues.append(f"{cname}:restarts {start_restarts}->{end_info['restart_count']}")
        if end_info["oom_killed"]:
            c_issues.append(f"{cname}:OOMKilled=true")
    add_item(
        "A9",
        "No container restarted or OOM killed during the run",
        len(c_issues) == 0,
        f"checked {len(c_end)} containers: 0 restarts, 0 OOM kills" if not c_issues else "; ".join(c_issues),
    )

overall_pass = quiescence_ok and all(it["status"] == "PASS" for it in items)

audit_obj = {
    "overall": "PASS" if overall_pass else "FAIL",
    "quiescence": {
        "status": "PASS" if quiescence_ok else "FAIL",
        "detail": quiescence_detail,
    },
    "items": items,
}

with open(os.path.join(results_dir, "audit.json"), "w") as f:
    json.dump(audit_obj, f, indent=2)

lines = [
    f"Quiescence: {audit_obj['quiescence']['status']} — {quiescence_detail}",
    "",
    f"{'ID':<4} | {'Status':<6} | {'Invariant':<62} | Details",
    "-" * 120,
]
for it in items:
    lines.append(f"{it['id']:<4} | {it['status']:<6} | {it['name']:<62} | {it['detail']}")
lines.append("-" * 120)
lines.append(f"OVERALL AUDIT: {audit_obj['overall']}")

report_txt = "\n".join(lines) + "\n"
with open(os.path.join(results_dir, "audit.txt"), "w") as f:
    f.write(report_txt)

print(report_txt)
sys.exit(0 if overall_pass else 1)
PYEOF
