#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OPTIONAL_FILE="${SCRIPT_DIR}/metrics-optional.txt"
CONVENTIONS_DOC="${REPO_ROOT}/docs/arch/09-conventions.md"

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:8081}"
ORDER_URL="${ORDER_URL:-http://localhost:8082}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:8083}"
NOTIFICATION_URL="${NOTIFICATION_URL:-http://localhost:8084}"
JWT_SECRET="${JWT_SECRET:-dev-only-hs256-secret-key-minimum-32-bytes-long!}"

# 1. Trigger one clean happy-path order flow unless SKIP_TRAFFIC=1
if [ "${SKIP_TRAFFIC:-0}" != "1" ]; then
  echo "[metrics-check] Generating happy-path traffic through gateway-service..."
  python3 - "$GATEWAY_URL" "$JWT_SECRET" << 'PYEOF'
import base64, hashlib, hmac, json, sys, time, urllib.error, urllib.request, uuid

gateway_url, jwt_secret = sys.argv[1], sys.argv[2]

def mint_jwt(sub, roles):
    header = {"alg": "HS256", "typ": "JWT"}
    now = int(time.time())
    payload = {"sub": sub, "roles": roles, "iat": now, "exp": now + 3600}
    b64 = lambda d: base64.urlsafe_b64encode(json.dumps(d, separators=(",", ":")).encode()).decode().rstrip("=")
    signing = f"{b64(header)}.{b64(payload)}".encode()
    sig = base64.urlsafe_b64encode(hmac.new(jwt_secret.encode(), signing, hashlib.sha256).digest()).decode().rstrip("=")
    return f"{signing.decode()}.{sig}"

def req(method, path, token=None, body=None, idem_key=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if idem_key:
        headers["Idempotency-Key"] = idem_key
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(f"{gateway_url}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, {}

admin_tok = mint_jwt(str(uuid.uuid4()), ["ADMIN", "USER"])
user_tok = mint_jwt(str(uuid.uuid4()), ["USER"])

now = time.time()
st, ev = req("POST", "/api/v1/admin/events", admin_tok, {
    "name": "metrics-probe-event",
    "venueName": "Probe Hall",
    "dropTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 60)),
    "eventTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + 7200))
})
assert st in (200, 201), f"Create event failed: {st}"
event_id = ev["eventId"]

st, _ = req("POST", f"/api/v1/admin/events/{event_id}/seats/bulk", admin_tok, {
    "seats": [{"sectionCode": "A", "rowLabel": "1", "seatNumber": "1", "priceCents": 10000}]
})
assert st in (200, 201), f"Create seats failed: {st}"

st, _ = req("PATCH", f"/api/v1/admin/events/{event_id}/status", admin_tok, {"status": "ON_SALE"})
assert st == 200, f"Update event status failed: {st}"

st, seats_resp = req("GET", f"/api/v1/events/{event_id}/seats")
assert st == 200 and seats_resp.get("seats"), f"List seats failed: {st} {seats_resp}"
seat_id = seats_resp["seats"][0]["seatId"]

st, res = req("POST", "/api/v1/reservations", user_tok, {
    "eventId": event_id,
    "seatIds": [seat_id]
}, idem_key=str(uuid.uuid4()))
assert st == 201, f"Hold failed: {st}"
res_id = res["reservationId"]

for _ in range(30):
    st, ord_body = req("GET", f"/api/v1/orders/{res_id}", user_tok)
    if st == 200 and ord_body.get("status") == "AWAITING_PAYMENT":
        break
    time.sleep(0.2)

st, _ = req("POST", f"/api/v1/orders/{res_id}/checkout", user_tok, {
    "paymentMethodToken": "tok_success"
}, idem_key=str(uuid.uuid4()))
assert st == 202, f"Checkout failed: {st}"

for _ in range(50):
    st, ord_body = req("GET", f"/api/v1/orders/{res_id}", user_tok)
    if st == 200 and ord_body.get("status") == "CONFIRMED":
        break
    time.sleep(0.3)
assert ord_body.get("status") == "CONFIRMED", f"Order did not confirm cleanly: {ord_body}"
# Allow notification-service and inventory payment consumer 1-2s to complete
time.sleep(2)
print(f"[metrics-check] Completed probe order {res_id} cleanly (CONFIRMED).")
PYEOF
fi

# 2. Probe unmatched paths on all 4 Java services to verify raw paths do NOT appear in uri tags
PROBE_PATH="/api/v1/nonexistent-probe/check-metrics-999"
for url in "$GATEWAY_URL" "$INVENTORY_URL" "$ORDER_URL" "$PAYMENT_URL"; do
  curl -s -o /dev/null "${url}${PROBE_PATH}" || true
  curl -s -o /dev/null "${url}/actuator/health" || true
done

# 3. Scrape and validate every service against docs/arch/09-conventions.md, outbox, consumer, SLO buckets, JVM, tags
python3 - "$CONVENTIONS_DOC" "$OPTIONAL_FILE" "$PROBE_PATH" \
  "gateway-service=${GATEWAY_URL}/actuator/prometheus" \
  "inventory-service=${INVENTORY_URL}/actuator/prometheus" \
  "order-service=${ORDER_URL}/actuator/prometheus" \
  "payment-service=${PAYMENT_URL}/actuator/prometheus" \
  "notification-service=${NOTIFICATION_URL}/metrics" << 'PYEOF'
import re, sys, urllib.request

conventions_path, optional_path, probe_path = sys.argv[1], sys.argv[2], sys.argv[3]
service_urls = dict(arg.split("=", 1) for arg in sys.argv[4:])

# Parse optional allowlist: set of (service, base_metric_name)
optional = {}
with open(optional_path) as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, just = [p.strip() for p in line.split("|", 1)]
        svc, metric = [p.strip() for p in key.split(":", 1)]
        optional[(svc, metric)] = just

# Expected metrics per service from docs/arch/09-conventions.md + ticketing-outbox + ticketing-consumer
all_java = ["gateway-service", "inventory-service", "order-service", "payment-service"]
outbox_services = ["inventory-service", "order-service", "payment-service"]
consumer_services = ["inventory-service", "order-service", "payment-service"]

expected_by_service = {s: set() for s in service_urls}

# Parse metrics table in docs/arch/09-conventions.md (Section 10.4)
with open(conventions_path) as f:
    in_table = False
    for line in f:
        if line.startswith("| Metric Name |"):
            in_table = True
            continue
        if in_table:
            if not line.startswith("|"):
                break
            if line.startswith("| :---"):
                continue
            cols = [c.strip() for c in line.strip("|").split("|")]
            if len(cols) < 4:
                continue
            metric_name = cols[0].strip("` ")
            emitting = cols[3]
            if "All services" in emitting:
                targets = list(service_urls.keys())
            elif "All Kafka consumers" in emitting:
                targets = consumer_services + ["notification-service"]
            else:
                targets = [s.strip("` ") for s in emitting.split(",")]
            for t in targets:
                if t in expected_by_service:
                    expected_by_service[t].add(metric_name)

# OutboxMetrics extra names (ticketing-outbox/OutboxMetrics.java)
outbox_extras = [
    "ticketing_outbox_unpublished_records",
    "outbox_pending_rows",
    "outbox_oldest_pending_age_seconds",
    "outbox_published_total",
    "outbox_publish_failures_total",
    "ticketing_outbox_publish_latency_seconds",
]
for s in outbox_services:
    expected_by_service[s].update(outbox_extras)

# ConsumerMetrics names (ticketing-consumer/ConsumerMetrics.java)
consumer_names = [
    "ticketing_kafka_consumer_dlt_total",
    "consumer_events_processed_total",
    "consumer_events_duplicate_total",
    "consumer_events_retried_total",
    "consumer_event_processing_seconds",
]
for s in consumer_services:
    expected_by_service[s].update(consumer_names)

# Also verify ReservationService reservation counter exported by PrometheusMeterRegistry (without _created suffix)
expected_by_service["inventory-service"].add("ticketing_reservations_total")

required_slo_buckets = {"0.025", "0.05", "0.1", "0.25", "0.5", "1.0", "2.5", "5.0"}

failed = False
for svc, url in service_urls.items():
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            body = resp.read().decode("utf-8")
    except Exception as e:
        print(f"[FAIL] {svc}: failed to scrape {url}: {e}")
        failed = True
        continue

    if probe_path in body:
        print(f"[FAIL] {svc}: unmatched probe path {probe_path} leaked into metrics output!")
        failed = True

    # Collect metric names from TYPE / sample lines
    present_bases = set()
    sample_lines = []
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        sample_lines.append(line)
        m = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)', line)
        if m:
            raw_name = m.group(1)
            base = re.sub(r'_(count|sum|bucket|max)$', '', raw_name)
            present_bases.add(raw_name)
            present_bases.add(base)

    # Check common service="<svc>" tag on documented/custom samples
    bad_service_tag = []
    for line in sample_lines:
        if any(line.startswith(prefix) for prefix in ("ticketing_", "consumer_", "outbox_", "inventory_", "order_", "payment_", "gateway_", "notification_", "http_server_requests_seconds")):
            if f'service="{svc}"' not in line:
                bad_service_tag.append(line.split("{")[0])
    if bad_service_tag:
        print(f"[FAIL] {svc}: samples missing service=\"{svc}\" common tag: {sorted(set(bad_service_tag))}")
        failed = True

    if svc in all_java:
        if "jvm_memory_used_bytes" not in present_bases:
            print(f"[FAIL] {svc}: missing JVM metrics (jvm_memory_used_bytes)")
            failed = True
        buckets_found = set()
        for line in sample_lines:
            if line.startswith("http_server_requests_seconds_bucket{"):
                m = re.search(r'le="([^"]+)"', line)
                if m:
                    buckets_found.add(m.group(1))
        missing_buckets = required_slo_buckets - buckets_found
        if missing_buckets:
            print(f"[FAIL] {svc}: missing http_server_requests_seconds SLO buckets: {sorted(missing_buckets)}")
            failed = True

    if svc == "order-service":
        if not any(l.startswith("ticketing_orders_materialization_lag_seconds_bucket{") for l in sample_lines):
            print(f"[FAIL] {svc}: ticketing_orders_materialization_lag_seconds_bucket histogram buckets missing")
            failed = True

    missing_required = []
    skipped_optional = []
    for expected in sorted(expected_by_service[svc]):
        if expected not in present_bases:
            if (svc, expected) in optional:
                skipped_optional.append(expected)
            else:
                missing_required.append(expected)

    if missing_required:
        print(f"[FAIL] {svc}: missing non-optional metrics: {missing_required}")
        failed = True
    else:
        print(f"[PASS] {svc}: all required metrics present (optional skipped: {len(skipped_optional)})")

if failed:
    sys.exit(1)
print("[OK] All services passed metrics, common-tag, JVM, SLO bucket, and URI hygiene checks.")
PYEOF
