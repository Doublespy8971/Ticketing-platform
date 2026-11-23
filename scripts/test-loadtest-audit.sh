#!/usr/bin/env bash
# =============================================================================
# scripts/test-loadtest-audit.sh — Self-test for scripts/loadtest-audit.sh
#
# Creates isolated scratch databases (cloned schema-only from the real DBs so
# live connections are untouched), proves:
#   1) A8 exact match:       R == C with U == 0   -> PASS (all A1..A9 PASS)
#   2) A8 bounded unknown:   R == C + 5 with U == 5 -> PASS
#   3) A8 phantom hold:      R == C - 1           -> FAIL (A8=FAIL)
#   4) A8 unexplained holds: R == C + 6 with U == 5 -> FAIL (A8=FAIL)
#   5) A1 corruption: duplicate active reservation item on one seat -> FAIL (A1=FAIL)
# Always drops the scratch databases and temporary directory on exit.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -z "${POSTGRES_USER:-}" ] && [ -f "${REPO_ROOT}/infra/.env" ]; then
    POSTGRES_USER=$(grep -E '^POSTGRES_USER=' "${REPO_ROOT}/infra/.env" | head -n 1 | cut -d= -f2- | tr -d '"' | tr -d "'")
fi
POSTGRES_USER="${POSTGRES_USER:-ticketing}"

SUFFIX="audit_test_$$"
SCRATCH_INV="inv_${SUFFIX}"
SCRATCH_ORD="ord_${SUFFIX}"
SCRATCH_PAY="pay_${SUFFIX}"
TMP_RESULTS=$(mktemp -d "${TMPDIR:-/tmp}/audit-selftest.XXXXXX")

cleanup() {
    docker exec postgres-inventory psql -U "$POSTGRES_USER" -d postgres -c "DROP DATABASE IF EXISTS ${SCRATCH_INV};" >/dev/null 2>&1 || true
    docker exec postgres-order psql -U "$POSTGRES_USER" -d postgres -c "DROP DATABASE IF EXISTS ${SCRATCH_ORD};" >/dev/null 2>&1 || true
    docker exec postgres-payment psql -U "$POSTGRES_USER" -d postgres -c "DROP DATABASE IF EXISTS ${SCRATCH_PAY};" >/dev/null 2>&1 || true
    rm -rf "$TMP_RESULTS"
}
trap cleanup EXIT INT TERM

echo "=== Self-testing scripts/loadtest-audit.sh ==="

clone_schema() {
    local container="$1" src_db="$2" dst_db="$3"
    docker exec "$container" psql -U "$POSTGRES_USER" -d postgres -c "DROP DATABASE IF EXISTS ${dst_db};" >/dev/null
    docker exec "$container" psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE ${dst_db};" >/dev/null
    docker exec "$container" sh -c "pg_dump -U '${POSTGRES_USER}' --schema-only '${src_db}' | psql -U '${POSTGRES_USER}' -d '${dst_db}'" >/dev/null
}

clone_schema postgres-inventory inventory_db "$SCRATCH_INV"
clone_schema postgres-order order_db "$SCRATCH_ORD"
clone_schema postgres-payment payment_db "$SCRATCH_PAY"

# Capture clean baseline snapshots (DLT + containers)
bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$TMP_RESULTS"
cp "${TMP_RESULTS}/containers-start.csv" "${TMP_RESULTS}/containers-end.csv"

RUN_ID="selftest-${SUFFIX}"
EV_ID="11111111-1111-4111-8111-111111111111"

# Seed clean consistent state with R = 7 reservations (1 CONFIRMED + 6 HELD on 7 distinct seats)
docker exec -i postgres-inventory psql -U "$POSTGRES_USER" -d "$SCRATCH_INV" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO events (event_id, name, venue_name, event_time, drop_time, status)
VALUES ('${EV_ID}', 'loadtest-${RUN_ID}-flash', 'Test Arena', NOW() + INTERVAL '1 day', NOW() - INTERVAL '1 hour', 'ON_SALE');

INSERT INTO seats (seat_id, event_id, section_code, row_label, seat_number, price_cents, status)
VALUES
  ('22222222-2222-4222-8222-222222222201', '${EV_ID}', 'S00', 'R01', '01', 10000, 'SOLD'),
  ('22222222-2222-4222-8222-222222222202', '${EV_ID}', 'S00', 'R01', '02', 10000, 'HELD'),
  ('22222222-2222-4222-8222-222222222203', '${EV_ID}', 'S00', 'R01', '03', 10000, 'HELD'),
  ('22222222-2222-4222-8222-222222222204', '${EV_ID}', 'S00', 'R01', '04', 10000, 'HELD'),
  ('22222222-2222-4222-8222-222222222205', '${EV_ID}', 'S00', 'R01', '05', 10000, 'HELD'),
  ('22222222-2222-4222-8222-222222222206', '${EV_ID}', 'S00', 'R01', '06', 10000, 'HELD'),
  ('22222222-2222-4222-8222-222222222207', '${EV_ID}', 'S00', 'R01', '07', 10000, 'HELD');

INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, expires_at, confirmed_at)
VALUES
  ('33333333-3333-4333-8333-333333333301', '${EV_ID}', '44444444-4444-4444-8444-444444444401', 'CONFIRMED', 10000, NOW() + INTERVAL '10 minutes', NOW()),
  ('33333333-3333-4333-8333-333333333302', '${EV_ID}', '44444444-4444-4444-8444-444444444402', 'HELD', 10000, NOW() + INTERVAL '10 minutes', NULL),
  ('33333333-3333-4333-8333-333333333303', '${EV_ID}', '44444444-4444-4444-8444-444444444403', 'HELD', 10000, NOW() + INTERVAL '10 minutes', NULL),
  ('33333333-3333-4333-8333-333333333304', '${EV_ID}', '44444444-4444-4444-8444-444444444404', 'HELD', 10000, NOW() + INTERVAL '10 minutes', NULL),
  ('33333333-3333-4333-8333-333333333305', '${EV_ID}', '44444444-4444-4444-8444-444444444405', 'HELD', 10000, NOW() + INTERVAL '10 minutes', NULL),
  ('33333333-3333-4333-8333-333333333306', '${EV_ID}', '44444444-4444-4444-8444-444444444406', 'HELD', 10000, NOW() + INTERVAL '10 minutes', NULL),
  ('33333333-3333-4333-8333-333333333307', '${EV_ID}', '44444444-4444-4444-8444-444444444407', 'HELD', 10000, NOW() + INTERVAL '10 minutes', NULL);

INSERT INTO reservation_items (reservation_item_id, reservation_id, event_id, seat_id, section_code, row_label, seat_number, price_cents, status)
VALUES
  ('66666666-6666-4666-8666-666666666601', '33333333-3333-4333-8333-333333333301', '${EV_ID}', '22222222-2222-4222-8222-222222222201', 'S00', 'R01', '01', 10000, 'CONFIRMED'),
  ('66666666-6666-4666-8666-666666666602', '33333333-3333-4333-8333-333333333302', '${EV_ID}', '22222222-2222-4222-8222-222222222202', 'S00', 'R01', '02', 10000, 'HELD'),
  ('66666666-6666-4666-8666-666666666603', '33333333-3333-4333-8333-333333333303', '${EV_ID}', '22222222-2222-4222-8222-222222222203', 'S00', 'R01', '03', 10000, 'HELD'),
  ('66666666-6666-4666-8666-666666666604', '33333333-3333-4333-8333-333333333304', '${EV_ID}', '22222222-2222-4222-8222-222222222204', 'S00', 'R01', '04', 10000, 'HELD'),
  ('66666666-6666-4666-8666-666666666605', '33333333-3333-4333-8333-333333333305', '${EV_ID}', '22222222-2222-4222-8222-222222222205', 'S00', 'R01', '05', 10000, 'HELD'),
  ('66666666-6666-4666-8666-666666666606', '33333333-3333-4333-8333-333333333306', '${EV_ID}', '22222222-2222-4222-8222-222222222206', 'S00', 'R01', '06', 10000, 'HELD'),
  ('66666666-6666-4666-8666-666666666607', '33333333-3333-4333-8333-333333333307', '${EV_ID}', '22222222-2222-4222-8222-222222222207', 'S00', 'R01', '07', 10000, 'HELD');
SQL

docker exec -i postgres-order psql -U "$POSTGRES_USER" -d "$SCRATCH_ORD" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO orders (order_id, reservation_id, user_id, event_id, status, total_amount_cents, reserved_at, expires_at, confirmed_at)
VALUES ('33333333-3333-4333-8333-333333333301', '33333333-3333-4333-8333-333333333301', '44444444-4444-4444-8444-444444444401', '${EV_ID}', 'CONFIRMED', 10000, NOW(), NOW() + INTERVAL '10 minutes', NOW());

INSERT INTO order_items (order_item_id, order_id, seat_id, section_code, row_label, seat_number, price_cents)
VALUES ('77777777-7777-4777-8777-777777777701', '33333333-3333-4333-8333-333333333301', '22222222-2222-4222-8222-222222222201', 'S00', 'R01', '01', 10000);
SQL

docker exec -i postgres-payment psql -U "$POSTGRES_USER" -d "$SCRATCH_PAY" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO payments (payment_id, order_id, reservation_id, user_id, amount_cents, payment_method_token, status, paid_at)
VALUES ('55555555-5555-4555-8555-555555555501', '33333333-3333-4333-8333-333333333301', '33333333-3333-4333-8333-333333333301', '44444444-4444-4444-8444-444444444401', 10000, 'tok_success', 'SUCCEEDED', NOW());
SQL

write_summary() {
    local c="$1" u="$2"
    cat > "${TMP_RESULTS}/flash-sale-summary.json" <<JSON
{
  "_meta": { "scenario": "flash-sale", "run_id": "${RUN_ID}" },
  "metrics": {
    "hold_created": { "type": "counter", "values": { "count": ${c} } },
    "hold_unknown_outcome": { "type": "counter", "values": { "count": ${u} } }
  }
}
JSON
}

assert_item_status() {
    local item_id="$1" expected="$2" branch_substr="$3"
    python3 - "${TMP_RESULTS}/audit.json" "$item_id" "$expected" "$branch_substr" <<'PYEOF'
import json, sys
p, item_id, expected, substr = sys.argv[1:]
d = json.load(open(p))
item = [i for i in d["items"] if i["id"] == item_id][0]
if item["status"] != expected:
    raise SystemExit(f"[FAIL] Expected {item_id}={expected}, got {item['status']} ({item['detail']})")
if substr and substr not in item["detail"]:
    raise SystemExit(f"[FAIL] Expected {item_id} detail to contain '{substr}', got: {item['detail']}")
print(f"[PASS] {item_id}={item['status']} | {item['detail']}")
PYEOF
}

run_audit() {
    set +e
    INVENTORY_DB="$SCRATCH_INV" ORDER_DB="$SCRATCH_ORD" PAYMENT_DB="$SCRATCH_PAY" \
    QUIESCENCE_TIMEOUT_SEC=30 \
    bash "${SCRIPT_DIR}/loadtest-audit.sh" "$TMP_RESULTS"
    LAST_RC=$?
    set -e
}

echo "--- Case 1: A8 Exact match (R=7 == C=7 with U=0) & A3 all published must PASS ---"
write_summary 7 0
run_audit
if [ "$LAST_RC" -ne 0 ]; then
    echo "[FAIL] Case 1 expected exit 0, got ${LAST_RC}"
    exit 1
fi
assert_item_status A8 PASS "PASS_EXACT_MATCH"
assert_item_status A3 PASS "PASS_ZERO_OBSERVED_NO_STUCK_ROWS"

echo "--- Case 1b: A3 fresh unpublished row published after 5s must PASS ---"
FRESH_EVT_ID="88888888-8888-4888-8888-888888888801"
docker exec -i postgres-inventory psql -U "$POSTGRES_USER" -d "$SCRATCH_INV" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, topic, event_type, correlation_id, schema_version, payload, created_at, published_at)
VALUES ('${FRESH_EVT_ID}', 'RESERVATION', '${EV_ID}', 'inventory.events', 'inventory.seat.released', '${FRESH_EVT_ID}', 1, '{}'::jsonb, NOW(), NULL);
SQL
(
    sleep 5
    docker exec postgres-inventory psql -U "$POSTGRES_USER" -d "$SCRATCH_INV" -c \
        "UPDATE outbox_events SET published_at = NOW() WHERE event_id = '${FRESH_EVT_ID}';" >/dev/null 2>&1 || true
) &
BG_PUB_PID=$!
run_audit
wait "$BG_PUB_PID" 2>/dev/null || true
if [ "$LAST_RC" -ne 0 ]; then
    echo "[FAIL] Case 1b (fresh row published after 5s) expected exit 0, got ${LAST_RC}"
    exit 1
fi
assert_item_status A3 PASS "PASS_ZERO_OBSERVED_NO_STUCK_ROWS"

echo "--- Case 1c: A3 stuck unpublished row older than 30s must FAIL A3 ---"
STUCK_EVT_ID="88888888-8888-4888-8888-888888888802"
docker exec -i postgres-inventory psql -U "$POSTGRES_USER" -d "$SCRATCH_INV" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, topic, event_type, correlation_id, schema_version, payload, created_at, published_at)
VALUES ('${STUCK_EVT_ID}', 'RESERVATION', '${EV_ID}', 'inventory.events', 'inventory.seat.reserved', '${STUCK_EVT_ID}', 1, '{}'::jsonb, NOW() - INTERVAL '45 seconds', NULL);
SQL
set +e
INVENTORY_DB="$SCRATCH_INV" ORDER_DB="$SCRATCH_ORD" PAYMENT_DB="$SCRATCH_PAY" \
QUIESCENCE_TIMEOUT_SEC=2 A3_POLL_TIMEOUT_SEC=5 \
bash "${SCRIPT_DIR}/loadtest-audit.sh" "$TMP_RESULTS"
LAST_RC=$?
set -e
if [ "$LAST_RC" -eq 0 ]; then
    echo "[FAIL] Case 1c (stuck row >30s) expected non-zero exit, got 0"
    exit 1
fi
assert_item_status A3 FAIL "FAIL_STUCK_ROWS_OLDER_THAN_30S"
assert_item_status A3 FAIL "inventory_db:inventory.seat.reserved"
# Clean up stuck row before continuing A8 & A1 cases
docker exec postgres-inventory psql -U "$POSTGRES_USER" -d "$SCRATCH_INV" -c \
    "DELETE FROM outbox_events WHERE event_id = '${STUCK_EVT_ID}';" >/dev/null

echo "--- Case 2: A8 Bounded unknown outcome (R=7 == C=2 + 5 with U=5) must PASS ---"
write_summary 2 5
run_audit
if [ "$LAST_RC" -ne 0 ]; then
    echo "[FAIL] Case 2 expected exit 0, got ${LAST_RC}"
    exit 1
fi
assert_item_status A8 PASS "PASS_BOUNDED_UNKNOWN_OUTCOME"

echo "--- Case 3: A8 Phantom hold (R=7 == C=8 - 1 with U=0) must FAIL A8 ---"
write_summary 8 0
run_audit
if [ "$LAST_RC" -eq 0 ]; then
    echo "[FAIL] Case 3 expected non-zero exit, got 0"
    exit 1
fi
assert_item_status A8 FAIL "FAIL_PHANTOM_HOLD"

echo "--- Case 4: A8 Unexplained holds (R=7 == C=1 + 6 with U=5) must FAIL A8 ---"
write_summary 1 5
run_audit
if [ "$LAST_RC" -eq 0 ]; then
    echo "[FAIL] Case 4 expected non-zero exit, got 0"
    exit 1
fi
assert_item_status A8 FAIL "FAIL_UNEXPLAINED_HOLDS"

echo "--- Case 5: Corrupted state (duplicate active reservation_item on same seat) must FAIL A1 ---"
write_summary 8 0
docker exec -i postgres-inventory psql -U "$POSTGRES_USER" -d "$SCRATCH_INV" -v ON_ERROR_STOP=1 <<SQL >/dev/null
DROP INDEX IF EXISTS idx_reservation_items_active_seat;
INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, expires_at)
VALUES ('33333333-3333-4333-8333-333333333308', '${EV_ID}', '44444444-4444-4444-8444-444444444408', 'HELD', 10000, NOW() + INTERVAL '10 minutes');
INSERT INTO reservation_items (reservation_item_id, reservation_id, event_id, seat_id, section_code, row_label, seat_number, price_cents, status)
VALUES ('66666666-6666-4666-8666-666666666608', '33333333-3333-4333-8333-333333333308', '${EV_ID}', '22222222-2222-4222-8222-222222222201', 'S00', 'R01', '01', 10000, 'HELD');
SQL

run_audit
if [ "$LAST_RC" -eq 0 ]; then
    echo "[FAIL] Corrupted state returned exit 0 (expected non-zero failure)!"
    exit 1
fi
assert_item_status A1 FAIL "seats_with_multiple_active_items=1"

echo "=== test-loadtest-audit.sh PASSED ==="
