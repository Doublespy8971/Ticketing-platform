#!/usr/bin/env bash
# =============================================================================
# scripts/test-loadtest-audit.sh — Self-test for scripts/loadtest-audit.sh
#
# Creates isolated scratch databases (cloned schema-only from the real DBs so
# live connections are untouched), proves:
#   1) A clean state passes every audit item (A1..A9 -> exit 0)
#   2) A deliberately corrupted state (duplicate active reservation item for
#      one seat in inventory_db) causes A1 to FAIL (exit != 0)
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
SEAT1_ID="22222222-2222-4222-8222-222222222221"
SEAT2_ID="22222222-2222-4222-8222-222222222222"
RES1_ID="33333333-3333-4333-8333-333333333331"
RES2_ID="33333333-3333-4333-8333-333333333332"
USER1_ID="44444444-4444-4444-8444-444444444441"
USER2_ID="44444444-4444-4444-8444-444444444442"
PAY1_ID="55555555-5555-4555-8555-555555555551"

# Seed clean consistent state across the 3 scratch DBs
docker exec -i postgres-inventory psql -U "$POSTGRES_USER" -d "$SCRATCH_INV" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO events (event_id, name, venue_name, event_time, drop_time, status)
VALUES ('${EV_ID}', 'loadtest-${RUN_ID}-flash', 'Test Arena', NOW() + INTERVAL '1 day', NOW() - INTERVAL '1 hour', 'ON_SALE');

INSERT INTO seats (seat_id, event_id, section_code, row_label, seat_number, price_cents, status)
VALUES
  ('${SEAT1_ID}', '${EV_ID}', 'S00', 'R01', '01', 10000, 'SOLD'),
  ('${SEAT2_ID}', '${EV_ID}', 'S00', 'R01', '02', 10000, 'AVAILABLE');

INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, expires_at, confirmed_at)
VALUES ('${RES1_ID}', '${EV_ID}', '${USER1_ID}', 'CONFIRMED', 10000, NOW() + INTERVAL '10 minutes', NOW());

INSERT INTO reservation_items (reservation_item_id, reservation_id, event_id, seat_id, section_code, row_label, seat_number, price_cents, status)
VALUES ('66666666-6666-4666-8666-666666666661', '${RES1_ID}', '${EV_ID}', '${SEAT1_ID}', 'S00', 'R01', '01', 10000, 'CONFIRMED');
SQL

docker exec -i postgres-order psql -U "$POSTGRES_USER" -d "$SCRATCH_ORD" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO orders (order_id, reservation_id, user_id, event_id, status, total_amount_cents, reserved_at, expires_at, confirmed_at)
VALUES ('${RES1_ID}', '${RES1_ID}', '${USER1_ID}', '${EV_ID}', 'CONFIRMED', 10000, NOW(), NOW() + INTERVAL '10 minutes', NOW());

INSERT INTO order_items (order_item_id, order_id, seat_id, section_code, row_label, seat_number, price_cents)
VALUES ('77777777-7777-4777-8777-777777777771', '${RES1_ID}', '${SEAT1_ID}', 'S00', 'R01', '01', 10000);
SQL

docker exec -i postgres-payment psql -U "$POSTGRES_USER" -d "$SCRATCH_PAY" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO payments (payment_id, order_id, reservation_id, user_id, amount_cents, payment_method_token, status, paid_at)
VALUES ('${PAY1_ID}', '${RES1_ID}', '${RES1_ID}', '${USER1_ID}', 10000, 'tok_success', 'SUCCEEDED', NOW());
SQL

cat > "${TMP_RESULTS}/flash-sale-summary.json" <<JSON
{
  "_meta": { "scenario": "flash-sale", "run_id": "${RUN_ID}" },
  "metrics": {
    "hold_created": { "type": "counter", "values": { "count": 1 } }
  }
}
JSON

echo "--- Case 1: Clean state must PASS ---"
if INVENTORY_DB="$SCRATCH_INV" ORDER_DB="$SCRATCH_ORD" PAYMENT_DB="$SCRATCH_PAY" \
   QUIESCENCE_TIMEOUT_SEC=30 \
   bash "${SCRIPT_DIR}/loadtest-audit.sh" "$TMP_RESULTS"; then
    echo "[PASS] Clean scratch state passed audit"
else
    echo "[FAIL] Clean scratch state unexpectedly failed audit!"
    exit 1
fi

echo "--- Case 2: Corrupted state (duplicate active reservation_item on same seat) must FAIL A1 ---"
# Drop partial unique index ONLY in the scratch DB so we can inject the oversell corruption
docker exec -i postgres-inventory psql -U "$POSTGRES_USER" -d "$SCRATCH_INV" -v ON_ERROR_STOP=1 <<SQL >/dev/null
DROP INDEX IF EXISTS idx_reservation_items_active_seat;
INSERT INTO reservations (reservation_id, event_id, user_id, status, total_amount_cents, expires_at)
VALUES ('${RES2_ID}', '${EV_ID}', '${USER2_ID}', 'HELD', 10000, NOW() + INTERVAL '10 minutes');
INSERT INTO reservation_items (reservation_item_id, reservation_id, event_id, seat_id, section_code, row_label, seat_number, price_cents, status)
VALUES ('66666666-6666-4666-8666-666666666662', '${RES2_ID}', '${EV_ID}', '${SEAT1_ID}', 'S00', 'R01', '01', 10000, 'HELD');
SQL

set +e
INVENTORY_DB="$SCRATCH_INV" ORDER_DB="$SCRATCH_ORD" PAYMENT_DB="$SCRATCH_PAY" \
QUIESCENCE_TIMEOUT_SEC=30 \
bash "${SCRIPT_DIR}/loadtest-audit.sh" "$TMP_RESULTS"
CORRUPT_RC=$?
set -e

if [ "$CORRUPT_RC" -eq 0 ]; then
    echo "[FAIL] Corrupted state returned exit 0 (expected non-zero failure)!"
    exit 1
fi

A1_STATUS=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print([i["status"] for i in d["items"] if i["id"]=="A1"][0])' "${TMP_RESULTS}/audit.json")
if [ "$A1_STATUS" != "FAIL" ]; then
    echo "[FAIL] Expected A1 status=FAIL on duplicate active seat item, got '${A1_STATUS}'!"
    exit 1
fi

echo "[PASS] Corrupted state correctly exited non-zero (${CORRUPT_RC}) with A1=FAIL."
echo "=== test-loadtest-audit.sh PASSED ==="
