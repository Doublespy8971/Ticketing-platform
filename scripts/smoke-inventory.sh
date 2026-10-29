#!/usr/bin/env bash
# ==============================================================================
# scripts/smoke-inventory.sh
# End-to-end smoke test for inventory-service
#
# Dependencies: bash, curl, jq, docker exec (psql, redis-cli)
# ==============================================================================

set -euo pipefail

INVENTORY_URL="${INVENTORY_URL:-http://localhost:8081}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres-inventory}"
REDIS_CONTAINER="${REDIS_CONTAINER:-redis}"
POSTGRES_USER="${POSTGRES_USER:-ticketing}"
POSTGRES_DB="inventory_db"

PASS_COUNT=0
FAIL_COUNT=0

record_pass() {
    local check_name="$1"
    echo "[PASS] ${check_name}"
    PASS_COUNT=$((PASS_COUNT + 1))
}

record_fail() {
    local check_name="$1"
    local reason="$2"
    echo "[FAIL] ${check_name}: ${reason}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

uuid_gen() {
    if command -v uuidgen >/dev/null 2>&1; then
        uuidgen | tr '[:upper:]' '[:lower:]'
    elif [ -f /proc/sys/kernel/random/uuid ]; then
        cat /proc/sys/kernel/random/uuid
    else
        od -x /dev/urandom | head -1 | awk '{OFS="-"; print $2$3,$4,$5,$6,$7$8$9}'
    fi
}

run_psql() {
    local sql="$1"
    docker exec "${POSTGRES_CONTAINER}" sh -c "psql -U \"\${POSTGRES_USER:-${POSTGRES_USER}}\" -d ${POSTGRES_DB} -t -A -c \"${sql}\""
}

run_redis() {
    docker exec "${REDIS_CONTAINER}" redis-cli "$@"
}

echo "=================================================================="
echo "Starting inventory-service end-to-end smoke test"
echo "Target URL:            ${INVENTORY_URL}"
echo "Postgres Container:    ${POSTGRES_CONTAINER}"
echo "Redis Container:       ${REDIS_CONTAINER}"
echo "=================================================================="

# ------------------------------------------------------------------------------
# 1) Check inventory-service health
# ------------------------------------------------------------------------------
CHECK_NAME="1. Check inventory-service health"
HEALTH_RAW=$(curl -s -w "\n%{http_code}" "${INVENTORY_URL}/actuator/health" || true)
HTTP_STATUS=$(echo "${HEALTH_RAW}" | tail -n1)
HEALTH_BODY=$(echo "${HEALTH_RAW}" | sed '$d')

if [ "${HTTP_STATUS}" = "200" ]; then
    HEALTH_STATUS=$(echo "${HEALTH_BODY}" | jq -r '.status // empty')
    if [ "${HEALTH_STATUS}" = "UP" ]; then
        record_pass "${CHECK_NAME} (status=${HEALTH_STATUS}, http=200)"
    else
        record_fail "${CHECK_NAME}" "Expected status 'UP', got '${HEALTH_STATUS}'"
    fi
else
    record_fail "${CHECK_NAME}" "Expected HTTP 200, got '${HTTP_STATUS}'. Body: ${HEALTH_BODY}"
fi

# ------------------------------------------------------------------------------
# 2) Create event via Admin API (past dropTime, future eventTime) & set ON_SALE
# ------------------------------------------------------------------------------
CHECK_NAME="2. Create event and make ON_SALE"
EVENT_NAME="Smoke Test Event $(date +%s)"
VENUE_NAME="Smoke Test Venue"
DROP_TIME=$(jq -r -n 'now - 3600 | todateiso8601')
EVENT_TIME=$(jq -r -n 'now + 86400 * 30 | todateiso8601')

CREATE_EVENT_BODY=$(jq -n \
    --arg name "${EVENT_NAME}" \
    --arg venueName "${VENUE_NAME}" \
    --arg eventTime "${EVENT_TIME}" \
    --arg dropTime "${DROP_TIME}" \
    '{name: $name, venueName: $venueName, eventTime: $eventTime, dropTime: $dropTime}')

CREATE_EVENT_RAW=$(curl -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/admin/events" \
    -H "Content-Type: application/json" \
    -H "X-User-Roles: ADMIN" \
    -d "${CREATE_EVENT_BODY}")

HTTP_STATUS=$(echo "${CREATE_EVENT_RAW}" | tail -n1)
RES_BODY=$(echo "${CREATE_EVENT_RAW}" | sed '$d')
EVENT_ID=$(echo "${RES_BODY}" | jq -r '.eventId // empty')
EVENT_STATUS=$(echo "${RES_BODY}" | jq -r '.status // empty')

if [ "${HTTP_STATUS}" = "201" ] && [ -n "${EVENT_ID}" ] && [ "${EVENT_STATUS}" = "SCHEDULED" ]; then
    UPDATE_STATUS_RAW=$(curl -s -w "\n%{http_code}" -X PATCH "${INVENTORY_URL}/api/v1/admin/events/${EVENT_ID}/status" \
        -H "Content-Type: application/json" \
        -H "X-User-Roles: ADMIN" \
        -d '{"status":"ON_SALE"}')

    UPDATE_HTTP_STATUS=$(echo "${UPDATE_STATUS_RAW}" | tail -n1)
    UPDATE_RES_BODY=$(echo "${UPDATE_STATUS_RAW}" | sed '$d')
    NEW_EVENT_STATUS=$(echo "${UPDATE_RES_BODY}" | jq -r '.status // empty')

    if [ "${UPDATE_HTTP_STATUS}" = "200" ] && [ "${NEW_EVENT_STATUS}" = "ON_SALE" ]; then
        record_pass "${CHECK_NAME} (eventId=${EVENT_ID}, status=ON_SALE)"
    else
        record_fail "${CHECK_NAME}" "Expected HTTP 200 with status 'ON_SALE', got HTTP '${UPDATE_HTTP_STATUS}'. Body: ${UPDATE_RES_BODY}"
    fi
else
    record_fail "${CHECK_NAME}" "Expected HTTP 201 with status 'SCHEDULED', got HTTP '${HTTP_STATUS}'. Body: ${RES_BODY}"
    EVENT_ID=""
fi

# ------------------------------------------------------------------------------
# 3) Bulk create 20 seats
# ------------------------------------------------------------------------------
CHECK_NAME="3. Bulk create 20 seats"
SEAT1_ID=""
SEAT2_ID=""
declare -a SEAT_IDS=()

if [ -n "${EVENT_ID}" ]; then
    BULK_SEATS_BODY=$(jq -n '[range(1; 21) | {sectionCode: "SEC-A", rowLabel: "1", seatNumber: (. | tostring), priceCents: 10000}] | {seats: .}')
    BULK_RAW=$(curl -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/admin/events/${EVENT_ID}/seats/bulk" \
        -H "Content-Type: application/json" \
        -H "X-User-Roles: ADMIN" \
        -d "${BULK_SEATS_BODY}")

    HTTP_STATUS=$(echo "${BULK_RAW}" | tail -n1)
    RES_BODY=$(echo "${BULK_RAW}" | sed '$d')
    CREATED_COUNT=$(echo "${RES_BODY}" | jq -r '.createdCount // 0')

    if [ "${HTTP_STATUS}" = "201" ] && [ "${CREATED_COUNT}" -eq 20 ]; then
        while IFS= read -r seat_id; do
            SEAT_IDS+=("${seat_id}")
        done < <(echo "${RES_BODY}" | jq -r '.seats[].seatId')

        if [ "${#SEAT_IDS[@]}" -eq 20 ]; then
            SEAT1_ID="${SEAT_IDS[0]}"
            SEAT2_ID="${SEAT_IDS[1]}"
            record_pass "${CHECK_NAME} (createdCount=${CREATED_COUNT})"
        else
            record_fail "${CHECK_NAME}" "Expected 20 seat IDs in response, got ${#SEAT_IDS[@]}"
        fi
    else
        record_fail "${CHECK_NAME}" "Expected HTTP 201 with createdCount=20, got HTTP '${HTTP_STATUS}'. Body: ${RES_BODY}"
    fi
else
    record_fail "${CHECK_NAME}" "Skipped because event creation failed"
fi

# ------------------------------------------------------------------------------
# 4) Hold seat 1 as user 1 (201) and hold same seat as user 2 (409 SEAT_ALREADY_TAKEN)
# ------------------------------------------------------------------------------
CHECK_NAME="4. Hold seat 1 as user 1 (201) and user 2 conflict (409 SEAT_ALREADY_TAKEN)"
USER1_ID=$(uuid_gen)
IDEMPOTENCY_KEY1=$(uuid_gen)
USER1_REQ_BODY=""
USER1_RES_BODY=""
USER1_RES_ID=""

if [ -n "${EVENT_ID}" ] && [ -n "${SEAT1_ID}" ]; then
    USER1_REQ_BODY=$(jq -n \
        --arg eventId "${EVENT_ID}" \
        --arg seatId "${SEAT1_ID}" \
        '{eventId: $eventId, seatIds: [$seatId]}')

    HOLD1_RAW=$(curl -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
        -H "Content-Type: application/json" \
        -H "X-User-Id: ${USER1_ID}" \
        -H "X-User-Roles: USER" \
        -H "Idempotency-Key: ${IDEMPOTENCY_KEY1}" \
        -d "${USER1_REQ_BODY}")

    HTTP_STATUS1=$(echo "${HOLD1_RAW}" | tail -n1)
    USER1_RES_BODY=$(echo "${HOLD1_RAW}" | sed '$d')
    USER1_RES_ID=$(echo "${USER1_RES_BODY}" | jq -r '.reservationId // empty')
    RES_STATUS=$(echo "${USER1_RES_BODY}" | jq -r '.status // empty')
    ORDER_ID=$(echo "${USER1_RES_BODY}" | jq -r '.orderId // empty')
    RET_USER_ID=$(echo "${USER1_RES_BODY}" | jq -r '.userId // empty')
    RET_TOTAL=$(echo "${USER1_RES_BODY}" | jq -r '.totalAmountCents // 0')
    RET_CURRENCY=$(echo "${USER1_RES_BODY}" | jq -r '.currency // empty')
    RET_SEAT_ID=$(echo "${USER1_RES_BODY}" | jq -r '.seats[0].seatId // empty')
    RET_RESERVED_AT=$(echo "${USER1_RES_BODY}" | jq -r '.reservedAt // empty')
    RET_EXPIRES_AT=$(echo "${USER1_RES_BODY}" | jq -r '.expiresAt // empty')

    USER1_OK=false
    if [ "${HTTP_STATUS1}" = "201" ] && \
       [ -n "${USER1_RES_ID}" ] && \
       [ "${USER1_RES_ID}" = "${ORDER_ID}" ] && \
       [ "${RET_USER_ID}" = "${USER1_ID}" ] && \
       [ "${RES_STATUS}" = "HELD" ] && \
       [ "${RET_SEAT_ID}" = "${SEAT1_ID}" ] && \
       [ "${RET_TOTAL}" -eq 10000 ] && \
       [ "${RET_CURRENCY}" = "USD" ] && \
       [ -n "${RET_RESERVED_AT}" ] && \
       [ -n "${RET_EXPIRES_AT}" ]; then
        USER1_OK=true
    fi

    # User 2 attempts to hold the same seat 1
    USER2_ID=$(uuid_gen)
    IDEMPOTENCY_KEY2=$(uuid_gen)
    HOLD2_RAW=$(curl -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
        -H "Content-Type: application/json" \
        -H "X-User-Id: ${USER2_ID}" \
        -H "X-User-Roles: USER" \
        -H "Idempotency-Key: ${IDEMPOTENCY_KEY2}" \
        -d "${USER1_REQ_BODY}")

    HTTP_STATUS2=$(echo "${HOLD2_RAW}" | tail -n1)
    USER2_RES_BODY=$(echo "${HOLD2_RAW}" | sed '$d')
    USER2_ERR_CODE=$(echo "${USER2_RES_BODY}" | jq -r '.errorCode // empty')

    if [ "${USER1_OK}" = "true" ] && [ "${HTTP_STATUS2}" = "409" ] && [ "${USER2_ERR_CODE}" = "SEAT_ALREADY_TAKEN" ]; then
        record_pass "${CHECK_NAME} (user1 HTTP 201 reservationId=${USER1_RES_ID}, user2 HTTP 409 SEAT_ALREADY_TAKEN)"
    else
        record_fail "${CHECK_NAME}" "User1 ok=${USER1_OK} (HTTP ${HTTP_STATUS1}), User2 HTTP ${HTTP_STATUS2} (errorCode='${USER2_ERR_CODE}')"
    fi
else
    record_fail "${CHECK_NAME}" "Skipped due to missing event or seats"
fi

# ------------------------------------------------------------------------------
# 5) Fire 50 parallel hold requests from 50 different users for seat 2
# ------------------------------------------------------------------------------
CHECK_NAME="5. 50 parallel hold requests for single seat (assert exactly 1x 201, 49x 409)"
PARALLEL_WINNER_RES_ID=""

if [ -n "${EVENT_ID}" ] && [ -n "${SEAT2_ID}" ]; then
    PARALLEL_DIR=$(mktemp -d)
    SEAT2_REQ_BODY=$(jq -n \
        --arg eventId "${EVENT_ID}" \
        --arg seatId "${SEAT2_ID}" \
        '{eventId: $eventId, seatIds: [$seatId]}')

    for i in $(seq 1 50); do
        (
            P_USER_ID=$(uuid_gen)
            P_IDEMPOTENCY_KEY=$(uuid_gen)
            P_CURL_RAW=$(curl -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
                -H "Content-Type: application/json" \
                -H "X-User-Id: ${P_USER_ID}" \
                -H "X-User-Roles: USER" \
                -H "Idempotency-Key: ${P_IDEMPOTENCY_KEY}" \
                -d "${SEAT2_REQ_BODY}" || true)

            P_STATUS=$(echo "${P_CURL_RAW}" | tail -n1)
            P_BODY=$(echo "${P_CURL_RAW}" | sed '$d')
            echo "${P_STATUS}" > "${PARALLEL_DIR}/status_${i}"
            echo "${P_BODY}" > "${PARALLEL_DIR}/body_${i}.json"
        ) &
    done
    wait

    COUNT_201=0
    COUNT_409=0
    for i in $(seq 1 50); do
        STATUS=$(cat "${PARALLEL_DIR}/status_${i}" 2>/dev/null || echo "000")
        if [ "${STATUS}" = "201" ]; then
            COUNT_201=$((COUNT_201 + 1))
            PARALLEL_WINNER_RES_ID=$(jq -r '.reservationId // empty' "${PARALLEL_DIR}/body_${i}.json")
        elif [ "${STATUS}" = "409" ]; then
            COUNT_409=$((COUNT_409 + 1))
        fi
    done
    rm -rf "${PARALLEL_DIR}"

    if [ "${COUNT_201}" -eq 1 ] && [ "${COUNT_409}" -eq 49 ]; then
        record_pass "${CHECK_NAME} (201s=${COUNT_201}, 409s=${COUNT_409})"
    else
        record_fail "${CHECK_NAME}" "Expected 1x 201 and 49x 409, got 201s=${COUNT_201}, 409s=${COUNT_409}"
    fi
else
    record_fail "${CHECK_NAME}" "Skipped due to missing event or seats"
fi

# ------------------------------------------------------------------------------
# 6) Idempotency replay (identical 201) and payload mismatch (422)
# ------------------------------------------------------------------------------
CHECK_NAME="6. Idempotency replay identical 201 and payload mismatch 422"

if [ -n "${EVENT_ID}" ] && [ -n "${USER1_RES_ID}" ] && [ "${#SEAT_IDS[@]}" -ge 3 ]; then
    # Part A: Replay identical request
    REPLAY_RAW=$(curl -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
        -H "Content-Type: application/json" \
        -H "X-User-Id: ${USER1_ID}" \
        -H "X-User-Roles: USER" \
        -H "Idempotency-Key: ${IDEMPOTENCY_KEY1}" \
        -d "${USER1_REQ_BODY}")

    REPLAY_STATUS=$(echo "${REPLAY_RAW}" | tail -n1)
    REPLAY_BODY=$(echo "${REPLAY_RAW}" | sed '$d')

    REPLAY_MATCH=false
    if [ "${REPLAY_STATUS}" = "201" ]; then
        # Compare canonical JSON
        DIFF=$(diff -u <(echo "${USER1_RES_BODY}" | jq -S .) <(echo "${REPLAY_BODY}" | jq -S .) || true)
        if [ -z "${DIFF}" ]; then
            REPLAY_MATCH=true
        fi
    fi

    # Part B: Same key, different body
    MISMATCH_REQ_BODY=$(jq -n \
        --arg eventId "${EVENT_ID}" \
        --arg seatId "${SEAT_IDS[2]}" \
        '{eventId: $eventId, seatIds: [$seatId]}')

    MISMATCH_RAW=$(curl -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
        -H "Content-Type: application/json" \
        -H "X-User-Id: ${USER1_ID}" \
        -H "X-User-Roles: USER" \
        -H "Idempotency-Key: ${IDEMPOTENCY_KEY1}" \
        -d "${MISMATCH_REQ_BODY}")

    MISMATCH_STATUS=$(echo "${MISMATCH_RAW}" | tail -n1)
    MISMATCH_BODY=$(echo "${MISMATCH_RAW}" | sed '$d')
    MISMATCH_ERR_CODE=$(echo "${MISMATCH_BODY}" | jq -r '.errorCode // empty')

    if [ "${REPLAY_MATCH}" = "true" ] && [ "${MISMATCH_STATUS}" = "422" ] && [ "${MISMATCH_ERR_CODE}" = "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH" ]; then
        record_pass "${CHECK_NAME} (replay HTTP 201 identical, mismatch HTTP 422 IDEMPOTENCY_KEY_PAYLOAD_MISMATCH)"
    else
        record_fail "${CHECK_NAME}" "Replay match=${REPLAY_MATCH} (HTTP ${REPLAY_STATUS}), Mismatch HTTP ${MISMATCH_STATUS} (errorCode='${MISMATCH_ERR_CODE}')"
    fi
else
    record_fail "${CHECK_NAME}" "Skipped due to missing initial reservation"
fi

# ------------------------------------------------------------------------------
# 7) Verify state in PostgreSQL via docker exec
# ------------------------------------------------------------------------------
CHECK_NAME="7. PostgreSQL state verification"

if [ -n "${EVENT_ID}" ] && [ -n "${SEAT1_ID}" ] && [ -n "${SEAT2_ID}" ]; then
    # Total successful holds = 2 (user 1 on seat 1, parallel winner on seat 2)
    ACTIVE_ITEMS=$(run_psql "SELECT count(*) FROM reservation_items WHERE event_id = '${EVENT_ID}'::uuid AND status IN ('HELD', 'CONFIRMED');")
    DUPLICATE_ITEMS=$(run_psql "SELECT count(*) FROM (SELECT seat_id FROM reservation_items WHERE event_id = '${EVENT_ID}'::uuid AND status IN ('HELD', 'CONFIRMED') GROUP BY seat_id HAVING count(*) > 1) d;")
    OUTBOX_ROWS=$(run_psql "SELECT count(*) FROM outbox_events WHERE event_type = 'inventory.seat.reserved' AND aggregate_id IN (SELECT reservation_id FROM reservations WHERE event_id = '${EVENT_ID}'::uuid);")
    HELD_SEATS=$(run_psql "SELECT count(*) FROM seats WHERE event_id = '${EVENT_ID}'::uuid AND seat_id IN ('${SEAT1_ID}'::uuid, '${SEAT2_ID}'::uuid) AND status = 'HELD';")
    AVAILABLE_SEATS=$(run_psql "SELECT count(*) FROM seats WHERE event_id = '${EVENT_ID}'::uuid AND status = 'AVAILABLE';")

    PG_OK=true
    PG_ERRORS=()

    if [ "${ACTIVE_ITEMS}" -ne 2 ]; then
        PG_OK=false
        PG_ERRORS+=("active_items=${ACTIVE_ITEMS} (expected 2)")
    fi
    if [ "${DUPLICATE_ITEMS}" -ne 0 ]; then
        PG_OK=false
        PG_ERRORS+=("duplicate_active_items=${DUPLICATE_ITEMS} (expected 0)")
    fi
    if [ "${OUTBOX_ROWS}" -ne 2 ]; then
        PG_OK=false
        PG_ERRORS+=("outbox_rows=${OUTBOX_ROWS} (expected 2)")
    fi
    if [ "${HELD_SEATS}" -ne 2 ]; then
        PG_OK=false
        PG_ERRORS+=("held_seats=${HELD_SEATS} (expected 2)")
    fi
    if [ "${AVAILABLE_SEATS}" -ne 18 ]; then
        PG_OK=false
        PG_ERRORS+=("available_seats=${AVAILABLE_SEATS} (expected 18)")
    fi

    if [ "${PG_OK}" = "true" ]; then
        record_pass "${CHECK_NAME} (active_items=2, duplicates=0, outbox_rows=2, held_seats=2, available=18)"
    else
        record_fail "${CHECK_NAME}" "${PG_ERRORS[*]}"
    fi
else
    record_fail "${CHECK_NAME}" "Skipped due to missing event or seats"
fi

# ------------------------------------------------------------------------------
# 8) Verify state in Redis via docker exec
# ------------------------------------------------------------------------------
CHECK_NAME="8. Redis state verification (held keys exist, TTL between 600 and 660)"

if [ -n "${EVENT_ID}" ] && [ -n "${SEAT1_ID}" ] && [ -n "${SEAT2_ID}" ]; then
    REDIS_KEY1="seat:${EVENT_ID}:${SEAT1_ID}"
    REDIS_KEY2="seat:${EVENT_ID}:${SEAT2_ID}"

    TTL1=$(run_redis ttl "${REDIS_KEY1}")
    TTL2=$(run_redis ttl "${REDIS_KEY2}")
    VAL1=$(run_redis get "${REDIS_KEY1}")
    VAL2=$(run_redis get "${REDIS_KEY2}")

    REDIS_OK=true
    REDIS_ERRORS=()

    # TTL must be between 600 and 660 seconds
    if ! [[ "${TTL1}" =~ ^[0-9]+$ ]] || [ "${TTL1}" -lt 600 ] || [ "${TTL1}" -gt 660 ]; then
        REDIS_OK=false
        REDIS_ERRORS+=("seat1_ttl=${TTL1} (expected 600..660)")
    fi
    if ! [[ "${TTL2}" =~ ^[0-9]+$ ]] || [ "${TTL2}" -lt 600 ] || [ "${TTL2}" -gt 660 ]; then
        REDIS_OK=false
        REDIS_ERRORS+=("seat2_ttl=${TTL2} (expected 600..660)")
    fi

    if [ -n "${USER1_RES_ID}" ] && [ "${VAL1}" != "${USER1_RES_ID}" ]; then
        REDIS_OK=false
        REDIS_ERRORS+=("seat1_val=${VAL1} (expected reservationId ${USER1_RES_ID})")
    fi
    if [ -n "${PARALLEL_WINNER_RES_ID}" ] && [ "${VAL2}" != "${PARALLEL_WINNER_RES_ID}" ]; then
        REDIS_OK=false
        REDIS_ERRORS+=("seat2_val=${VAL2} (expected winner ${PARALLEL_WINNER_RES_ID})")
    fi

    if [ "${REDIS_OK}" = "true" ]; then
        record_pass "${CHECK_NAME} (seat1_ttl=${TTL1}s, seat2_ttl=${TTL2}s)"
    else
        record_fail "${CHECK_NAME}" "${REDIS_ERRORS[*]}"
    fi
else
    record_fail "${CHECK_NAME}" "Skipped due to missing event or seats"
fi

# ------------------------------------------------------------------------------
# 9) Final summary
# ------------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo "SMOKE TEST SUMMARY"
echo "=================================================================="
echo "Passed: ${PASS_COUNT}"
echo "Failed: ${FAIL_COUNT}"

if [ "${FAIL_COUNT}" -gt 0 ]; then
    echo "Result: FAILURE"
    exit 1
else
    echo "Result: SUCCESS"
    exit 0
fi

