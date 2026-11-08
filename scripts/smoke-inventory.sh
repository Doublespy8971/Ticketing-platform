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
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
POSTGRES_USER="${POSTGRES_USER:-ticketing}"
POSTGRES_DB="inventory_db"

PASS_COUNT=0
FAIL_COUNT=0
TOTAL_START_TIME=$(date +%s)
CURRENT_CHECK_START="${TOTAL_START_TIME}"

start_check() {
    CHECK_NAME="$1"
    CURRENT_CHECK_START=$(date +%s)
}

record_pass() {
    local check_name="$1"
    local took_str=""
    if [ -n "${CURRENT_CHECK_START:-}" ]; then
        local now_ts
        now_ts=$(date +%s)
        local elapsed=$((now_ts - CURRENT_CHECK_START))
        took_str=" (took ${elapsed}s)"
        if [ "${elapsed}" -gt 20 ]; then
            echo "[WARNING] ${check_name} took ${elapsed}s (>20s)"
        fi
    fi
    echo "[PASS] ${check_name}${took_str}"
    PASS_COUNT=$((PASS_COUNT + 1))
}

record_fail() {
    local check_name="$1"
    local reason="$2"
    local took_str=""
    if [ -n "${CURRENT_CHECK_START:-}" ]; then
        local now_ts
        now_ts=$(date +%s)
        local elapsed=$((now_ts - CURRENT_CHECK_START))
        took_str=" (took ${elapsed}s)"
        if [ "${elapsed}" -gt 20 ]; then
            echo "[WARNING] ${check_name} took ${elapsed}s (>20s)"
        fi
    fi
    echo "[FAIL] ${check_name}: ${reason}${took_str}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

run_docker_exec() {
    local timeout_sec=15
    local exit_code=0
    if command -v timeout >/dev/null 2>&1; then
        timeout "${timeout_sec}" command docker exec "$@" || exit_code=$?
    elif command -v gtimeout >/dev/null 2>&1; then
        gtimeout "${timeout_sec}" command docker exec "$@" || exit_code=$?
    else
        local tmp_out
        tmp_out=$(mktemp)
        command docker exec "$@" <&0 >"${tmp_out}" 2>&1 &
        local cmd_pid=$!
        local limit_tenths=$((timeout_sec * 10))
        local elapsed_tenths=0
        local timed_out=0

        while kill -0 "${cmd_pid}" 2>/dev/null; do
            if [ "${elapsed_tenths}" -ge "${limit_tenths}" ]; then
                timed_out=1
                break
            fi
            sleep 0.1
            elapsed_tenths=$((elapsed_tenths + 1))
        done

        if [ "${timed_out}" -eq 1 ]; then
            kill -TERM "${cmd_pid}" 2>/dev/null || true
            sleep 0.1
            kill -KILL "${cmd_pid}" 2>/dev/null || true
            wait "${cmd_pid}" 2>/dev/null || true
            rm -f "${tmp_out}"
            record_fail "${CHECK_NAME:-Unknown check}" "docker exec timed out after ${timeout_sec}s (docker exec $*)"
            return 124
        else
            wait "${cmd_pid}" 2>/dev/null || exit_code=$?
            cat "${tmp_out}"
            rm -f "${tmp_out}"
        fi
    fi

    if [ "${exit_code}" -eq 124 ] || [ "${exit_code}" -eq 137 ] || [ "${exit_code}" -eq 143 ]; then
        record_fail "${CHECK_NAME:-Unknown check}" "docker exec timed out after ${timeout_sec}s (docker exec $*)"
        return 124
    fi
    return "${exit_code}"
}

docker() {
    if [ "${1:-}" = "exec" ]; then
        shift
        run_docker_exec "$@"
    else
        command docker "$@"
    fi
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
echo "Kafka Container:       ${KAFKA_CONTAINER}"
echo "=================================================================="

# ------------------------------------------------------------------------------
# 1) Check inventory-service health
# ------------------------------------------------------------------------------
start_check "1. Check inventory-service health"
HEALTH_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" "${INVENTORY_URL}/actuator/health" || true)
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
start_check "2. Create event and make ON_SALE"
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

CREATE_EVENT_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/admin/events" \
    -H "Content-Type: application/json" \
    -H "X-User-Roles: ADMIN" \
    -d "${CREATE_EVENT_BODY}")

HTTP_STATUS=$(echo "${CREATE_EVENT_RAW}" | tail -n1)
RES_BODY=$(echo "${CREATE_EVENT_RAW}" | sed '$d')
EVENT_ID=$(echo "${RES_BODY}" | jq -r '.eventId // empty')
EVENT_STATUS=$(echo "${RES_BODY}" | jq -r '.status // empty')

if [ "${HTTP_STATUS}" = "201" ] && [ -n "${EVENT_ID}" ] && [ "${EVENT_STATUS}" = "SCHEDULED" ]; then
    UPDATE_STATUS_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X PATCH "${INVENTORY_URL}/api/v1/admin/events/${EVENT_ID}/status" \
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
start_check "3. Bulk create 20 seats"
SEAT1_ID=""
SEAT2_ID=""
declare -a SEAT_IDS=()

if [ -n "${EVENT_ID}" ]; then
    BULK_SEATS_BODY=$(jq -n '[range(1; 21) | {sectionCode: "SEC-A", rowLabel: "1", seatNumber: (. | tostring), priceCents: 10000}] | {seats: .}')
    BULK_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/admin/events/${EVENT_ID}/seats/bulk" \
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
start_check "4. Hold seat 1 as user 1 (201) and user 2 conflict (409 SEAT_ALREADY_TAKEN)"
USER1_ID=$(uuid_gen)
IDEMPOTENCY_KEY1=$(uuid_gen)
USER1_REQ_BODY=""
USER1_RES_BODY=""
USER1_RES_ID=""
HOLD_TS_SEAT1=""

if [ -n "${EVENT_ID}" ] && [ -n "${SEAT1_ID}" ]; then
    USER1_REQ_BODY=$(jq -n \
        --arg eventId "${EVENT_ID}" \
        --arg seatId "${SEAT1_ID}" \
        '{eventId: $eventId, seatIds: [$seatId]}')

    HOLD1_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
        -H "Content-Type: application/json" \
        -H "X-User-Id: ${USER1_ID}" \
        -H "X-User-Roles: USER" \
        -H "Idempotency-Key: ${IDEMPOTENCY_KEY1}" \
        -d "${USER1_REQ_BODY}")
    HOLD1_AFTER_TS=$(date +%s)

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
        HOLD_TS_SEAT1="${HOLD1_AFTER_TS}"
    fi

    # User 2 attempts to hold the same seat 1
    USER2_ID=$(uuid_gen)
    IDEMPOTENCY_KEY2=$(uuid_gen)
    HOLD2_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
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
start_check "5. 50 parallel hold requests for single seat (assert exactly 1x 201, 49x 409)"
PARALLEL_WINNER_RES_ID=""
HOLD_TS_SEAT2=""

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
            P_CURL_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
                -H "Content-Type: application/json" \
                -H "X-User-Id: ${P_USER_ID}" \
                -H "X-User-Roles: USER" \
                -H "Idempotency-Key: ${P_IDEMPOTENCY_KEY}" \
                -d "${SEAT2_REQ_BODY}" || true)
            P_TS=$(date +%s)

            P_STATUS=$(echo "${P_CURL_RAW}" | tail -n1)
            P_BODY=$(echo "${P_CURL_RAW}" | sed '$d')
            echo "${P_STATUS}" > "${PARALLEL_DIR}/status_${i}"
            echo "${P_BODY}" > "${PARALLEL_DIR}/body_${i}.json"
            echo "${P_TS}" > "${PARALLEL_DIR}/epoch_${i}"
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
            HOLD_TS_SEAT2=$(cat "${PARALLEL_DIR}/epoch_${i}" 2>/dev/null || date +%s)
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
start_check "6. Idempotency replay identical 201 and payload mismatch 422"

if [ -n "${EVENT_ID}" ] && [ -n "${USER1_RES_ID}" ] && [ "${#SEAT_IDS[@]}" -ge 3 ]; then
    # Part A: Replay identical request
    REPLAY_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
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

    MISMATCH_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
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
start_check "7. PostgreSQL state verification"

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
start_check "8. Redis state verification (held keys exist, dynamic TTL lower bound)"

if [ -n "${EVENT_ID}" ] && [ -n "${SEAT1_ID}" ] && [ -n "${SEAT2_ID}" ]; then
    REDIS_KEY1="seat:${EVENT_ID}:${SEAT1_ID}"
    REDIS_KEY2="seat:${EVENT_ID}:${SEAT2_ID}"

    TTL1=$(run_redis ttl "${REDIS_KEY1}")
    TTL2=$(run_redis ttl "${REDIS_KEY2}")
    VAL1=$(run_redis get "${REDIS_KEY1}")
    VAL2=$(run_redis get "${REDIS_KEY2}")

    NOW_TS=$(date +%s)
    ELAPSED1=$((NOW_TS - ${HOLD_TS_SEAT1:-NOW_TS}))
    [ "${ELAPSED1}" -lt 0 ] && ELAPSED1=0
    LOWER_BOUND1=$((660 - ELAPSED1 - 10))

    ELAPSED2=$((NOW_TS - ${HOLD_TS_SEAT2:-NOW_TS}))
    [ "${ELAPSED2}" -lt 0 ] && ELAPSED2=0
    LOWER_BOUND2=$((660 - ELAPSED2 - 10))

    REDIS_OK=true
    REDIS_ERRORS=()

    # TTL must be > 0, at most 660, and at least 660 - elapsed - 10
    if ! [[ "${TTL1}" =~ ^[0-9]+$ ]] || [ "${TTL1}" -le 0 ] || [ "${TTL1}" -gt 660 ] || [ "${TTL1}" -lt "${LOWER_BOUND1}" ]; then
        REDIS_OK=false
        REDIS_ERRORS+=("seat1_ttl=${TTL1} (elapsed=${ELAPSED1}s, lower_bound=${LOWER_BOUND1}s, expected >0 and <=660 and >=${LOWER_BOUND1})")
    fi
    if ! [[ "${TTL2}" =~ ^[0-9]+$ ]] || [ "${TTL2}" -le 0 ] || [ "${TTL2}" -gt 660 ] || [ "${TTL2}" -lt "${LOWER_BOUND2}" ]; then
        REDIS_OK=false
        REDIS_ERRORS+=("seat2_ttl=${TTL2} (elapsed=${ELAPSED2}s, lower_bound=${LOWER_BOUND2}s, expected >0 and <=660 and >=${LOWER_BOUND2})")
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
        record_pass "${CHECK_NAME} (seat1: ttl=${TTL1}s elapsed=${ELAPSED1}s lower_bound=${LOWER_BOUND1}s; seat2: ttl=${TTL2}s elapsed=${ELAPSED2}s lower_bound=${LOWER_BOUND2}s)"
    else
        record_fail "${CHECK_NAME}" "${REDIS_ERRORS[*]}"
    fi
else
    record_fail "${CHECK_NAME}" "Skipped due to missing event or seats"
fi

# ------------------------------------------------------------------------------
# 9) Expiry check run after existing checks
# ------------------------------------------------------------------------------
echo ""
echo "--- Expiry check ---"
start_check "9. Expiry check"
SECTION_EXPIRY_START=$(date +%s)

EXP_EVENT_NAME="Expiry Check Event $(date +%s)"
EXP_DROP_TIME=$(jq -r -n 'now - 3600 | todateiso8601')
EXP_EVENT_TIME=$(jq -r -n 'now + 86400 * 30 | todateiso8601')

EXP_CREATE_EVENT_BODY=$(jq -n \
    --arg name "${EXP_EVENT_NAME}" \
    --arg venueName "${VENUE_NAME}" \
    --arg eventTime "${EXP_EVENT_TIME}" \
    --arg dropTime "${EXP_DROP_TIME}" \
    '{name: $name, venueName: $venueName, eventTime: $eventTime, dropTime: $dropTime}')

EXP_CREATE_EVENT_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/admin/events" \
    -H "Content-Type: application/json" \
    -H "X-User-Roles: ADMIN" \
    -d "${EXP_CREATE_EVENT_BODY}")

EXP_EVENT_HTTP_STATUS=$(echo "${EXP_CREATE_EVENT_RAW}" | tail -n1)
EXP_EVENT_BODY=$(echo "${EXP_CREATE_EVENT_RAW}" | sed '$d')
EXP_EVENT_ID=$(echo "${EXP_EVENT_BODY}" | jq -r '.eventId // empty')

if [ "${EXP_EVENT_HTTP_STATUS}" = "201" ] && [ -n "${EXP_EVENT_ID}" ]; then
    # Make ON_SALE via admin status endpoint
    EXP_STATUS_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X PATCH "${INVENTORY_URL}/api/v1/admin/events/${EXP_EVENT_ID}/status" \
        -H "Content-Type: application/json" \
        -H "X-User-Roles: ADMIN" \
        -d '{"status":"ON_SALE"}')
    EXP_STATUS_CODE=$(echo "${EXP_STATUS_RAW}" | tail -n1)

    # Bulk create 5 seats
    EXP_SEATS_BODY=$(jq -n '[range(1; 6) | {sectionCode: "SEC-EXP", rowLabel: "1", seatNumber: (. | tostring), priceCents: 10000}] | {seats: .}')
    EXP_SEATS_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/admin/events/${EXP_EVENT_ID}/seats/bulk" \
        -H "Content-Type: application/json" \
        -H "X-User-Roles: ADMIN" \
        -d "${EXP_SEATS_BODY}")
    EXP_SEATS_HTTP=$(echo "${EXP_SEATS_RAW}" | tail -n1)
    EXP_SEATS_RES=$(echo "${EXP_SEATS_RAW}" | sed '$d')
    EXP_TARGET_SEAT_ID=$(echo "${EXP_SEATS_RES}" | jq -r '.seats[0].seatId // empty')

    # Hold one seat through API
    EXP_USER1_ID=$(uuid_gen)
    EXP_IDEMP1=$(uuid_gen)
    EXP_HOLD_BODY=$(jq -n \
        --arg eventId "${EXP_EVENT_ID}" \
        --arg seatId "${EXP_TARGET_SEAT_ID}" \
        '{eventId: $eventId, seatIds: [$seatId]}')

    EXP_HOLD_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
        -H "Content-Type: application/json" \
        -H "X-User-Id: ${EXP_USER1_ID}" \
        -H "X-User-Roles: USER" \
        -H "Idempotency-Key: ${EXP_IDEMP1}" \
        -d "${EXP_HOLD_BODY}")

    EXP_HOLD_HTTP=$(echo "${EXP_HOLD_RAW}" | tail -n1)
    EXP_HOLD_RES=$(echo "${EXP_HOLD_RAW}" | sed '$d')
    EXP_RES_ID=$(echo "${EXP_HOLD_RES}" | jq -r '.reservationId // empty')

    if [ "${EXP_HOLD_HTTP}" = "201" ] && [ -n "${EXP_RES_ID}" ]; then
        # Record seat version before expiry
        SEAT_VER_BEFORE=$(run_psql "SELECT version FROM seats WHERE seat_id = '${EXP_TARGET_SEAT_ID}'::uuid;" || true)

        # Force expiry: update reservation in PostgreSQL through docker exec
        # BOTH created_at = NOW() - INTERVAL '1 hour' and expires_at = NOW() - INTERVAL '1 minute'
        run_psql "UPDATE reservations SET created_at = NOW() - INTERVAL '1 hour', expires_at = NOW() - INTERVAL '1 minute' WHERE reservation_id = '${EXP_RES_ID}'::uuid;" || true

        # Poll for up to 10 seconds, once per second, until status is EXPIRED
        POLL_STATUS=""
        EXPIRY_POLL_OK=false
        CMD_EXIT=0
        for i in $(seq 1 10); do
            CMD_EXIT=0
            POLL_STATUS=$(run_psql "SELECT status FROM reservations WHERE reservation_id = '${EXP_RES_ID}'::uuid;" || CMD_EXIT=$?)
            if [ "${CMD_EXIT}" -eq 124 ]; then
                break
            fi
            if [ "${POLL_STATUS}" = "EXPIRED" ]; then
                EXPIRY_POLL_OK=true
                break
            fi
            sleep 1
        done
        if [ "${EXPIRY_POLL_OK}" != "true" ] && [ "${CMD_EXIT}" -ne 124 ]; then
            record_fail "${CHECK_NAME}" "Polling loop expired (timed out after 10s) waiting for reservation status EXPIRED (last status: '${POLL_STATUS}')"
        fi

        # 1. Assert reservation status EXPIRED with expired_at set
        RES_CHECK=$(run_psql "SELECT count(*) FROM reservations WHERE reservation_id = '${EXP_RES_ID}'::uuid AND status = 'EXPIRED' AND expired_at IS NOT NULL;" || true)
        if [ "${RES_CHECK:-0}" -eq 1 ]; then
            record_pass "Expiry check: reservation status EXPIRED with expired_at set"
        else
            record_fail "Expiry check: reservation status EXPIRED with expired_at set" "status=${POLL_STATUS}, res_check=${RES_CHECK}"
        fi

        # 2. Assert its items EXPIRED
        ITEMS_CHECK=$(run_psql "SELECT count(*) FROM reservation_items WHERE reservation_id = '${EXP_RES_ID}'::uuid AND status = 'EXPIRED';" || true)
        ITEMS_TOTAL=$(run_psql "SELECT count(*) FROM reservation_items WHERE reservation_id = '${EXP_RES_ID}'::uuid;" || true)
        if [ "${ITEMS_CHECK:-0}" -ge 1 ] && [ "${ITEMS_CHECK:-0}" -eq "${ITEMS_TOTAL:-0}" ]; then
            record_pass "Expiry check: reservation items EXPIRED"
        else
            record_fail "Expiry check: reservation items EXPIRED" "expired_items=${ITEMS_CHECK}, total_items=${ITEMS_TOTAL}"
        fi

        # 3. Assert the seat AVAILABLE in PostgreSQL with version increased
        SEAT_ROW=$(run_psql "SELECT status || '|' || version FROM seats WHERE seat_id = '${EXP_TARGET_SEAT_ID}'::uuid;" || true)
        SEAT_STATUS=$(echo "${SEAT_ROW}" | cut -d'|' -f1)
        SEAT_VER_AFTER=$(echo "${SEAT_ROW}" | cut -d'|' -f2)

        if [ "${SEAT_STATUS}" = "AVAILABLE" ] && [ -n "${SEAT_VER_BEFORE:-}" ] && [ -n "${SEAT_VER_AFTER:-}" ] && [ "${SEAT_VER_AFTER}" -gt "${SEAT_VER_BEFORE}" ] 2>/dev/null; then
            record_pass "Expiry check: seat AVAILABLE in PostgreSQL with version increased (${SEAT_VER_BEFORE} -> ${SEAT_VER_AFTER})"
        else
            record_fail "Expiry check: seat AVAILABLE in PostgreSQL with version increased" "status=${SEAT_STATUS}, version_before=${SEAT_VER_BEFORE}, version_after=${SEAT_VER_AFTER}"
        fi

        # 4. Assert exactly one outbox_events row of type inventory.reservation.expired for that reservation
        OUTBOX_EXPIRED_COUNT=$(run_psql "SELECT count(*) FROM outbox_events WHERE event_type = 'inventory.reservation.expired' AND aggregate_id = '${EXP_RES_ID}'::uuid;" || true)
        if [ "${OUTBOX_EXPIRED_COUNT:-0}" -eq 1 ]; then
            record_pass "Expiry check: exactly one outbox_events row of type inventory.reservation.expired"
        else
            record_fail "Expiry check: exactly one outbox_events row of type inventory.reservation.expired" "count=${OUTBOX_EXPIRED_COUNT}"
        fi

        # 5. Assert the Redis key of the seat no longer exists
        REDIS_SEAT_KEY="seat:${EXP_EVENT_ID}:${EXP_TARGET_SEAT_ID}"
        REDIS_KEY_EXISTS=$(run_redis exists "${REDIS_SEAT_KEY}" || true)
        if [ "${REDIS_KEY_EXISTS:-1}" -eq 0 ]; then
            record_pass "Expiry check: Redis key of the seat no longer exists"
        else
            record_fail "Expiry check: Redis key of the seat no longer exists" "exists=${REDIS_KEY_EXISTS}"
        fi

        # 6. Assert a new hold of the same seat by another user returns 201
        EXP_USER2_ID=$(uuid_gen)
        EXP_IDEMP2=$(uuid_gen)
        NEW_HOLD_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
            -H "Content-Type: application/json" \
            -H "X-User-Id: ${EXP_USER2_ID}" \
            -H "X-User-Roles: USER" \
            -H "Idempotency-Key: ${EXP_IDEMP2}" \
            -d "${EXP_HOLD_BODY}")

        NEW_HOLD_HTTP=$(echo "${NEW_HOLD_RAW}" | tail -n1)
        NEW_HOLD_RES=$(echo "${NEW_HOLD_RAW}" | sed '$d')
        NEW_RES_STATUS=$(echo "${NEW_HOLD_RES}" | jq -r '.status // empty')

        if [ "${NEW_HOLD_HTTP}" = "201" ] && [ "${NEW_RES_STATUS}" = "HELD" ]; then
            record_pass "Expiry check: new hold of the same seat by another user returns 201"
        else
            record_fail "Expiry check: new hold of the same seat by another user returns 201" "HTTP=${NEW_HOLD_HTTP}, body=${NEW_HOLD_RES}"
        fi
    else
        record_fail "Expiry check: initial hold" "HTTP=${EXP_HOLD_HTTP}, body=${EXP_HOLD_RES}"
    fi
else
    record_fail "Expiry check: event creation" "HTTP=${EXP_EVENT_HTTP_STATUS}, body=${EXP_EVENT_BODY}"
fi
EXPIRY_TOOK=$(( $(date +%s) - SECTION_EXPIRY_START ))
echo "--- Expiry check (took ${EXPIRY_TOOK}s) ---"

# ------------------------------------------------------------------------------
# 10) Kafka outbox + payment.events consumer checks via docker exec on kafka
# ------------------------------------------------------------------------------
echo ""
echo "--- Kafka check ---"
start_check "10. Kafka check"
SECTION_KAFKA_START=$(date +%s)

if [ -n "${EVENT_ID}" ] && [ "${#SEAT_IDS[@]}" -ge 5 ]; then
    KAFKA_SEAT1="${SEAT_IDS[3]}"
    KAFKA_SEAT2="${SEAT_IDS[4]}"

    # 10a. Create Hold A on KAFKA_SEAT1 and verify inventory.seat.reserved on inventory.events + published_at IS NOT NULL
    KAFKA_USER1=$(uuid_gen)
    KAFKA_IDEMP1=$(uuid_gen)
    KAFKA_CORR1=$(uuid_gen)
    KAFKA_HOLD1_BODY=$(jq -n \
        --arg eventId "${EVENT_ID}" \
        --arg userId "${KAFKA_USER1}" \
        --arg seatId "${KAFKA_SEAT1}" \
        '{eventId: $eventId, userId: $userId, seatIds: [$seatId]}')

    KAFKA_HOLD1_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
        -H "Content-Type: application/json" \
        -H "X-User-Id: ${KAFKA_USER1}" \
        -H "X-User-Roles: USER" \
        -H "X-Correlation-Id: ${KAFKA_CORR1}" \
        -H "Idempotency-Key: ${KAFKA_IDEMP1}" \
        -d "${KAFKA_HOLD1_BODY}")
    KAFKA_HOLD1_HTTP=$(echo "${KAFKA_HOLD1_RAW}" | tail -n1)
    KAFKA_HOLD1_RES=$(echo "${KAFKA_HOLD1_RAW}" | sed '$d')
    KAFKA_RES1_ID=$(echo "${KAFKA_HOLD1_RES}" | jq -r '.reservationId // empty')

    if [ "${KAFKA_HOLD1_HTTP}" = "201" ] && [ -n "${KAFKA_RES1_ID}" ]; then
        PUBLISHED_COUNT=0
        OUTBOX_RESERVED_POLL_OK=false
        CMD_EXIT=0
        for i in $(seq 1 20); do
            CMD_EXIT=0
            PUBLISHED_COUNT=$(run_psql "SELECT count(*) FROM outbox_events WHERE aggregate_id = '${KAFKA_RES1_ID}'::uuid AND event_type = 'inventory.seat.reserved' AND published_at IS NOT NULL;" || CMD_EXIT=$?)
            if [ "${CMD_EXIT}" -eq 124 ]; then
                break
            fi
            if [ "${PUBLISHED_COUNT:-0}" -eq 1 ]; then
                OUTBOX_RESERVED_POLL_OK=true
                break
            fi
            sleep 0.5
        done
        if [ "${OUTBOX_RESERVED_POLL_OK}" != "true" ] && [ "${CMD_EXIT}" -ne 124 ]; then
            record_fail "${CHECK_NAME}" "Polling loop expired (timed out after 10s) waiting for outbox inventory.seat.reserved publication"
        fi

        KAFKA_RESERVED_MSG=$(docker exec "${KAFKA_CONTAINER}" sh -c \
            "${KAFKA_BIN}/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic inventory.events --from-beginning --timeout-ms 5000 2>/dev/null || true" \
            | grep "${KAFKA_RES1_ID}" | grep "inventory.seat.reserved" || true)

        if [ "${PUBLISHED_COUNT:-0}" -eq 1 ] && [ -n "${KAFKA_RESERVED_MSG}" ]; then
            record_pass "Kafka check: inventory.seat.reserved published on inventory.events and published_at IS NOT NULL (reservationId=${KAFKA_RES1_ID})"
        else
            record_fail "Kafka check: inventory.seat.reserved published on inventory.events and published_at IS NOT NULL" "published_count=${PUBLISHED_COUNT}, msg_found=$([ -n "${KAFKA_RESERVED_MSG}" ] && echo yes || echo no)"
        fi

        # 10b. Produce payment.succeeded for Hold A and verify CONFIRMED / SOLD and inventory.seat.confirmed
        PAYMENT_EVENT1_ID=$(uuid_gen)
        PAYMENT1_ID=$(uuid_gen)
        PAID_AT=$(jq -r -n 'now | todateiso8601')
        PAYMENT_SUCCEEDED_JSON=$(jq -c -n \
            --arg eventId "${PAYMENT_EVENT1_ID}" \
            --arg occurredAt "${PAID_AT}" \
            --arg correlationId "${KAFKA_CORR1}" \
            --arg paymentId "${PAYMENT1_ID}" \
            --arg orderId "${KAFKA_RES1_ID}" \
            --arg reservationId "${KAFKA_RES1_ID}" \
            --arg userId "${KAFKA_USER1}" \
            --arg paidAt "${PAID_AT}" \
            '{
                eventId: $eventId,
                eventType: "payment.succeeded",
                occurredAt: $occurredAt,
                correlationId: $correlationId,
                schemaVersion: 1,
                payload: {
                    paymentId: $paymentId,
                    orderId: $orderId,
                    reservationId: $reservationId,
                    userId: $userId,
                    amountCents: 10000,
                    currency: "USD",
                    providerReference: ("pi_" + $paymentId),
                    paidAt: $paidAt
                }
            }')

        docker exec -i "${KAFKA_CONTAINER}" sh -c \
            "${KAFKA_BIN}/kafka-console-producer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic payment.events --property parse.key=true --property key.separator=:" \
            <<< "${KAFKA_RES1_ID}:${PAYMENT_SUCCEEDED_JSON}" || true

        CONFIRMED_STATUS=""
        CONFIRMED_POLL_OK=false
        CMD_EXIT=0
        for i in $(seq 1 20); do
            CMD_EXIT=0
            CONFIRMED_STATUS=$(run_psql "SELECT status FROM reservations WHERE reservation_id = '${KAFKA_RES1_ID}'::uuid;" || CMD_EXIT=$?)
            if [ "${CMD_EXIT}" -eq 124 ]; then
                break
            fi
            if [ "${CONFIRMED_STATUS}" = "CONFIRMED" ]; then
                CONFIRMED_POLL_OK=true
                break
            fi
            sleep 0.5
        done
        if [ "${CONFIRMED_POLL_OK}" != "true" ] && [ "${CMD_EXIT}" -ne 124 ]; then
            record_fail "${CHECK_NAME}" "Polling loop expired (timed out after 10s) waiting for reservation status CONFIRMED (last status: '${CONFIRMED_STATUS}')"
        fi

        SEAT1_STATUS=$(run_psql "SELECT status FROM seats WHERE seat_id = '${KAFKA_SEAT1}'::uuid;" || true)
        CONFIRMED_OUTBOX_POLL_OK=false
        CMD_EXIT=0
        CONFIRMED_OUTBOX=$(run_psql "SELECT count(*) FROM outbox_events WHERE aggregate_id = '${KAFKA_RES1_ID}'::uuid AND event_type = 'inventory.seat.confirmed' AND published_at IS NOT NULL;" || CMD_EXIT=$?)
        for i in $(seq 1 10); do
            if [ "${CONFIRMED_OUTBOX:-0}" -eq 1 ]; then
                CONFIRMED_OUTBOX_POLL_OK=true
                break
            fi
            sleep 0.5
            CMD_EXIT=0
            CONFIRMED_OUTBOX=$(run_psql "SELECT count(*) FROM outbox_events WHERE aggregate_id = '${KAFKA_RES1_ID}'::uuid AND event_type = 'inventory.seat.confirmed' AND published_at IS NOT NULL;" || CMD_EXIT=$?)
            if [ "${CMD_EXIT}" -eq 124 ]; then
                break
            fi
        done
        if [ "${CONFIRMED_OUTBOX_POLL_OK}" != "true" ] && [ "${CMD_EXIT}" -ne 124 ]; then
            record_fail "${CHECK_NAME}" "Polling loop expired (timed out after 5s) waiting for outbox inventory.seat.confirmed publication"
        fi

        KAFKA_CONFIRMED_MSG=$(docker exec "${KAFKA_CONTAINER}" sh -c \
            "${KAFKA_BIN}/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic inventory.events --from-beginning --timeout-ms 5000 2>/dev/null || true" \
            | grep "${KAFKA_RES1_ID}" | grep "inventory.seat.confirmed" || true)

        if [ "${CONFIRMED_STATUS}" = "CONFIRMED" ] && [ "${SEAT1_STATUS}" = "SOLD" ] && [ -n "${KAFKA_CONFIRMED_MSG}" ]; then
            record_pass "Kafka check: payment.succeeded -> CONFIRMED / SOLD and inventory.seat.confirmed on inventory.events"
        else
            record_fail "Kafka check: payment.succeeded -> CONFIRMED / SOLD and inventory.seat.confirmed on inventory.events" "res_status=${CONFIRMED_STATUS}, seat_status=${SEAT1_STATUS}, msg_found=$([ -n "${KAFKA_CONFIRMED_MSG}" ] && echo yes || echo no)"
        fi
    else
        record_fail "Kafka check: create hold for payment.succeeded" "HTTP=${KAFKA_HOLD1_HTTP}, body=${KAFKA_HOLD1_RES}"
    fi

    # 10c. Create Hold B on KAFKA_SEAT2, produce payment.failed, verify RELEASED and seat can be held again
    KAFKA_USER2=$(uuid_gen)
    KAFKA_IDEMP2=$(uuid_gen)
    KAFKA_CORR2=$(uuid_gen)
    KAFKA_HOLD2_BODY=$(jq -n \
        --arg eventId "${EVENT_ID}" \
        --arg userId "${KAFKA_USER2}" \
        --arg seatId "${KAFKA_SEAT2}" \
        '{eventId: $eventId, userId: $userId, seatIds: [$seatId]}')

    KAFKA_HOLD2_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
        -H "Content-Type: application/json" \
        -H "X-User-Id: ${KAFKA_USER2}" \
        -H "X-User-Roles: USER" \
        -H "X-Correlation-Id: ${KAFKA_CORR2}" \
        -H "Idempotency-Key: ${KAFKA_IDEMP2}" \
        -d "${KAFKA_HOLD2_BODY}")
    KAFKA_HOLD2_HTTP=$(echo "${KAFKA_HOLD2_RAW}" | tail -n1)
    KAFKA_HOLD2_RES=$(echo "${KAFKA_HOLD2_RAW}" | sed '$d')
    KAFKA_RES2_ID=$(echo "${KAFKA_HOLD2_RES}" | jq -r '.reservationId // empty')

    if [ "${KAFKA_HOLD2_HTTP}" = "201" ] && [ -n "${KAFKA_RES2_ID}" ]; then
        PAYMENT_EVENT2_ID=$(uuid_gen)
        PAYMENT2_ID=$(uuid_gen)
        FAILED_AT=$(jq -r -n 'now | todateiso8601')
        PAYMENT_FAILED_JSON=$(jq -c -n \
            --arg eventId "${PAYMENT_EVENT2_ID}" \
            --arg occurredAt "${FAILED_AT}" \
            --arg correlationId "${KAFKA_CORR2}" \
            --arg paymentId "${PAYMENT2_ID}" \
            --arg orderId "${KAFKA_RES2_ID}" \
            --arg reservationId "${KAFKA_RES2_ID}" \
            --arg userId "${KAFKA_USER2}" \
            --arg failedAt "${FAILED_AT}" \
            '{
                eventId: $eventId,
                eventType: "payment.failed",
                occurredAt: $occurredAt,
                correlationId: $correlationId,
                schemaVersion: 1,
                payload: {
                    paymentId: $paymentId,
                    orderId: $orderId,
                    reservationId: $reservationId,
                    userId: $userId,
                    amountCents: 10000,
                    currency: "USD",
                    failureCode: "CARD_DECLINED",
                    failureMessage: "Insufficient funds",
                    failedAt: $failedAt
                }
            }')

        docker exec -i "${KAFKA_CONTAINER}" sh -c \
            "${KAFKA_BIN}/kafka-console-producer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic payment.events --property parse.key=true --property key.separator=:" \
            <<< "${KAFKA_RES2_ID}:${PAYMENT_FAILED_JSON}" || true

        RELEASED_STATUS=""
        RELEASED_POLL_OK=false
        CMD_EXIT=0
        for i in $(seq 1 20); do
            CMD_EXIT=0
            RELEASED_STATUS=$(run_psql "SELECT status FROM reservations WHERE reservation_id = '${KAFKA_RES2_ID}'::uuid;" || CMD_EXIT=$?)
            if [ "${CMD_EXIT}" -eq 124 ]; then
                break
            fi
            if [ "${RELEASED_STATUS}" = "RELEASED" ]; then
                RELEASED_POLL_OK=true
                break
            fi
            sleep 0.5
        done
        if [ "${RELEASED_POLL_OK}" != "true" ] && [ "${CMD_EXIT}" -ne 124 ]; then
            record_fail "${CHECK_NAME}" "Polling loop expired (timed out after 10s) waiting for reservation status RELEASED (last status: '${RELEASED_STATUS}')"
        fi

        # Re-hold the released seat as another user
        KAFKA_USER3=$(uuid_gen)
        KAFKA_IDEMP3=$(uuid_gen)
        REHOLD_BODY=$(jq -n \
            --arg eventId "${EVENT_ID}" \
            --arg userId "${KAFKA_USER3}" \
            --arg seatId "${KAFKA_SEAT2}" \
            '{eventId: $eventId, userId: $userId, seatIds: [$seatId]}')
        REHOLD_RAW=$(curl --max-time 10 --connect-timeout 3 -s -w "\n%{http_code}" -X POST "${INVENTORY_URL}/api/v1/reservations" \
            -H "Content-Type: application/json" \
            -H "X-User-Id: ${KAFKA_USER3}" \
            -H "X-User-Roles: USER" \
            -H "Idempotency-Key: ${KAFKA_IDEMP3}" \
            -d "${REHOLD_BODY}")
        REHOLD_HTTP=$(echo "${REHOLD_RAW}" | tail -n1)
        REHOLD_RES=$(echo "${REHOLD_RAW}" | sed '$d')
        REHOLD_STATUS=$(echo "${REHOLD_RES}" | jq -r '.status // empty')

        if [ "${RELEASED_STATUS}" = "RELEASED" ] && [ "${REHOLD_HTTP}" = "201" ] && [ "${REHOLD_STATUS}" = "HELD" ]; then
            record_pass "Kafka check: payment.failed -> RELEASED and seat can be held again (HTTP 201)"
        else
            record_fail "Kafka check: payment.failed -> RELEASED and seat can be held again" "released_status=${RELEASED_STATUS}, rehold_http=${REHOLD_HTTP}, rehold_status=${REHOLD_STATUS}"
        fi
    else
        record_fail "Kafka check: create hold for payment.failed" "HTTP=${KAFKA_HOLD2_HTTP}, body=${KAFKA_HOLD2_RES}"
    fi
else
    record_fail "Kafka check" "Skipped due to missing event or seats"
fi
KAFKA_TOOK=$(( $(date +%s) - SECTION_KAFKA_START ))
echo "--- Kafka check (took ${KAFKA_TOOK}s) ---"

# ------------------------------------------------------------------------------
# 11) Final summary
# ------------------------------------------------------------------------------
TOTAL_ELAPSED=$(( $(date +%s) - TOTAL_START_TIME ))
echo ""
echo "=================================================================="
echo "SMOKE TEST SUMMARY"
echo "=================================================================="
echo "Passed: ${PASS_COUNT}"
echo "Failed: ${FAIL_COUNT}"
echo "Total runtime: ${TOTAL_ELAPSED}s"

if [ "${FAIL_COUNT}" -gt 0 ]; then
    echo "Result: FAILURE"
    exit 1
else
    echo "Result: SUCCESS"
    exit 0
fi

