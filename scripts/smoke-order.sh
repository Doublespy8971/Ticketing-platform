#!/usr/bin/env bash
# =============================================================================
# smoke-order.sh — Live End-to-End Smoke Test for order-service
# =============================================================================
set -uo pipefail

BASE_URL="${ORDER_BASE_URL:-http://localhost:8082}"
POSTGRES_DB="${POSTGRES_ORDER_DB:-order_db}"
POSTGRES_USER="${POSTGRES_USER:-ticketing}"
SCRIPT_START_EPOCH=$(date +%s)

PASS_COUNT=0
FAIL_COUNT=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

pass() {
    local msg="$1"
    local suffix="${2:-}"
    PASS_COUNT=$((PASS_COUNT + 1))
    if [ -n "$suffix" ]; then
        printf "${GREEN}[PASS]${NC} %s %s\n" "$msg" "$suffix"
    else
        printf "${GREEN}[PASS]${NC} %s\n" "$msg"
    fi
}

fail() {
    local msg="$1"
    local suffix="${2:-}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    if [ -n "$suffix" ]; then
        printf "${RED}[FAIL]${NC} %s %s\n" "$msg" "$suffix"
    else
        printf "${RED}[FAIL]${NC} %s\n" "$msg"
    fi
}

warn() {
    printf "${YELLOW}[WARNING]${NC} %s\n" "$1"
}

info() {
    printf "${CYAN}[INFO]${NC} %s\n" "$1"
}

finish_check() {
    local check_label="$1"
    local start_epoch="$2"
    local status="$3"
    local msg="$4"
    local end_epoch
    end_epoch=$(date +%s)
    local took=$((end_epoch - start_epoch))
    local suffix="(took ${took}s)"
    if [ "$status" = "PASS" ]; then
        pass "$msg" "$suffix"
    else
        fail "$msg" "$suffix"
    fi
    if [ "$took" -gt 20 ]; then
        warn "${check_label} took ${took}s (> 20s threshold)"
    fi
}

# -----------------------------------------------------------------------------
# Timeout execution helper (15s for docker exec, 10s for curl)
# -----------------------------------------------------------------------------
run_with_timeout() {
    local duration="$1"
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "$duration" "$@"
        return $?
    elif command -v gtimeout >/dev/null 2>&1; then
        gtimeout "$duration" "$@"
        return $?
    else
        local tmp_out
        tmp_out=$(mktemp "${TMPDIR:-/tmp}/smoke_timeout.XXXXXX") || return 1
        "$@" >"$tmp_out" &
        local cmd_pid=$!
        local ticks=0
        local max_ticks=$((duration * 10))
        while kill -0 "$cmd_pid" 2>/dev/null; do
            if [ "$ticks" -ge "$max_ticks" ]; then
                pkill -TERM -P "$cmd_pid" 2>/dev/null || true
                kill -TERM "$cmd_pid" 2>/dev/null || true
                sleep 0.1
                pkill -KILL -P "$cmd_pid" 2>/dev/null || true
                kill -KILL "$cmd_pid" 2>/dev/null || true
                wait "$cmd_pid" 2>/dev/null || true
                cat "$tmp_out"
                rm -f "$tmp_out"
                printf "Command timed out after %ss: %s\n" "$duration" "$*" >&2
                return 124
            fi
            sleep 0.1
            ticks=$((ticks + 1))
        done
        wait "$cmd_pid"
        local rc=$?
        cat "$tmp_out"
        rm -f "$tmp_out"
        return "$rc"
    fi
}

docker_exec_timed() {
    local check_name="$1"
    shift
    local out
    out=$(run_with_timeout 15 docker exec "$@")
    local rc=$?
    if [ "$rc" -eq 124 ]; then
        fail "${check_name}: docker exec timed out after 15s (docker exec $*)" >&2
    fi
    printf "%s" "$out"
    return "$rc"
}

docker_exec_stdin_timed() {
    local check_name="$1"
    local payload="$2"
    shift 2
    local out
    out=$(printf "%s\n" "$payload" | run_with_timeout 15 docker exec -i "$@")
    local rc=$?
    if [ "$rc" -eq 124 ]; then
        fail "${check_name}: docker exec -i timed out after 15s (docker exec -i $*)" >&2
    fi
    printf "%s" "$out"
    return "$rc"
}

find_container() {
    local primary="$1"
    local fallback="$2"
    if run_with_timeout 15 docker ps --format '{{.Names}}' | grep -qx "$primary"; then
        echo "$primary"
    elif run_with_timeout 15 docker ps --format '{{.Names}}' | grep -qx "$fallback"; then
        echo "$fallback"
    else
        echo "$primary"
    fi
}

POSTGRES_CONTAINER=$(find_container "postgres-order" "ticketing-postgres-order")
KAFKA_CONTAINER=$(find_container "kafka" "ticketing-kafka")

uuid_v4() {
    python3 -c 'import uuid; print(str(uuid.uuid4()))'
}

iso_now() {
    python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))'
}

iso_plus_seconds() {
    local secs="$1"
    python3 -c "import datetime; print((datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=${secs})).strftime('%Y-%m-%dT%H:%M:%SZ'))"
}

json_field() {
    local json="$1"
    local expr="$2"
    python3 -c "import json, sys; data = json.loads(sys.argv[1]); print($expr)" "$json" 2>/dev/null || echo ""
}

HTTP_STATUS=""
HTTP_BODY=""
HTTP_HEADERS=""
HTTP_TIMED_OUT=0
LAST_POLL_STATUS=""
LAST_POLL_BODY=""

http_request() {
    local check_name="$1"
    local method="$2"
    local url="$3"
    shift 3

    local body=""
    if [ "$#" -gt 0 ]; then
        case "$1" in
            -*)
                # Option or header argument (e.g. -H)
                ;;
            *)
                body="$1"
                shift
                ;;
        esac
    fi

    local tmp_body
    local tmp_headers
    tmp_body=$(mktemp "${TMPDIR:-/tmp}/smoke_body.XXXXXX") || return 1
    tmp_headers=$(mktemp "${TMPDIR:-/tmp}/smoke_headers.XXXXXX") || { rm -f "$tmp_body"; return 1; }

    HTTP_TIMED_OUT=0
    HTTP_STATUS=""
    HTTP_BODY=""
    HTTP_HEADERS=""

    local curl_rc=0
    if [ -n "$body" ]; then
        HTTP_STATUS=$(curl -sS --connect-timeout 3 --max-time 10 \
            -D "$tmp_headers" \
            -o "$tmp_body" \
            -w "%{http_code}" \
            -X "$method" \
            "$url" \
            -H "Content-Type: application/json" \
            "$@" \
            --data "$body") || curl_rc=$?
    else
        HTTP_STATUS=$(curl -sS --connect-timeout 3 --max-time 10 \
            -D "$tmp_headers" \
            -o "$tmp_body" \
            -w "%{http_code}" \
            -X "$method" \
            "$url" \
            "$@") || curl_rc=$?
    fi

    HTTP_BODY=$(cat "$tmp_body" 2>/dev/null || echo "")
    HTTP_HEADERS=$(cat "$tmp_headers" 2>/dev/null || echo "")
    rm -f "$tmp_body" "$tmp_headers"

    if [ "$curl_rc" -ne 0 ]; then
        HTTP_TIMED_OUT=1
        HTTP_STATUS="${HTTP_STATUS:-000}"
        return 1
    fi
    return 0
}

psql_query() {
    local check_name="$1"
    local sql="$2"
    docker_exec_timed "$check_name" "$POSTGRES_CONTAINER" sh -c "psql -U \"\${POSTGRES_USER:-${POSTGRES_USER}}\" -d \"${POSTGRES_DB}\" -t -A -c \"$sql\"" | tr -d '[:space:]'
}

get_order_row_count() {
    local check_name="$1"
    local order_id="$2"
    local count
    count=$(docker_exec_timed "$check_name" "$POSTGRES_CONTAINER" sh -c \
        "psql -U \"\${POSTGRES_USER:-${POSTGRES_USER}}\" -d \"${POSTGRES_DB}\" -t -A -c \"SELECT COUNT(*) FROM orders WHERE order_id = '${order_id}';\"" 2>/dev/null | tr -d '[:space:]' || true)
    echo "${count:-0}"
}

get_inventory_dlt_offsets() {
    local check_name="$1"
    local topic="inventory.events-order-service-inventory-cg-dlt"
    local offsets
    offsets=$(docker_exec_timed "$check_name" "$KAFKA_CONTAINER" sh -c "
        if [ -x /opt/kafka/bin/kafka-get-offsets.sh ]; then
            /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic '${topic}' --time -1 2>/dev/null
        elif command -v kafka-get-offsets >/dev/null 2>&1; then
            kafka-get-offsets --bootstrap-server kafka:9092 --topic '${topic}' --time -1 2>/dev/null
        elif [ -x /opt/kafka/bin/kafka-run-class.sh ]; then
            /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic '${topic}' --time -1 2>/dev/null || \
            /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic '${topic}' --time -1 2>/dev/null
        elif command -v kafka-run-class >/dev/null 2>&1; then
            kafka-run-class org.apache.kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic '${topic}' --time -1 2>/dev/null || \
            kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic '${topic}' --time -1 2>/dev/null
        fi
    " 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)
    echo "${offsets:-unknown}"
}

format_materialization_failure() {
    local check_label="$1"
    local check_prefix="$2"
    local order_id="$3"
    local expected_status="$4"
    local timeout_secs="$5"

    local last_status="${LAST_POLL_STATUS:-${HTTP_STATUS:-none}}"
    local last_body="${LAST_POLL_BODY:-${HTTP_BODY:-none}}"
    local row_count
    row_count=$(get_order_row_count "${check_label} (query orders count)" "$order_id")
    local dlt_offsets
    dlt_offsets=$(get_inventory_dlt_offsets "${check_label} (query DLT offsets)")

    echo "${check_prefix} did not materialize in ${expected_status} within ${timeout_secs}s (last HTTP status: ${last_status}, body: ${last_body}, orders table row count: ${row_count}, DLT end offsets: ${dlt_offsets})"
}

produce_kafka_event() {
    local check_name="$1"
    local topic="$2"
    local key="$3"
    local json_line="$4"
    docker_exec_stdin_timed "$check_name" "${key}:${json_line}" "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 \
        --topic "$topic" \
        --property "parse.key=true" \
        --property "key.separator=:" >/dev/null
}

build_seat_reserved_envelope() {
    local order_id="$1"
    local user_id="$2"
    local event_id="$3"
    local seat_id="$4"
    local evt_id
    evt_id=$(uuid_v4)
    local corr_id
    corr_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    local exp_ts
    exp_ts=$(iso_plus_seconds 600)
    printf '{"eventId":"%s","eventType":"inventory.seat.reserved","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"reservationId":"%s","userId":"%s","eventId":"%s","seats":[{"seatId":"%s","sectionCode":"FLOOR-A","rowLabel":"1","seatNumber":"10","priceCents":15000}],"totalAmountCents":15000,"currency":"USD","reservedAt":"%s","expiresAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$order_id" "$user_id" "$event_id" "$seat_id" "$now_ts" "$exp_ts"
}

build_reservation_expired_envelope() {
    local order_id="$1"
    local event_id="$2"
    local seat_id="$3"
    local evt_id
    evt_id=$(uuid_v4)
    local corr_id
    corr_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    printf '{"eventId":"%s","eventType":"inventory.reservation.expired","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"reservationId":"%s","eventId":"%s","seatIds":["%s"],"expiredAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$order_id" "$event_id" "$seat_id" "$now_ts"
}

build_seat_confirmed_envelope() {
    local order_id="$1"
    local event_id="$2"
    local seat_id="$3"
    local payment_id="$4"
    local evt_id
    evt_id=$(uuid_v4)
    local corr_id
    corr_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    printf '{"eventId":"%s","eventType":"inventory.seat.confirmed","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"reservationId":"%s","orderId":"%s","paymentId":"%s","eventId":"%s","seatIds":["%s"],"confirmedAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$order_id" "$order_id" "$payment_id" "$event_id" "$seat_id" "$now_ts"
}

build_seat_confirmation_rejected_envelope() {
    local order_id="$1"
    local payment_id="$2"
    local evt_id
    evt_id=$(uuid_v4)
    local corr_id
    corr_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    printf '{"eventId":"%s","eventType":"inventory.seat.confirmation_rejected","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"reservationId":"%s","orderId":"%s","paymentId":"%s","reason":"RESERVATION_ALREADY_EXPIRED","rejectedAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$order_id" "$order_id" "$payment_id" "$now_ts"
}

build_payment_succeeded_envelope() {
    local order_id="$1"
    local user_id="$2"
    local payment_id="$3"
    local evt_id
    evt_id=$(uuid_v4)
    local corr_id
    corr_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    printf '{"eventId":"%s","eventType":"payment.succeeded","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"paymentId":"%s","orderId":"%s","reservationId":"%s","userId":"%s","amountCents":15000,"currency":"USD","providerReference":"psp_ref_%s","paidAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$payment_id" "$order_id" "$order_id" "$user_id" "$payment_id" "$now_ts"
}

build_payment_failed_envelope() {
    local order_id="$1"
    local user_id="$2"
    local payment_id="$3"
    local evt_id
    evt_id=$(uuid_v4)
    local corr_id
    corr_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    printf '{"eventId":"%s","eventType":"payment.failed","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"paymentId":"%s","orderId":"%s","reservationId":"%s","userId":"%s","amountCents":15000,"currency":"USD","failureCode":"CARD_DECLINED","failureMessage":"Insufficient funds","failedAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$payment_id" "$order_id" "$order_id" "$user_id" "$now_ts"
}

build_payment_refunded_envelope() {
    local order_id="$1"
    local user_id="$2"
    local payment_id="$3"
    local evt_id
    evt_id=$(uuid_v4)
    local corr_id
    corr_id=$(uuid_v4)
    local refund_id
    refund_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    printf '{"eventId":"%s","eventType":"payment.refunded","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"paymentId":"%s","refundId":"%s","orderId":"%s","reservationId":"%s","userId":"%s","amountCents":15000,"currency":"USD","providerReference":"psp_refund_%s","refundedAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$payment_id" "$refund_id" "$order_id" "$order_id" "$user_id" "$refund_id" "$now_ts"
}

poll_order_status() {
    local check_name="$1"
    local order_id="$2"
    local user_id="$3"
    local expected_status="$4"
    local max_wait_secs="${5:-10}"

    local start_ts
    start_ts=$(date +%s)
    local current_status=""
    while true; do
        http_request "$check_name" GET "${BASE_URL}/api/v1/orders/${order_id}" "" -H "X-User-Id: ${user_id}" || true
        LAST_POLL_STATUS="$HTTP_STATUS"
        LAST_POLL_BODY="$HTTP_BODY"
        if [ "$HTTP_STATUS" = "200" ]; then
            current_status=$(json_field "$HTTP_BODY" "data.get('status','')")
            if [ "$current_status" = "$expected_status" ]; then
                return 0
            fi
        fi
        local now_ts
        now_ts=$(date +%s)
        if [ $((now_ts - start_ts)) -ge "$max_wait_secs" ]; then
            return 1
        fi
        sleep 0.2
    done
}

poll_outbox_published() {
    local check_name="$1"
    local order_id="$2"
    local event_type="$3"
    local max_wait_secs="${4:-10}"

    local start_ts
    start_ts=$(date +%s)
    while true; do
        local count
        count=$(psql_query "$check_name" "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '${order_id}' AND event_type = '${event_type}' AND published_at IS NOT NULL;")
        if [ "$count" = "1" ] || [ "${count:-0}" -ge 1 ] 2>/dev/null; then
            return 0
        fi
        local now_ts
        now_ts=$(date +%s)
        if [ $((now_ts - start_ts)) -ge "$max_wait_secs" ]; then
            return 1
        fi
        sleep 0.2
    done
}

verify_order_event_on_kafka() {
    local check_name="$1"
    local order_id="$2"
    local expected_event_type="$3"

    local kafka_dump
    kafka_dump=$(docker_exec_timed "$check_name" "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9092 \
        --topic order.events \
        --from-beginning \
        --property print.key=true \
        --property key.separator="|" \
        --timeout-ms 4000 2>/dev/null || true)

    local matched_line
    matched_line=$(printf "%s\n" "$kafka_dump" | grep "^${order_id}|" | grep "\"eventType\":\"${expected_event_type}\"" | tail -n 1 || true)
    if [ -n "$matched_line" ]; then
        return 0
    fi
    return 1
}

info "Starting order-service smoke tests against ${BASE_URL}..."
info "Using containers: postgres=${POSTGRES_CONTAINER}, kafka=${KAFKA_CONTAINER}"

# =============================================================================
# 1. Health Check
# =============================================================================
CHECK1_START=$(date +%s)
http_request "Check 1 (GET /actuator/health)" GET "${BASE_URL}/actuator/health" || true
HEALTH_STATUS=$(json_field "$HTTP_BODY" "data.get('status','')")
if [ "$HTTP_STATUS" = "200" ] && [ "$HEALTH_STATUS" = "UP" ]; then
    finish_check "Check 1 (GET /actuator/health)" "$CHECK1_START" "PASS" "1. GET /actuator/health returned 200 UP"
else
    finish_check "Check 1 (GET /actuator/health)" "$CHECK1_START" "FAIL" "1. GET /actuator/health expected 200 UP, got status=${HTTP_STATUS} body=${HTTP_BODY}"
fi

# =============================================================================
# 2. Early Checkout for Unknown Order -> 409 ORDER_NOT_READY + Retry-After: 1
# =============================================================================
CHECK2_START=$(date +%s)
UNKNOWN_ORDER_ID=$(uuid_v4)
UNKNOWN_USER_ID=$(uuid_v4)
UNKNOWN_IDEMP_KEY=$(uuid_v4)
if http_request "Check 2 (Early checkout unknown order)" POST "${BASE_URL}/api/v1/orders/${UNKNOWN_ORDER_ID}/checkout" \
    '{"paymentMethodToken":"pm_tok_visa"}' \
    -H "X-User-Id: ${UNKNOWN_USER_ID}" \
    -H "Idempotency-Key: ${UNKNOWN_IDEMP_KEY}"; then
    ERR_CODE=$(json_field "$HTTP_BODY" "data.get('errorCode','')")
    RETRY_AFTER_HDR=$(printf "%s\n" "$HTTP_HEADERS" | tr -d '\r' | grep -i '^Retry-After:' | awk '{print $2}' || true)
    if [ "$HTTP_STATUS" = "409" ] && [ "$ERR_CODE" = "ORDER_NOT_READY" ] && [ "$RETRY_AFTER_HDR" = "1" ]; then
        finish_check "Check 2 (Early checkout unknown order)" "$CHECK2_START" "PASS" "2. Early checkout for unknown order returned 409 ORDER_NOT_READY with Retry-After: 1"
    else
        finish_check "Check 2 (Early checkout unknown order)" "$CHECK2_START" "FAIL" "2. Early checkout expected 409 ORDER_NOT_READY + Retry-After: 1, got status=${HTTP_STATUS} errorCode=${ERR_CODE} Retry-After=${RETRY_AFTER_HDR}"
    fi
else
    finish_check "Check 2 (Early checkout unknown order)" "$CHECK2_START" "FAIL" "2. Early checkout expected 409 ORDER_NOT_READY + Retry-After: 1, got status=${HTTP_STATUS} body=${HTTP_BODY}"
fi

# =============================================================================
# 3. Order 1 Happy Path: seat.reserved -> RESERVED -> POST /checkout (202 PAYMENT_PENDING + order.payment.requested) -> payment.succeeded + seat.confirmed -> CONFIRMED + order.confirmed
# =============================================================================
CHECK3_START=$(date +%s)
ORDER1_ID=$(uuid_v4)
USER1_ID=$(uuid_v4)
EVENT1_ID=$(uuid_v4)
SEAT1_ID=$(uuid_v4)
PAYMENT1_ID=$(uuid_v4)
IDEMP1_KEY=$(uuid_v4)

RESERVED1_JSON=$(build_seat_reserved_envelope "$ORDER1_ID" "$USER1_ID" "$EVENT1_ID" "$SEAT1_ID")
produce_kafka_event "Check 3 (Order 1 seat.reserved)" "inventory.events" "$ORDER1_ID" "$RESERVED1_JSON"

if poll_order_status "Check 3 (Order 1 poll RESERVED)" "$ORDER1_ID" "$USER1_ID" "RESERVED" 10; then
    if http_request "Check 3 (Order 1 POST /checkout)" POST "${BASE_URL}/api/v1/orders/${ORDER1_ID}/checkout" \
        '{"paymentMethodToken":"pm_tok_visa_success"}' \
        -H "X-User-Id: ${USER1_ID}" \
        -H "Idempotency-Key: ${IDEMP1_KEY}"; then
        CHK_STATUS=$(json_field "$HTTP_BODY" "data.get('status','')")
        if [ "$HTTP_STATUS" = "202" ] && [ "$CHK_STATUS" = "PAYMENT_PENDING" ]; then
            PAID1_JSON=$(build_payment_succeeded_envelope "$ORDER1_ID" "$USER1_ID" "$PAYMENT1_ID")
            CONF1_JSON=$(build_seat_confirmed_envelope "$ORDER1_ID" "$EVENT1_ID" "$SEAT1_ID" "$PAYMENT1_ID")
            produce_kafka_event "Check 3 (Order 1 payment.succeeded)" "payment.events" "$ORDER1_ID" "$PAID1_JSON"
            produce_kafka_event "Check 3 (Order 1 seat.confirmed)" "inventory.events" "$ORDER1_ID" "$CONF1_JSON"

            if poll_order_status "Check 3 (Order 1 poll CONFIRMED)" "$ORDER1_ID" "$USER1_ID" "CONFIRMED" 10 \
                && poll_outbox_published "Check 3 (Order 1 outbox order.payment.requested)" "$ORDER1_ID" "order.payment.requested" 10 \
                && poll_outbox_published "Check 3 (Order 1 outbox order.confirmed)" "$ORDER1_ID" "order.confirmed" 10 \
                && verify_order_event_on_kafka "Check 3 (Order 1 kafka order.payment.requested)" "$ORDER1_ID" "order.payment.requested" \
                && verify_order_event_on_kafka "Check 3 (Order 1 kafka order.confirmed)" "$ORDER1_ID" "order.confirmed"; then
                finish_check "Check 3 (Order 1 happy path)" "$CHECK3_START" "PASS" "3. Order 1 happy path: seat.reserved -> RESERVED -> checkout (202 PAYMENT_PENDING + order.payment.requested) -> payment.succeeded + seat.confirmed -> CONFIRMED + order.confirmed on order.events"
            else
                finish_check "Check 3 (Order 1 happy path)" "$CHECK3_START" "FAIL" "3. Order 1 happy path did not reach CONFIRMED or publish order.payment.requested / order.confirmed on order.events"
            fi
        else
            finish_check "Check 3 (Order 1 happy path)" "$CHECK3_START" "FAIL" "3. Order 1 checkout expected 202 PAYMENT_PENDING, got HTTP ${HTTP_STATUS} status=${CHK_STATUS}"
        fi
    fi
else
    finish_check "Check 3 (Order 1 happy path)" "$CHECK3_START" "FAIL" \
        "$(format_materialization_failure "Check 3 (Order 1 happy path)" "3. Order 1" "$ORDER1_ID" "RESERVED" 10)"
fi

# =============================================================================
# 4. Order 2 Payment Failed Path: seat.reserved -> checkout -> payment.failed -> CANCELLED_PAYMENT_FAILED
# =============================================================================
CHECK4_START=$(date +%s)
ORDER2_ID=$(uuid_v4)
USER2_ID=$(uuid_v4)
EVENT2_ID=$(uuid_v4)
SEAT2_ID=$(uuid_v4)
PAYMENT2_ID=$(uuid_v4)
IDEMP2_KEY=$(uuid_v4)

RESERVED2_JSON=$(build_seat_reserved_envelope "$ORDER2_ID" "$USER2_ID" "$EVENT2_ID" "$SEAT2_ID")
produce_kafka_event "Check 4 (Order 2 seat.reserved)" "inventory.events" "$ORDER2_ID" "$RESERVED2_JSON"

if poll_order_status "Check 4 (Order 2 poll RESERVED)" "$ORDER2_ID" "$USER2_ID" "RESERVED" 10; then
    if http_request "Check 4 (Order 2 POST /checkout)" POST "${BASE_URL}/api/v1/orders/${ORDER2_ID}/checkout" \
        '{"paymentMethodToken":"pm_tok_visa_declined"}' \
        -H "X-User-Id: ${USER2_ID}" \
        -H "Idempotency-Key: ${IDEMP2_KEY}" && [ "$HTTP_STATUS" = "202" ]; then
        FAIL2_JSON=$(build_payment_failed_envelope "$ORDER2_ID" "$USER2_ID" "$PAYMENT2_ID")
        produce_kafka_event "Check 4 (Order 2 payment.failed)" "payment.events" "$ORDER2_ID" "$FAIL2_JSON"

        if poll_order_status "Check 4 (Order 2 poll CANCELLED_PAYMENT_FAILED)" "$ORDER2_ID" "$USER2_ID" "CANCELLED_PAYMENT_FAILED" 10 \
            && poll_outbox_published "Check 4 (Order 2 outbox order.cancelled)" "$ORDER2_ID" "order.cancelled" 10; then
            finish_check "Check 4 (Order 2 payment.failed)" "$CHECK4_START" "PASS" "4. Order 2 payment.failed path transitioned to CANCELLED_PAYMENT_FAILED and published order.cancelled"
        else
            finish_check "Check 4 (Order 2 payment.failed)" "$CHECK4_START" "FAIL" "4. Order 2 did not reach CANCELLED_PAYMENT_FAILED or publish order.cancelled"
        fi
    else
        finish_check "Check 4 (Order 2 payment.failed)" "$CHECK4_START" "FAIL" "4. Order 2 checkout failed with HTTP ${HTTP_STATUS}"
    fi
else
    finish_check "Check 4 (Order 2 payment.failed)" "$CHECK4_START" "FAIL" \
        "$(format_materialization_failure "Check 4 (Order 2 payment.failed)" "4. Order 2" "$ORDER2_ID" "RESERVED" 10)"
fi

# =============================================================================
# 5. Order 3 Late Payment & Refund Path: reservation.expired -> payment.succeeded -> confirmation_rejected -> REFUND_PENDING (1 order.refund.requested) -> payment.refunded -> CANCELLED_REFUNDED
# =============================================================================
CHECK5_START=$(date +%s)
ORDER3_ID=$(uuid_v4)
USER3_ID=$(uuid_v4)
EVENT3_ID=$(uuid_v4)
SEAT3_ID=$(uuid_v4)
PAYMENT3_ID=$(uuid_v4)

RESERVED3_JSON=$(build_seat_reserved_envelope "$ORDER3_ID" "$USER3_ID" "$EVENT3_ID" "$SEAT3_ID")
produce_kafka_event "Check 5 (Order 3 seat.reserved)" "inventory.events" "$ORDER3_ID" "$RESERVED3_JSON"

if poll_order_status "Check 5 (Order 3 poll RESERVED)" "$ORDER3_ID" "$USER3_ID" "RESERVED" 10; then
    EXPIRED3_JSON=$(build_reservation_expired_envelope "$ORDER3_ID" "$EVENT3_ID" "$SEAT3_ID")
    produce_kafka_event "Check 5 (Order 3 reservation.expired)" "inventory.events" "$ORDER3_ID" "$EXPIRED3_JSON"

    if poll_order_status "Check 5 (Order 3 poll CANCELLED_EXPIRED)" "$ORDER3_ID" "$USER3_ID" "CANCELLED_EXPIRED" 10; then
        PAID3_JSON=$(build_payment_succeeded_envelope "$ORDER3_ID" "$USER3_ID" "$PAYMENT3_ID")
        REJ3_JSON=$(build_seat_confirmation_rejected_envelope "$ORDER3_ID" "$PAYMENT3_ID")
        produce_kafka_event "Check 5 (Order 3 payment.succeeded)" "payment.events" "$ORDER3_ID" "$PAID3_JSON"
        produce_kafka_event "Check 5 (Order 3 confirmation_rejected)" "inventory.events" "$ORDER3_ID" "$REJ3_JSON"

        if poll_order_status "Check 5 (Order 3 poll REFUND_PENDING)" "$ORDER3_ID" "$USER3_ID" "REFUND_PENDING" 10 \
            && poll_outbox_published "Check 5 (Order 3 outbox order.refund.requested)" "$ORDER3_ID" "order.refund.requested" 10; then
            # Wait briefly to ensure both payment.succeeded and confirmation_rejected were processed
            sleep 0.5
            REFUND_REQ_COUNT=$(psql_query "Check 5 (Order 3 count order.refund.requested)" \
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '${ORDER3_ID}' AND event_type = 'order.refund.requested';")

            REFUNDED3_JSON=$(build_payment_refunded_envelope "$ORDER3_ID" "$USER3_ID" "$PAYMENT3_ID")
            produce_kafka_event "Check 5 (Order 3 payment.refunded)" "payment.events" "$ORDER3_ID" "$REFUNDED3_JSON"

            if [ "$REFUND_REQ_COUNT" = "1" ] \
                && poll_order_status "Check 5 (Order 3 poll CANCELLED_REFUNDED)" "$ORDER3_ID" "$USER3_ID" "CANCELLED_REFUNDED" 10 \
                && verify_order_event_on_kafka "Check 5 (Order 3 kafka order.refund.requested)" "$ORDER3_ID" "order.refund.requested"; then
                finish_check "Check 5 (Order 3 late payment & refund)" "$CHECK5_START" "PASS" "5. Order 3 late payment path: reservation.expired -> payment.succeeded -> confirmation_rejected -> REFUND_PENDING (1 order.refund.requested) -> payment.refunded -> CANCELLED_REFUNDED"
            else
                finish_check "Check 5 (Order 3 late payment & refund)" "$CHECK5_START" "FAIL" "5. Order 3 expected 1 order.refund.requested (got ${REFUND_REQ_COUNT}) and final status CANCELLED_REFUNDED"
            fi
        else
            finish_check "Check 5 (Order 3 late payment & refund)" "$CHECK5_START" "FAIL" "5. Order 3 did not reach REFUND_PENDING or publish order.refund.requested"
        fi
    else
        finish_check "Check 5 (Order 3 late payment & refund)" "$CHECK5_START" "FAIL" "5. Order 3 did not transition to CANCELLED_EXPIRED"
    fi
else
    finish_check "Check 5 (Order 3 late payment & refund)" "$CHECK5_START" "FAIL" \
        "$(format_materialization_failure "Check 5 (Order 3 late payment & refund)" "5. Order 3" "$ORDER3_ID" "RESERVED" 10)"
fi

# =============================================================================
# Summary
# =============================================================================
SCRIPT_END_EPOCH=$(date +%s)
TOTAL_RUNTIME=$((SCRIPT_END_EPOCH - SCRIPT_START_EPOCH))
echo ""
echo "============================================================================="
printf "Smoke Test Summary: ${GREEN}%d passed${NC}, ${RED}%d failed${NC} (total runtime: %ds)\n" "$PASS_COUNT" "$FAIL_COUNT" "$TOTAL_RUNTIME"
echo "============================================================================="

if [ "$FAIL_COUNT" -gt 0 ]; then
    exit 1
fi
exit 0

