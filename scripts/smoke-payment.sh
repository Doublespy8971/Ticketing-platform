#!/usr/bin/env bash
# =============================================================================
# smoke-payment.sh — Live End-to-End Smoke Test for payment-service
# =============================================================================
set -uo pipefail

BASE_URL="${PAYMENT_BASE_URL:-http://localhost:8083}"
POSTGRES_DB="${POSTGRES_PAYMENT_DB:-payment_db}"
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
    if [ "$took" -gt 30 ]; then
        warn "${check_label} took ${took}s (> 30s threshold)"
    fi
}

# -----------------------------------------------------------------------------
# Timeout execution helper (15s for docker exec, 10s for curl)
# -----------------------------------------------------------------------------
_run_with_timeout_impl() {
    local duration="$1"
    local stdin_file="$2"
    shift 2
    if command -v timeout >/dev/null 2>&1; then
        if [ -n "$stdin_file" ]; then
            timeout "$duration" "$@" <"$stdin_file"
        else
            timeout "$duration" "$@"
        fi
        return $?
    elif command -v gtimeout >/dev/null 2>&1; then
        if [ -n "$stdin_file" ]; then
            gtimeout "$duration" "$@" <"$stdin_file"
        else
            gtimeout "$duration" "$@"
        fi
        return $?
    else
        local tmp_out
        tmp_out=$(mktemp "${TMPDIR:-/tmp}/smoke_timeout.XXXXXX") || return 1
        if [ -n "$stdin_file" ]; then
            "$@" <"$stdin_file" >"$tmp_out" &
        else
            "$@" >"$tmp_out" &
        fi
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

run_with_timeout() {
    local duration="$1"
    shift
    _run_with_timeout_impl "$duration" "" "$@"
}

run_with_timeout_stdin() {
    local duration="$1"
    local stdin_file="$2"
    shift 2
    _run_with_timeout_impl "$duration" "$stdin_file" "$@"
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
    local tmp_in
    tmp_in=$(mktemp "${TMPDIR:-/tmp}/smoke_stdin.XXXXXX") || return 1
    printf "%s\n" "$payload" >"$tmp_in"
    local out
    out=$(run_with_timeout_stdin 15 "$tmp_in" docker exec -i "$@")
    local rc=$?
    rm -f "$tmp_in"
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

POSTGRES_CONTAINER=$(find_container "postgres-payment" "ticketing-postgres-payment")
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

http_request() {
    local check_name="$1"
    local method="$2"
    local url="$3"
    shift 3

    local body=""
    if [ "$#" -gt 0 ]; then
        case "$1" in
            -*)
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
    docker_exec_timed "$check_name" "$POSTGRES_CONTAINER" sh -c \
        "psql -U \"\${POSTGRES_USER:-${POSTGRES_USER}}\" -d \"${POSTGRES_DB}\" -t -A -c \"$sql\"" | tr -d '[:space:]'
}

get_topic_total_end_offset() {
    local check_name="$1"
    local topic="$2"
    local out
    out=$(docker_exec_timed "${check_name} (GetOffsetShell ${topic})" "$KAFKA_CONTAINER" \
        /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell \
        --bootstrap-server kafka:9092 \
        --topic "$topic" \
        --time -1 2>/dev/null) || return 1
    if [ -z "$out" ]; then
        return 1
    fi
    printf "%s\n" "$out" | awk -F: '{sum += $NF} END {print sum+0}'
}

produce_kafka_event() {
    local check_name="$1"
    local topic="$2"
    local key="$3"
    local json_line="$4"
    local event_type
    event_type=$(json_field "$json_line" "data.get('eventType','')")
    event_type="${event_type:-unknown}"

    local before_offset
    if ! before_offset=$(get_topic_total_end_offset "$check_name" "$topic"); then
        fail "${check_name}: failed to read end offsets before producing (topic=${topic}, key=${key}, eventType=${event_type})"
        return 1
    fi

    if ! docker_exec_stdin_timed "$check_name" "${key}:${json_line}" "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 \
        --topic "$topic" \
        --property "parse.key=true" \
        --property "key.separator=:" >/dev/null; then
        fail "${check_name}: kafka-console-producer failed (topic=${topic}, key=${key}, eventType=${event_type})"
        return 1
    fi

    local after_offset
    if ! after_offset=$(get_topic_total_end_offset "$check_name" "$topic"); then
        fail "${check_name}: failed to read end offsets after producing (topic=${topic}, key=${key}, eventType=${event_type})"
        return 1
    fi

    local delta=$((after_offset - before_offset))
    if [ "$delta" -ne 1 ]; then
        fail "${check_name}: produced event not reflected in topic end offsets (topic=${topic}, key=${key}, eventType=${event_type}, expected delta=1, got delta=${delta}, before=${before_offset}, after=${after_offset})"
        return 1
    fi
    return 0
}

poll_payment_status() {
    local check_name="$1"
    local order_id="$2"
    local expected_status="$3"
    local max_wait_secs="${4:-30}"

    local start_ts
    start_ts=$(date +%s)
    while true; do
        local current_status
        current_status=$(psql_query "$check_name" "SELECT status FROM payments WHERE order_id = '${order_id}';")
        if [ "$current_status" = "$expected_status" ]; then
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

poll_outbox_published() {
    local check_name="$1"
    local order_id="$2"
    local event_type="$3"
    local max_wait_secs="${4:-30}"

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

verify_payment_event_on_kafka() {
    local check_name="$1"
    local order_id="$2"
    local expected_event_type="$3"
    local max_wait_secs="${4:-30}"

    local start_ts
    start_ts=$(date +%s)
    while true; do
        local kafka_dump
        kafka_dump=$(docker_exec_timed "$check_name" "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
            --bootstrap-server kafka:9092 \
            --topic payment.events \
            --from-beginning \
            --property print.key=true \
            --property key.separator="|" \
            --timeout-ms 4000 2>/dev/null || true)

        local matched_line
        matched_line=$(printf "%s\n" "$kafka_dump" | grep "^${order_id}|" | grep "\"eventType\":\"${expected_event_type}\"" | tail -n 1 || true)
        if [ -n "$matched_line" ]; then
            return 0
        fi
        local now_ts
        now_ts=$(date +%s)
        if [ $((now_ts - start_ts)) -ge "$max_wait_secs" ]; then
            return 1
        fi
        sleep 0.5
    done
}

print_payment_check_failure_diagnostics() {
    local check_name="$1"
    local order_id="$2"

    info "--- Failure Diagnostics for Order ID: ${order_id} ---"

    # 1. Row from payments table (status, failure_code, provider_reference) or none exists
    local payment_row
    payment_row=$(docker_exec_timed "${check_name} (diagnostics payments)" "$POSTGRES_CONTAINER" sh -c \
        "psql -U \"\${POSTGRES_USER:-${POSTGRES_USER}}\" -d \"${POSTGRES_DB}\" -t -A -c \"SELECT CONCAT('status=', status, ', failure_code=', COALESCE(failure_code, 'NULL'), ', provider_reference=', COALESCE(provider_reference, 'NULL')) FROM payments WHERE order_id = '${order_id}';\"" 2>/dev/null | tr -d '\r')
    if [ -n "$payment_row" ]; then
        printf "${YELLOW}[DIAGNOSTIC]${NC} payments row: %s\n" "$payment_row"
    else
        printf "${YELLOW}[DIAGNOSTIC]${NC} payments row: none exists\n"
    fi

    # 2. Unpublished or published outbox rows for that aggregate id
    local outbox_rows
    outbox_rows=$(docker_exec_timed "${check_name} (diagnostics outbox)" "$POSTGRES_CONTAINER" sh -c \
        "psql -U \"\${POSTGRES_USER:-${POSTGRES_USER}}\" -d \"${POSTGRES_DB}\" -t -A -c \"SELECT CONCAT('event_id=', event_id, ', event_type=', event_type, ', published_at=', COALESCE(published_at::text, 'NULL')) FROM outbox_events WHERE aggregate_id = '${order_id}' ORDER BY created_at ASC;\"" 2>/dev/null | tr -d '\r')
    if [ -n "$outbox_rows" ]; then
        printf "${YELLOW}[DIAGNOSTIC]${NC} outbox rows for %s:\n" "$order_id"
        while IFS= read -r line; do
            [ -n "$line" ] && printf "    %s\n" "$line"
        done <<< "$outbox_rows"
    else
        printf "${YELLOW}[DIAGNOSTIC]${NC} outbox rows for %s: none exists\n" "$order_id"
    fi

    # 3. End offsets of the retry and dead-letter topics of the order consumer group of payment-service
    local cg="payment-service-order-cg"
    local retry0_topic="order.events-${cg}-retry-0"
    local retry1_topic="order.events-${cg}-retry-1"
    local dlt_topic="order.events-${cg}-dlt"

    for t in "$retry0_topic" "$retry1_topic" "$dlt_topic"; do
        local topic_offsets
        topic_offsets=$(docker_exec_timed "${check_name} (offsets ${t})" "$KAFKA_CONTAINER" \
            /opt/kafka/bin/kafka-get-offsets.sh \
            --bootstrap-server kafka:9092 \
            --topic "$t" \
            --time -1 2>/dev/null | tr -d '\r')
        if [ -n "$topic_offsets" ]; then
            local formatted
            formatted=$(echo "$topic_offsets" | tr '\n' ' ' | sed 's/ $//')
            printf "${YELLOW}[DIAGNOSTIC]${NC} end offsets for %s: %s\n" "$t" "$formatted"
        else
            printf "${YELLOW}[DIAGNOSTIC]${NC} end offsets for %s: (none or unavailable)\n" "$t"
        fi
    done
    info "--- End Diagnostics ---"
}

count_matching_kafka_events() {
    local check_name="$1"
    local order_id="$2"
    local event_type="$3"

    local kafka_dump
    kafka_dump=$(docker_exec_timed "$check_name" "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9092 \
        --topic payment.events \
        --from-beginning \
        --property print.key=true \
        --property key.separator="|" \
        --timeout-ms 4000 2>/dev/null || true)

    printf "%s\n" "$kafka_dump" | grep "^${order_id}|" | grep -c "\"eventType\":\"${event_type}\"" || true
}

build_order_payment_requested_envelope() {
    local order_id="$1"
    local user_id="$2"
    local event_id="$3"
    local token="$4"
    local amount="${5:-15000}"
    local evt_id="${6:-$(uuid_v4)}"
    local corr_id
    corr_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    local exp_ts
    exp_ts=$(iso_plus_seconds 600)
    printf '{"eventId":"%s","eventType":"order.payment.requested","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"orderId":"%s","reservationId":"%s","userId":"%s","eventId":"%s","amountCents":%s,"currency":"USD","paymentMethodToken":"%s","requestedAt":"%s","reservationExpiresAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$order_id" "$order_id" "$user_id" "$event_id" "$amount" "$token" "$now_ts" "$exp_ts"
}

build_order_refund_requested_envelope() {
    local order_id="$1"
    local payment_id="$2"
    local user_id="$3"
    local amount="${4:-15000}"
    local reason="${5:-LATE_PAYMENT_EXPIRED_HOLD}"
    local evt_id="${6:-$(uuid_v4)}"
    local corr_id
    corr_id=$(uuid_v4)
    local now_ts
    now_ts=$(iso_now)
    printf '{"eventId":"%s","eventType":"order.refund.requested","occurredAt":"%s","correlationId":"%s","schemaVersion":1,"payload":{"orderId":"%s","reservationId":"%s","paymentId":"%s","userId":"%s","amountCents":%s,"currency":"USD","reason":"%s","requestedAt":"%s"}}' \
        "$evt_id" "$now_ts" "$corr_id" "$order_id" "$order_id" "$payment_id" "$user_id" "$amount" "$reason" "$now_ts"
}

info "Starting payment-service smoke tests against ${BASE_URL}..."
info "Using containers: postgres=${POSTGRES_CONTAINER}, kafka=${KAFKA_CONTAINER}"

MOCK_DECLINE_RATE=""
http_request "Query mock decline rate" GET "${BASE_URL}/actuator/env/ticketing.payment.provider.mock.decline-rate" || true
if [ "$HTTP_STATUS" = "200" ] && [ -n "$HTTP_BODY" ]; then
    MOCK_DECLINE_RATE=$(json_field "$HTTP_BODY" "data.get('property',{}).get('value','')")
fi

if [ -n "$MOCK_DECLINE_RATE" ]; then
    info "Effective mock decline rate: ${MOCK_DECLINE_RATE}"
else
    info "Effective mock decline rate not exposed; tok_success makes the run independent of it"
fi

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
# 2. Successful Payment: tok_success -> SUCCEEDED + payment.succeeded on payment.events
# =============================================================================
CHECK2_START=$(date +%s)
ORDER1_ID=$(uuid_v4)
USER1_ID=$(uuid_v4)
EVENT1_ID=$(uuid_v4)
PAYMENT1_EVT_ID=$(uuid_v4)

REQ1_JSON=$(build_order_payment_requested_envelope "$ORDER1_ID" "$USER1_ID" "$EVENT1_ID" "tok_success" 15000 "$PAYMENT1_EVT_ID")
if produce_kafka_event "Check 2 (Order 1 payment.requested)" "order.events" "$ORDER1_ID" "$REQ1_JSON"; then
    if poll_payment_status "Check 2 (Order 1 poll SUCCEEDED)" "$ORDER1_ID" "SUCCEEDED" 30 \
        && poll_outbox_published "Check 2 (Order 1 outbox payment.succeeded)" "$ORDER1_ID" "payment.succeeded" 30 \
        && verify_payment_event_on_kafka "Check 2 (Order 1 kafka payment.succeeded)" "$ORDER1_ID" "payment.succeeded" 30; then
        PAYMENT1_ID=$(psql_query "Check 2 (Order 1 get payment_id)" "SELECT payment_id FROM payments WHERE order_id = '${ORDER1_ID}';")
        finish_check "Check 2 (Successful payment)" "$CHECK2_START" "PASS" "2. Successful payment: SUCCEEDED in payment_db and payment.succeeded on payment.events"
    else
        print_payment_check_failure_diagnostics "Check 2 (Successful payment)" "$ORDER1_ID"
        finish_check "Check 2 (Successful payment)" "$CHECK2_START" "FAIL" "2. Order 1 (${ORDER1_ID}) did not reach SUCCEEDED or publish payment.succeeded within 30s"
    fi
else
    print_payment_check_failure_diagnostics "Check 2 (Successful payment)" "$ORDER1_ID"
    finish_check "Check 2 (Successful payment)" "$CHECK2_START" "FAIL" "2. Failed to produce order.payment.requested for Order 1 (${ORDER1_ID})"
fi

# =============================================================================
# 3. Declined Payment: tok_decline -> FAILED + payment.failed on payment.events
# =============================================================================
CHECK3_START=$(date +%s)
ORDER2_ID=$(uuid_v4)
USER2_ID=$(uuid_v4)
EVENT2_ID=$(uuid_v4)
PAYMENT2_EVT_ID=$(uuid_v4)

REQ2_JSON=$(build_order_payment_requested_envelope "$ORDER2_ID" "$USER2_ID" "$EVENT2_ID" "tok_decline" 15000 "$PAYMENT2_EVT_ID")
if produce_kafka_event "Check 3 (Order 2 payment.requested tok_decline)" "order.events" "$ORDER2_ID" "$REQ2_JSON"; then
    if poll_payment_status "Check 3 (Order 2 poll FAILED)" "$ORDER2_ID" "FAILED" 30 \
        && poll_outbox_published "Check 3 (Order 2 outbox payment.failed)" "$ORDER2_ID" "payment.failed" 30 \
        && verify_payment_event_on_kafka "Check 3 (Order 2 kafka payment.failed)" "$ORDER2_ID" "payment.failed" 30; then
        FAIL_CODE=$(psql_query "Check 3 (Order 2 failure_code)" "SELECT failure_code FROM payments WHERE order_id = '${ORDER2_ID}';")
        if [ "$FAIL_CODE" = "CARD_DECLINED" ]; then
            finish_check "Check 3 (Declined payment)" "$CHECK3_START" "PASS" "3. Declined payment: FAILED in payment_db with failureCode=CARD_DECLINED and payment.failed on payment.events"
        else
            print_payment_check_failure_diagnostics "Check 3 (Declined payment)" "$ORDER2_ID"
            finish_check "Check 3 (Declined payment)" "$CHECK3_START" "FAIL" "3. Order 2 (${ORDER2_ID}) expected failure_code=CARD_DECLINED, got '${FAIL_CODE}'"
        fi
    else
        print_payment_check_failure_diagnostics "Check 3 (Declined payment)" "$ORDER2_ID"
        finish_check "Check 3 (Declined payment)" "$CHECK3_START" "FAIL" "3. Order 2 (${ORDER2_ID}) did not reach FAILED or publish payment.failed within 30s"
    fi
else
    print_payment_check_failure_diagnostics "Check 3 (Declined payment)" "$ORDER2_ID"
    finish_check "Check 3 (Declined payment)" "$CHECK3_START" "FAIL" "3. Failed to produce order.payment.requested for Order 2 (${ORDER2_ID})"
fi

# =============================================================================
# 4. Duplicate Event Idempotency: replay same order.payment.requested -> exactly one payment
# =============================================================================
CHECK4_START=$(date +%s)
# Re-send exact same REQ2_JSON with same eventId
if produce_kafka_event "Check 4 (Order 2 duplicate delivery)" "order.events" "$ORDER2_ID" "$REQ2_JSON"; then
    sleep 2
    ORDER2_COUNT=$(psql_query "Check 4 (Order 2 count payments)" "SELECT COUNT(*) FROM payments WHERE order_id = '${ORDER2_ID}';")
    ORDER2_OUTBOX_COUNT=$(psql_query "Check 4 (Order 2 count outbox)" "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '${ORDER2_ID}' AND event_type = 'payment.failed';")
    if [ "$ORDER2_COUNT" = "1" ] && [ "$ORDER2_OUTBOX_COUNT" = "1" ]; then
        finish_check "Check 4 (Duplicate payment event)" "$CHECK4_START" "PASS" "4. Duplicate payment event processed idempotently: exactly 1 payment row and 1 outbox row"
    else
        print_payment_check_failure_diagnostics "Check 4 (Duplicate payment event)" "$ORDER2_ID"
        finish_check "Check 4 (Duplicate payment event)" "$CHECK4_START" "FAIL" "4. Duplicate payment event for Order 2 (${ORDER2_ID}) resulted in payments count=${ORDER2_COUNT}, outbox count=${ORDER2_OUTBOX_COUNT}"
    fi
else
    print_payment_check_failure_diagnostics "Check 4 (Duplicate payment event)" "$ORDER2_ID"
    finish_check "Check 4 (Duplicate payment event)" "$CHECK4_START" "FAIL" "4. Failed to produce duplicate event for Order 2 (${ORDER2_ID})"
fi

# =============================================================================
# 5. Compensating Refund: order.refund.requested -> REFUNDED + payment.refunded on payment.events
# =============================================================================
CHECK5_START=$(date +%s)
if [ -n "${PAYMENT1_ID:-}" ]; then
    REFUND1_EVT_ID=$(uuid_v4)
    REF1_JSON=$(build_order_refund_requested_envelope "$ORDER1_ID" "$PAYMENT1_ID" "$USER1_ID" 15000 "LATE_PAYMENT_EXPIRED_HOLD" "$REFUND1_EVT_ID")
    if produce_kafka_event "Check 5 (Order 1 refund.requested)" "order.events" "$ORDER1_ID" "$REF1_JSON"; then
        if poll_payment_status "Check 5 (Order 1 poll REFUNDED)" "$ORDER1_ID" "REFUNDED" 30 \
            && poll_outbox_published "Check 5 (Order 1 outbox payment.refunded)" "$ORDER1_ID" "payment.refunded" 30 \
            && verify_payment_event_on_kafka "Check 5 (Order 1 kafka payment.refunded)" "$ORDER1_ID" "payment.refunded" 30; then
            finish_check "Check 5 (Compensating refund)" "$CHECK5_START" "PASS" "5. Compensating refund: payment moved to REFUNDED and payment.refunded published"
        else
            print_payment_check_failure_diagnostics "Check 5 (Compensating refund)" "$ORDER1_ID"
            finish_check "Check 5 (Compensating refund)" "$CHECK5_START" "FAIL" "5. Order 1 (${ORDER1_ID}) refund did not reach REFUNDED or publish payment.refunded within 30s"
        fi
    else
        print_payment_check_failure_diagnostics "Check 5 (Compensating refund)" "$ORDER1_ID"
        finish_check "Check 5 (Compensating refund)" "$CHECK5_START" "FAIL" "5. Failed to produce order.refund.requested for Order 1 (${ORDER1_ID})"
    fi
else
    finish_check "Check 5 (Compensating refund)" "$CHECK5_START" "FAIL" "5. Skipped because Order 1 payment was not recorded"
fi

# =============================================================================
# 6. Second Refund Request: second refund request is a no-op, asserting nothing new
# =============================================================================
CHECK6_START=$(date +%s)
if [ -n "${PAYMENT1_ID:-}" ]; then
    REFUND2_EVT_ID=$(uuid_v4)
    REF2_JSON=$(build_order_refund_requested_envelope "$ORDER1_ID" "$PAYMENT1_ID" "$USER1_ID" 15000 "LATE_PAYMENT_EXPIRED_HOLD" "$REFUND2_EVT_ID")
    if produce_kafka_event "Check 6 (Order 1 second refund request)" "order.events" "$ORDER1_ID" "$REF2_JSON"; then
        sleep 2
        ORDER1_REFUND_OUTBOX_COUNT=$(psql_query "Check 6 (Order 1 count refund outbox)" "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '${ORDER1_ID}' AND event_type = 'payment.refunded';")
        ORDER1_STATUS=$(psql_query "Check 6 (Order 1 status)" "SELECT status FROM payments WHERE order_id = '${ORDER1_ID}';")
        if [ "$ORDER1_STATUS" = "REFUNDED" ] && [ "$ORDER1_REFUND_OUTBOX_COUNT" = "1" ]; then
            finish_check "Check 6 (Second refund request no-op)" "$CHECK6_START" "PASS" "6. Second refund request was an idempotent no-op: exactly 1 payment.refunded outbox row"
        else
            print_payment_check_failure_diagnostics "Check 6 (Second refund request no-op)" "$ORDER1_ID"
            finish_check "Check 6 (Second refund request no-op)" "$CHECK6_START" "FAIL" "6. Second refund request for Order 1 (${ORDER1_ID}) expected 1 outbox row, found ${ORDER1_REFUND_OUTBOX_COUNT}"
        fi
    else
        print_payment_check_failure_diagnostics "Check 6 (Second refund request no-op)" "$ORDER1_ID"
        finish_check "Check 6 (Second refund request no-op)" "$CHECK6_START" "FAIL" "6. Failed to produce second refund request"
    fi
else
    finish_check "Check 6 (Second refund request no-op)" "$CHECK6_START" "FAIL" "6. Skipped because Order 1 payment was not recorded"
fi

# =============================================================================
# Summary
# =============================================================================
SCRIPT_END_EPOCH=$(date +%s)
TOTAL_RUNTIME=$((SCRIPT_END_EPOCH - SCRIPT_START_EPOCH))
echo ""
echo "============================================================================="
printf "Payment Smoke Test Summary: ${GREEN}%d passed${NC}, ${RED}%d failed${NC} (total runtime: %ds)\n" "$PASS_COUNT" "$FAIL_COUNT" "$TOTAL_RUNTIME"
echo "============================================================================="

if [ "$FAIL_COUNT" -gt 0 ]; then
    exit 1
fi
exit 0

