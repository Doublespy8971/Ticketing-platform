#!/usr/bin/env bash
# =============================================================================
# smoke-notification.sh — Live End-to-End Smoke Test for notification-service
# =============================================================================
set -uo pipefail

BASE_URL="${NOTIFICATION_BASE_URL:-http://localhost:8084}"
KAFKA_GROUP="${NOTIFICATION_KAFKA_GROUP:-notification-service-order-cg}"
TOPIC="${NOTIFICATION_TOPIC:-order.events}"
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

KAFKA_CONTAINER=$(find_container "kafka" "ticketing-kafka")

uuid_v4() {
    python3 -c 'import uuid; print(str(uuid.uuid4()))'
}

iso_now() {
    python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))'
}

json_field() {
    local json="$1"
    local expr="$2"
    python3 -c "import json, sys; data = json.loads(sys.argv[1]); print($expr)" "$json" 2>/dev/null || echo ""
}

HTTP_STATUS=""
HTTP_BODY=""
HTTP_HEADERS=""

http_request() {
    local method="$1"
    local url="$2"
    shift 2

    local tmp_body
    local tmp_headers
    tmp_body=$(mktemp "${TMPDIR:-/tmp}/smoke_body.XXXXXX") || return 1
    tmp_headers=$(mktemp "${TMPDIR:-/tmp}/smoke_headers.XXXXXX") || { rm -f "$tmp_body"; return 1; }

    HTTP_STATUS=""
    HTTP_BODY=""
    HTTP_HEADERS=""

    local curl_rc=0
    HTTP_STATUS=$(curl -sS --connect-timeout 3 --max-time 10 \
        -D "$tmp_headers" \
        -o "$tmp_body" \
        -w "%{http_code}" \
        -X "$method" \
        "$url" \
        "$@") || curl_rc=$?

    HTTP_BODY=$(cat "$tmp_body" 2>/dev/null || echo "")
    HTTP_HEADERS=$(cat "$tmp_headers" 2>/dev/null || echo "")
    rm -f "$tmp_body" "$tmp_headers"

    if [ "$curl_rc" -ne 0 ]; then
        HTTP_STATUS="${HTTP_STATUS:-000}"
        return 1
    fi
    return 0
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

    local before_offset
    if ! before_offset=$(get_topic_total_end_offset "$check_name" "$topic"); then
        fail "${check_name}: failed to read end offsets before producing (topic=${topic}, key=${key})"
        return 1
    fi

    if ! docker_exec_stdin_timed "$check_name" "${key}:${json_line}" "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 \
        --topic "$topic" \
        --property "parse.key=true" \
        --property "key.separator=:" >/dev/null; then
        fail "${check_name}: kafka-console-producer failed (topic=${topic}, key=${key})"
        return 1
    fi

    local after_offset
    if ! after_offset=$(get_topic_total_end_offset "$check_name" "$topic"); then
        fail "${check_name}: failed to read end offsets after producing (topic=${topic}, key=${key})"
        return 1
    fi

    local delta=$((after_offset - before_offset))
    if [ "$delta" -ne 1 ]; then
        fail "${check_name}: produced event not reflected in topic end offsets (topic=${topic}, key=${key}, expected delta=1, got delta=${delta})"
        return 1
    fi
    return 0
}

get_consumer_group_lag() {
    local check_name="$1"
    local cg="$2"
    local out
    out=$(docker_exec_timed "${check_name} (ConsumerGroupLag ${cg})" "$KAFKA_CONTAINER" \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:9092 \
        --group "$cg" \
        --describe 2>/dev/null) || return 1

    printf "%s\n" "$out" | awk -v g="$cg" '$1 == g && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}'
}

wait_for_consumer_lag_zero() {
    local check_name="$1"
    local cg="$2"
    local max_wait="${3:-15}"

    local start_ts
    start_ts=$(date +%s)
    while true; do
        local lag
        lag=$(get_consumer_group_lag "$check_name" "$cg" || echo "999")
        if [ "$lag" -eq 0 ]; then
            return 0
        fi

        local now_ts
        now_ts=$(date +%s)
        if [ $((now_ts - start_ts)) -ge "$max_wait" ]; then
            warn "${check_name}: consumer lag did not reach 0 within ${max_wait}s (current lag: ${lag})"
            return 1
        fi
        sleep 0.5
    done
}

get_metric_counter() {
    local event_type="$1"
    local outcome="$2"

    http_request "GET" "${BASE_URL}/metrics" || return 1

    local val
    val=$(printf "%s\n" "$HTTP_BODY" | awk -v et="$event_type" -v oc="$outcome" '
        $0 ~ /^notification_events_total/ && $0 ~ ("event_type=\"" et "\"") && $0 ~ ("outcome=\"" oc "\"") {
            print $NF
        }
    ' | tail -n 1)

    if [ -z "$val" ]; then
        echo "0"
    else
        printf "%.0f" "$val"
    fi
}

# =============================================================================
# Smoke Tests
# =============================================================================

info "Starting live smoke tests for notification-service against ${BASE_URL}..."

# -----------------------------------------------------------------------------
# Check 1: Health & Readiness Endpoints
# -----------------------------------------------------------------------------
check_start=$(date +%s)
info "Check 1: Verifying /health and /ready..."

if ! http_request "GET" "${BASE_URL}/health"; then
    finish_check "Check 1" "$check_start" "FAIL" "GET /health unreachable at ${BASE_URL}"
else
    status_field=$(json_field "$HTTP_BODY" "data.get('status','')")
    if [ "$HTTP_STATUS" -eq 200 ] && [ "$status_field" = "UP" ]; then
        http_request "GET" "${BASE_URL}/ready"
        ready_status=$(json_field "$HTTP_BODY" "data.get('status','')")
        if [ "$HTTP_STATUS" -eq 200 ] && [ "$ready_status" = "UP" ]; then
            finish_check "Check 1" "$check_start" "PASS" "notification-service is UP and READY"
        else
            finish_check "Check 1" "$check_start" "FAIL" "GET /ready returned status=${HTTP_STATUS}, body=${HTTP_BODY}"
        fi
    else
        finish_check "Check 1" "$check_start" "FAIL" "GET /health returned status=${HTTP_STATUS}, body=${HTTP_BODY}"
    fi
fi

# -----------------------------------------------------------------------------
# Check 2: Produce valid order.confirmed -> lag reaches 0 & metric sent incremented
# -----------------------------------------------------------------------------
check_start=$(date +%s)
info "Check 2: Producing order.confirmed and asserting consumption within 15s..."

CONFIRMED_EVENT_ID=$(uuid_v4)
ORDER_ID=$(uuid_v4)
USER_ID=$(uuid_v4)
EVENT_ID=$(uuid_v4)
PAYMENT_ID=$(uuid_v4)
SEAT_ID=$(uuid_v4)
OCCURRED_AT=$(iso_now)

INITIAL_SENT=$(get_metric_counter "order.confirmed" "sent")
INITIAL_DUP=$(get_metric_counter "order.confirmed" "duplicate")

CONFIRMED_ENVELOPE=$(cat <<EOF
{
  "eventId": "${CONFIRMED_EVENT_ID}",
  "eventType": "order.confirmed",
  "occurredAt": "${OCCURRED_AT}",
  "correlationId": "$(uuid_v4)",
  "schemaVersion": 1,
  "payload": {
    "orderId": "${ORDER_ID}",
    "reservationId": "${ORDER_ID}",
    "userId": "${USER_ID}",
    "eventId": "${EVENT_ID}",
    "paymentId": "${PAYMENT_ID}",
    "seats": [
      {
        "seatId": "${SEAT_ID}",
        "sectionCode": "FLOOR-A",
        "rowLabel": "1",
        "seatNumber": "101",
        "priceCents": 15000
      }
    ],
    "totalAmountCents": 15000,
    "currency": "USD",
    "confirmedAt": "${OCCURRED_AT}"
  }
}
EOF
)
CONFIRMED_ENVELOPE_ONE_LINE=$(printf "%s" "$CONFIRMED_ENVELOPE" | tr -d '\n')

if produce_kafka_event "Check 2 (Produce order.confirmed)" "$TOPIC" "$ORDER_ID" "$CONFIRMED_ENVELOPE_ONE_LINE"; then
    if wait_for_consumer_lag_zero "Check 2 (Wait lag 0)" "$KAFKA_GROUP" 15; then
        AFTER_SENT=$(get_metric_counter "order.confirmed" "sent")
        DIFF_SENT=$((AFTER_SENT - INITIAL_SENT))
        if [ "$DIFF_SENT" -eq 1 ]; then
            finish_check "Check 2" "$check_start" "PASS" "order.confirmed consumed, lag=0, sent counter increased by 1"
        else
            finish_check "Check 2" "$check_start" "FAIL" "expected sent counter to increase by 1, before=${INITIAL_SENT}, after=${AFTER_SENT}"
        fi
    else
        finish_check "Check 2" "$check_start" "FAIL" "consumer group lag did not reach zero within 15s"
    fi
else
    finish_check "Check 2" "$check_start" "FAIL" "failed to produce order.confirmed event"
fi

# -----------------------------------------------------------------------------
# Check 3: Produce duplicate order.confirmed -> duplicate counter increases, sent does not
# -----------------------------------------------------------------------------
check_start=$(date +%s)
info "Check 3: Producing duplicate order.confirmed and asserting deduplication..."

BEFORE_DUP=$(get_metric_counter "order.confirmed" "duplicate")
BEFORE_SENT=$(get_metric_counter "order.confirmed" "sent")

if produce_kafka_event "Check 3 (Produce duplicate order.confirmed)" "$TOPIC" "$ORDER_ID" "$CONFIRMED_ENVELOPE_ONE_LINE"; then
    if wait_for_consumer_lag_zero "Check 3 (Wait lag 0)" "$KAFKA_GROUP" 15; then
        AFTER_DUP=$(get_metric_counter "order.confirmed" "duplicate")
        AFTER_SENT=$(get_metric_counter "order.confirmed" "sent")
        DIFF_DUP=$((AFTER_DUP - BEFORE_DUP))
        DIFF_SENT=$((AFTER_SENT - BEFORE_SENT))

        if [ "$DIFF_DUP" -ge 1 ] && [ "$DIFF_SENT" -eq 0 ]; then
            finish_check "Check 3" "$check_start" "PASS" "duplicate order.confirmed deduplicated: duplicate counter increased, sent unchanged"
        else
            finish_check "Check 3" "$check_start" "FAIL" "unexpected counters on duplicate: sent diff=${DIFF_SENT}, dup diff=${DIFF_DUP}"
        fi
    else
        finish_check "Check 3" "$check_start" "FAIL" "consumer group lag did not reach zero on duplicate within 15s"
    fi
else
    finish_check "Check 3" "$check_start" "FAIL" "failed to produce duplicate order.confirmed"
fi

# -----------------------------------------------------------------------------
# Check 4: Produce order.cancelled -> sent counter for order.cancelled increases
# -----------------------------------------------------------------------------
check_start=$(date +%s)
info "Check 4: Producing order.cancelled and asserting consumption..."

CANCELLED_EVENT_ID=$(uuid_v4)
CANCELLED_ORDER_ID=$(uuid_v4)
CANCELLED_USER_ID=$(uuid_v4)
CANCELLED_EVENT_UUID=$(uuid_v4)
CANCELLED_OCCURRED_AT=$(iso_now)

BEFORE_CANCELLED_SENT=$(get_metric_counter "order.cancelled" "sent")

CANCELLED_ENVELOPE=$(cat <<EOF
{
  "eventId": "${CANCELLED_EVENT_ID}",
  "eventType": "order.cancelled",
  "occurredAt": "${CANCELLED_OCCURRED_AT}",
  "correlationId": "$(uuid_v4)",
  "schemaVersion": 1,
  "payload": {
    "orderId": "${CANCELLED_ORDER_ID}",
    "reservationId": "${CANCELLED_ORDER_ID}",
    "userId": "${CANCELLED_USER_ID}",
    "eventId": "${CANCELLED_EVENT_UUID}",
    "finalStatus": "CANCELLED_PAYMENT_FAILED",
    "cancellationReason": "PAYMENT_FAILED",
    "cancelledAt": "${CANCELLED_OCCURRED_AT}"
  }
}
EOF
)
CANCELLED_ENVELOPE_ONE_LINE=$(printf "%s" "$CANCELLED_ENVELOPE" | tr -d '\n')

if produce_kafka_event "Check 4 (Produce order.cancelled)" "$TOPIC" "$CANCELLED_ORDER_ID" "$CANCELLED_ENVELOPE_ONE_LINE"; then
    if wait_for_consumer_lag_zero "Check 4 (Wait lag 0)" "$KAFKA_GROUP" 15; then
        AFTER_CANCELLED_SENT=$(get_metric_counter "order.cancelled" "sent")
        DIFF_CANCELLED_SENT=$((AFTER_CANCELLED_SENT - BEFORE_CANCELLED_SENT))
        if [ "$DIFF_CANCELLED_SENT" -eq 1 ]; then
            finish_check "Check 4" "$check_start" "PASS" "order.cancelled consumed, lag=0, sent counter increased by 1"
        else
            finish_check "Check 4" "$check_start" "FAIL" "expected cancelled sent counter to increase by 1, got diff=${DIFF_CANCELLED_SENT}"
        fi
    else
        finish_check "Check 4" "$check_start" "FAIL" "consumer group lag did not reach zero for order.cancelled within 15s"
    fi
else
    finish_check "Check 4" "$check_start" "FAIL" "failed to produce order.cancelled event"
fi

# -----------------------------------------------------------------------------
# Check 5: Produce malformed record -> invalid counter increases & service stays healthy
# -----------------------------------------------------------------------------
check_start=$(date +%s)
info "Check 5: Producing malformed record and asserting service stays healthy..."

BEFORE_INVALID=$(get_metric_counter "unknown" "invalid")

MALFORMED_PAYLOAD="not-a-valid-json-payload-at-all"
MALFORMED_KEY=$(uuid_v4)

if produce_kafka_event "Check 5 (Produce malformed)" "$TOPIC" "$MALFORMED_KEY" "$MALFORMED_PAYLOAD"; then
    if wait_for_consumer_lag_zero "Check 5 (Wait lag 0)" "$KAFKA_GROUP" 15; then
        AFTER_INVALID=$(get_metric_counter "unknown" "invalid")
        DIFF_INVALID=$((AFTER_INVALID - BEFORE_INVALID))

        http_request "GET" "${BASE_URL}/health"
        if [ "$HTTP_STATUS" -eq 200 ] && [ "$DIFF_INVALID" -ge 1 ]; then
            finish_check "Check 5" "$check_start" "PASS" "malformed record committed, invalid counter increased, service healthy"
        else
            finish_check "Check 5" "$check_start" "FAIL" "malformed record check failed: health status=${HTTP_STATUS}, invalid diff=${DIFF_INVALID}"
        fi
    else
        finish_check "Check 5" "$check_start" "FAIL" "consumer group lag did not reach zero for malformed record within 15s"
    fi
else
    finish_check "Check 5" "$check_start" "FAIL" "failed to produce malformed event"
fi

# -----------------------------------------------------------------------------
# Check 6: Produce unknown event type -> ignored counter increases & service stays healthy
# -----------------------------------------------------------------------------
check_start=$(date +%s)
info "Check 6: Producing unknown event type and asserting ignored counter..."

BEFORE_IGNORED=$(get_metric_counter "unknown" "ignored")
UNKNOWN_EVENT_ID=$(uuid_v4)
UNKNOWN_ORDER_ID=$(uuid_v4)

UNKNOWN_ENVELOPE=$(cat <<EOF
{
  "eventId": "${UNKNOWN_EVENT_ID}",
  "eventType": "order.unrecognized.event",
  "occurredAt": "$(iso_now)",
  "correlationId": "$(uuid_v4)",
  "schemaVersion": 1,
  "payload": {
    "amountCents": 1000
  }
}
EOF
)
UNKNOWN_ENVELOPE_ONE_LINE=$(printf "%s" "$UNKNOWN_ENVELOPE" | tr -d '\n')

if produce_kafka_event "Check 6 (Produce unknown event)" "$TOPIC" "$UNKNOWN_ORDER_ID" "$UNKNOWN_ENVELOPE_ONE_LINE"; then
    if wait_for_consumer_lag_zero "Check 6 (Wait lag 0)" "$KAFKA_GROUP" 15; then
        AFTER_IGNORED=$(get_metric_counter "unknown" "ignored")
        DIFF_IGNORED=$((AFTER_IGNORED - BEFORE_IGNORED))

        http_request "GET" "${BASE_URL}/health"
        if [ "$HTTP_STATUS" -eq 200 ] && [ "$DIFF_IGNORED" -ge 1 ]; then
            finish_check "Check 6" "$check_start" "PASS" "unknown event type committed, ignored counter increased, service healthy"
        else
            finish_check "Check 6" "$check_start" "FAIL" "unknown event check failed: health status=${HTTP_STATUS}, ignored diff=${DIFF_IGNORED}"
        fi
    else
        finish_check "Check 6" "$check_start" "FAIL" "consumer group lag did not reach zero for unknown event within 15s"
    fi
else
    finish_check "Check 6" "$check_start" "FAIL" "failed to produce unknown event"
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
SCRIPT_END_EPOCH=$(date +%s)
TOTAL_TOOK=$((SCRIPT_END_EPOCH - SCRIPT_START_EPOCH))

printf "\n=============================================================================\n"
printf "Smoke Test Summary for notification-service (total time: %ds):\n" "$TOTAL_TOOK"
printf "  ${GREEN}Passed: %d${NC}\n" "$PASS_COUNT"
if [ "$FAIL_COUNT" -gt 0 ]; then
    printf "  ${RED}Failed: %d${NC}\n" "$FAIL_COUNT"
    printf "=============================================================================\n"
    exit 1
else
    printf "  ${GREEN}All checks PASSED${NC}\n"
    printf "=============================================================================\n"
    exit 0
fi
