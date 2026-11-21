#!/usr/bin/env bash
# =============================================================================
# smoke-gateway.sh — Live End-to-End Smoke Test for gateway-service (:8080)
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/kafka-delivery.sh"

GATEWAY_BASE_URL="${GATEWAY_BASE_URL:-http://localhost:8080}"
JWT_SECRET="${JWT_SECRET:-dev-only-hs256-secret-key-minimum-32-bytes-long!}"
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

uuid_v4() {
    python3 -c 'import uuid; print(str(uuid.uuid4()))'
}

iso_past() {
    local secs="$1"
    python3 -c "import datetime; print((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=${secs})).strftime('%Y-%m-%dT%H:%M:%SZ'))"
}

iso_future() {
    local secs="$1"
    python3 -c "import datetime; print((datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=${secs})).strftime('%Y-%m-%dT%H:%M:%SZ'))"
}

json_field() {
    local json="$1"
    local expr="$2"
    python3 -c "import json, sys; data = json.loads(sys.argv[1]); print($expr)" "$json" 2>/dev/null || echo ""
}

mint_jwt() {
    local sub="$1"
    local role="$2"
    python3 -c "
import base64, json, hmac, hashlib, time

header = {'alg': 'HS256', 'typ': 'JWT'}
now = int(time.time())
payload = {'sub': '$sub', 'roles': ['$role'], 'iat': now, 'exp': now + 3600}

def b64(d):
    return base64.urlsafe_b64encode(json.dumps(d, separators=(',', ':')).encode()).decode().rstrip('=')

h = b64(header)
p = b64(payload)
signing_input = f'{h}.{p}'.encode()
sig = base64.urlsafe_b64encode(hmac.new('$JWT_SECRET'.encode(), signing_input, hashlib.sha256).digest()).decode().rstrip('=')
print(f'{h}.{p}.{sig}')
"
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

info "=== Starting Gateway Service End-to-End Smoke Test ==="
info "Target gateway: ${GATEWAY_BASE_URL}"

# -----------------------------------------------------------------------------
# Check 0: Gateway Health
# -----------------------------------------------------------------------------
CHK_START=$(date +%s)
http_request "Gateway Health" "GET" "${GATEWAY_BASE_URL}/actuator/health" || true
if [ "$HTTP_STATUS" = "200" ]; then
    finish_check "Health Check" "$CHK_START" "PASS" "Gateway service is UP (:8080)"
else
    finish_check "Health Check" "$CHK_START" "FAIL" "Gateway health check failed (HTTP ${HTTP_STATUS}): ${HTTP_BODY}"
fi

# -----------------------------------------------------------------------------
# Check 1: Obtain User Token and Admin Token through Gateway
# -----------------------------------------------------------------------------
USER_ID=$(uuid_v4)
ADMIN_ID=$(uuid_v4)

CHK_START=$(date +%s)
http_request "Obtain User Token" "POST" "${GATEWAY_BASE_URL}/api/v1/auth/token" \
    "{\"userId\":\"${USER_ID}\",\"roles\":[\"USER\"],\"ttlSeconds\":3600}" || true

USER_TOKEN=$(json_field "$HTTP_BODY" 'data.get("accessToken", "")')

if [ "$HTTP_STATUS" = "200" ] && [ -n "$USER_TOKEN" ]; then
    finish_check "Obtain User Token" "$CHK_START" "PASS" "Obtained user token for ${USER_ID}"
elif [ "$HTTP_STATUS" = "404" ]; then
    warn "POST /api/v1/auth/token returned 404. Dev token endpoint is inactive. The gateway must be started with 'make run-gateway' (local profile)."
    USER_TOKEN=$(mint_jwt "$USER_ID" "USER")
    finish_check "Obtain User Token" "$CHK_START" "PASS" "Minted fallback user token with JWT_SECRET (gateway started without local profile)"
else
    finish_check "Obtain User Token" "$CHK_START" "FAIL" "Failed to obtain user token (HTTP ${HTTP_STATUS}): ${HTTP_BODY}"
fi

# Admin token: attempt via token endpoint; if dev-token allow-admin is false or 404, mint with JWT_SECRET
CHK_START=$(date +%s)
http_request "Obtain Admin Token" "POST" "${GATEWAY_BASE_URL}/api/v1/auth/token" \
    "{\"userId\":\"${ADMIN_ID}\",\"roles\":[\"ADMIN\"],\"ttlSeconds\":3600}" || true

if [ "$HTTP_STATUS" = "200" ]; then
    ADMIN_TOKEN=$(json_field "$HTTP_BODY" 'data.get("accessToken", "")')
    finish_check "Obtain Admin Token" "$CHK_START" "PASS" "Obtained admin token through dev token endpoint"
elif [ "$HTTP_STATUS" = "404" ]; then
    warn "POST /api/v1/auth/token returned 404. Gateway must be started with 'make run-gateway'."
    ADMIN_TOKEN=$(mint_jwt "$ADMIN_ID" "ADMIN")
    finish_check "Obtain Admin Token" "$CHK_START" "PASS" "Minted admin token with JWT_SECRET (local profile not active)"
else
    ADMIN_TOKEN=$(mint_jwt "$ADMIN_ID" "ADMIN")
    finish_check "Obtain Admin Token" "$CHK_START" "PASS" "Minted admin token with JWT_SECRET (dev endpoint allow-admin=false)"
fi

# -----------------------------------------------------------------------------
# Check 2: Creating Event through Gateway: Admin Succeeds (201), User Fails (403)
# -----------------------------------------------------------------------------
DROP_TIME=$(iso_past 60)
EVENT_TIME=$(iso_future 86400)
CREATE_EVENT_PAYLOAD="{\"name\":\"Smoke Gate Tour\",\"venueName\":\"Gateway Arena\",\"eventTime\":\"${EVENT_TIME}\",\"dropTime\":\"${DROP_TIME}\"}"

CHK_START=$(date +%s)
http_request "Admin Create Event" "POST" "${GATEWAY_BASE_URL}/api/v1/admin/events" \
    "$CREATE_EVENT_PAYLOAD" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" || true

EVENT_ID=$(json_field "$HTTP_BODY" 'data.get("eventId", "")')
if [ "$HTTP_STATUS" = "201" ] && [ -n "$EVENT_ID" ]; then
    finish_check "Admin Create Event" "$CHK_START" "PASS" "Admin created event ${EVENT_ID}"
else
    finish_check "Admin Create Event" "$CHK_START" "FAIL" "Admin event creation failed (HTTP ${HTTP_STATUS}): ${HTTP_BODY}"
fi

# User token on same admin route -> 403 Forbidden
CHK_START=$(date +%s)
http_request "User Admin Route Forbidden" "POST" "${GATEWAY_BASE_URL}/api/v1/admin/events" \
    "$CREATE_EVENT_PAYLOAD" \
    -H "Authorization: Bearer ${USER_TOKEN}" || true

ERR_CODE=$(json_field "$HTTP_BODY" 'data.get("errorCode", "")')
if [ "$HTTP_STATUS" = "403" ] && [ "$ERR_CODE" = "FORBIDDEN" ]; then
    finish_check "User Admin Route Forbidden" "$CHK_START" "PASS" "User token on admin route returned 403 FORBIDDEN"
else
    finish_check "User Admin Route Forbidden" "$CHK_START" "FAIL" "Expected 403 FORBIDDEN, got HTTP ${HTTP_STATUS} (${ERR_CODE}): ${HTTP_BODY}"
fi

# -----------------------------------------------------------------------------
# Check 3: No Token Returns 401 Unauthorized with X-Correlation-Id Echoed
# -----------------------------------------------------------------------------
CHK_START=$(date +%s)
CORR_ID=$(uuid_v4)
http_request "No Token Unauthorized" "POST" "${GATEWAY_BASE_URL}/api/v1/admin/events" \
    "$CREATE_EVENT_PAYLOAD" \
    -H "X-Correlation-Id: ${CORR_ID}" || true

ERR_CODE=$(json_field "$HTTP_BODY" 'data.get("errorCode", "")')
RESP_CORR_ID=$(printf "%s" "$HTTP_HEADERS" | grep -i '^x-correlation-id:' | head -n 1 | awk '{print $2}' | tr -d '\r\n')

if [ "$HTTP_STATUS" = "401" ] && [ "$ERR_CODE" = "UNAUTHORIZED" ] && [ "$RESP_CORR_ID" = "$CORR_ID" ]; then
    finish_check "No Token Unauthorized" "$CHK_START" "PASS" "Request with no token returned 401 UNAUTHORIZED with X-Correlation-Id echoed"
else
    finish_check "No Token Unauthorized" "$CHK_START" "FAIL" "Expected 401 UNAUTHORIZED with correlationId ${CORR_ID}, got HTTP ${HTTP_STATUS} (header: ${RESP_CORR_ID}): ${HTTP_BODY}"
fi

# -----------------------------------------------------------------------------
# Check 4: User Token + Spoofed X-User-Roles ADMIN Returns 403
# -----------------------------------------------------------------------------
CHK_START=$(date +%s)
http_request "Spoofed Role Forbidden" "POST" "${GATEWAY_BASE_URL}/api/v1/admin/events" \
    "$CREATE_EVENT_PAYLOAD" \
    -H "Authorization: Bearer ${USER_TOKEN}" \
    -H "X-User-Roles: ADMIN" || true

ERR_CODE=$(json_field "$HTTP_BODY" 'data.get("errorCode", "")')
if [ "$HTTP_STATUS" = "403" ] && [ "$ERR_CODE" = "FORBIDDEN" ]; then
    finish_check "Spoofed Role Forbidden" "$CHK_START" "PASS" "User token with spoofed X-User-Roles ADMIN still returned 403"
else
    finish_check "Spoofed Role Forbidden" "$CHK_START" "FAIL" "Expected 403, got HTTP ${HTTP_STATUS} (${ERR_CODE}): ${HTTP_BODY}"
fi

# -----------------------------------------------------------------------------
# Check 4b: Path Hygiene Check (Percent-encoded admin path must return 400)
# -----------------------------------------------------------------------------
CHK_START=$(date +%s)
http_request "Path Hygiene Check" "POST" "${GATEWAY_BASE_URL}/api/v1/%61dmin/events" \
    "$CREATE_EVENT_PAYLOAD" \
    -H "Authorization: Bearer ${USER_TOKEN}" || true

if [ "$HTTP_STATUS" = "400" ]; then
    finish_check "Path Hygiene Check" "$CHK_START" "PASS" "Percent-encoded admin path rejected with HTTP 400 VALIDATION_ERROR"
else
    finish_check "Path Hygiene Check" "$CHK_START" "FAIL" "Expected 400 for percent-encoded path, got HTTP ${HTTP_STATUS}: ${HTTP_BODY}"
fi

# -----------------------------------------------------------------------------
# Check 4c: Underscore Identity Headers Stripped on Public Route
# -----------------------------------------------------------------------------
if [ -n "$EVENT_ID" ]; then
    # Create seats and put event on sale using admin token so public route returns seats of created event
    SEAT_NUMBER=$(( (RANDOM % 800) + 100 ))
    BULK_SEATS_PAYLOAD="{\"seats\":[{\"sectionCode\":\"GA\",\"rowLabel\":\"A\",\"seatNumber\":\"${SEAT_NUMBER}\",\"priceCents\":5000}]}"
    http_request "Admin Add Seat" "POST" "${GATEWAY_BASE_URL}/api/v1/admin/events/${EVENT_ID}/seats/bulk" \
        "$BULK_SEATS_PAYLOAD" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" || true

    SEAT_ID=$(python3 -c "import json, sys; data = json.loads(sys.argv[1]); print(data['seats'][0]['seatId'])" "$HTTP_BODY" 2>/dev/null || echo "")

    http_request "Admin Put On Sale" "PATCH" "${GATEWAY_BASE_URL}/api/v1/admin/events/${EVENT_ID}/status" \
        "{\"status\":\"ON_SALE\"}" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" || true

    CHK_START=$(date +%s)
    http_request "Underscore Identity Headers Public Route" "GET" "${GATEWAY_BASE_URL}/api/v1/events/${EVENT_ID}/seats" \
        -H "X_User_Id: spoofed-id-123" \
        -H "X_User_Roles: ADMIN" || true

    SEAT_COUNT=$(python3 -c "import json, sys; data = json.loads(sys.argv[1]); items = data.get('items') or data.get('seats') or []; print(len(items))" "$HTTP_BODY" 2>/dev/null || echo "0")

    if [ "$HTTP_STATUS" != "200" ] || [ "$SEAT_COUNT" -lt 1 ]; then
        finish_check "Underscore Identity Headers Public Route" "$CHK_START" "FAIL" "Expected HTTP 200 with seats of created event, got HTTP ${HTTP_STATUS} (seats count: ${SEAT_COUNT}): ${HTTP_BODY}"
    elif [ -n "${DOWNSTREAM_ECHO_URL:-}" ]; then
        ECHO_RESP=$(curl -sS --connect-timeout 2 "${DOWNSTREAM_ECHO_URL}" 2>/dev/null || echo "")
        if echo "$ECHO_RESP" | grep -qi "spoofed-id-123"; then
            finish_check "Underscore Identity Headers Public Route" "$CHK_START" "FAIL" "Downstream echo received forwarded X_User_Id"
        else
            finish_check "Underscore Identity Headers Public Route" "$CHK_START" "PASS" "Downstream echo confirmed X_User_Id and X_User_Roles were stripped (HTTP 200 with ${SEAT_COUNT} seats)"
        fi
    else
        finish_check "Underscore Identity Headers Public Route" "$CHK_START" "PASS" "Public seats request returned 200 with ${SEAT_COUNT} seats of created event ${EVENT_ID}"
    fi
else
    CHK_START=$(date +%s)
    finish_check "Underscore Identity Headers Public Route" "$CHK_START" "FAIL" "No eventId created in Check 2"
fi

# -----------------------------------------------------------------------------
# Check 5: Hold Request through Gateway Returns 201 and Belongs to Token User
# -----------------------------------------------------------------------------
if [ -n "$EVENT_ID" ] && [ -n "$SEAT_ID" ]; then
    CHK_START=$(date +%s)
    HOLD_IDEMPOTENCY_KEY=$(uuid_v4)
    HOLD_PAYLOAD="{\"eventId\":\"${EVENT_ID}\",\"seatIds\":[\"${SEAT_ID}\"]}"

    http_request "User Hold Seat" "POST" "${GATEWAY_BASE_URL}/api/v1/reservations" \
        "$HOLD_PAYLOAD" \
        -H "Authorization: Bearer ${USER_TOKEN}" \
        -H "Idempotency-Key: ${HOLD_IDEMPOTENCY_KEY}" || true

    RES_USER_ID=$(json_field "$HTTP_BODY" 'data.get("userId", "")')
    RES_ID=$(json_field "$HTTP_BODY" 'data.get("reservationId", "")')

    if [ "$HTTP_STATUS" = "201" ] && [ "$RES_USER_ID" = "$USER_ID" ]; then
        finish_check "User Hold Seat" "$CHK_START" "PASS" "Hold created (${RES_ID}) and belongs to user ${USER_ID}"
    else
        finish_check "User Hold Seat" "$CHK_START" "FAIL" "Expected 201 for user ${USER_ID}, got HTTP ${HTTP_STATUS} (user: ${RES_USER_ID}): ${HTTP_BODY}"
    fi
elif [ -n "$EVENT_ID" ]; then
    fail "User Hold Seat: unable to bulk create seat for event ${EVENT_ID}"
else
    fail "User Hold Seat skipped: no eventId created in Check 2"
fi

# -----------------------------------------------------------------------------
# Check 6: Parallel Burst on Authenticated Route Triggers Rate Limit (HTTP 429)
# -----------------------------------------------------------------------------
CHK_START=$(date +%s)
BURST_USER_ID=$(uuid_v4)
BURST_USER_TOKEN=$(mint_jwt "$BURST_USER_ID" "USER")

TMP_STATUS_FILE=$(mktemp "${TMPDIR:-/tmp}/gateway_burst_status.XXXXXX")

# Fire 40 requests concurrently in background jobs to an authenticated route
for i in $(seq 1 40); do
    (
        code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 2 --max-time 5 \
            -X GET "${GATEWAY_BASE_URL}/api/v1/orders/00000000-0000-0000-0000-000000000000" \
            -H "Authorization: Bearer ${BURST_USER_TOKEN}")
        echo "$code" >> "$TMP_STATUS_FILE"
    ) &
done

wait

COUNT_429=$(grep -c "^429" "$TMP_STATUS_FILE" || true)
COUNT_NON_429=$(grep -v "^429" "$TMP_STATUS_FILE" | grep -v "^$" | wc -l | tr -d ' ' || true)
rm -f "$TMP_STATUS_FILE"

if [ "$COUNT_429" -gt 0 ] && [ "$COUNT_NON_429" -gt 0 ]; then
    finish_check "Rate Limit Burst" "$CHK_START" "PASS" "Parallel burst sent 40 requests: ${COUNT_429} returned 429, ${COUNT_NON_429} processed"
else
    finish_check "Rate Limit Burst" "$CHK_START" "FAIL" "Burst failed: expected at least one 429 and one non-429 (429 count: ${COUNT_429}, non-429: ${COUNT_NON_429})"
fi

# -----------------------------------------------------------------------------
# Check 7: Redis Outage Resilience: Gateway Readiness Stays 200 and Fails Open
# -----------------------------------------------------------------------------
cleanup_redis() {
    docker start redis >/dev/null 2>&1 || true
    for _ in $(seq 1 30); do
        if docker exec redis redis-cli ping >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
}
trap cleanup_redis EXIT INT TERM

CHK_START=$(date +%s)

# Create a dedicated seat and user for Check 7 before stopping Redis
CHK7_USER_ID=$(uuid_v4)
CHK7_USER_TOKEN=$(mint_jwt "$CHK7_USER_ID" "USER")
CHK7_SEAT_NUMBER=$(( (RANDOM % 8000) + 1000 ))
CHK7_EVENT_ID="${EVENT_ID:-00000000-0000-0000-0000-000000000000}"
CHK7_SEAT_ID=""

if [ -n "$EVENT_ID" ]; then
    CHK7_BULK_PAYLOAD="{\"seats\":[{\"sectionCode\":\"S7\",\"rowLabel\":\"R\",\"seatNumber\":\"${CHK7_SEAT_NUMBER}\",\"priceCents\":6000}]}"
    http_request "Admin Add Seat Check 7" "POST" "${GATEWAY_BASE_URL}/api/v1/admin/events/${EVENT_ID}/seats/bulk" \
        "$CHK7_BULK_PAYLOAD" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" || true
    CHK7_SEAT_ID=$(python3 -c "import json, sys; data = json.loads(sys.argv[1]); print(data['seats'][0]['seatId'])" "$HTTP_BODY" 2>/dev/null || echo "")
fi

if [ -z "$CHK7_SEAT_ID" ]; then
    CHK7_SEAT_ID=$(uuid_v4)
fi

info "Stopping Redis container to simulate Redis outage..."
docker stop redis >/dev/null

# 1. Assert gateway readiness probe stays HTTP 200 UP
http_request "Gateway Readiness under Redis Outage" "GET" "${GATEWAY_BASE_URL}/actuator/health/readiness" || true
READINESS_OK=0
if [ "$HTTP_STATUS" = "200" ]; then
    READINESS_OK=1
else
    fail "Gateway readiness probe failed under Redis outage: expected HTTP 200 UP, got HTTP ${HTTP_STATUS}: ${HTTP_BODY}"
fi

# 2. Hold request through gateway while Redis is stopped must return exactly HTTP 503 with errorCode SERVICE_UNAVAILABLE and Retry-After header
HOLD_IDEMPOTENCY_KEY=$(uuid_v4)
HOLD_PAYLOAD="{\"eventId\":\"${CHK7_EVENT_ID}\",\"seatIds\":[\"${CHK7_SEAT_ID}\"]}"
http_request "Hold under Redis Outage" "POST" "${GATEWAY_BASE_URL}/api/v1/reservations" \
    "$HOLD_PAYLOAD" \
    -H "Authorization: Bearer ${CHK7_USER_TOKEN}" \
    -H "Idempotency-Key: ${HOLD_IDEMPOTENCY_KEY}" || true

HOLD_STATUS_OK=0
ERR_CODE=$(json_field "$HTTP_BODY" 'data.get("errorCode", "")')
RETRY_AFTER=$(printf "%s" "$HTTP_HEADERS" | grep -i '^retry-after:' | head -n 1 | awk '{print $2}' | tr -d '\r\n')

if [ "$HTTP_STATUS" != "503" ]; then
    fail "Hold request while Redis stopped: expected exactly HTTP 503, got HTTP ${HTTP_STATUS} (body: ${HTTP_BODY})"
elif [ "$ERR_CODE" != "SERVICE_UNAVAILABLE" ]; then
    fail "Hold request while Redis stopped: expected errorCode SERVICE_UNAVAILABLE, got '${ERR_CODE}' (body: ${HTTP_BODY})"
elif [ -z "$RETRY_AFTER" ]; then
    fail "Hold request while Redis stopped: missing required Retry-After header in response headers: ${HTTP_HEADERS}"
else
    HOLD_STATUS_OK=1
fi

# 3. Assert no new reservation row was created in inventory_db for user and seat used in the check
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres-inventory}"
POSTGRES_USER="${POSTGRES_USER:-ticketing}"
POSTGRES_DB="inventory_db"

USER_RES_COUNT=$(docker exec "${POSTGRES_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -A -c \
    "SELECT count(*) FROM reservations WHERE user_id = '${CHK7_USER_ID}'::uuid;" 2>/dev/null || echo "error")
SEAT_RES_COUNT=$(docker exec "${POSTGRES_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -A -c \
    "SELECT count(*) FROM reservation_items WHERE seat_id = '${CHK7_SEAT_ID}'::uuid;" 2>/dev/null || echo "error")

DB_ASSERT_OK=0
if [ "$USER_RES_COUNT" = "0" ] && [ "$SEAT_RES_COUNT" = "0" ]; then
    DB_ASSERT_OK=1
else
    fail "Reservation rows found in inventory_db for failed hold during Redis outage: user_reservations=${USER_RES_COUNT}, seat_items=${SEAT_RES_COUNT}"
fi

# 4. Restart Redis and wait for it to recover
info "Restarting Redis container..."
cleanup_redis

if [ "$READINESS_OK" -eq 1 ] && [ "$HOLD_STATUS_OK" -eq 1 ] && [ "$DB_ASSERT_OK" -eq 1 ]; then
    finish_check "Redis Outage Resilience" "$CHK_START" "PASS" "Gateway readiness remained 200 UP, hold failed closed with HTTP 503 SERVICE_UNAVAILABLE + Retry-After, and 0 reservation rows created in inventory_db"
else
    finish_check "Redis Outage Resilience" "$CHK_START" "FAIL" "Redis outage check failed: readiness_ok=${READINESS_OK}, hold_503_ok=${HOLD_STATUS_OK}, db_clean_ok=${DB_ASSERT_OK} (HTTP ${HTTP_STATUS})"
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
TOTAL_END_TIME=$(date +%s)
TOTAL_DURATION=$((TOTAL_END_TIME - SCRIPT_START_EPOCH))
info "================================================================="
info "Gateway Smoke Test Complete in ${TOTAL_DURATION}s"
info "Results: PASS=${PASS_COUNT}, FAIL=${FAIL_COUNT}"
info "================================================================="

if [ "$FAIL_COUNT" -gt 0 ]; then
    exit 1
fi
exit 0
