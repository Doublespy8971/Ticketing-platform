#!/usr/bin/env bash
# ==============================================================================
# scripts/test-kafka-delivery.sh
# Self-test script for scripts/lib/kafka-delivery.sh.
#
# Asserts:
# 1) Helper succeeds when a marker event is delivered while a background process
#    concurrently publishes unrelated records to the same topic.
# 2) Helper fails with a clear message ("stdin may not have been forwarded")
#    when nothing is produced and the broker grows by zero.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/kafka-delivery.sh"

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
TEST_TOPIC="order.events"

echo "=== Running Kafka Delivery Self-Test against ${KAFKA_CONTAINER} (topic=${TEST_TOPIC}) ==="

uuid_v4() {
    python3 -c "import uuid; print(str(uuid.uuid4()))"
}

# -----------------------------------------------------------------------------
# Case 1: Marker event delivered while second background loop produces noise
# -----------------------------------------------------------------------------
echo "[INFO] Case 1: Producing marker event while concurrent noise records are published..."

MARKER_ID=$(uuid_v4)
MARKER_PAYLOAD=$(printf '{"eventId":"%s","eventType":"test.selftest.marker","payload":{"tag":"valid"}}' "$MARKER_ID")

STOP_FILE=$(mktemp "${TMPDIR:-/tmp}/selftest_stop.XXXXXX")
rm -f "$STOP_FILE"

(
    for i in $(seq 1 15); do
        if [ -f "$STOP_FILE" ]; then
            break
        fi
        noise_id=$(uuid_v4)
        noise_json=$(printf '{"eventId":"%s","eventType":"test.selftest.noise","payload":{"tag":"noise"}}' "$noise_id")
        docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server kafka:9092 \
            --topic "$TEST_TOPIC" \
            --property "parse.key=true" \
            --property "key.separator=:" \
            <<< "noise-key:${noise_json}" >/dev/null 2>&1 || true
        sleep 0.1
    done
) &
BG_PID=$!

cleanup() {
    touch "$STOP_FILE"
    wait "$BG_PID" 2>/dev/null || true
    rm -f "$STOP_FILE"
}
trap cleanup EXIT

if ! produce_kafka_event "Case 1 (Concurrent Noise)" "$TEST_TOPIC" "marker-${MARKER_ID}" "$MARKER_PAYLOAD"; then
    echo "[FAIL] Case 1: produce_kafka_event failed during concurrent background traffic"
    exit 1
fi

touch "$STOP_FILE"
wait "$BG_PID" 2>/dev/null || true
rm -f "$STOP_FILE"
trap - EXIT

echo "[PASS] Case 1: Marker event successfully verified despite concurrent topic growth"

# -----------------------------------------------------------------------------
# Case 2: Nothing produced -> helper fails with "stdin may not have been forwarded"
# -----------------------------------------------------------------------------
echo "[INFO] Case 2: Simulating no records produced (asserting helper fails)..."

DUMMY_ID=$(uuid_v4)
DUMMY_PAYLOAD=$(printf '{"eventId":"%s","eventType":"test.selftest.noproduce","payload":{"tag":"none"}}' "$DUMMY_ID")

CASE2_OUT=$(mktemp "${TMPDIR:-/tmp}/case2_out.XXXXXX")
set +e
KAFKA_DELIVERY_TIMEOUT_SECS=2 TEST_SIMULATE_NO_PRODUCE=1 \
    produce_kafka_event "Case 2 (No Produce)" "$TEST_TOPIC" "dummy-key" "$DUMMY_PAYLOAD" >"$CASE2_OUT" 2>&1
CASE2_RC=$?
set -e

CASE2_LOG=$(cat "$CASE2_OUT")
rm -f "$CASE2_OUT"

if [ "$CASE2_RC" -ne 0 ] && echo "$CASE2_LOG" | grep -q "stdin may not have been forwarded"; then
    echo "[PASS] Case 2: Helper failed as expected when broker grew by zero with clear message"
else
    echo "[FAIL] Case 2: Expected failure with 'stdin may not have been forwarded', got rc=${CASE2_RC}, output=${CASE2_LOG}"
    exit 1
fi

echo "=================================================================="
echo "All Kafka delivery self-tests passed successfully!"
echo "=================================================================="
exit 0
