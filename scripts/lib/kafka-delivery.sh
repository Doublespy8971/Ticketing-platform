#!/usr/bin/env bash
# ==============================================================================
# scripts/lib/kafka-delivery.sh
# Shared helper for robust, outbox-safe Kafka delivery verification.
#
# Verifies that this producer's event(s) reached the topic exactly once,
# independent of concurrent writers (e.g. outbox publishers on the same topic).
# ==============================================================================

# Helper for timed docker exec; delegates to caller's docker_exec_timed if present
_kd_docker_exec() {
    local check_name="$1"
    shift
    if type docker_exec_timed >/dev/null 2>&1; then
        docker_exec_timed "$check_name" "$@"
    else
        docker exec "$@"
    fi
}

# Helper for timed docker exec with stdin; delegates to caller's docker_exec_stdin_timed if present
_kd_docker_exec_stdin() {
    local check_name="$1"
    local payload="$2"
    shift 2
    if type docker_exec_stdin_timed >/dev/null 2>&1; then
        docker_exec_stdin_timed "$check_name" "$payload" "$@"
    else
        local tmp_in
        tmp_in=$(mktemp "${TMPDIR:-/tmp}/kd_stdin.XXXXXX") || return 1
        printf "%s\n" "$payload" >"$tmp_in"
        local out
        out=$(docker exec -i "$@" <"$tmp_in")
        local rc=$?
        rm -f "$tmp_in"
        printf "%s" "$out"
        return "$rc"
    fi
}

# Failure reporting; delegates to caller's fail() function if defined
_kd_fail() {
    local check_name="$1"
    local msg="$2"
    if type fail >/dev/null 2>&1; then
        fail "${check_name}: ${msg}"
    else
        echo "[FAIL] ${check_name}: ${msg}" >&2
    fi
}

# Reads end offsets for all partitions of a topic
_kd_get_partition_offsets() {
    local check_name="$1"
    local container="$2"
    local topic="$3"
    local out
    out=$(_kd_docker_exec "$check_name (GetOffsetShell $topic)" "$container" sh -c "
        if [ -x /opt/kafka/bin/kafka-get-offsets.sh ]; then
            /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic '${topic}' --time -1 2>/dev/null
        elif [ -x /opt/kafka/bin/kafka-run-class.sh ]; then
            /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic '${topic}' --time -1 2>/dev/null
        fi
    " 2>/dev/null)
    printf "%s" "$out"
}

# Backward compatibility: reads total end offset across all partitions
get_topic_total_end_offset() {
    local check_name="$1"
    local topic="$2"
    local container="${KAFKA_CONTAINER:-kafka}"
    local out
    out=$(_kd_get_partition_offsets "$check_name" "$container" "$topic") || return 1
    if [ -z "$out" ]; then
        return 1
    fi
    printf "%s\n" "$out" | awk -F: '{sum += $NF} END {print sum+0}'
}

# Main producer + delivery verification function
produce_and_verify_kafka_delivery() {
    local check_name="$1"
    local topic="$2"
    local key="$3"
    local json_line="$4"
    local container="${KAFKA_CONTAINER:-kafka}"

    # Extract eventType for diagnostics
    local event_type
    event_type=$(python3 -c "
import json, sys
try:
    data = json.loads(sys.stdin.read().strip())
    print(data.get('eventType', 'unknown'))
except Exception:
    print('unknown')
" <<< "$json_line" 2>/dev/null || echo "unknown")

    # 1. Read end offsets of all partitions before producing
    local before_raw
    before_raw=$(_kd_get_partition_offsets "$check_name" "$container" "$topic")
    if [ -z "$before_raw" ]; then
        _kd_fail "$check_name" "failed to read end offsets before producing (topic=${topic}, key=${key}, eventType=${event_type})"
        return 1
    fi

    # 2. Produce the record(s) as today (stdin must be forwarded)
    local payload
    if [ -n "$key" ]; then
        payload="${key}:${json_line}"
    else
        payload="${json_line}"
    fi

    if [ "${TEST_SIMULATE_NO_PRODUCE:-0}" != "1" ]; then
        if ! _kd_docker_exec_stdin "$check_name" "$payload" "$container" \
            /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server kafka:9092 \
            --topic "$topic" \
            --property "parse.key=true" \
            --property "key.separator=:" >/dev/null; then
            _kd_fail "$check_name" "kafka-console-producer failed (topic=${topic}, key=${key}, eventType=${event_type})"
            return 1
        fi
    fi

    # 3. Read end offsets after producing (wait loop up to 10s for broker to reflect new offsets)
    local start_ts=$(date +%s)
    local timeout_secs="${KAFKA_DELIVERY_TIMEOUT_SECS:-10}"
    local after_raw=""
    local delta=0
    local grew_info=""

    while true; do
        after_raw=$(_kd_get_partition_offsets "$check_name" "$container" "$topic")
        local parse_res
        parse_res=$(python3 -c "
import sys

def parse_offsets(s):
    res = {}
    for line in s.strip().splitlines():
        parts = line.strip().split(':')
        if len(parts) >= 3:
            try:
                res[int(parts[-2])] = int(parts[-1])
            except ValueError:
                pass
    return res

b = parse_offsets(sys.argv[1])
a = parse_offsets(sys.argv[2])

tot_b = sum(b.values())
tot_a = sum(a.values())
delta = tot_a - tot_b

grew = []
for p in sorted(a.keys()):
    diff = a[p] - b.get(p, 0)
    if diff > 0:
        grew.append(f'{p}:{b.get(p, 0)}:{diff}')

print(f'{delta}|{\" \".join(grew)}')
" "$before_raw" "$after_raw" 2>/dev/null || echo "0|")

        delta="${parse_res%%|*}"
        grew_info="${parse_res#*|}"

        if [ "${delta:-0}" -ge 1 ]; then
            break
        fi

        local now_ts=$(date +%s)
        if [ $((now_ts - start_ts)) -ge "$timeout_secs" ]; then
            break
        fi
        sleep 0.2
    done

    # Failure condition: broker grew by zero within 10 seconds
    if [ "${delta:-0}" -eq 0 ]; then
        _kd_fail "$check_name" "broker grew by zero on all partitions within ${timeout_secs} seconds of producing (topic=${topic}, key=${key}, eventType=${event_type}): stdin may not have been forwarded"
        return 1
    fi

    # Failure condition: delta smaller than number of events produced
    local expected_count
    expected_count=$(python3 -c "
import sys
count = 0
for line in sys.stdin.read().strip().splitlines():
    if line.strip():
        count += 1
print(max(1, count))
" <<< "$json_line" 2>/dev/null || echo "1")

    if [ "${delta:-0}" -lt "${expected_count:-1}" ]; then
        _kd_fail "$check_name" "delta smaller than number of events produced (topic=${topic}, key=${key}, eventType=${event_type}, expected at least ${expected_count}, got delta=${delta})"
        return 1
    fi

    # 4. For every partition whose end offset grew, read range [before, after)
    local all_consumed=""
    for item in $grew_info; do
        local part="${item%%:*}"
        local rest="${item#*:}"
        local b_off="${rest%%:*}"
        local cnt="${rest#*:}"

        local range_out
        range_out=$(_kd_docker_exec "$check_name (consume range p$part)" "$container" \
            /opt/kafka/bin/kafka-console-consumer.sh \
            --bootstrap-server kafka:9092 \
            --topic "$topic" \
            --partition "$part" \
            --offset "$b_off" \
            --max-messages "$cnt" \
            --timeout-ms 8000 \
            --property print.key=true 2>/dev/null)
        all_consumed="${all_consumed}${range_out}"$'\n'
    done

    # 5. Look for each produced eventId (exact match on '\"eventId\":\"<id>\"')
    local tmp_payload
    tmp_payload=$(mktemp "${TMPDIR:-/tmp}/kd_payload.XXXXXX") || return 1
    local tmp_consumed
    tmp_consumed=$(mktemp "${TMPDIR:-/tmp}/kd_consumed.XXXXXX") || { rm -f "$tmp_payload"; return 1; }

    printf "%s\n" "$json_line" >"$tmp_payload"
    printf "%s\n" "$all_consumed" >"$tmp_consumed"

    local verify_err
    verify_err=$(python3 -c "
import sys, json, re

topic = sys.argv[1]
key = sys.argv[2]
event_type = sys.argv[3]
before_raw = sys.argv[4]
after_raw = sys.argv[5]
payload_file = sys.argv[6]
consumed_file = sys.argv[7]

with open(payload_file, 'r', encoding='utf-8') as f:
    payload_content = f.read()

with open(consumed_file, 'r', encoding='utf-8') as f:
    consumed_content = f.read()

# Extract produced eventIds
produced_event_ids = []
for line in payload_content.strip().splitlines():
    line = line.strip()
    if not line:
        continue
    if ':' in line and not line.startswith('{'):
        line = line.split(':', 1)[1].strip()
    try:
        obj = json.loads(line)
        if isinstance(obj, dict) and 'eventId' in obj:
            produced_event_ids.append(str(obj['eventId']))
    except Exception:
        pass

if not produced_event_ids:
    # Regex fallback for json-like payloads
    matches = re.findall(r'\"eventId\"\s*:\s*\"([^\"]+)\"', payload_content)
    if matches:
        produced_event_ids = matches

def parse_offsets(s):
    res = {}
    for line in s.strip().splitlines():
        parts = line.strip().split(':')
        if len(parts) >= 3:
            try:
                res[int(parts[-2])] = int(parts[-1])
            except ValueError:
                pass
    return res

before_map = parse_offsets(before_raw)
after_map = parse_offsets(after_raw)

missing_event_ids = []
duplicate_event_ids = []

if produced_event_ids:
    for eid in produced_event_ids:
        pattern = re.compile(rf'\"eventId\"\s*:\s*\"{re.escape(eid)}\"')
        matches = pattern.findall(consumed_content)
        count = len(matches)
        if count == 0:
            missing_event_ids.append(eid)
        elif count > 1:
            duplicate_event_ids.append(f'{eid} (count={count})')
else:
    target = key if key else payload_content.strip()
    count = consumed_content.count(target)
    if count == 0:
        missing_event_ids.append(target)
    elif count > 1:
        duplicate_event_ids.append(f'{target} (count={count})')

if not missing_event_ids and not duplicate_event_ids:
    sys.exit(0)

part_summary = []
all_parts = sorted(set(list(before_map.keys()) + list(after_map.keys())))
for p in all_parts:
    b = before_map.get(p, 0)
    a = after_map.get(p, 0)
    part_summary.append(f'p{p}: before={b} after={a}')
part_str = ', '.join(part_summary)

preview = consumed_content[:300].replace('\n', '\\n')

msg = (
    f'Kafka delivery verification failed: topic={topic}, key={key}, eventType={event_type}, '
    f'missing_event_ids=[{\", \".join(missing_event_ids)}], '
    f'duplicate_event_ids=[{\", \".join(duplicate_event_ids)}], '
    f'per_partition_offsets=[{part_str}], '
    f'range_preview=\'{preview}\''
)
print(msg)
sys.exit(1)
" "$topic" "$key" "$event_type" "$before_raw" "$after_raw" "$tmp_payload" "$tmp_consumed")
    local verify_rc=$?

    rm -f "$tmp_payload" "$tmp_consumed"

    if [ "$verify_rc" -ne 0 ]; then
        _kd_fail "$check_name" "$verify_err"
        return 1
    fi

    return 0
}

# Thin wrapper default
produce_kafka_event() {
    produce_and_verify_kafka_delivery "$@"
}
