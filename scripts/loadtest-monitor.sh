#!/usr/bin/env bash
# =============================================================================
# scripts/loadtest-monitor.sh — Background Docker resource & health monitor
#
# Subcommands:
#   snapshot-start <results_dir>  Capture container restart/OOM baseline & DLT offsets
#   start          <results_dir>  Start background 5s docker stats CSV collector
#   stop           <results_dir>  Stop background collector
#   snapshot-end   <results_dir>  Stop collector, capture end state, flag restarts/OOMs
# =============================================================================
set -euo pipefail

CMD="${1:-}"
RESULTS_DIR="${2:-}"
NETWORK_NAME="${DOCKER_NETWORK:-ticketing-net}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"

if [ -z "$CMD" ] || [ -z "$RESULTS_DIR" ]; then
    echo "Usage: $0 <snapshot-start|start|stop|snapshot-end> <results_dir>" >&2
    exit 1
fi

mkdir -p "$RESULTS_DIR"

capture_containers_csv() {
    local outfile="$1"
    echo "container,restart_count,oom_killed,mem_limit_bytes,status" > "$outfile"
    local containers
    containers=$(docker ps -a --filter "network=${NETWORK_NAME}" --format '{{.Names}}' | sort)
    if [ -z "$containers" ]; then
        return 0
    fi
    while IFS= read -r cname; do
        [ -z "$cname" ] && continue
        # Skip transient k6 or kafka-init containers
        case "$cname" in
            kafka-init|k6-*) continue ;;
        esac
        local line
        line=$(docker inspect --format '{{.Name}},{{.RestartCount}},{{.State.OOMKilled}},{{.HostConfig.Memory}},{{.State.Status}}' "$cname" 2>/dev/null || true)
        if [ -n "$line" ]; then
            echo "${line#/}" >> "$outfile"
        fi
    done <<< "$containers"
}

capture_dlt_offsets() {
    local outfile="$1"
    : > "$outfile"
    local dlt_topics
    dlt_topics=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:9092 --list 2>/dev/null | grep -- '-dlt$' | sort || true)
    if [ -z "$dlt_topics" ]; then
        return 0
    fi
    while IFS= read -r topic; do
        [ -z "$topic" ] && continue
        docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
            --bootstrap-server kafka:9092 --topic "$topic" --time -1 2>/dev/null | sort >> "$outfile"
    done <<< "$dlt_topics"
}

stop_collector() {
    local pid_file="${RESULTS_DIR}/monitor.pid"
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file" 2>/dev/null || true)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            for _ in $(seq 1 10); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.2
            done
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$pid_file"
    fi
}

case "$CMD" in
    snapshot-start)
        capture_containers_csv "${RESULTS_DIR}/containers-start.csv"
        capture_dlt_offsets "${RESULTS_DIR}/dlt-before.txt"
        echo "[monitor] Captured start container snapshot and DLT baseline in ${RESULTS_DIR}"
        ;;

    start)
        stop_collector
        STATS_CSV="${RESULTS_DIR}/docker-stats.csv"
        if [ ! -f "$STATS_CSV" ]; then
            echo "timestamp_utc,container,cpu_pct,mem_usage,mem_limit,mem_pct" > "$STATS_CSV"
        fi
        (
            trap 'exit 0' TERM INT
            while true; do
                ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
                raw=$(docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}' 2>/dev/null || true)
                if [ -n "$raw" ]; then
                    while IFS= read -r row; do
                        [ -z "$row" ] && continue
                        # Format: name,12.34%,123.4MiB / 512MiB,24.10%
                        cname="${row%%,*}"
                        rest="${row#*,}"
                        cpu_pct="${rest%%,*}"
                        rest="${rest#*,}"
                        mem_field="${rest%%,*}"
                        mem_pct="${rest#*,}"
                        mem_usage="${mem_field%% / *}"
                        mem_limit="${mem_field#* / }"
                        printf '%s,%s,%s,%s,%s,%s\n' \
                            "$ts" "$cname" "${cpu_pct%\%}" "$mem_usage" "$mem_limit" "${mem_pct%\%}" >> "$STATS_CSV"
                    done <<< "$raw"
                fi
                sleep 5
            done
        ) >/dev/null 2>&1 &
        echo $! > "${RESULTS_DIR}/monitor.pid"
        echo "[monitor] Started 5s docker stats collector (PID $(cat "${RESULTS_DIR}/monitor.pid")) -> ${STATS_CSV}"
        ;;

    stop)
        stop_collector
        echo "[monitor] Stopped docker stats collector"
        ;;

    snapshot-end)
        stop_collector
        capture_containers_csv "${RESULTS_DIR}/containers-end.csv"
        echo "[monitor] Captured end container snapshot in ${RESULTS_DIR}/containers-end.csv"
        # Prominently report any restart or OOM kill
        python3 - "${RESULTS_DIR}/containers-start.csv" "${RESULTS_DIR}/containers-end.csv" <<'PYEOF'
import csv, sys

start_path, end_path = sys.argv[1], sys.argv[2]
start = {}
try:
    with open(start_path, newline="") as f:
        for row in csv.DictReader(f):
            start[row["container"]] = row
except FileNotFoundError:
    pass

issues = []
try:
    with open(end_path, newline="") as f:
        for row in csv.DictReader(f):
            c = row["container"]
            r_end = int(row.get("restart_count", "0") or 0)
            r_start = int(start.get(c, {}).get("restart_count", "0") or 0)
            oom = str(row.get("oom_killed", "false")).lower() == "true"
            if r_end > r_start:
                issues.append(f"RESTART: container '{c}' restarted during run ({r_start} -> {r_end})")
            if oom:
                issues.append(f"OOM_KILLED: container '{c}' has OOMKilled=true")
except FileNotFoundError:
    pass

if issues:
    print("==========================================================================")
    print("[ALERT] CONTAINER RESTART OR OOM KILL DETECTED DURING LOAD TEST!")
    for item in issues:
        print(f"  - {item}")
    print("==========================================================================")
else:
    print("[monitor] Clean container health: 0 restarts and 0 OOM kills detected.")
PYEOF
        ;;

    *)
        echo "Unknown subcommand: $CMD" >&2
        exit 1
        ;;
esac
