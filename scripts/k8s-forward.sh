#!/usr/bin/env bash
# =============================================================================
# scripts/k8s-forward.sh — Background supervised kubectl port-forward manager
# Subcommands: start | stop | status
# Forwards the exact host ports expected by E2E and smoke tools; refuses if any
# port is already in use; auto-restarts dropped forwards up to MAX_RETRIES.
# =============================================================================
set -euo pipefail

CMD="${1:-}"
NAMESPACE="${K8S_NAMESPACE:-ticketing}"
CLUSTER_NAME="${K3D_CLUSTER_NAME:-ticketing}"
STATE_DIR="${K8S_FORWARD_STATE_DIR:-/tmp/ticketing-k8s-forward}"
MAX_RETRIES="${K8S_FORWARD_MAX_RETRIES:-10}"

FORWARDS=(
    "svc/gateway-service:8080:8080"
    "svc/inventory-service:8081:8081"
    "svc/order-service:8082:8082"
    "svc/payment-service:8083:8083"
    "svc/notification-service:8084:8084"
    "svc/postgres-inventory:5433:5432"
    "svc/postgres-order:5434:5432"
    "svc/postgres-payment:5435:5432"
    "svc/redis:6379:6379"
    "svc/kafka:29092:29092"
)

check_ports_free() {
    local conflicts=()
    for spec in "${FORWARDS[@]}"; do
        IFS=':' read -r _target local_port _remote_port <<< "$spec"
        if command -v lsof >/dev/null 2>&1; then
            local owner
            owner=$(lsof -nP -iTCP:"$local_port" -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $1 "(pid=" $2 ",user=" $3 ")"}' | paste -sd ',' - || true)
            if [ -n "$owner" ]; then
                conflicts+=("port ${local_port} -> ${owner}")
            fi
        fi
    done

    if [ "${#conflicts[@]}" -gt 0 ]; then
        echo "[ERROR] Cannot start k8s port-forwarding; required host port(s) are already in use:" >&2
        printf '  - %s\n' "${conflicts[@]}" >&2
        echo "[ERROR] Stop the owning process(es) (e.g. 'make k8s-forward-stop' or 'make stack-down && make obs-down') and retry." >&2
        exit 1
    fi
}

cmd_stop() {
    if [ ! -d "$STATE_DIR" ]; then
        echo "[k8s-forward] No active port-forward state found (${STATE_DIR})."
        return 0
    fi

    touch "${STATE_DIR}/STOP"
    if [ -f "${STATE_DIR}/pids" ]; then
        while IFS= read -r pid; do
            [ -n "$pid" ] || continue
            # Kill child kubectl port-forward processes first, then supervisor
            pkill -TERM -P "$pid" >/dev/null 2>&1 || true
            kill -TERM "$pid" >/dev/null 2>&1 || true
        done < "${STATE_DIR}/pids"
        sleep 1
        while IFS= read -r pid; do
            [ -n "$pid" ] || continue
            pkill -KILL -P "$pid" >/dev/null 2>&1 || true
            kill -KILL "$pid" >/dev/null 2>&1 || true
        done < "${STATE_DIR}/pids"
    fi
    rm -rf "$STATE_DIR"
    echo "[k8s-forward] Stopped all port-forwards cleanly."
}

cmd_start() {
    kubectl config use-context "k3d-${CLUSTER_NAME}" >/dev/null
    check_ports_free

    rm -rf "$STATE_DIR"
    mkdir -p "$STATE_DIR"
    : > "${STATE_DIR}/pids"

    echo "[k8s-forward] Starting ${#FORWARDS[@]} supervised port-forwards (namespace=${NAMESPACE}, max_retries=${MAX_RETRIES})..."
    for spec in "${FORWARDS[@]}"; do
        IFS=':' read -r target local_port remote_port <<< "$spec"
        safe_name="${target//\//_}_${local_port}"
        log_file="${STATE_DIR}/${safe_name}.log"

        (
            retries=0
            while [ ! -f "${STATE_DIR}/STOP" ]; do
                echo "[$(date -u +%H:%M:%SZ)] Starting kubectl port-forward ${target} ${local_port}:${remote_port} (attempt $((retries + 1)))" >> "$log_file"
                kubectl -n "$NAMESPACE" port-forward --address 127.0.0.1 "$target" "${local_port}:${remote_port}" >> "$log_file" 2>&1 &
                child_pid=$!
                wait "$child_pid" || true
                if [ -f "${STATE_DIR}/STOP" ]; then
                    break
                fi
                retries=$((retries + 1))
                if [ "$retries" -ge "$MAX_RETRIES" ]; then
                    echo "[$(date -u +%H:%M:%SZ)] Reached max retries (${MAX_RETRIES}) for ${target} ${local_port}:${remote_port}; stopping." >> "$log_file"
                    break
                fi
                sleep 1
            done
        ) >/dev/null 2>&1 &
        sup_pid=$!
        echo "$sup_pid" >> "${STATE_DIR}/pids"
    done

    # Wait up to 20s for every forwarded port to accept TCP connections
    local deadline=$(( $(date +%s) + 20 ))
    while true; do
        local not_ready=()
        for spec in "${FORWARDS[@]}"; do
            IFS=':' read -r target local_port _remote <<< "$spec"
            if ! nc -z -w 1 127.0.0.1 "$local_port" >/dev/null 2>&1; then
                not_ready+=("${target}:${local_port}")
            fi
        done
        if [ "${#not_ready[@]}" -eq 0 ]; then
            echo "[k8s-forward] All ${#FORWARDS[@]} port-forwards are active:"
            for spec in "${FORWARDS[@]}"; do
                IFS=':' read -r target local_port remote_port <<< "$spec"
                echo "  127.0.0.1:${local_port} -> ${NAMESPACE}/${target}:${remote_port}"
            done
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "[ERROR] Timed out waiting for port-forwards: ${not_ready[*]}" >&2
            for lf in "${STATE_DIR}"/*.log; do
                echo "--- $(basename "$lf") ---" >&2
                tail -n 10 "$lf" >&2 || true
            done
            cmd_stop
            exit 1
        fi
        sleep 1
    done
}

cmd_status() {
    if [ ! -f "${STATE_DIR}/pids" ]; then
        echo "[k8s-forward] No active port-forwards."
        return 0
    fi
    local alive=0 total=0
    while IFS= read -r pid; do
        [ -n "$pid" ] || continue
        total=$((total + 1))
        if kill -0 "$pid" >/dev/null 2>&1; then
            alive=$((alive + 1))
        fi
    done < "${STATE_DIR}/pids"
    echo "[k8s-forward] Supervisors running: ${alive}/${total} (state dir: ${STATE_DIR})"
    for spec in "${FORWARDS[@]}"; do
        IFS=':' read -r target local_port remote_port <<< "$spec"
        if nc -z -w 1 127.0.0.1 "$local_port" >/dev/null 2>&1; then
            echo "  [UP]   127.0.0.1:${local_port} -> ${target}:${remote_port}"
        else
            echo "  [DOWN] 127.0.0.1:${local_port} -> ${target}:${remote_port}"
        fi
    done
}

case "$CMD" in
    start)  cmd_start ;;
    stop)   cmd_stop ;;
    status) cmd_status ;;
    *)
        echo "Usage: $0 {start|stop|status}" >&2
        exit 1
        ;;
esac
