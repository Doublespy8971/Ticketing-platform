#!/usr/bin/env bash
# =============================================================================
# scripts/loadtest-run.sh — Orchestrator for k6 Docker runs, monitoring, audit, report
#
# Subcommands:
#   flash   Run Scenario A (flash-sale.js)
#   journey Run Scenario B (checkout-journey.js)
#   all     Run flash -> journey -> audit -> report in one timestamped results dir
#   audit   Run scripts/loadtest-audit.sh against RESULTS_DIR (or latest)
#   report  Run scripts/loadtest-report.py against RESULTS_DIR (or latest)
# =============================================================================
set -euo pipefail

CMD="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

K6_IMAGE="${K6_IMAGE:-grafana/k6:0.54.0@sha256:1f40432b1cbe7234e977f96c362c9bc550a2d2b583d014dd8669fe40d3e9e755}"
DOCKER_NETWORK="${DOCKER_NETWORK:-ticketing-net}"
BASE_URL="${BASE_URL:-http://gateway-service:8080}"
PROM_RW_URL="${K6_PROMETHEUS_RW_SERVER_URL:-http://prometheus:9090/api/v1/write}"
LOAD_SCALE="${LOAD_SCALE:-1.0}"
LOAD_PUBLISH="${LOAD_PUBLISH:-false}"

# Read JWT_SECRET silently from infra/.env without ever printing or writing it
load_jwt_secret() {
    if [ -n "${JWT_SECRET:-}" ]; then
        export JWT_SECRET
        return 0
    fi
    local env_file="${REPO_ROOT}/infra/.env"
    if [ ! -f "$env_file" ]; then
        cp "${REPO_ROOT}/infra/.env.example" "$env_file"
    fi
    JWT_SECRET=$(grep -E '^JWT_SECRET=' "$env_file" | head -n 1 | cut -d= -f2- | tr -d '\r' | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
    if [ -z "$JWT_SECRET" ]; then
        echo "[ERROR] JWT_SECRET not found in infra/.env" >&2
        exit 1
    fi
    export JWT_SECRET
}

latest_results_dir() {
    local latest
    latest=$(find "${REPO_ROOT}/loadtest/results" -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1)
    if [ -z "$latest" ]; then
        echo "[ERROR] No existing results directory found under loadtest/results/" >&2
        exit 1
    fi
    echo "$latest"
}

init_results_dir() {
    if [ -n "${RESULTS_DIR:-}" ]; then
        mkdir -p "$RESULTS_DIR"
        echo "$RESULTS_DIR"
    else
        local ts
        ts=$(date -u +"%Y%m%dT%H%M%SZ")
        local dir="${REPO_ROOT}/loadtest/results/${ts}"
        mkdir -p "$dir"
        echo "$dir"
    fi
}

write_run_meta() {
    local rdir="$1" run_id="$2" start_epoch="$3" end_epoch="$4" scenarios="$5"
    python3 - "$rdir" "$run_id" "$start_epoch" "$end_epoch" "$scenarios" "$LOAD_SCALE" "$K6_IMAGE" <<'PYEOF'
import json, os, sys
rdir, run_id, start_e, end_e, scenarios, scale, k6_image = sys.argv[1:]
meta = {
    "run_id": run_id,
    "start_epoch": int(start_e),
    "end_epoch": int(end_e),
    "scenarios": [s for s in scenarios.split(",") if s],
    "load_scale": float(scale),
    "k6_image": k6_image,
}
with open(os.path.join(rdir, "run-meta.json"), "w") as f:
    json.dump(meta, f, indent=2)
PYEOF
}

run_k6_scenario() {
    local script_name="$1"
    local rdir="$2"
    local run_id="$3"
    local summary_file="$4"
    local extra_env_args=()

    if [ -n "${REUSE_EVENT_ID:-}" ]; then
        extra_env_args+=("-e" "REUSE_EVENT_ID=${REUSE_EVENT_ID}")
    fi

    # Forward optional tuning env vars if set in caller environment
    for var in USER_POOL_SIZE SEATS_PER_EVENT \
               FLASH_RAMP1_SEC FLASH_STAGE1_RATE FLASH_STAGE1_SEC \
               FLASH_RAMP2_SEC FLASH_STAGE2_RATE FLASH_STAGE2_SEC FLASH_RAMPDOWN_SEC \
               FLASH_PRE_ALLOCATED_VUS FLASH_MAX_VUS \
               JOURNEY_RATE JOURNEY_DURATION_SEC JOURNEY_PRE_ALLOCATED_VUS JOURNEY_MAX_VUS; do
        if [ -n "${!var:-}" ]; then
            extra_env_args+=("-e" "${var}=${!var}")
        fi
    done

    local log_file="${rdir}/${script_name%.js}.log"
    echo "=== Running k6 scenario: ${script_name} (LOAD_SCALE=${LOAD_SCALE}, run_id=${run_id}) ==="

    # Pass JWT_SECRET via -e JWT_SECRET (reads from exported env without exposing value in argv)
    local rc=0
    docker run --rm \
        --name "k6-${script_name%.js}-${run_id}" \
        --network "${DOCKER_NETWORK}" \
        --user "$(id -u):$(id -g)" \
        -v "${REPO_ROOT}/loadtest:/loadtest:ro" \
        -v "${rdir}:/results:rw" \
        -e "BASE_URL=${BASE_URL}" \
        -e JWT_SECRET \
        -e "LOAD_SCALE=${LOAD_SCALE}" \
        -e "LOADTEST_RUN_ID=${run_id}" \
        -e "SUMMARY_PATH=/results/${summary_file}" \
        -e "K6_PROMETHEUS_RW_SERVER_URL=${PROM_RW_URL}" \
        -e "K6_PROMETHEUS_RW_TREND_STATS=p(50),p(95),p(99),avg,max" \
        ${extra_env_args[@]+"${extra_env_args[@]}"} \
        "${K6_IMAGE}" run \
        -o experimental-prometheus-rw \
        "/loadtest/${script_name}" 2>&1 | tee "$log_file" || rc=${PIPESTATUS[0]}
    return "$rc"
}

case "$CMD" in
    flash)
        load_jwt_secret
        RDIR=$(init_results_dir)
        RUN_ID=$(basename "$RDIR")
        START_EPOCH=$(date +%s)
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM
        set +e
        run_k6_scenario "flash-sale.js" "$RDIR" "$RUN_ID" "flash-sale-summary.json"
        K6_RC=$?
        set -e
        trap - EXIT INT TERM
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR"
        END_EPOCH=$(date +%s)
        write_run_meta "$RDIR" "$RUN_ID" "$START_EPOCH" "$END_EPOCH" "flash-sale"
        echo "[load-flash] Results written to ${RDIR} (k6 exit=${K6_RC})"
        exit "$K6_RC"
        ;;

    journey)
        load_jwt_secret
        RDIR=$(init_results_dir)
        RUN_ID=$(basename "$RDIR")
        START_EPOCH=$(date +%s)
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM
        set +e
        run_k6_scenario "checkout-journey.js" "$RDIR" "$RUN_ID" "checkout-journey-summary.json"
        K6_RC=$?
        set -e
        trap - EXIT INT TERM
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR"
        END_EPOCH=$(date +%s)
        write_run_meta "$RDIR" "$RUN_ID" "$START_EPOCH" "$END_EPOCH" "checkout-journey"
        echo "[load-journey] Results written to ${RDIR} (k6 exit=${K6_RC})"
        exit "$K6_RC"
        ;;

    all)
        load_jwt_secret
        RDIR=$(init_results_dir)
        RUN_ID=$(basename "$RDIR")
        START_EPOCH=$(date +%s)

        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM

        set +e
        run_k6_scenario "flash-sale.js" "$RDIR" "$RUN_ID" "flash-sale-summary.json"
        FLASH_RC=$?
        set -e

        # Reuse the second 5000-seat event created during flash-sale setup if available
        JOURNEY_EV=$(grep -Eo 'LOADTEST_JOURNEY_EVENT_ID=[0-9a-fA-F-]+' "${RDIR}/flash-sale.log" | head -n 1 | cut -d= -f2 || true)
        if [ -n "$JOURNEY_EV" ]; then
            export REUSE_EVENT_ID="$JOURNEY_EV"
        fi

        set +e
        run_k6_scenario "checkout-journey.js" "$RDIR" "$RUN_ID" "checkout-journey-summary.json"
        JOURNEY_RC=$?
        set -e
        unset REUSE_EVENT_ID

        trap - EXIT INT TERM
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR"
        END_EPOCH=$(date +%s)
        write_run_meta "$RDIR" "$RUN_ID" "$START_EPOCH" "$END_EPOCH" "flash-sale,checkout-journey"

        set +e
        bash "${SCRIPT_DIR}/loadtest-audit.sh" "$RDIR"
        AUDIT_RC=$?
        set -e

        PUBLISH_ARGS=()
        if [ "${LOAD_PUBLISH}" = "true" ]; then
            PUBLISH_ARGS+=("--publish")
        fi
        python3 "${SCRIPT_DIR}/loadtest-report.py" "$RDIR" ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}

        echo "=========================================================================="
        echo "Load Test Run Complete: ${RDIR}"
        echo "  flash-sale exit:       ${FLASH_RC}"
        echo "  checkout-journey exit: ${JOURNEY_RC}"
        echo "  audit exit:            ${AUDIT_RC}"
        echo "  report:                ${RDIR}/report.md"
        echo "=========================================================================="

        if [ "$FLASH_RC" -ne 0 ] || [ "$JOURNEY_RC" -ne 0 ] || [ "$AUDIT_RC" -ne 0 ]; then
            exit 1
        fi
        exit 0
        ;;

    audit)
        RDIR="${RESULTS_DIR:-$(latest_results_dir)}"
        bash "${SCRIPT_DIR}/loadtest-audit.sh" "$RDIR"
        ;;

    report)
        RDIR="${RESULTS_DIR:-$(latest_results_dir)}"
        PUBLISH_ARGS=()
        if [ "${LOAD_PUBLISH}" = "true" ]; then
            PUBLISH_ARGS+=("--publish")
        fi
        python3 "${SCRIPT_DIR}/loadtest-report.py" "$RDIR" ${PUBLISH_ARGS[@]+"${PUBLISH_ARGS[@]}"}
        ;;

    *)
        echo "Usage: $0 <flash|journey|all|audit|report>" >&2
        exit 1
        ;;
esac
