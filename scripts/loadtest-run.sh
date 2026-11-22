#!/usr/bin/env bash
# =============================================================================
# scripts/loadtest-run.sh — Orchestrator for k6 Docker runs, monitoring, audit, report
#
# Subcommands:
#   flash               Run Scenario A (flash-sale.js — overload profile)
#   journey             Run Scenario B (checkout-journey.js — overload profile)
#   all                 Run flash -> journey -> audit -> report in one results dir
#   breakpoint-hold     Run breakpoint-hold.js -> audit -> breakpoint.py -> report
#   breakpoint-journey  Run breakpoint-journey.js -> audit -> breakpoint.py -> report
#   breakpoint          Run both breakpoints (stack-reset between) & derive sustainable-profile.json
#   sustained           Run sustained-hold.js -> sustained-journey.js -> audit -> report
#   sustained-x3        Run 3 VALID sustained runs (max 6 attempts, stack-reset + obs-up between)
#   soak                Run stack-reset + obs-up -> soak.js -> audit -> soak-report.py -> report
#   audit               Run scripts/loadtest-audit.sh against RESULTS_DIR (or latest)
#   report              Run scripts/loadtest-report.py against RESULTS_DIR (or latest)
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

print_host_state() {
    local rdir="${1:-}"
    local power_src="unknown"
    local on_battery=0

    if command -v pmset >/dev/null 2>&1; then
        local batt_line
        batt_line=$(pmset -g batt 2>/dev/null | head -n 2 | tr '\n' ' ' || true)
        power_src="${batt_line:-unknown}"
        if echo "$batt_line" | grep -qi "Battery Power"; then
            on_battery=1
        fi
    elif [ -d /sys/class/power_supply ]; then
        local ac_online
        ac_online=$(cat /sys/class/power_supply/AC*/online /sys/class/power_supply/ADP*/online 2>/dev/null | head -n 1 || true)
        if [ "$ac_online" = "1" ]; then
            power_src="AC Power"
        elif [ "$ac_online" = "0" ]; then
            power_src="Battery Power"
            on_battery=1
        fi
    fi

    local docker_vm="unavailable"
    local dinfo
    dinfo=$(docker info --format '{{.NCPU}}|{{.MemTotal}}' 2>/dev/null || true)
    if [[ "$dinfo" == *"|"* ]]; then
        local ncpu="${dinfo%%|*}"
        local mem_b="${dinfo#*|}"
        local mem_gib
        mem_gib=$(python3 -c "print(f'{int(${mem_b}) / (1024**3):.2f} GiB')" 2>/dev/null || echo "${mem_b} B")
        docker_vm="${ncpu} CPUs, ${mem_gib}"
    fi

    local running_cnt
    running_cnt=$(docker ps -q 2>/dev/null | wc -l | tr -d ' ')
    local load_avg
    load_avg=$(uptime 2>/dev/null | sed -E 's/.*load averages?: //' || echo "unknown")

    local banner
    banner=$(cat <<EOF
=== Host State Preflight ===
  Power Source:       ${power_src}
  Docker VM:          ${docker_vm}
  Running Containers: ${running_cnt}
  Host Load Average:  ${load_avg}
============================
EOF
)
    echo "$banner"
    if [ "$on_battery" -eq 1 ]; then
        echo "[WARN] Host is running on BATTERY power! CPU throttling or sleep may skew or invalidate measurements."
    fi
    if [ -n "$rdir" ] && [ -d "$rdir" ]; then
        printf '%s\n' "$banner" > "${rdir}/host-state.txt"
    fi
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

    for var in USER_POOL_SIZE SEATS_PER_EVENT \
               FLASH_RAMP1_SEC FLASH_STAGE1_RATE FLASH_STAGE1_SEC \
               FLASH_RAMP2_SEC FLASH_STAGE2_RATE FLASH_STAGE2_SEC FLASH_RAMPDOWN_SEC \
               FLASH_PRE_ALLOCATED_VUS FLASH_MAX_VUS \
               JOURNEY_RATE JOURNEY_DURATION_SEC JOURNEY_PRE_ALLOCATED_VUS JOURNEY_MAX_VUS \
               START STEP MAX STEP_SECONDS BP_PRE_ALLOCATED_VUS BP_MAX_VUS BP_HOLD_EVENTS \
               SUSTAINED_RAMP_SEC SUSTAINED_PLATEAU_SEC SUSTAINED_RAMPDOWN_SEC \
               SOAK_MINUTES; do
        if [ -n "${!var:-}" ]; then
            extra_env_args+=("-e" "${var}=${!var}")
        fi
    done

    local log_file="${rdir}/${script_name%.js}.log"
    echo "=== Running k6 scenario: ${script_name} (LOAD_SCALE=${LOAD_SCALE}, run_id=${run_id}) ==="

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

run_single_breakpoint() {
    local kind="$1" # hold or journey
    local script_name="breakpoint-${kind}.js"
    local summary_name="breakpoint-${kind}-summary.json"

    load_jwt_secret
    local rdir
    rdir=$(init_results_dir)
    local run_id
    run_id=$(basename "$rdir")
    print_host_state "$rdir"
    local start_epoch
    start_epoch=$(date +%s)

    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$rdir"
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$rdir"
    trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "'"$rdir"'" || true' EXIT INT TERM

    local k6_rc=0
    run_k6_scenario "$script_name" "$rdir" "$run_id" "$summary_name" || k6_rc=$?

    trap - EXIT INT TERM
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$rdir"
    local end_epoch
    end_epoch=$(date +%s)
    write_run_meta "$rdir" "$run_id" "$start_epoch" "$end_epoch" "breakpoint-${kind}"

    local audit_rc=0
    bash "${SCRIPT_DIR}/loadtest-audit.sh" "$rdir" || audit_rc=$?

    python3 "${SCRIPT_DIR}/loadtest-breakpoint.py" "$rdir"
    python3 "${SCRIPT_DIR}/loadtest-report.py" "$rdir"

    echo "=========================================================================="
    echo "Breakpoint (${kind}) Run Complete: ${rdir}"
    echo "  k6 exit:     ${k6_rc}"
    echo "  audit exit:  ${audit_rc}"
    echo "  breakpoint:  ${rdir}/breakpoint.json"
    echo "  report:      ${rdir}/report.md"
    echo "=========================================================================="
    LAST_BP_DIR="$rdir"
    if [ "$k6_rc" -ne 0 ] || [ "$audit_rc" -ne 0 ] || [ -f "${rdir}/INVALID" ]; then
        return 1
    fi
    return 0
}

run_single_sustained() {
    load_jwt_secret
    if [ ! -f "${REPO_ROOT}/loadtest/sustainable-profile.json" ]; then
        echo "[ERROR] loadtest/sustainable-profile.json not found. Run 'make load-breakpoint' first." >&2
        return 1
    fi
    local rdir
    rdir=$(init_results_dir)
    local run_id
    run_id=$(basename "$rdir")
    print_host_state "$rdir"
    local start_epoch
    start_epoch=$(date +%s)

    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$rdir"
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$rdir"
    trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "'"$rdir"'" || true' EXIT INT TERM

    local hold_rc=0
    run_k6_scenario "sustained-hold.js" "$rdir" "$run_id" "sustained-hold-summary.json" || hold_rc=$?
    local journey_rc=0
    run_k6_scenario "sustained-journey.js" "$rdir" "$run_id" "sustained-journey-summary.json" || journey_rc=$?

    trap - EXIT INT TERM
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$rdir"
    local end_epoch
    end_epoch=$(date +%s)
    write_run_meta "$rdir" "$run_id" "$start_epoch" "$end_epoch" "sustained-hold,sustained-journey"

    local audit_rc=0
    bash "${SCRIPT_DIR}/loadtest-audit.sh" "$rdir" || audit_rc=$?

    python3 "${SCRIPT_DIR}/loadtest-report.py" "$rdir"

    echo "=========================================================================="
    echo "Sustained Run Complete: ${rdir}"
    echo "  sustained-hold exit:    ${hold_rc}"
    echo "  sustained-journey exit: ${journey_rc}"
    echo "  audit exit:             ${audit_rc}"
    echo "  invalid:                $([ -f "${rdir}/INVALID" ] && echo YES || echo NO)"
    echo "  report:                 ${rdir}/report.md"
    echo "=========================================================================="

    LAST_SUSTAINED_DIR="$rdir"
    if [ -f "${rdir}/INVALID" ] || [ "$hold_rc" -ne 0 ] || [ "$journey_rc" -ne 0 ] || [ "$audit_rc" -ne 0 ]; then
        return 1
    fi
    return 0
}

case "$CMD" in
    flash)
        load_jwt_secret
        RDIR=$(init_results_dir)
        RUN_ID=$(basename "$RDIR")
        print_host_state "$RDIR"
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
        print_host_state "$RDIR"
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
        print_host_state "$RDIR"
        START_EPOCH=$(date +%s)

        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM

        set +e
        run_k6_scenario "flash-sale.js" "$RDIR" "$RUN_ID" "flash-sale-summary.json"
        FLASH_RC=$?
        set -e

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

        # Overload runs never publish docs/load-test-report.md (publish gate is reserved for sustained runs)
        python3 "${SCRIPT_DIR}/loadtest-report.py" "$RDIR"

        echo "=========================================================================="
        echo "Load Test Run Complete: ${RDIR}"
        echo "  flash-sale exit:       ${FLASH_RC}"
        echo "  checkout-journey exit: ${JOURNEY_RC}"
        echo "  audit exit:            ${AUDIT_RC}"
        echo "  invalid:               $([ -f "${RDIR}/INVALID" ] && echo YES || echo NO)"
        echo "  report:                ${RDIR}/report.md"
        echo "=========================================================================="

        if [ "$FLASH_RC" -ne 0 ] || [ "$JOURNEY_RC" -ne 0 ] || [ "$AUDIT_RC" -ne 0 ] || [ -f "${RDIR}/INVALID" ]; then
            exit 1
        fi
        exit 0
        ;;

    breakpoint-hold)
        run_single_breakpoint "hold"
        ;;

    breakpoint-journey)
        run_single_breakpoint "journey"
        ;;

    breakpoint)
        unset RESULTS_DIR
        make -C "$REPO_ROOT" stack-reset
        make -C "$REPO_ROOT" obs-up
        HOLD_BP_RC=0
        run_single_breakpoint "hold" || HOLD_BP_RC=$?
        HOLD_BP_DIR="$LAST_BP_DIR"

        make -C "$REPO_ROOT" stack-reset
        make -C "$REPO_ROOT" obs-up
        JOURNEY_BP_RC=0
        run_single_breakpoint "journey" || JOURNEY_BP_RC=$?
        JOURNEY_BP_DIR="$LAST_BP_DIR"

        COMBINE_RC=0
        python3 "${SCRIPT_DIR}/loadtest-breakpoint.py" \
            --combine-profile "$HOLD_BP_DIR" "$JOURNEY_BP_DIR" \
            --profile-out "${REPO_ROOT}/loadtest/sustainable-profile.json" || COMBINE_RC=$?

        echo "=========================================================================="
        echo "Breakpoint Suite Complete"
        echo "  hold breakpoint dir:    ${HOLD_BP_DIR} (rc=${HOLD_BP_RC})"
        echo "  journey breakpoint dir: ${JOURNEY_BP_DIR} (rc=${JOURNEY_BP_RC})"
        echo "  profile combine exit:   ${COMBINE_RC}"
        echo "=========================================================================="
        if [ "$HOLD_BP_RC" -ne 0 ] || [ "$JOURNEY_BP_RC" -ne 0 ] || [ "$COMBINE_RC" -ne 0 ]; then
            exit 1
        fi
        exit 0
        ;;

    sustained)
        run_single_sustained
        ;;

    sustained-x3)
        unset RESULTS_DIR
        VALID_DIRS=()
        ALL_DIRS=()
        PASS_COUNT=0
        ATTEMPT=0
        MAX_ATTEMPTS=6

        while [ "${#VALID_DIRS[@]}" -lt 3 ] && [ "$ATTEMPT" -lt "$MAX_ATTEMPTS" ]; do
            ATTEMPT=$((ATTEMPT + 1))
            echo "=========================================================================="
            echo "Sustained Run Attempt ${ATTEMPT}/${MAX_ATTEMPTS} (valid collected so far: ${#VALID_DIRS[@]}/3)"
            echo "=========================================================================="
            make -C "$REPO_ROOT" stack-reset
            make -C "$REPO_ROOT" obs-up
            S_RC=0
            run_single_sustained || S_RC=$?
            ALL_DIRS+=("$LAST_SUSTAINED_DIR")
            if [ -f "${LAST_SUSTAINED_DIR}/INVALID" ]; then
                echo "[sustained-x3] Attempt ${ATTEMPT} (${LAST_SUSTAINED_DIR}) was INVALID — excluded and not counted."
            else
                VALID_DIRS+=("$LAST_SUSTAINED_DIR")
                if [ "$S_RC" -eq 0 ]; then
                    PASS_COUNT=$((PASS_COUNT + 1))
                fi
            fi
        done

        echo "=========================================================================="
        echo "Sustained x3 Summary: ${#VALID_DIRS[@]} valid run(s) across ${ATTEMPT} attempt(s); ${PASS_COUNT}/${#VALID_DIRS[@]} passed all checks"
        for d in "${ALL_DIRS[@]}"; do
            echo "  - ${d} ($([ -f "${d}/INVALID" ] && echo INVALID || echo VALID))"
        done
        echo "=========================================================================="

        if [ "${LOAD_PUBLISH}" = "true" ] && [ "${#VALID_DIRS[@]}" -eq 3 ]; then
            OVERLOAD_ARGS=()
            for ov in 20261121T190134Z 20261122T120819Z 20261122T125331Z 20261122T130512Z; do
                if [ -d "${REPO_ROOT}/loadtest/results/${ov}" ]; then
                    OVERLOAD_ARGS+=("${REPO_ROOT}/loadtest/results/${ov}")
                fi
            done
            python3 "${SCRIPT_DIR}/loadtest-report.py" \
                --publish-sustained "${VALID_DIRS[@]}" \
                --profile "${REPO_ROOT}/loadtest/sustainable-profile.json" \
                --overload ${OVERLOAD_ARGS[@]+"${OVERLOAD_ARGS[@]}"} || true
        fi

        if [ "${#VALID_DIRS[@]}" -lt 3 ] || [ "$PASS_COUNT" -ne 3 ]; then
            exit 1
        fi
        exit 0
        ;;

    soak)
        load_jwt_secret
        if [ ! -f "${REPO_ROOT}/loadtest/sustainable-profile.json" ]; then
            echo "[ERROR] loadtest/sustainable-profile.json not found. Run 'make load-breakpoint' first." >&2
            exit 1
        fi
        make -C "$REPO_ROOT" stack-reset
        make -C "$REPO_ROOT" obs-up
        RDIR=$(init_results_dir)
        RUN_ID=$(basename "$RDIR")
        print_host_state "$RDIR"
        START_EPOCH=$(date +%s)

        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM

        set +e
        run_k6_scenario "soak.js" "$RDIR" "$RUN_ID" "soak-summary.json"
        SOAK_K6_RC=$?
        set -e

        trap - EXIT INT TERM
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR"
        END_EPOCH=$(date +%s)
        write_run_meta "$RDIR" "$RUN_ID" "$START_EPOCH" "$END_EPOCH" "soak"

        set +e
        bash "${SCRIPT_DIR}/loadtest-audit.sh" "$RDIR"
        AUDIT_RC=$?
        python3 "${SCRIPT_DIR}/loadtest-soak-report.py" "$RDIR"
        SOAK_REPORT_RC=$?
        set -e

        python3 "${SCRIPT_DIR}/loadtest-report.py" "$RDIR"

        echo "=========================================================================="
        echo "Soak Run Complete: ${RDIR}"
        echo "  soak k6 exit:     ${SOAK_K6_RC}"
        echo "  audit exit:       ${AUDIT_RC}"
        echo "  soak drift exit:  ${SOAK_REPORT_RC}"
        echo "  invalid:          $([ -f "${RDIR}/INVALID" ] && echo YES || echo NO)"
        echo "  report:           ${RDIR}/report.md"
        echo "=========================================================================="

        if [ "$SOAK_K6_RC" -ne 0 ] || [ "$AUDIT_RC" -ne 0 ] || [ "$SOAK_REPORT_RC" -ne 0 ] || [ -f "${RDIR}/INVALID" ]; then
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
        echo "Usage: $0 <flash|journey|all|breakpoint-hold|breakpoint-journey|breakpoint|sustained|sustained-x3|soak|audit|report>" >&2
        exit 1
        ;;
esac
