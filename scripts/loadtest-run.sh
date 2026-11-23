#!/usr/bin/env bash
# =============================================================================
# scripts/loadtest-run.sh — Orchestrator for quiet-host preflight, external seat
# seeding, k6 Docker runs, monitoring, audit, breakpoint, tail analysis & report
#
# Subcommands:
#   preflight           Run quiet-host preflight gate (power, 60s load avg, Docker RAM)
#   flash               Run overload Scenario A (flash-sale.js)
#   journey             Run overload Scenario B (checkout-journey.js)
#   all                 Run flash -> journey -> audit -> report
#   breakpoint-hold     Preflight -> seed -> breakpoint-hold.js -> audit -> bp -> tail -> report
#   breakpoint-journey  Preflight -> seed -> breakpoint-journey.js -> audit -> bp -> tail -> report
#   breakpoint          Run both breakpoints (stack-reset between) & derive profile
#   steady              Preflight -> seed -> steady-hold.js -> audit -> tail -> report
#   sustained           Preflight -> seed -> sustained-hold -> sustained-journey -> audit -> report
#   sustained-x3        Run 3 VALID sustained runs (max 6 attempts)
#   soak                Preflight -> seed -> soak.js -> audit -> soak-report -> report
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
SEED_BASE_URL="${SEED_BASE_URL:-http://127.0.0.1:8080}"
PROM_RW_URL="${K6_PROMETHEUS_RW_SERVER_URL:-http://prometheus:9090/api/v1/write}"
LOAD_SCALE="${LOAD_SCALE:-1.0}"
LOAD_PUBLISH="${LOAD_PUBLISH:-false}"
ALLOW_BATTERY="${ALLOW_BATTERY:-0}"
PREFLIGHT_SETTLE_TIMEOUT_SECONDS="${PREFLIGHT_SETTLE_TIMEOUT_SECONDS:-360}"
PREFLIGHT_SETTLE_INTERVAL_SECONDS="${PREFLIGHT_SETTLE_INTERVAL_SECONDS:-15}"
PREFLIGHT_MAX_LOAD="${PREFLIGHT_MAX_LOAD:-3.0}"

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

# Quiet-host preflight gate (macOS & Linux aware):
#  1) Power source check: refuse on battery unless ALLOW_BATTERY=1 (marks NOT_PUBLISHABLE)
#  2) Settle phase: poll 1-minute load average every 15s for up to 6 minutes; gate passes
#     as soon as three consecutive samples are <= 3.0; refuse (exit non-zero, printing top 5 CPU)
#     if limit not reached within 6 minutes.
#  3) Print Docker VM CPU/RAM, sum of container memory limits, and sum of actual idle memory;
#     warn if actual idle memory > 70% of Docker VM memory
#  4) Save everything to results/<ts>/host-state.txt if rdir is given
run_preflight() {
    local rdir="${1:-}"
    python3 - "$rdir" "$ALLOW_BATTERY" "$PREFLIGHT_SETTLE_TIMEOUT_SECONDS" "$PREFLIGHT_SETTLE_INTERVAL_SECONDS" "$PREFLIGHT_MAX_LOAD" <<'PYEOF'
import os, platform, re, subprocess, sys, time

rdir, allow_batt_s, max_wait_sec_s, interval_sec_s, max_load_s = sys.argv[1:6]
allow_batt = allow_batt_s == "1"
max_wait_sec = max(15, int(max_wait_sec_s))
interval_sec = max(1, int(interval_sec_s))
max_load = float(max_load_s)
sys_name = platform.system()

def cmd(args, timeout=10):
    try:
        return subprocess.check_output(args, stderr=subprocess.STDOUT, timeout=timeout).decode("utf-8", errors="replace").strip()
    except Exception:
        return ""

# 1. Power source
power_source = "unknown"
on_battery = False
if sys_name == "Darwin":
    batt = cmd(["pmset", "-g", "batt"])
    if batt:
        power_source = " | ".join(ln.strip() for ln in batt.splitlines()[:2])
        if "Battery Power" in batt:
            on_battery = True
else:
    ps_dir = "/sys/class/power_supply"
    if os.path.isdir(ps_dir):
        for entry in sorted(os.listdir(ps_dir)):
            online_p = os.path.join(ps_dir, entry, "online")
            if os.path.isfile(online_p) and entry.startswith(("AC", "ADP", "ACAD")):
                val = open(online_p).read().strip()
                power_source = "AC Power" if val == "1" else "Battery Power"
                on_battery = (val == "0")
                break

# 2. Docker VM CPU, memory, configured limits sum, actual idle memory sum
dinfo = cmd(["docker", "info", "--format", "{{.NCPU}}|{{.MemTotal}}"])
vm_cpus = "unavailable"
vm_mem_bytes = 0
vm_mem_gib = 0.0
if "|" in dinfo:
    nc, mb = dinfo.split("|", 1)
    vm_cpus = nc.strip()
    if mb.strip().isdigit():
        vm_mem_bytes = int(mb.strip())
        vm_mem_gib = vm_mem_bytes / (1024 ** 3)

cids = [x for x in cmd(["docker", "ps", "-q"]).splitlines() if x.strip()]
running_cnt = len(cids)
limits_sum_bytes = 0
if cids:
    insp = cmd(["docker", "inspect", "--format", "{{.HostConfig.Memory}}"] + cids)
    for ln in insp.splitlines():
        if ln.strip().isdigit():
            limits_sum_bytes += int(ln.strip())

def parse_mem_mib(s):
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\s*$", s.strip())
    if not m:
        return 0.0
    v, u = float(m.group(1)), m.group(2).lower()
    if u == "b": return v / (1024.0 * 1024.0)
    if u in ("kib", "kb", "k"): return v / 1024.0
    if u in ("mib", "mb", "m"): return v
    if u in ("gib", "gb", "g"): return v * 1024.0
    return v

actual_idle_mib = 0.0
stats_raw = cmd(["docker", "stats", "--no-stream", "--format", "{{.Name}},{{.MemUsage}}"], timeout=15)
for ln in stats_raw.splitlines():
    if "," not in ln:
        continue
    mem_field = ln.split(",", 1)[1]
    usage_str = mem_field.split("/")[0].strip()
    actual_idle_mib += parse_mem_mib(usage_str)

vm_mem_mib = vm_mem_bytes / (1024.0 * 1024.0) if vm_mem_bytes > 0 else 0.0
limits_sum_mib = limits_sum_bytes / (1024.0 * 1024.0)
idle_mem_pct = (actual_idle_mib / vm_mem_mib * 100.0) if vm_mem_mib > 0 else 0.0

# 3. Settle phase: poll 1-minute load average every interval_sec (default 15s) for up to max_wait_sec (default 360s = 6 min)
# Pass as soon as three consecutive samples are <= max_load (default 3.0).
def get_load1():
    try:
        l1, l5, l15 = os.getloadavg()
        return l1, l5, l15
    except Exception:
        return 0.0, 0.0, 0.0

samples = []
consecutive_ok = 0
settled = False
t_start = time.monotonic()
print(f"[preflight] Entering quiet-host settle phase (polling every {interval_sec}s up to {max_wait_sec}s for 3 consecutive samples <= {max_load:.1f})...", flush=True)

while True:
    l1, l5, l15 = get_load1()
    elapsed = int(round(time.monotonic() - t_start))
    sample_item = (elapsed, round(l1, 2), round(l5, 2), round(l15, 2))
    samples.append(sample_item)
    print(f"  [preflight] sample #{len(samples):02d} (t={elapsed:03d}s): load1={l1:.2f}, load5={l5:.2f}, load15={l15:.2f}", flush=True)

    if l1 <= max_load:
        consecutive_ok += 1
        if consecutive_ok >= 3:
            settled = True
            print(f"[preflight] Quiet host reached after {elapsed}s: 3 consecutive samples <= {max_load:.1f} (samples: {samples[-3][1]:.2f}, {samples[-2][1]:.2f}, {samples[-1][1]:.2f}).", flush=True)
            break
    else:
        consecutive_ok = 0

    if elapsed >= max_wait_sec:
        break
    time.sleep(interval_sec)

final_l1, final_l5, final_l15 = samples[-1][1], samples[-1][2], samples[-1][3]
sample_str = ", ".join(f"t={e}s:{a:.2f}" for e, a, _, _ in samples)
trend_str = " -> ".join(f"{a:.2f}" for _, a, _, _ in samples)

top5_procs = ""
if sys_name == "Darwin":
    top5_procs = cmd(["ps", "-Arco", "%cpu,comm"])
else:
    top5_procs = cmd(["ps", "-eo", "%cpu,comm", "--sort=-%cpu"])
top5_lines = top5_procs.splitlines()[:6] if top5_procs else ["(unavailable)"]

lines = [
    "=== Quiet-Host Preflight Gate ===",
    f"  Power Source:            {power_source}",
    f"  Docker VM:               {vm_cpus} CPUs, {vm_mem_gib:.2f} GiB ({vm_mem_mib:.0f} MiB)",
    f"  Running Containers:      {running_cnt}",
    f"  Sum Container Mem Limit: {limits_sum_mib:.0f} MiB ({(limits_sum_mib / vm_mem_mib * 100.0) if vm_mem_mib else 0:.1f}% of VM)",
    f"  Sum Actual Idle Mem:     {actual_idle_mib:.1f} MiB ({idle_mem_pct:.1f}% of VM)",
    f"  Settle Samples (count):  {len(samples)} ({'SETTLED (3 consecutive <= ' + str(max_load) + ')' if settled else 'TIMEOUT EXCEEDED'})",
    f"  Load Avg Trend:          {trend_str}",
    f"  Host Load Avg (final):   {final_l1:.2f} {final_l5:.2f} {final_l15:.2f} [samples: {sample_str}]",
    "=================================",
]

warnings = []
if idle_mem_pct > 70.0:
    warnings.append(f"[WARN] Actual idle container memory ({actual_idle_mib:.1f} MiB = {idle_mem_pct:.1f}%) exceeds 70% of Docker VM memory ({vm_mem_mib:.0f} MiB)!")

refusals = []
if on_battery:
    if allow_batt:
        warnings.append("[WARN] Host is on BATTERY power (ALLOW_BATTERY=1 set); marking run NOT_PUBLISHABLE.")
        if rdir and os.path.isdir(rdir):
            with open(os.path.join(rdir, "NOT_PUBLISHABLE"), "w") as f:
                f.write(f"Run executed on Battery Power with ALLOW_BATTERY=1 ({power_source})\n")
    else:
        refusals.append(
            f"REFUSED: Host is running on Battery Power ({power_source}). Plug into AC power or set ALLOW_BATTERY=1 (marks run NOT_PUBLISHABLE)."
        )

if not settled:
    refusals.append(
        f"REFUSED: Host 1-minute load average did not settle at or below {max_load:.1f} (3 consecutive samples) within {max_wait_sec}s. Final load1={final_l1:.2f}, trend: {trend_str}."
    )

report_text = "\n".join(lines) + "\n"
if warnings:
    report_text += "\n".join(warnings) + "\n"
if refusals:
    report_text += "\n".join(refusals) + "\nTop 5 CPU processes:\n" + "\n".join("  " + ln for ln in top5_lines) + "\n"

print(report_text, end="")
if rdir and os.path.isdir(rdir):
    with open(os.path.join(rdir, "host-state.txt"), "w") as f:
        f.write(report_text)

if refusals:
    sys.exit(2)
PYEOF
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
    if [ -f "${rdir}/seats.json" ]; then
        extra_env_args+=("-e" "SEATS_MANIFEST=/results/seats.json")
    fi

    for var in USER_POOL_SIZE SEATS_PER_EVENT SEAT_SUPPLY \
               WARMUP_SECONDS WARMUP_RATE \
               FLASH_RAMP1_SEC FLASH_STAGE1_RATE FLASH_STAGE1_SEC \
               FLASH_RAMP2_SEC FLASH_STAGE2_RATE FLASH_STAGE2_SEC FLASH_RAMPDOWN_SEC \
               FLASH_PRE_ALLOCATED_VUS FLASH_MAX_VUS \
               JOURNEY_RATE JOURNEY_DURATION_SEC JOURNEY_PRE_ALLOCATED_VUS JOURNEY_MAX_VUS \
               START STEP MAX STEP_SECONDS BP_PRE_ALLOCATED_VUS BP_MAX_VUS \
               RATE MINUTES STEADY_RAMP_SEC STEADY_RAMPDOWN_SEC \
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

    run_preflight "$rdir"

    local def_start def_step def_max def_step_s def_w_rate def_supply
    if [ "$kind" = "hold" ]; then
        def_start="${START:-25}"
        def_step="${STEP:-25}"
        def_max="${MAX:-600}"
        def_step_s="${STEP_SECONDS:-30}"
        def_w_rate="${WARMUP_RATE:-50}"
        def_supply="${SEAT_SUPPLY:-ample}"
    else
        def_start="${START:-2}"
        def_step="${STEP:-2}"
        def_max="${MAX:-40}"
        def_step_s="${STEP_SECONDS:-45}"
        def_w_rate="${WARMUP_RATE:-4}"
        def_supply="ample"
    fi
    local def_w_sec="${WARMUP_SECONDS:-90}"

    python3 "${SCRIPT_DIR}/loadtest-seed.py" \
        --mode staircase \
        --base-url "${SEED_BASE_URL}" \
        --run-id "${run_id}" \
        --label "bp${kind}" \
        --output "${rdir}/seats.json" \
        --seat-supply "${def_supply}" \
        --start "${def_start}" \
        --step "${def_step}" \
        --max-rate "${def_max}" \
        --step-seconds "${def_step_s}" \
        --warmup-rate "${def_w_rate}" \
        --warmup-seconds "${def_w_sec}"

    local start_epoch
    start_epoch=$(date +%s)

    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$rdir"
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$rdir"
    trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "'"$rdir"'" || true' EXIT INT TERM

    local k6_rc=0
    SEAT_SUPPLY="${def_supply}" run_k6_scenario "$script_name" "$rdir" "$run_id" "$summary_name" || k6_rc=$?

    trap - EXIT INT TERM
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$rdir"
    local end_epoch
    end_epoch=$(date +%s)
    write_run_meta "$rdir" "$run_id" "$start_epoch" "$end_epoch" "breakpoint-${kind}"

    local audit_rc=0
    bash "${SCRIPT_DIR}/loadtest-audit.sh" "$rdir" || audit_rc=$?

    python3 "${SCRIPT_DIR}/loadtest-breakpoint.py" "$rdir"
    python3 "${SCRIPT_DIR}/loadtest-tail-analysis.py" "$rdir"
    python3 "${SCRIPT_DIR}/loadtest-report.py" "$rdir"

    echo "=========================================================================="
    echo "Breakpoint (${kind}) Run Complete: ${rdir}"
    echo "  k6 exit:     ${k6_rc}"
    echo "  audit exit:  ${audit_rc}"
    echo "  breakpoint:  ${rdir}/breakpoint.json"
    echo "  tail:        ${rdir}/tail-analysis.md"
    echo "  report:      ${rdir}/report.md"
    echo "=========================================================================="
    LAST_BP_DIR="$rdir"
    if [ "$k6_rc" -ne 0 ] || [ "$audit_rc" -ne 0 ] || [ -f "${rdir}/INVALID" ]; then
        return 1
    fi
    return 0
}

run_steady_hold() {
    load_jwt_secret
    local rdir
    rdir=$(init_results_dir)
    local run_id
    run_id=$(basename "$rdir")

    run_preflight "$rdir"

    local rate="${RATE:-100}"
    local mins="${MINUTES:-5}"
    local dur_s
    dur_s=$(python3 -c "print(max(30, round(float('${mins}') * 60)))")
    local w_sec="${WARMUP_SECONDS:-90}"
    local w_rate="${WARMUP_RATE:-50}"
    local supply="${SEAT_SUPPLY:-ample}"

    python3 "${SCRIPT_DIR}/loadtest-seed.py" \
        --mode steady \
        --base-url "${SEED_BASE_URL}" \
        --run-id "${run_id}" \
        --label "steadyhold" \
        --output "${rdir}/seats.json" \
        --seat-supply "${supply}" \
        --rate "${rate}" \
        --duration-seconds "${dur_s}" \
        --ramp-seconds 30 \
        --warmup-rate "${w_rate}" \
        --warmup-seconds "${w_sec}"

    local start_epoch
    start_epoch=$(date +%s)

    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$rdir"
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$rdir"
    trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "'"$rdir"'" || true' EXIT INT TERM

    local k6_rc=0
    RATE="${rate}" MINUTES="${mins}" SEAT_SUPPLY="${supply}" run_k6_scenario "steady-hold.js" "$rdir" "$run_id" "steady-hold-summary.json" || k6_rc=$?

    trap - EXIT INT TERM
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$rdir"
    local end_epoch
    end_epoch=$(date +%s)
    write_run_meta "$rdir" "$run_id" "$start_epoch" "$end_epoch" "steady-hold"

    local audit_rc=0
    bash "${SCRIPT_DIR}/loadtest-audit.sh" "$rdir" || audit_rc=$?

    python3 "${SCRIPT_DIR}/loadtest-tail-analysis.py" "$rdir"
    python3 "${SCRIPT_DIR}/loadtest-report.py" "$rdir"

    echo "=========================================================================="
    echo "Steady Hold Run Complete: ${rdir}"
    echo "  rate / mins: ${rate} req/s x ${mins} min (supply=${supply})"
    echo "  k6 exit:     ${k6_rc}"
    echo "  audit exit:  ${audit_rc}"
    echo "  invalid:     $([ -f "${rdir}/INVALID" ] && echo YES || echo NO)"
    echo "  tail:        ${rdir}/tail-analysis.md"
    echo "  report:      ${rdir}/report.md"
    echo "=========================================================================="
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
    run_preflight "$rdir"

    local h_rate j_rate
    h_rate=$(python3 -c "import json; print(json.load(open('${REPO_ROOT}/loadtest/sustainable-profile.json'))['hold']['sustainable_rps'])")
    j_rate=$(python3 -c "import json; print(json.load(open('${REPO_ROOT}/loadtest/sustainable-profile.json'))['journey']['sustainable_jps'])")

    local start_epoch
    start_epoch=$(date +%s)

    bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$rdir"
    bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$rdir"
    trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "'"$rdir"'" || true' EXIT INT TERM

    python3 "${SCRIPT_DIR}/loadtest-seed.py" \
        --mode steady --base-url "${SEED_BASE_URL}" --run-id "${run_id}" --label "susthold" \
        --output "${rdir}/seats.json" --seat-supply "${SEAT_SUPPLY:-ample}" \
        --rate "${h_rate}" --duration-seconds "${SUSTAINED_PLATEAU_SEC:-300}" --ramp-seconds 30 \
        --warmup-rate "${WARMUP_RATE:-50}" --warmup-seconds "${WARMUP_SECONDS:-90}"
    local hold_rc=0
    run_k6_scenario "sustained-hold.js" "$rdir" "$run_id" "sustained-hold-summary.json" || hold_rc=$?

    python3 "${SCRIPT_DIR}/loadtest-seed.py" \
        --mode steady --base-url "${SEED_BASE_URL}" --run-id "${run_id}" --label "sustjourney" \
        --output "${rdir}/seats.json" --seat-supply ample \
        --rate "${j_rate}" --duration-seconds "${SUSTAINED_PLATEAU_SEC:-300}" --ramp-seconds 30 \
        --warmup-rate "${WARMUP_RATE:-4}" --warmup-seconds "${WARMUP_SECONDS:-90}"
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

    LAST_SUSTAINED_DIR="$rdir"
    if [ -f "${rdir}/INVALID" ] || [ "$hold_rc" -ne 0 ] || [ "$journey_rc" -ne 0 ] || [ "$audit_rc" -ne 0 ]; then
        return 1
    fi
    return 0
}

case "$CMD" in
    preflight)
        run_preflight "${RESULTS_DIR:-}"
        ;;

    flash)
        load_jwt_secret
        RDIR=$(init_results_dir)
        RUN_ID=$(basename "$RDIR")
        run_preflight "$RDIR"
        START_EPOCH=$(date +%s)
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM
        K6_RC=0
        run_k6_scenario "flash-sale.js" "$RDIR" "$RUN_ID" "flash-sale-summary.json" || K6_RC=$?
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
        run_preflight "$RDIR"
        START_EPOCH=$(date +%s)
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM
        K6_RC=0
        run_k6_scenario "checkout-journey.js" "$RDIR" "$RUN_ID" "checkout-journey-summary.json" || K6_RC=$?
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
        run_preflight "$RDIR"
        START_EPOCH=$(date +%s)

        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM

        FLASH_RC=0
        run_k6_scenario "flash-sale.js" "$RDIR" "$RUN_ID" "flash-sale-summary.json" || FLASH_RC=$?

        JOURNEY_EV=$(grep -Eo 'LOADTEST_JOURNEY_EVENT_ID=[0-9a-fA-F-]+' "${RDIR}/flash-sale.log" | head -n 1 | cut -d= -f2 || true)
        if [ -n "$JOURNEY_EV" ]; then
            export REUSE_EVENT_ID="$JOURNEY_EV"
        fi

        JOURNEY_RC=0
        run_k6_scenario "checkout-journey.js" "$RDIR" "$RUN_ID" "checkout-journey-summary.json" || JOURNEY_RC=$?
        unset REUSE_EVENT_ID

        trap - EXIT INT TERM
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR"
        END_EPOCH=$(date +%s)
        write_run_meta "$RDIR" "$RUN_ID" "$START_EPOCH" "$END_EPOCH" "flash-sale,checkout-journey"

        AUDIT_RC=0
        bash "${SCRIPT_DIR}/loadtest-audit.sh" "$RDIR" || AUDIT_RC=$?
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
        HOLD_BP_DIR="${LAST_BP_DIR:-}"

        make -C "$REPO_ROOT" stack-reset
        make -C "$REPO_ROOT" obs-up
        JOURNEY_BP_RC=0
        run_single_breakpoint "journey" || JOURNEY_BP_RC=$?
        JOURNEY_BP_DIR="${LAST_BP_DIR:-}"

        COMBINE_RC=0
        if [ -n "$HOLD_BP_DIR" ] && [ -n "$JOURNEY_BP_DIR" ]; then
            python3 "${SCRIPT_DIR}/loadtest-breakpoint.py" \
                --combine-profile "$HOLD_BP_DIR" "$JOURNEY_BP_DIR" \
                --profile-out "${REPO_ROOT}/loadtest/sustainable-profile.json" || COMBINE_RC=$?
        else
            COMBINE_RC=2
        fi

        echo "=========================================================================="
        echo "Breakpoint Suite Complete"
        echo "  hold breakpoint dir:    ${HOLD_BP_DIR:-none} (rc=${HOLD_BP_RC})"
        echo "  journey breakpoint dir: ${JOURNEY_BP_DIR:-none} (rc=${JOURNEY_BP_RC})"
        echo "  profile combine exit:   ${COMBINE_RC}"
        echo "=========================================================================="
        if [ "$HOLD_BP_RC" -ne 0 ] || [ "$JOURNEY_BP_RC" -ne 0 ] || [ "$COMBINE_RC" -ne 0 ]; then
            exit 1
        fi
        exit 0
        ;;

    steady)
        run_steady_hold
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
            make -C "$REPO_ROOT" stack-reset
            make -C "$REPO_ROOT" obs-up
            S_RC=0
            run_single_sustained || S_RC=$?
            ALL_DIRS+=("${LAST_SUSTAINED_DIR:-}")
            if [ -n "${LAST_SUSTAINED_DIR:-}" ] && [ ! -f "${LAST_SUSTAINED_DIR}/INVALID" ]; then
                VALID_DIRS+=("$LAST_SUSTAINED_DIR")
                if [ "$S_RC" -eq 0 ]; then
                    PASS_COUNT=$((PASS_COUNT + 1))
                fi
            fi
        done

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
        run_preflight "$RDIR"

        J_RATE=$(python3 -c "import json; print(json.load(open('${REPO_ROOT}/loadtest/sustainable-profile.json'))['journey']['sustainable_jps'])")
        SOAK_DUR_S=$(python3 -c "print(max(60, round(float('${SOAK_MINUTES:-20}') * 60)))")
        python3 "${SCRIPT_DIR}/loadtest-seed.py" \
            --mode steady --base-url "${SEED_BASE_URL}" --run-id "${RUN_ID}" --label "soak" \
            --output "${RDIR}/seats.json" --seat-supply ample \
            --rate "${J_RATE}" --duration-seconds "${SOAK_DUR_S}" --ramp-seconds 30 \
            --warmup-rate "${WARMUP_RATE:-4}" --warmup-seconds "${WARMUP_SECONDS:-90}"

        START_EPOCH=$(date +%s)
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-start "$RDIR"
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" start "$RDIR"
        trap 'bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR" || true' EXIT INT TERM

        SOAK_K6_RC=0
        run_k6_scenario "soak.js" "$RDIR" "$RUN_ID" "soak-summary.json" || SOAK_K6_RC=$?

        trap - EXIT INT TERM
        bash "${SCRIPT_DIR}/loadtest-monitor.sh" snapshot-end "$RDIR"
        END_EPOCH=$(date +%s)
        write_run_meta "$RDIR" "$RUN_ID" "$START_EPOCH" "$END_EPOCH" "soak"

        AUDIT_RC=0
        bash "${SCRIPT_DIR}/loadtest-audit.sh" "$RDIR" || AUDIT_RC=$?
        SOAK_REPORT_RC=0
        python3 "${SCRIPT_DIR}/loadtest-soak-report.py" "$RDIR" || SOAK_REPORT_RC=$?
        python3 "${SCRIPT_DIR}/loadtest-report.py" "$RDIR"

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
        echo "Usage: $0 <preflight|flash|journey|all|breakpoint-hold|breakpoint-journey|breakpoint|steady|sustained|sustained-x3|soak|audit|report>" >&2
        exit 1
        ;;
esac
