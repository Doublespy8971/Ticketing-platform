#!/usr/bin/env python3
"""
scripts/loadtest-breakpoint.py — Breakpoint & Sustainable Rate Calculator

Python 3 standard library only.
Reads k6 summary JSON of a breakpoint run (breakpoint-hold-summary.json or
breakpoint-journey-summary.json), evaluates each staircase step against the
SLOs and stability rules, prints the per-step table, breakpoint, sustainable
rate, and exact derivation, and writes results/<ts>/breakpoint.json.

Breakpoint rule:
  Highest step such that this step AND ALL lower steps satisfy:
    - hold p99 < 300 ms
    - (journey only) checkout p99 < 500 ms and saga p95 < 5000 ms
    - error rate < 0.001 (0.1%)
    - unknown outcomes == 0
    - dropped iterations == 0
    - achieved requests > 0

Sustainable rate rule:
  floor((0.70 * breakpoint) / step_size) * step_size, never below 1 * step_size.
  If no step passes, reports NO BREAKPOINT FOUND and does not create/overwrite
  loadtest/sustainable-profile.json.
"""

import argparse
import datetime
import json
import math
import os
import sys
import urllib.parse
import urllib.request


def prom_instant(prom_url, expr, eval_time):
    if not prom_url or not eval_time:
        return None
    params = {"query": expr, "time": str(int(eval_time))}
    url = f"{prom_url.rstrip('/')}/api/v1/query?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=4) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("status") != "success":
            return None
        res = data.get("data", {}).get("result", [])
        if not res:
            return 0.0
        return float(res[0]["value"][1])
    except Exception:
        return None


def compute_sustainable_rate(breakpoint_val, step_size):
    if breakpoint_val is None or breakpoint_val <= 0 or step_size <= 0:
        return None, "NO BREAKPOINT FOUND"
    raw_70 = 0.70 * breakpoint_val
    rounded = int(math.floor(raw_70 / step_size) * step_size)
    sustainable = max(int(step_size), rounded)
    derivation = (
        f"floor(0.70 * {breakpoint_val} / {step_size}) * {step_size} = "
        f"floor({raw_70:.2f} / {step_size}) * {step_size} = {rounded}; "
        f"clamped >= {step_size} => {sustainable}"
    )
    return sustainable, derivation


def analyze_breakpoint_summary(summary_data, prom_url=None, enable_live=True):
    meta = summary_data.get("_meta", {})
    metrics = summary_data.get("metrics", {})
    scenario = meta.get("scenario", "unknown")
    is_journey = scenario == "breakpoint-journey"

    steps_meta = meta.get("steps", [])
    step_size = int(meta.get("step_jps" if is_journey else "step_rps") or 1)
    unit = "journeys/s" if is_journey else "req/s"
    global_dropped = int(metrics.get("dropped_iterations", {}).get("values", {}).get("count", 0) or 0)

    prom_reachable = False
    if enable_live and prom_url:
        probe = prom_instant(prom_url, "up", steps_meta[-1].get("end_epoch") if steps_meta else None)
        prom_reachable = probe is not None

    rows = []
    for s in steps_meta:
        tag = s["tag"]
        target = float(s.get("target_jps" if is_journey else "target_rps") or 0)
        dur = float(s.get("duration_s") or 1)

        req_m = metrics.get(f"bp_step_reqs{{step:{tag}}}", {}).get("values", {})
        achieved_count = int(req_m.get("count", 0) or 0)
        achieved_rate = achieved_count / dur if dur > 0 else 0.0

        err_m = metrics.get(f"bp_step_error{{step:{tag}}}", {}).get("values", {})
        error_rate = float(err_m.get("rate", 0.0) or 0.0)

        unk_m = metrics.get(f"bp_step_unknown{{step:{tag}}}", {}).get("values", {})
        unknown_count = int(unk_m.get("count", 0) or 0)
        unknown_rate = (unknown_count / achieved_count) if achieved_count > 0 else 0.0

        hold_v = metrics.get(f"http_req_duration{{step:{tag},name:hold}}", {}).get("values", {})
        hold_p50 = hold_v.get("med")
        hold_p95 = hold_v.get("p(95)")
        hold_p99 = hold_v.get("p(99)")

        chk_p50 = chk_p95 = chk_p99 = None
        saga_p50 = saga_p95 = saga_p99 = None
        if is_journey:
            chk_v = metrics.get(f"http_req_duration{{step:{tag},name:checkout}}", {}).get("values", {})
            chk_p50 = chk_v.get("med")
            chk_p95 = chk_v.get("p(95)")
            chk_p99 = chk_v.get("p(99)")
            saga_v = metrics.get(f"saga_latency{{step:{tag}}}", {}).get("values", {})
            saga_p50 = saga_v.get("med")
            saga_p95 = saga_v.get("p(95)")
            saga_p99 = saga_v.get("p(99)")

        # Determine dropped iterations during this step
        dropped_step = 0
        dropped_source = "k6_global_zero"
        if global_dropped == 0:
            dropped_step = 0
            dropped_source = "k6_global_zero"
        elif prom_reachable and s.get("start_epoch") and s.get("end_epoch"):
            v_start = prom_instant(prom_url, "sum(k6_dropped_iterations_total)", s["start_epoch"]) or 0.0
            v_end = prom_instant(prom_url, "sum(k6_dropped_iterations_total)", s["end_epoch"]) or 0.0
            dropped_step = max(0, int(round(v_end - v_start)))
            dropped_source = "prometheus_delta"
        else:
            # Offline fallback when global_dropped > 0: detect arrival shortfall > 2%
            expected_count = target * dur
            if achieved_count < expected_count * 0.98:
                dropped_step = max(1, int(round(expected_count - achieved_count)))
            else:
                dropped_step = 0
            dropped_source = "arrival_shortfall_estimate"

        # Step pass criteria
        reasons = []
        if achieved_count <= 0:
            reasons.append("no_requests")
        if hold_p99 is None or float(hold_p99) >= 300.0:
            reasons.append(f"hold_p99={hold_p99}")
        if is_journey:
            if chk_p99 is None or float(chk_p99) >= 500.0:
                reasons.append(f"checkout_p99={chk_p99}")
            if saga_p95 is None or float(saga_p95) >= 5000.0:
                reasons.append(f"saga_p95={saga_p95}")
        if error_rate >= 0.001:
            reasons.append(f"err_rate={error_rate:.5f}")
        if unknown_count != 0:
            reasons.append(f"unknown={unknown_count}")
        if dropped_step != 0:
            reasons.append(f"dropped={dropped_step}")

        step_ok = len(reasons) == 0
        rows.append({
            "step": tag,
            "target_rate": target,
            "achieved_count": achieved_count,
            "achieved_rate": round(achieved_rate, 2),
            "hold_p50_ms": round(float(hold_p50), 2) if hold_p50 is not None else None,
            "hold_p95_ms": round(float(hold_p95), 2) if hold_p95 is not None else None,
            "hold_p99_ms": round(float(hold_p99), 2) if hold_p99 is not None else None,
            "checkout_p50_ms": round(float(chk_p50), 2) if chk_p50 is not None else None,
            "checkout_p95_ms": round(float(chk_p95), 2) if chk_p95 is not None else None,
            "checkout_p99_ms": round(float(chk_p99), 2) if chk_p99 is not None else None,
            "saga_p50_ms": round(float(saga_p50), 2) if saga_p50 is not None else None,
            "saga_p95_ms": round(float(saga_p95), 2) if saga_p95 is not None else None,
            "saga_p99_ms": round(float(saga_p99), 2) if saga_p99 is not None else None,
            "error_rate": round(error_rate, 6),
            "unknown_count": unknown_count,
            "unknown_rate": round(unknown_rate, 6),
            "dropped_iterations": dropped_step,
            "dropped_source": dropped_source,
            "pass": step_ok,
            "fail_reasons": reasons,
        })

    # Breakpoint = highest step such that THIS step AND ALL lower steps pass
    breakpoint_step = None
    breakpoint_rate = None
    for r in rows:
        if r["pass"]:
            breakpoint_step = r["step"]
            breakpoint_rate = int(r["target_rate"])
        else:
            break

    sustainable_rate, derivation = compute_sustainable_rate(breakpoint_rate, step_size)

    return {
        "scenario": scenario,
        "run_id": meta.get("run_id", "unknown"),
        "analyzed_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "unit": unit,
        "step_size": step_size,
        "step_seconds": int(meta.get("step_seconds") or 0),
        "global_dropped_iterations": global_dropped,
        "breakpoint_found": breakpoint_rate is not None,
        "breakpoint_step": breakpoint_step,
        "breakpoint_rate": breakpoint_rate,
        "sustainable_rate": sustainable_rate,
        "derivation": derivation,
        "steps": rows,
    }


def render_breakpoint_markdown(result):
    is_journey = result["scenario"] == "breakpoint-journey"
    unit = result["unit"]
    lines = []
    lines.append(f"### Breakpoint Analysis — `{result['scenario']}` (`{result['run_id']}`)")
    lines.append("")
    if is_journey:
        lines.append("| Step | Target (j/s) | Achieved (j/s) | Hold p50 / p95 / p99 (ms) | Checkout p50 / p95 / p99 (ms) | Saga p50 / p95 / p99 (ms) | Error Rate | Unknown (cnt / rate) | Dropped | Status |")
        lines.append("| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |")
    else:
        lines.append("| Step | Target (req/s) | Achieved (req/s) | Hold p50 (ms) | Hold p95 (ms) | Hold p99 (ms) | Error Rate | Unknown (cnt / rate) | Dropped | Status |")
        lines.append("| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |")

    def fms(v):
        return f"{v:.2f}" if v is not None else "n/a"

    for r in result["steps"]:
        st = "PASS" if r["pass"] else "FAIL"
        unk_str = f"{r['unknown_count']} ({r['unknown_rate'] * 100:.3f}%)"
        err_str = f"{r['error_rate'] * 100:.3f}%"
        if is_journey:
            h_str = f"{fms(r['hold_p50_ms'])} / {fms(r['hold_p95_ms'])} / {fms(r['hold_p99_ms'])}"
            c_str = f"{fms(r['checkout_p50_ms'])} / {fms(r['checkout_p95_ms'])} / {fms(r['checkout_p99_ms'])}"
            s_str = f"{fms(r['saga_p50_ms'])} / {fms(r['saga_p95_ms'])} / {fms(r['saga_p99_ms'])}"
            lines.append(
                f"| `{r['step']}` | {r['target_rate']:.0f} | {r['achieved_rate']:.2f} | {h_str} | {c_str} | {s_str} | {err_str} | {unk_str} | {r['dropped_iterations']} | **{st}** |"
            )
        else:
            lines.append(
                f"| `{r['step']}` | {r['target_rate']:.0f} | {r['achieved_rate']:.2f} | {fms(r['hold_p50_ms'])} | {fms(r['hold_p95_ms'])} | {fms(r['hold_p99_ms'])} | {err_str} | {unk_str} | {r['dropped_iterations']} | **{st}** |"
            )

    lines.append("")
    if result["breakpoint_found"]:
        lines.append(f"- **Breakpoint**: `{result['breakpoint_rate']} {unit}` (step `{result['breakpoint_step']}`)")
        lines.append(f"- **Derived Sustainable Rate**: **`{result['sustainable_rate']} {unit}`**")
        lines.append(f"- **Derivation**: `{result['derivation']}`")
    else:
        lines.append("- **Breakpoint**: **NO BREAKPOINT FOUND** (step `s01` did not satisfy all criteria)")
        lines.append("- **Derived Sustainable Rate**: none (sustainable profile not created)")
    lines.append("")
    return "\n".join(lines)


def combine_sustainable_profile(hold_dir, journey_dir, out_path):
    hold_invalid = os.path.isfile(os.path.join(hold_dir, "INVALID"))
    journey_invalid = os.path.isfile(os.path.join(journey_dir, "INVALID"))
    if hold_invalid or journey_invalid:
        print("[breakpoint] Refusing to generate sustainable-profile.json: one or more breakpoint runs are INVALID.")
        return False

    hold_bp_path = os.path.join(hold_dir, "breakpoint.json")
    journey_bp_path = os.path.join(journey_dir, "breakpoint.json")
    if not os.path.isfile(hold_bp_path) or not os.path.isfile(journey_bp_path):
        print("[breakpoint] Missing breakpoint.json in hold or journey directory; sustainable-profile.json not created.")
        return False

    with open(hold_bp_path) as f:
        h = json.load(f)
    with open(journey_bp_path) as f:
        j = json.load(f)

    if not h.get("breakpoint_found") or not j.get("breakpoint_found"):
        print(
            f"[breakpoint] NO BREAKPOINT FOUND (hold_found={h.get('breakpoint_found')}, "
            f"journey_found={j.get('breakpoint_found')}); sustainable-profile.json not created."
        )
        return False

    profile = {
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rule": "sustainable = max(step_size, floor((0.70 * breakpoint) / step_size) * step_size)",
        "hold": {
            "breakpoint_run_id": h["run_id"],
            "breakpoint_step": h["breakpoint_step"],
            "breakpoint_rps": h["breakpoint_rate"],
            "step_rps": h["step_size"],
            "sustainable_rps": h["sustainable_rate"],
            "derivation": h["derivation"],
        },
        "journey": {
            "breakpoint_run_id": j["run_id"],
            "breakpoint_step": j["breakpoint_step"],
            "breakpoint_jps": j["breakpoint_rate"],
            "step_jps": j["step_size"],
            "sustainable_jps": j["sustainable_rate"],
            "derivation": j["derivation"],
        },
    }
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(profile, f, indent=2)
        f.write("\n")
    print(f"[breakpoint] Generated {out_path} (hold={profile['hold']['sustainable_rps']} req/s, journey={profile['journey']['sustainable_jps']} journeys/s)")
    return True


def main():
    parser = argparse.ArgumentParser(description="Analyze k6 breakpoint run and derive sustainable rate")
    parser.add_argument("results_dir", nargs="?", help="Results directory containing breakpoint-*-summary.json")
    parser.add_argument("--prometheus-url", default=os.environ.get("PROMETHEUS_URL", "http://127.0.0.1:9090"))
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--combine-profile", nargs=2, metavar=("HOLD_DIR", "JOURNEY_DIR"), help="Generate sustainable-profile.json from hold and journey breakpoint dirs")
    parser.add_argument("--profile-out", default="loadtest/sustainable-profile.json")
    args = parser.parse_args()

    if args.combine_profile:
        ok = combine_sustainable_profile(args.combine_profile[0], args.combine_profile[1], args.profile_out)
        sys.exit(0 if ok else 2)

    if not args.results_dir:
        parser.error("results_dir is required unless --combine-profile is used")

    rdir = os.path.abspath(args.results_dir)
    summary_path = None
    for candidate in ("breakpoint-hold-summary.json", "breakpoint-journey-summary.json"):
        p = os.path.join(rdir, candidate)
        if os.path.isfile(p):
            summary_path = p
            break
    if not summary_path:
        print(f"[ERROR] No breakpoint-*-summary.json found in {rdir}", file=sys.stderr)
        sys.exit(1)

    with open(summary_path) as f:
        sdata = json.load(f)

    result = analyze_breakpoint_summary(sdata, prom_url=args.prometheus_url, enable_live=not args.offline)
    out_json = os.path.join(rdir, "breakpoint.json")
    with open(out_json, "w") as f:
        json.dump(result, f, indent=2)
        f.write("\n")

    md = render_breakpoint_markdown(result)
    with open(os.path.join(rdir, "breakpoint.md"), "w") as f:
        f.write(md)

    print(md)
    sys.exit(0)


if __name__ == "__main__":
    main()
