#!/usr/bin/env python3
"""
scripts/loadtest-soak-report.py — Soak Drift Detector & Windowed Report Generator

Python 3 standard library only.
Computes 2-minute windowed metrics over a soak run from Prometheus and docker-stats.csv:
  - p99 of http_server_requests for hold (inventory-service POST /api/v1/reservations)
    and checkout (order-service POST /api/v1/orders/{orderId}/checkout)
  - JVM old-generation heap used (jvm_memory_used_bytes{area="heap",id="G1 Old Gen"},
    explicitly stated; fallback to total heap if old-gen label is unavailable)
  - GC pause seconds per 2-minute window per service (jvm_gc_pause_seconds_sum)
  - Consumer lag per 2-minute window per group
  - Outbox oldest pending age per 2-minute window per service
  - HikariCP pending per 2-minute window per service
  - Container memory (MiB) per 2-minute window from docker-stats.csv

Drift rules:
  DRIFT is flagged when ANY of:
    1) Last window p99 > 1.3 * first full window p99 (hold or checkout)
    2) Linear regression slope of heap after GC > 0 AND fitted total growth > 20%
    3) Consumer lag, outbox oldest pending age, or HikariCP pending grows
       monotonically over the last 10 minutes (last 5 consecutive 2-minute windows)

Soak verdict is PASS only when: no INVALID marker, no DRIFT, all k6 thresholds
passed, and post-run audit passed.
"""

import argparse
import csv
import datetime
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request


WINDOW_SECONDS = 120
MONOTONIC_TAIL_WINDOWS = 5  # 5 x 2 min = 10 minutes


def parse_mem_to_mib(val_str):
    if not val_str:
        return 0.0
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\s*$", val_str.strip())
    if not m:
        return 0.0
    num = float(m.group(1))
    unit = m.group(2).lower()
    if unit == "b":
        return num / (1024.0 * 1024.0)
    if unit in ("kib", "kb", "k"):
        return num / 1024.0
    if unit in ("mib", "mb", "m"):
        return num
    if unit in ("gib", "gb", "g"):
        return num * 1024.0
    if unit in ("tib", "tb", "t"):
        return num * 1024.0 * 1024.0
    return num


def prom_query_at(prom_url, expr, eval_time):
    params = {"query": expr, "time": str(int(eval_time))}
    url = f"{prom_url.rstrip('/')}/api/v1/query?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if payload.get("status") != "success":
            return None
        return payload.get("data", {}).get("result", [])
    except Exception:
        return None


def safe_float(val):
    try:
        v = float(val)
        return None if (math.isnan(v) or math.isinf(v)) else v
    except (TypeError, ValueError):
        return None


def linear_regression(xs, ys):
    """Returns (slope, intercept) for simple OLS over (xs, ys), or (0.0, 0.0) if < 2 points."""
    n = len(xs)
    if n < 2:
        return 0.0, (ys[0] if n == 1 else 0.0)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    if denom == 0:
        return 0.0, mean_y
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denom
    intercept = mean_y - slope * mean_x
    return slope, intercept


def is_strictly_monotonic_increasing(vals, min_points=MONOTONIC_TAIL_WINDOWS):
    clean = [v for v in vals if v is not None]
    if len(clean) < min_points:
        return False
    tail = clean[-min_points:]
    if tail[-1] <= 0:
        return False
    return all(tail[i + 1] > tail[i] for i in range(len(tail) - 1))


def detect_drift(windows):
    """
    Pure drift-detection function over a list of 2-minute window dicts:
      {
        "window_index": int,
        "end_epoch": int,
        "hold_p99_ms": float|None,
        "checkout_p99_ms": float|None,
        "heap_old_gen_mib": {service: float},
        "gc_pause_s": {service: float},
        "consumer_lag": {group: float},
        "outbox_age_s": {service: float},
        "hikari_pending": {service: float},
        "container_mem_mib": {container: float},
      }
    Returns {"drift_detected": bool, "findings": [str, ...], "details": {...}}.
    """
    findings = []
    details = {"p99_ratios": {}, "heap_regressions": {}, "monotonic_checks": {}}

    if len(windows) < 2:
        return {"drift_detected": False, "findings": [], "details": details}

    # 1. p99 drift: last window p99 > 1.3 * first full window p99
    for field, label in (("hold_p99_ms", "hold"), ("checkout_p99_ms", "checkout")):
        vals = [w.get(field) for w in windows if w.get(field) is not None and w.get(field) > 0]
        if len(vals) >= 2:
            first_v = vals[0]
            last_v = vals[-1]
            ratio = last_v / first_v
            details["p99_ratios"][label] = {
                "first_ms": round(first_v, 2),
                "last_ms": round(last_v, 2),
                "ratio": round(ratio, 3),
            }
            if ratio > 1.3:
                findings.append(
                    f"DRIFT(p99:{label}): last window p99 ({last_v:.2f} ms) exceeds 1.3x first full window ({first_v:.2f} ms, ratio={ratio:.2f}x)"
                )

    # 2. Heap after GC drift: linear regression slope > 0 and total fitted growth > 20%
    heap_services = sorted({svc for w in windows for svc in (w.get("heap_old_gen_mib") or {}).keys()})
    for svc in heap_services:
        pts = [
            (idx, w["heap_old_gen_mib"][svc])
            for idx, w in enumerate(windows)
            if svc in (w.get("heap_old_gen_mib") or {}) and w["heap_old_gen_mib"][svc] is not None
        ]
        if len(pts) >= 2:
            xs = [float(p[0]) for p in pts]
            ys = [float(p[1]) for p in pts]
            slope, intercept = linear_regression(xs, ys)
            fitted_start = intercept + slope * xs[0]
            total_growth = slope * (xs[-1] - xs[0])
            growth_ratio = (total_growth / fitted_start) if fitted_start > 0 else 0.0
            details["heap_regressions"][svc] = {
                "slope_mib_per_window": round(slope, 4),
                "fitted_start_mib": round(fitted_start, 2),
                "fitted_end_mib": round(fitted_start + total_growth, 2),
                "growth_pct": round(growth_ratio * 100.0, 2),
            }
            if slope > 0 and growth_ratio > 0.20:
                findings.append(
                    f"DRIFT(heap:{svc}): positive old-gen heap slope ({slope:.2f} MiB/window) with {growth_ratio * 100:.1f}% fitted growth (>20%)"
                )

    # 3. Monotonic growth over the last 10 minutes (5 consecutive 2-min windows) for lag, outbox age, Hikari pending
    for metric_key, label in (
        ("consumer_lag", "consumer_lag"),
        ("outbox_age_s", "outbox_oldest_pending_age"),
        ("hikari_pending", "hikari_pending"),
    ):
        targets = sorted({k for w in windows for k in (w.get(metric_key) or {}).keys()})
        for target in targets:
            series = [(w.get(metric_key) or {}).get(target) for w in windows]
            mono = is_strictly_monotonic_increasing(series, MONOTONIC_TAIL_WINDOWS)
            details["monotonic_checks"][f"{label}:{target}"] = {
                "monotonic_last_10m": mono,
                "tail_values": [round(v, 3) if v is not None else None for v in series[-MONOTONIC_TAIL_WINDOWS:]],
            }
            if mono:
                tail_str = " -> ".join(f"{v:.3f}" for v in series[-MONOTONIC_TAIL_WINDOWS:] if v is not None)
                findings.append(
                    f"DRIFT({label}:{target}): strictly monotonic growth over last 10 minutes ({tail_str})"
                )

    return {
        "drift_detected": len(findings) > 0,
        "findings": findings,
        "details": details,
    }


def collect_docker_window_mem(docker_stats_csv, window_start, window_end):
    if not os.path.isfile(docker_stats_csv):
        return {}
    buckets = {}
    with open(docker_stats_csv, newline="") as f:
        for row in csv.DictReader(f):
            ts_str = (row.get("timestamp_utc") or "").strip()
            c = (row.get("container") or "").strip()
            if not ts_str or not c or c.startswith("k6-"):
                continue
            try:
                t = int(datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp())
            except ValueError:
                continue
            if window_start <= t <= window_end:
                mib = parse_mem_to_mib(row.get("mem_usage", "0MiB"))
                buckets.setdefault(c, []).append(mib)
    return {c: round(max(vs), 1) for c, vs in sorted(buckets.items())}


def collect_soak_windows(results_dir, prom_url, start_epoch, end_epoch):
    docker_csv = os.path.join(results_dir, "docker-stats.csv")
    windows = []
    heap_metric_used = 'jvm_memory_used_bytes{area="heap",id="G1 Old Gen"}'

    # Build full 2-minute windows across [start_epoch, end_epoch]
    w_end = start_epoch + WINDOW_SECONDS
    idx = 1
    while w_end <= end_epoch:
        w_start = w_end - WINDOW_SECONDS
        row = {
            "window_index": idx,
            "start_epoch": w_start,
            "end_epoch": w_end,
            "label": f"W{idx:02d} ({(w_start - start_epoch)//60}-{(w_end - start_epoch)//60}m)",
            "hold_p99_ms": None,
            "checkout_p99_ms": None,
            "heap_old_gen_mib": {},
            "gc_pause_s": {},
            "consumer_lag": {},
            "outbox_age_s": {},
            "hikari_pending": {},
            "container_mem_mib": collect_docker_window_mem(docker_csv, w_start, w_end),
        }

        h_res = prom_query_at(
            prom_url,
            'histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",method="POST"}[2m])))',
            w_end,
        ) or []
        if h_res:
            v = safe_float(h_res[0]["value"][1])
            row["hold_p99_ms"] = round(v * 1000.0, 2) if v is not None else None

        c_res = prom_query_at(
            prom_url,
            'histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="order-service",uri="/api/v1/orders/{orderId}/checkout",method="POST"}[2m])))',
            w_end,
        ) or []
        if c_res:
            v = safe_float(c_res[0]["value"][1])
            row["checkout_p99_ms"] = round(v * 1000.0, 2) if v is not None else None

        # Old-gen heap after GC proxy: minimum G1 Old Gen bytes over the 2-minute window (tracks post-GC floor)
        heap_res = prom_query_at(
            prom_url,
            'min_over_time(jvm_memory_used_bytes{area="heap",id="G1 Old Gen"}[2m:10s])',
            w_end,
        ) or []
        if not heap_res:
            heap_metric_used = 'sum by (service) (min_over_time(jvm_memory_used_bytes{area="heap"}[2m:10s]))'
            heap_res = prom_query_at(prom_url, heap_metric_used, w_end) or []
        for r in heap_res:
            svc = r["metric"].get("service", "unknown")
            v = safe_float(r["value"][1])
            if v is not None:
                row["heap_old_gen_mib"][svc] = round(v / (1024.0 * 1024.0), 2)

        gc_res = prom_query_at(prom_url, "sum by (service) (increase(jvm_gc_pause_seconds_sum[2m]))", w_end) or []
        for r in gc_res:
            v = safe_float(r["value"][1])
            if v is not None:
                row["gc_pause_s"][r["metric"].get("service", "unknown")] = round(v, 3)

        lag_res = prom_query_at(
            prom_url,
            "max_over_time((sum by (consumergroup) (clamp_min(kafka_consumergroup_lag, 0)))[2m:15s])",
            w_end,
        ) or []
        for r in lag_res:
            v = safe_float(r["value"][1])
            if v is not None:
                row["consumer_lag"][r["metric"].get("consumergroup", "unknown")] = round(v, 1)

        outbox_res = prom_query_at(prom_url, "max_over_time(outbox_oldest_pending_age_seconds[2m:10s])", w_end) or []
        for r in outbox_res:
            v = safe_float(r["value"][1])
            if v is not None:
                row["outbox_age_s"][r["metric"].get("service", "unknown")] = round(v, 3)

        hik_res = prom_query_at(prom_url, "max_over_time(hikaricp_connections_pending[2m:10s])", w_end) or []
        for r in hik_res:
            v = safe_float(r["value"][1])
            if v is not None:
                row["hikari_pending"][r["metric"].get("service", "unknown")] = round(v, 1)

        windows.append(row)
        idx += 1
        w_end += WINDOW_SECONDS

    return windows, heap_metric_used


def evaluate_soak_run(results_dir, prom_url="http://127.0.0.1:9090", enable_live=True):
    results_dir = os.path.abspath(results_dir)
    invalid_path = os.path.join(results_dir, "INVALID")
    is_invalid = os.path.isfile(invalid_path)
    invalid_reason = open(invalid_path).read().strip() if is_invalid else ""

    soak_summary = {}
    sp = os.path.join(results_dir, "soak-summary.json")
    if os.path.isfile(sp):
        with open(sp) as f:
            soak_summary = json.load(f)

    run_meta = {}
    mp = os.path.join(results_dir, "run-meta.json")
    if os.path.isfile(mp):
        with open(mp) as f:
            run_meta = json.load(f)

    audit = None
    ap = os.path.join(results_dir, "audit.json")
    if os.path.isfile(ap):
        with open(ap) as f:
            audit = json.load(f)

    smeta = soak_summary.get("_meta", {})
    start_e = int(smeta.get("plateau_start_epoch") or run_meta.get("start_epoch") or 0)
    end_e = int(smeta.get("plateau_end_epoch") or run_meta.get("end_epoch") or 0)

    if enable_live and start_e > 0 and end_e > start_e:
        windows, heap_metric_used = collect_soak_windows(results_dir, prom_url, start_e, end_e)
    else:
        windows, heap_metric_used = [], 'min_over_time(jvm_memory_used_bytes{area="heap",id="G1 Old Gen"}[2m:10s])'

    drift = detect_drift(windows)

    # Check k6 thresholds
    k6_thresholds = []
    for mname, mdata in sorted(soak_summary.get("metrics", {}).items()):
        for expr, tinfo in sorted((mdata.get("thresholds") or {}).items()):
            k6_thresholds.append({"metric": mname, "expr": expr, "ok": bool(tinfo.get("ok", False))})
    thresholds_ok = bool(k6_thresholds) and all(t["ok"] for t in k6_thresholds)

    audit_ok = bool(audit) and audit.get("overall") == "PASS"

    if is_invalid:
        verdict = "INVALID (NOT COUNTED)"
        passed = False
    else:
        passed = (not drift["drift_detected"]) and thresholds_ok and audit_ok
        verdict = "PASS" if passed else "FAIL"

    report_obj = {
        "run_id": smeta.get("run_id") or run_meta.get("run_id") or os.path.basename(results_dir),
        "verdict": verdict,
        "passed": passed,
        "invalid": is_invalid,
        "invalid_reason": invalid_reason,
        "thresholds_ok": thresholds_ok,
        "audit_ok": audit_ok,
        "drift_detected": drift["drift_detected"],
        "drift_findings": drift["findings"],
        "drift_details": drift["details"],
        "heap_metric_used": heap_metric_used,
        "window_seconds": WINDOW_SECONDS,
        "windows": windows,
    }
    return report_obj


def render_soak_markdown(obj):
    lines = []
    lines.append(f"### Soak Stability & Drift Analysis (`{obj['run_id']}`)")
    lines.append("")
    if obj["invalid"]:
        lines.append(f"> [!CAUTION]")
        lines.append(f"> **RUN INVALID — NOT COUNTED**: {obj['invalid_reason']}")
        lines.append("")
    lines.append(f"- **Soak Verdict**: **{obj['verdict']}**")
    lines.append(f"- **Drift Flagged**: **{'YES (DRIFT)' if obj['drift_detected'] else 'NO'}**")
    lines.append(f"- **k6 Thresholds**: **{'PASS' if obj['thresholds_ok'] else 'FAIL'}**")
    lines.append(f"- **Post-Run Audit**: **{'PASS' if obj['audit_ok'] else 'FAIL'}**")
    lines.append(f"- **Old-Gen Heap Metric Used**: `{obj['heap_metric_used']}` (2-minute window floor approximating post-GC old-generation occupancy)")
    lines.append("")

    if obj["drift_findings"]:
        lines.append("#### Drift Flags Raised")
        lines.append("")
        for f in obj["drift_findings"]:
            lines.append(f"- {f}")
        lines.append("")

    if obj["windows"]:
        lines.append("#### 2-Minute Windowed Time Series")
        lines.append("")
        lines.append("| Window | Hold p99 (ms) | Checkout p99 (ms) | Max Lag | Max Outbox Age (s) | Max Hikari Pend | Heap Old Gen MiB (gw/inv/ord/pay) | GC Pause s (gw/inv/ord/pay) |")
        lines.append("| :--- | ---: | ---: | ---: | ---: | ---: | :--- | :--- |")
        for w in obj["windows"]:
            hp = f"{w['hold_p99_ms']:.2f}" if w["hold_p99_ms"] is not None else "n/a"
            cp = f"{w['checkout_p99_ms']:.2f}" if w["checkout_p99_ms"] is not None else "n/a"
            mlag = max(w["consumer_lag"].values()) if w["consumer_lag"] else 0.0
            mout = max(w["outbox_age_s"].values()) if w["outbox_age_s"] else 0.0
            mhik = max(w["hikari_pending"].values()) if w["hikari_pending"] else 0.0
            svcs = ("gateway-service", "inventory-service", "order-service", "payment-service")
            heap_s = "/".join(f"{w['heap_old_gen_mib'].get(s, 0.0):.1f}" for s in svcs)
            gc_s = "/".join(f"{w['gc_pause_s'].get(s, 0.0):.3f}" for s in svcs)
            lines.append(f"| `{w['label']}` | {hp} | {cp} | {mlag:.1f} | {mout:.3f} | {mhik:.1f} | {heap_s} | {gc_s} |")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Analyze soak run for latency/memory/queue drift")
    parser.add_argument("results_dir", help="Soak results directory")
    parser.add_argument("--prometheus-url", default=os.environ.get("PROMETHEUS_URL", "http://127.0.0.1:9090"))
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()

    obj = evaluate_soak_run(args.results_dir, prom_url=args.prometheus_url, enable_live=not args.offline)
    with open(os.path.join(args.results_dir, "soak-report.json"), "w") as f:
        json.dump(obj, f, indent=2)
        f.write("\n")
    md = render_soak_markdown(obj)
    with open(os.path.join(args.results_dir, "soak-report.md"), "w") as f:
        f.write(md)
    print(md)
    sys.exit(0 if obj["passed"] else 1)


if __name__ == "__main__":
    main()
