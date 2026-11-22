#!/usr/bin/env python3
"""
scripts/loadtest-report.py — Reproducible Load Test Report Generator

Python 3 standard library only.
Reads k6 summary JSONs, docker-stats.csv, container snapshots, DLT snapshots,
audit.json, and Prometheus API metrics for the run window, and renders
results/<ts>/report.md.

Publishes to docs/load-test-report.md ONLY when --publish (or LOAD_PUBLISH=true)
is passed AND every scenario threshold AND every audit item A1..A9 passed.
"""

import argparse
import csv
import datetime
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request


def status_str(passed):
    return "PASS" if passed else "FAIL"


def run_cmd(cmd, timeout=5):
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=timeout)
        return out.decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def parse_mem_to_mib(val_str):
    """Converts docker stats memory strings (e.g. '123.4MiB', '1.2GiB', '512KiB') to MiB."""
    if not val_str:
        return 0.0
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\s*$", val_str.strip())
    if not m:
        return 0.0
    num = float(m.group(1))
    unit = m.group(2).lower()
    if unit in ("b",):
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


def fmt_bytes_limit(limit_bytes):
    b = int(limit_bytes or 0)
    if b <= 0:
        return "unlimited"
    mib = b / (1024.0 * 1024.0)
    if mib >= 1024.0:
        return f"{mib / 1024.0:.2f} GiB"
    return f"{mib:.0f} MiB"


def collect_environment(results_dir, run_meta, enable_live=True):
    env = {}
    sys_name = platform.system()
    if sys_name == "Darwin":
        prod_ver = run_cmd(["sw_vers", "-productVersion"])
        build_ver = run_cmd(["sw_vers", "-buildVersion"])
        env["os"] = f"macOS {prod_ver} ({build_ver}) {platform.machine()}" if prod_ver else platform.platform()
        cpu_model = run_cmd(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor() or "unknown"
        mem_bytes_s = run_cmd(["sysctl", "-n", "hw.memsize"])
        total_mem_gib = f"{int(mem_bytes_s) / (1024 ** 3):.1f} GiB" if mem_bytes_s.isdigit() else "unknown"
    else:
        env["os"] = platform.platform()
        cpu_model = platform.processor() or "unknown"
        if os.path.isfile("/proc/cpuinfo"):
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        cpu_model = line.split(":", 1)[1].strip()
                        break
        total_mem_gib = "unknown"
        if os.path.isfile("/proc/meminfo"):
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        total_mem_gib = f"{kb / (1024 ** 2):.1f} GiB"
                        break

    env["cpu_model"] = cpu_model
    env["cpu_cores"] = str(os.cpu_count() or "unknown")
    env["host_memory"] = total_mem_gib
    env["k6_image"] = run_meta.get("k6_image", "unknown")

    if enable_live:
        dinfo = run_cmd(["docker", "info", "--format", "{{.NCPU}}|{{.MemTotal}}"])
        if "|" in dinfo:
            ncpu, memtot = dinfo.split("|", 1)
            env["docker_vm_cpus"] = ncpu.strip()
            env["docker_vm_memory"] = (
                f"{int(memtot.strip()) / (1024 ** 3):.2f} GiB" if memtot.strip().isdigit() else memtot.strip()
            )
        else:
            env["docker_vm_cpus"] = "unavailable"
            env["docker_vm_memory"] = "unavailable"

        java_ver_raw = run_cmd(["docker", "exec", "inventory-service", "java", "-version"])
        java_lines = [ln.strip() for ln in java_ver_raw.splitlines() if ln.strip() and not ln.startswith("Picked up ")]
        env["java_version"] = java_lines[0] if java_lines else (java_ver_raw.splitlines()[0] if java_ver_raw else "unavailable")

        pg_ver_raw = run_cmd(["docker", "exec", "postgres-inventory", "postgres", "--version"])
        env["postgres_version"] = pg_ver_raw or "unavailable"
    else:
        env["docker_vm_cpus"] = "n/a (offline)"
        env["docker_vm_memory"] = "n/a (offline)"
        env["java_version"] = "n/a (offline)"
        env["postgres_version"] = "n/a (offline)"

    # Container count & service memory limits from snapshot
    snap_path = os.path.join(results_dir, "containers-end.csv")
    if not os.path.isfile(snap_path):
        snap_path = os.path.join(results_dir, "containers-start.csv")
    mem_limits = {}
    if os.path.isfile(snap_path):
        with open(snap_path, newline="") as f:
            for row in csv.DictReader(f):
                mem_limits[row["container"]] = fmt_bytes_limit(row.get("mem_limit_bytes", "0"))
    env["container_count"] = len(mem_limits)
    env["service_memory_limits"] = mem_limits
    return env


def prom_query(prom_url, expr, eval_time=None):
    params = {"query": expr}
    if eval_time is not None:
        params["time"] = str(int(eval_time))
    url = f"{prom_url.rstrip('/')}/api/v1/query?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if payload.get("status") != "success":
            return None
        return payload.get("data", {}).get("result", [])
    except Exception:
        return None


def collect_prometheus_metrics(prom_url, run_meta, enable_live=True):
    if not enable_live:
        return {"available": False, "note": "Prometheus live queries skipped (offline mode)"}

    start_e = int(run_meta.get("start_epoch") or 0)
    end_e = int(run_meta.get("end_epoch") or 0)
    if start_e > 0 and end_e > start_e:
        window_s = max(30, end_e - start_e + 15)
        eval_t = end_e
    else:
        window_s = 300
        eval_t = None

    # Probe reachability
    probe = prom_query(prom_url, "up", eval_t)
    if probe is None:
        return {"available": False, "note": f"Prometheus unreachable at {prom_url}"}

    out = {"available": True, "window_seconds": window_s}

    # 1. Peak consumer lag per group
    lag_res = prom_query(
        prom_url,
        f"max_over_time((sum by (consumergroup) (clamp_min(kafka_consumergroup_lag, 0)))[{window_s}s:15s])",
        eval_t,
    ) or []
    out["peak_consumer_lag"] = {
        r["metric"].get("consumergroup", "unknown"): float(r["value"][1])
        for r in lag_res if "value" in r
    }

    # 2. Peak outbox oldest pending age per service
    outbox_res = prom_query(
        prom_url,
        f"max_over_time(outbox_oldest_pending_age_seconds[{window_s}s:5s])",
        eval_t,
    ) or []
    out["peak_outbox_oldest_age_s"] = {
        r["metric"].get("service", r["metric"].get("job", "unknown")): float(r["value"][1])
        for r in outbox_res if "value" in r
    }

    # 3. Peak JVM heap used ratio per service
    heap_res = prom_query(
        prom_url,
        f'max_over_time((sum by (service) (jvm_memory_used_bytes{{area="heap"}}) / sum by (service) (jvm_memory_max_bytes{{area="heap"}} > 0))[{window_s}s:5s])',
        eval_t,
    ) or []
    out["peak_jvm_heap_ratio"] = {
        r["metric"].get("service", "unknown"): float(r["value"][1])
        for r in heap_res if "value" in r
    }

    # 4. Gateway 5xx and 429 counts during window
    g5xx = prom_query(
        prom_url,
        f'sum(increase(gateway_requests_total{{status_class="5xx"}}[{window_s}s])) or vector(0)',
        eval_t,
    ) or []
    out["gateway_5xx_count"] = float(g5xx[0]["value"][1]) if g5xx else 0.0

    g429 = prom_query(
        prom_url,
        f"sum(increase(gateway_rate_limited_total[{window_s}s])) or vector(0)",
        eval_t,
    ) or []
    out["gateway_429_count"] = float(g429[0]["value"][1]) if g429 else 0.0

    # 5. Max in-flight checkouts (saga PAYMENT_PENDING delta + active HTTP POST checkouts)
    inflight_saga = prom_query(
        prom_url,
        (
            f'max_over_time(clamp_min('
            f'(sum(ticketing_orders_checkout_total{{service="order-service",outcome="ACCEPTED"}}) or vector(0)) - '
            f'(sum(ticketing_orders_terminal_total{{service="order-service",final_status=~"CONFIRMED|CANCELLED_PAYMENT_FAILED|CANCELLED_REFUNDED"}}) or vector(0)),'
            f' 0)[{window_s}s:5s])'
        ),
        eval_t,
    ) or []
    out["max_inflight_checkouts_saga"] = float(inflight_saga[0]["value"][1]) if inflight_saga else 0.0

    inflight_http = prom_query(
        prom_url,
        f'max_over_time((sum(http_server_requests_active_seconds_gcount{{service="order-service",method="POST"}}) or vector(0))[{window_s}s:5s])',
        eval_t,
    ) or []
    out["max_inflight_checkouts_http"] = float(inflight_http[0]["value"][1]) if inflight_http else 0.0

    # 6. 5xx count by service over run window
    svc_5xx_res = prom_query(
        prom_url,
        f'sum by (service) (increase(http_server_requests_seconds_count{{status=~"5.."}}[{window_s}s]))',
        eval_t,
    ) or []
    svc_5xx = {
        r["metric"].get("service", "unknown"): float(r["value"][1])
        for r in svc_5xx_res if "value" in r
    }
    svc_5xx["gateway-service"] = max(svc_5xx.get("gateway-service", 0.0), out["gateway_5xx_count"])
    out["service_5xx_counts"] = svc_5xx

    # 7. Peak HikariCP pending connections per service
    hikari_res = prom_query(
        prom_url,
        f"max_over_time(hikaricp_connections_pending[{window_s}s:5s])",
        eval_t,
    ) or []
    out["peak_hikari_pending"] = {
        r["metric"].get("service", r["metric"].get("pool", "unknown")): float(r["value"][1])
        for r in hikari_res if "value" in r
    }

    # 8. Total JVM GC pause seconds per service over run window (jvm_gc_pause_seconds_sum)
    gc_res = prom_query(
        prom_url,
        f"sum by (service) (increase(jvm_gc_pause_seconds_sum[{window_s}s]))",
        eval_t,
    ) or []
    out["total_jvm_gc_pause_seconds"] = {
        r["metric"].get("service", "unknown"): float(r["value"][1])
        for r in gc_res if "value" in r
    }

    return out


def extract_thresholds(k6_summary):
    """Returns list of dicts: {metric, expr, measured, ok} computed strictly from k6 summary."""
    results = []
    metrics = k6_summary.get("metrics", {})
    for metric_name, mdata in sorted(metrics.items()):
        thresholds = mdata.get("thresholds")
        if not thresholds:
            continue
        vals = mdata.get("values", {})
        mtype = mdata.get("type", "")
        for expr, tinfo in sorted(thresholds.items()):
            ok = bool(tinfo.get("ok", False))
            measured = format_threshold_measured(expr, vals, mtype)
            results.append({
                "metric": metric_name,
                "expr": expr,
                "measured": measured,
                "ok": ok,
            })
    return results


def format_threshold_measured(expr, vals, mtype):
    m = re.match(r"^\s*(p\(\d+(?:\.\d+)?\)|avg|min|med|max|count|rate|value)\s*([<>=!]+)", expr)
    if m:
        stat_key = m.group(1)
        if stat_key in vals:
            v = vals[stat_key]
            if stat_key == "count":
                return str(int(v))
            if stat_key == "rate":
                return f"{float(v):.6f}"
            if mtype == "trend":
                return f"{float(v):.2f} ms"
            return f"{float(v):.4f}"
    if "count" in vals:
        return str(int(vals["count"]))
    if "rate" in vals:
        return f"{float(vals['rate']):.6f}"
    if "p(99)" in vals:
        return f"{float(vals['p(99)']):.2f} ms"
    return "n/a"


def trend_row(label, metrics_dict, metric_key):
    m = metrics_dict.get(metric_key)
    if not m or "values" not in m:
        return f"| {label} | n/a | n/a | n/a | n/a | n/a |"
    v = m["values"]
    def fmt(k):
        return f"{float(v[k]):.2f} ms" if k in v and v[k] is not None else "n/a"
    p50 = fmt("med") if "med" in v else fmt("p(50)")
    return f"| {label} | {p50} | {fmt('p(90)')} | {fmt('p(95)')} | {fmt('p(99)')} | {fmt('max')} |"


def parse_docker_stats(csv_path):
    """Returns per-container {avg_cpu, peak_cpu, avg_mem_mib, peak_mem_mib, peak_mem_pct, samples}."""
    if not os.path.isfile(csv_path):
        return {}
    buckets = {}
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            c = row.get("container", "").strip()
            if not c:
                continue
            try:
                cpu = float(row.get("cpu_pct", "0") or 0)
            except ValueError:
                cpu = 0.0
            mem_mib = parse_mem_to_mib(row.get("mem_usage", "0MiB"))
            try:
                mem_pct = float(row.get("mem_pct", "0") or 0)
            except ValueError:
                mem_pct = 0.0
            b = buckets.setdefault(c, {"cpus": [], "mems": [], "mem_pcts": []})
            b["cpus"].append(cpu)
            b["mems"].append(mem_mib)
            b["mem_pcts"].append(mem_pct)

    summary = {}
    for c, b in sorted(buckets.items()):
        n = len(b["cpus"])
        summary[c] = {
            "samples": n,
            "avg_cpu": sum(b["cpus"]) / n if n else 0.0,
            "peak_cpu": max(b["cpus"]) if n else 0.0,
            "avg_mem_mib": sum(b["mems"]) / n if n else 0.0,
            "peak_mem_mib": max(b["mems"]) if n else 0.0,
            "peak_mem_pct": max(b["mem_pcts"]) if n else 0.0,
        }
    return summary


def summarize_container_health(results_dir):
    start_p = os.path.join(results_dir, "containers-start.csv")
    end_p = os.path.join(results_dir, "containers-end.csv")
    if not os.path.isfile(start_p) or not os.path.isfile(end_p):
        return None
    start = {}
    with open(start_p, newline="") as f:
        for row in csv.DictReader(f):
            start[row["container"]] = int(row.get("restart_count", "0") or 0)
    restarts = []
    ooms = []
    count = 0
    with open(end_p, newline="") as f:
        for row in csv.DictReader(f):
            count += 1
            c = row["container"]
            r_end = int(row.get("restart_count", "0") or 0)
            r_start = start.get(c, 0)
            if r_end > r_start:
                restarts.append(f"{c} ({r_start}->{r_end})")
            if str(row.get("oom_killed", "false")).lower() == "true":
                ooms.append(c)
    return {"checked": count, "restarts": restarts, "ooms": ooms}


def generate_report(results_dir, prom_url="http://127.0.0.1:9090", publish=False,
                    publish_dest=None, enable_live=True):
    results_dir = os.path.abspath(results_dir)
    if not os.path.isdir(results_dir):
        raise FileNotFoundError(f"Results directory not found: {results_dir}")

    def load_json_if_exists(path, default=None):
        if not os.path.isfile(path):
            return default
        with open(path) as f:
            return json.load(f)

    meta_path = os.path.join(results_dir, "run-meta.json")
    run_meta = load_json_if_exists(meta_path, {})

    flash_path = os.path.join(results_dir, "flash-sale-summary.json")
    journey_path = os.path.join(results_dir, "checkout-journey-summary.json")
    audit_path = os.path.join(results_dir, "audit.json")

    flash_data = load_json_if_exists(flash_path)
    journey_data = load_json_if_exists(journey_path)
    audit_data = load_json_if_exists(audit_path)

    env = collect_environment(results_dir, run_meta, enable_live=enable_live)
    prom = collect_prometheus_metrics(prom_url, run_meta, enable_live=enable_live)
    if prom.get("available"):
        with open(os.path.join(results_dir, "prometheus-metrics.json"), "w") as f:
            json.dump(prom, f, indent=2)
    docker_stats = parse_docker_stats(os.path.join(results_dir, "docker-stats.csv"))
    container_health = summarize_container_health(results_dir)

    flash_thresholds = extract_thresholds(flash_data) if flash_data else []
    journey_thresholds = extract_thresholds(journey_data) if journey_data else []

    all_thresholds = flash_thresholds + journey_thresholds
    scenarios_present = bool(flash_data or journey_data)
    thresholds_ok = scenarios_present and all(t["ok"] for t in all_thresholds)

    if audit_data is None:
        audit_status = "NOT RUN"
        audit_ok = False
    else:
        audit_items = audit_data.get("items", [])
        q_ok = audit_data.get("quiescence", {}).get("status") == "PASS"
        audit_ok = bool(audit_items) and q_ok and all(it.get("status") == "PASS" for it in audit_items)
        audit_status = status_str(audit_ok)

    overall_ok = thresholds_ok and audit_ok
    overall_status = status_str(overall_ok) if audit_data is not None else ("FAIL" if not thresholds_ok else "INCOMPLETE (AUDIT NOT RUN)")

    run_id = run_meta.get("run_id") or os.path.basename(results_dir)
    generated_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines = []
    lines.append(f"# Ticketing Platform Load Test Report — `{run_id}`")
    lines.append("")
    lines.append(f"- **Generated (UTC)**: `{generated_at}`")
    lines.append(f"- **Results Directory**: `{results_dir}`")
    lines.append(f"- **Load Scale**: `{run_meta.get('load_scale', 'unknown')}`")
    lines.append(f"- **Overall Verdict**: **{overall_status}**")
    lines.append(f"- **Thresholds Verdict**: **{status_str(thresholds_ok) if scenarios_present else 'NOT RUN'}** ({sum(1 for t in all_thresholds if t['ok'])}/{len(all_thresholds)} passed)")
    lines.append(f"- **Post-Run Audit Verdict**: **{audit_status}**")
    lines.append("")

    # 1. Environment
    lines.append("## 1. Environment")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("| :--- | :--- |")
    lines.append(f"| Host OS | `{env['os']}` |")
    lines.append(f"| Host CPU | `{env['cpu_model']}` ({env['cpu_cores']} logical cores) |")
    lines.append(f"| Host Memory | `{env['host_memory']}` |")
    lines.append(f"| Docker VM CPU / Memory | `{env['docker_vm_cpus']}` CPUs / `{env['docker_vm_memory']}` |")
    lines.append(f"| k6 Image | `{env['k6_image']}` |")
    lines.append(f"| Java Version (container) | `{env['java_version']}` |")
    lines.append(f"| PostgreSQL Version | `{env['postgres_version']}` |")
    lines.append(f"| Monitored Containers | `{env['container_count']}` |")
    lines.append("")

    if env["service_memory_limits"]:
        lines.append("### Service Memory Limits")
        lines.append("")
        lines.append("| Container | Memory Limit |")
        lines.append("| :--- | :--- |")
        for cname, lim in sorted(env["service_memory_limits"].items()):
            lines.append(f"| `{cname}` | `{lim}` |")
        lines.append("")

    # 2. Scenario A: Flash Sale
    lines.append("## 2. Scenario A — Flash Sale (`loadtest/flash-sale.js`)")
    lines.append("")
    if flash_data is None:
        lines.append("_Scenario A was NOT RUN in this results directory._")
        lines.append("")
    else:
        fm = flash_data.get("metrics", {})
        fmeta = flash_data.get("_meta", {})
        stages = fmeta.get("stages", [])
        stage_desc = ", ".join(f"{s['name']}:{s['duration_s']}s->{s['target_rps']}rps" for s in stages) if stages else "n/a"

        lines.append("### Configuration")
        lines.append("")
        lines.append(f"- **Executor**: `ramping-arrival-rate` (scale=`{fmeta.get('load_scale', 'n/a')}`)")
        lines.append(f"- **Stages**: `{stage_desc}`")
        lines.append(f"- **VUs**: `preAllocatedVUs={fmeta.get('pre_allocated_vus', 'n/a')}`, `maxVUs={fmeta.get('max_vus', 'n/a')}`")
        lines.append(f"- **Pool / Inventory**: `{fmeta.get('user_pool_size', 'n/a')}` distinct users, `{fmeta.get('seats_per_event', 'n/a')}` seats (uniform random contention)")
        lines.append("")

        lines.append("### Thresholds")
        lines.append("")
        lines.append("| Metric | Threshold | Measured | Status |")
        lines.append("| :--- | :--- | :--- | :--- |")
        for t in flash_thresholds:
            lines.append(f"| `{t['metric']}` | `{t['expr']}` | `{t['measured']}` | **{status_str(t['ok'])}** |")
        lines.append("")

        def cnt(m_dict, key):
            return int(m_dict.get(key, {}).get("values", {}).get("count", 0) or 0)

        h_created = cnt(fm, "hold_created")
        h_conflict = cnt(fm, "hold_conflict")
        h_429 = cnt(fm, "hold_rate_limited")
        h_unexp = cnt(fm, "hold_unexpected")
        h_total = h_created + h_conflict + h_429 + h_unexp
        h_5xx_rate = float(fm.get("hold_5xx", {}).get("values", {}).get("rate", 0.0) or 0.0)
        dropped_iters = cnt(fm, "dropped_iterations")

        lines.append("### Request Counts by Outcome")
        lines.append("")
        lines.append("| Outcome | Count |")
        lines.append("| :--- | ---: |")
        lines.append(f"| `201 Created` (`hold_created`) | {h_created} |")
        lines.append(f"| `409 Conflict` (`hold_conflict`) | {h_conflict} |")
        lines.append(f"| `429 Rate Limited` (`hold_rate_limited`) | {h_429} |")
        lines.append(f"| Unexpected / Other (`hold_unexpected`) | {h_unexp} |")
        lines.append(f"| **Total Hold Requests** | **{h_total}** |")
        lines.append(f"| `hold_5xx` Rate | `{h_5xx_rate:.6f}` |")
        lines.append(f"| k6 `dropped_iterations` | {dropped_iters} |")
        lines.append("")

        # Achieved sustained throughput during plateau stages (computed from plateau counters, not targets!)
        p1_reqs = cnt(fm, "plateau1_reqs")
        p2_reqs = cnt(fm, "plateau2_reqs")
        p1_dur = float(fmeta.get("plateau1_duration_s") or 0)
        p2_dur = float(fmeta.get("plateau2_duration_s") or 0)
        p1_ach = (p1_reqs / p1_dur) if p1_dur > 0 else 0.0
        p2_ach = (p2_reqs / p2_dur) if p2_dur > 0 else 0.0
        overall_rps = float(fm.get("http_reqs", {}).get("values", {}).get("rate", 0.0) or 0.0)

        lines.append("### Throughput Achieved (Computed)")
        lines.append("")
        lines.append("| Window | Requests | Duration (s) | Target (req/s) | Achieved (req/s) |")
        lines.append("| :--- | ---: | ---: | ---: | ---: |")
        lines.append(f"| Plateau 1 | {p1_reqs} | {p1_dur:.0f} | {fmeta.get('plateau1_target_rps', 'n/a')} | **{p1_ach:.2f}** |")
        lines.append(f"| Plateau 2 | {p2_reqs} | {p2_dur:.0f} | {fmeta.get('plateau2_target_rps', 'n/a')} | **{p2_ach:.2f}** |")
        lines.append(f"| Overall Run Avg (`http_reqs`) | {cnt(fm, 'http_reqs')} | {float(flash_data.get('state', {}).get('testRunDurationMs', 0)) / 1000.0:.1f} | — | {overall_rps:.2f} |")
        lines.append("")

        lines.append("### Latency Table")
        lines.append("")
        lines.append("| Metric | p50 | p90 | p95 | p99 | max |")
        lines.append("| :--- | ---: | ---: | ---: | ---: | ---: |")
        lines.append(trend_row("`http_req_duration{name:hold}`", fm, "http_req_duration{name:hold}"))
        lines.append(trend_row("`hold_duration`", fm, "hold_duration"))
        lines.append("")

    # 3. Scenario B: Checkout Journey
    lines.append("## 3. Scenario B — Checkout Journey (`loadtest/checkout-journey.js`)")
    lines.append("")
    if journey_data is None:
        lines.append("_Scenario B was NOT RUN in this results directory._")
        lines.append("")
    else:
        jm = journey_data.get("metrics", {})
        jmeta = journey_data.get("_meta", {})

        lines.append("### Configuration")
        lines.append("")
        lines.append(f"- **Executor**: `constant-arrival-rate` (scale=`{jmeta.get('load_scale', 'n/a')}`)")
        lines.append(f"- **Rate / Duration**: `{jmeta.get('journey_rate_target', 'n/a')} journeys/s` for `{jmeta.get('journey_duration_s', 'n/a')}s`")
        lines.append(f"- **VUs**: `preAllocatedVUs={jmeta.get('pre_allocated_vus', 'n/a')}`, `maxVUs={jmeta.get('max_vus', 'n/a')}`")
        lines.append(f"- **Mix**: 90% `tok_success`, 5% `tok_decline`, 5% abandoned holds; poll every `{jmeta.get('poll_interval_ms', 250)}ms` up to `{jmeta.get('poll_timeout_ms', 30000)}ms`")
        lines.append("")

        lines.append("### Thresholds")
        lines.append("")
        lines.append("| Metric | Threshold | Measured | Status |")
        lines.append("| :--- | :--- | :--- | :--- |")
        for t in journey_thresholds:
            lines.append(f"| `{t['metric']}` | `{t['expr']}` | `{t['measured']}` | **{status_str(t['ok'])}** |")
        lines.append("")

        def jcnt(key):
            return int(jm.get(key, {}).get("values", {}).get("count", 0) or 0)

        j_holds = jcnt("hold_created")
        j_conf = jcnt("journey_confirmed")
        j_decl = jcnt("journey_payment_failed")
        j_aban = jcnt("journey_abandoned")
        j_to = jcnt("journeys_timed_out")
        j_unexp = jcnt("unexpected_statuses")
        j_retries = jcnt("order_not_ready_retries")
        j_iters = jcnt("iterations")
        j_dropped = jcnt("dropped_iterations")
        j_dur_s = float(journey_data.get("state", {}).get("testRunDurationMs", 0) or 0) / 1000.0
        j_target_dur_s = float(jmeta.get("journey_duration_s") or j_dur_s or 1.0)
        j_achieved_jps = j_iters / j_target_dur_s if j_target_dur_s > 0 else 0.0
        j_http_rps = float(jm.get("http_reqs", {}).get("values", {}).get("rate", 0.0) or 0.0)

        lines.append("### Journey Counts by Outcome")
        lines.append("")
        lines.append("| Outcome | Count |")
        lines.append("| :--- | ---: |")
        lines.append(f"| Holds Created (`hold_created`) | {j_holds} |")
        lines.append(f"| Final `CONFIRMED` (`journey_confirmed`) | {j_conf} |")
        lines.append(f"| Final `CANCELLED_PAYMENT_FAILED` (`journey_payment_failed`) | {j_decl} |")
        lines.append(f"| Abandoned Holds (`journey_abandoned`) | {j_aban} |")
        lines.append(f"| Timed Out (`journeys_timed_out`) | {j_to} |")
        lines.append(f"| Unexpected Statuses (`unexpected_statuses`) | {j_unexp} |")
        lines.append(f"| `409 ORDER_NOT_READY` Retries (`order_not_ready_retries`) | {j_retries} |")
        lines.append(f"| k6 `dropped_iterations` | {j_dropped} |")
        lines.append("")

        lines.append("### Throughput Achieved (Computed)")
        lines.append("")
        lines.append("| Metric | Achieved |")
        lines.append("| :--- | ---: |")
        lines.append(f"| Completed Journeys / Target Duration ({j_iters} / {j_target_dur_s:.0f}s) | **{j_achieved_jps:.2f} journeys/s** |")
        lines.append(f"| Total HTTP Request Rate (`http_reqs`) | {j_http_rps:.2f} req/s |")
        lines.append("")

        lines.append("### Latency & Saga Latency Table")
        lines.append("")
        lines.append("| Metric | p50 | p90 | p95 | p99 | max |")
        lines.append("| :--- | ---: | ---: | ---: | ---: | ---: |")
        lines.append(trend_row("`http_req_duration{name:hold}`", jm, "http_req_duration{name:hold}"))
        lines.append(trend_row("`http_req_duration{name:checkout}`", jm, "http_req_duration{name:checkout}"))
        lines.append(trend_row("`order_materialization_latency`", jm, "order_materialization_latency"))
        lines.append(trend_row("`checkout_duration`", jm, "checkout_duration"))
        lines.append(trend_row("`saga_latency`", jm, "saga_latency"))
        lines.append("")

    # 4. Prometheus Run-Window Telemetry
    lines.append("## 4. Prometheus Run-Window Metrics")
    lines.append("")
    if not prom.get("available"):
        lines.append(f"_{prom.get('note', 'Prometheus data unavailable')}_")
        lines.append("")
    else:
        lines.append(f"Query window: `{prom['window_seconds']}s` ending at run completion.")
        lines.append("")
        lines.append(f"- **Gateway 5xx Count (window)**: `{prom['gateway_5xx_count']:.2f}`")
        lines.append(f"- **Gateway 429 Count (window)**: `{prom['gateway_429_count']:.2f}`")
        lines.append(f"- **Max In-Flight Checkouts (saga delta / HTTP active)**: `{prom['max_inflight_checkouts_saga']:.1f}` / `{prom['max_inflight_checkouts_http']:.1f}`")
        lines.append("")

        lines.append("| Metric Category | Target | Peak Value |")
        lines.append("| :--- | :--- | ---: |")
        for cg, val in sorted(prom.get("peak_consumer_lag", {}).items()):
            lines.append(f"| Peak Consumer Lag | `{cg}` | {val:.1f} |")
        for svc, val in sorted(prom.get("peak_outbox_oldest_age_s", {}).items()):
            lines.append(f"| Peak Outbox Oldest Pending Age | `{svc}` | {val:.3f} s |")
        for svc, val in sorted(prom.get("peak_jvm_heap_ratio", {}).items()):
            lines.append(f"| Peak JVM Heap Used Ratio | `{svc}` | {val * 100.0:.1f}% |")
        for svc, val in sorted(prom.get("peak_hikari_pending", {}).items()):
            lines.append(f"| Peak HikariCP Pending | `{svc}` | {val:.1f} |")
        for svc, val in sorted(prom.get("total_jvm_gc_pause_seconds", {}).items()):
            lines.append(f"| Total JVM GC Pause Time | `{svc}` | {val:.3f} s |")
        for svc, val in sorted(prom.get("service_5xx_counts", {}).items()):
            lines.append(f"| Service 5xx Count | `{svc}` | {val:.1f} |")
        lines.append("")

    # 5. Container Resource Usage
    lines.append("## 5. Resource Usage (`docker-stats.csv`)")
    lines.append("")
    if container_health is not None:
        r_str = "none" if not container_health["restarts"] else ", ".join(container_health["restarts"])
        o_str = "none" if not container_health["ooms"] else ", ".join(container_health["ooms"])
        lines.append(f"- **Container Restarts During Run**: `{r_str}` (checked {container_health['checked']} containers)")
        lines.append(f"- **Container OOM Kills During Run**: `{o_str}`")
        lines.append("")

    if not docker_stats:
        lines.append("_No docker-stats.csv samples recorded._")
        lines.append("")
    else:
        top3 = sorted(docker_stats.items(), key=lambda kv: kv[1]["peak_cpu"], reverse=True)[:3]
        lines.append("### Top 3 CPU Consumers (by Peak CPU%)")
        lines.append("")
        for idx, (cname, st) in enumerate(top3, 1):
            lines.append(f"{idx}. `{cname}` — Peak CPU `{st['peak_cpu']:.1f}%`, Avg CPU `{st['avg_cpu']:.1f}%`, Peak Mem `{st['peak_mem_mib']:.1f} MiB`")
        lines.append("")

        lines.append("### Per-Container Resource Table")
        lines.append("")
        lines.append("| Container | Samples | Avg CPU% | Peak CPU% | Avg Mem (MiB) | Peak Mem (MiB) | Peak Mem% |")
        lines.append("| :--- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for cname, st in sorted(docker_stats.items()):
            lines.append(
                f"| `{cname}` | {st['samples']} | {st['avg_cpu']:.1f}% | {st['peak_cpu']:.1f}% | "
                f"{st['avg_mem_mib']:.1f} | {st['peak_mem_mib']:.1f} | {st['peak_mem_pct']:.1f}% |"
            )
        lines.append("")

    # 6. Post-Run Audit
    lines.append("## 6. Post-Run Invariant Audit (`scripts/loadtest-audit.sh`)")
    lines.append("")
    if audit_data is None:
        lines.append("- **Audit Status**: **NOT RUN** (`audit.json` not present in results directory)")
        lines.append("")
    else:
        q = audit_data.get("quiescence", {})
        lines.append(f"- **Quiescence**: **{q.get('status', 'UNKNOWN')}** — {q.get('detail', '')}")
        lines.append(f"- **Audit Overall**: **{audit_status}**")
        lines.append("")
        lines.append("| ID | Status | Invariant | Details |")
        lines.append("| :--- | :--- | :--- | :--- |")
        for it in audit_data.get("items", []):
            lines.append(f"| `{it['id']}` | **{it['status']}** | {it['name']} | {it['detail']} |")
        lines.append("")

    # 7. Findings & Limits
    lines.append("## 7. Findings and Limits")
    lines.append("")
    lines.append("- **Single-node Docker on a laptop**: All application services, Kafka, Redis, three PostgreSQL instances, observability containers, and the k6 load generator run on the same machine inside one Docker VM.")
    lines.append("- **Shared CPU contention**: k6 shares host CPU and memory bandwidth with the system under test, so tail latencies under peak load reflect both service overhead and local scheduler contention.")
    lines.append("- **Non-production scope**: These results are **not** production capacity numbers and are only meaningful relative to each other (for regression tracking and architectural verification across identical local configurations).")
    lines.append("")

    report_md = "\n".join(lines)
    report_path = os.path.join(results_dir, "report.md")
    with open(report_path, "w") as f:
        f.write(report_md)

    published = False
    publish_message = ""
    if publish:
        if overall_ok:
            dest = publish_dest or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "load-test-report.md"))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copyfile(report_path, dest)
            published = True
            publish_message = f"Published passing report to {dest}"
        else:
            reasons = []
            if not scenarios_present:
                reasons.append("no k6 scenario summaries present")
            elif not thresholds_ok:
                reasons.append("one or more k6 thresholds failed")
            if audit_data is None:
                reasons.append("audit was NOT RUN")
            elif not audit_ok:
                reasons.append("one or more audit items failed")
            publish_message = f"Skipped publishing docs/load-test-report.md because: {'; '.join(reasons)}"
    else:
        publish_message = "LOAD_PUBLISH is not enabled; report kept in results directory only."

    return {
        "report_path": report_path,
        "overall_ok": overall_ok,
        "overall_status": overall_status,
        "audit_status": audit_status,
        "published": published,
        "publish_message": publish_message,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate load test report from results directory")
    parser.add_argument("results_dir", help="Path to loadtest/results/<ts>")
    parser.add_argument("--prometheus-url", default=os.environ.get("PROMETHEUS_URL", "http://127.0.0.1:9090"))
    parser.add_argument("--publish", action="store_true", help="Copy report to docs/load-test-report.md if all checks pass")
    parser.add_argument("--publish-dest", default=None, help="Override publish destination path")
    parser.add_argument("--offline", action="store_true", help="Disable live Prometheus and docker CLI queries")
    args = parser.parse_args()

    publish_flag = args.publish or (os.environ.get("LOAD_PUBLISH", "false").lower() == "true")
    res = generate_report(
        args.results_dir,
        prom_url=args.prometheus_url,
        publish=publish_flag,
        publish_dest=args.publish_dest,
        enable_live=not args.offline,
    )
    print(f"[report] Generated {res['report_path']} (overall={res['overall_status']}, audit={res['audit_status']})")
    print(f"[report] {res['publish_message']}")


if __name__ == "__main__":
    main()
