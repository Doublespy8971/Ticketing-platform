#!/usr/bin/env python3
"""
scripts/loadtest-compare.py — Baseline vs After Load-Test Comparison Tool

Python 3 standard library only.
Reads baseline and after result directories under loadtest/results/<ts>/ and
prints a Markdown table comparing baseline versus after (median and min..max
range across the after runs) for:
  - Achieved plateau throughput (Flash Sale Plateau 1 & 2, Journey rate)
  - Hold p99 (Flash Sale & Journey), Checkout p99, Saga p95
  - Count of 5xx by service
  - Peak consumer lag per group
  - Peak HikariCP pending per service
  - Total JVM GC pause seconds per service (jvm_gc_pause_seconds_sum over run window)
  - Peak CPU of top three containers
  - Audit result and time to quiescence

All values are read strictly from result files (k6 summary JSONs, docker-stats.csv,
audit.json, prometheus-metrics.json, report.md fallback for legacy baseline runs)
or live Prometheus when --prometheus-url is enabled and reachable.
"""

import argparse
import csv
import json
import os
import re
import statistics
import sys
import urllib.parse
import urllib.request


def load_json(path):
    if not os.path.isfile(path):
        return None
    with open(path) as f:
        return json.load(f)


def parse_report_prom_fallback(report_path):
    """Extracts Prometheus metrics from report.md if prometheus-metrics.json does not exist."""
    if not os.path.isfile(report_path):
        return {}
    out = {
        "peak_consumer_lag": {},
        "peak_outbox_oldest_age_s": {},
        "peak_hikari_pending": {},
        "total_jvm_gc_pause_seconds": {},
        "service_5xx_counts": {},
    }
    with open(report_path) as f:
        for line in f:
            m_g5 = re.search(r"\*\*Gateway 5xx Count \(window\)\*\*:\s*`([0-9.]+)`", line)
            if m_g5:
                out["service_5xx_counts"]["gateway-service"] = float(m_g5.group(1))
            m_row = re.match(r"^\|\s*([^|]+?)\s*\|\s*`([^`]+)`\s*\|\s*([0-9.]+)(?:\s*s|%)?\s*\|", line)
            if not m_row:
                continue
            cat, target, val = m_row.group(1).strip(), m_row.group(2).strip(), float(m_row.group(3))
            if cat == "Peak Consumer Lag":
                out["peak_consumer_lag"][target] = val
            elif cat == "Peak Outbox Oldest Pending Age":
                out["peak_outbox_oldest_age_s"][target] = val
            elif cat == "Peak HikariCP Pending":
                out["peak_hikari_pending"][target] = val
            elif cat == "Total JVM GC Pause Time":
                out["total_jvm_gc_pause_seconds"][target] = val
            elif cat == "Service 5xx Count":
                out["service_5xx_counts"][target] = val
    return out


def query_prom_window(prom_url, run_meta):
    if not prom_url:
        return None
    start_e = int(run_meta.get("start_epoch") or 0)
    end_e = int(run_meta.get("end_epoch") or 0)
    if not (start_e > 0 and end_e > start_e):
        return None
    window_s = max(30, end_e - start_e + 15)

    def pq(expr):
        url = f"{prom_url.rstrip('/')}/api/v1/query?" + urllib.parse.urlencode(
            {"query": expr, "time": str(end_e)}
        )
        try:
            with urllib.request.urlopen(url, timeout=4) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if data.get("status") != "success":
                return None
            return data.get("data", {}).get("result", [])
        except Exception:
            return None

    probe = pq("up")
    if probe is None:
        return None

    res = {
        "peak_consumer_lag": {},
        "peak_hikari_pending": {},
        "total_jvm_gc_pause_seconds": {},
        "service_5xx_counts": {},
    }
    for r in pq(f"max_over_time((sum by (consumergroup) (clamp_min(kafka_consumergroup_lag, 0)))[{window_s}s:15s])") or []:
        res["peak_consumer_lag"][r["metric"].get("consumergroup", "unknown")] = float(r["value"][1])
    for r in pq(f"max_over_time(hikaricp_connections_pending[{window_s}s:5s])") or []:
        res["peak_hikari_pending"][r["metric"].get("service", "unknown")] = float(r["value"][1])
    for r in pq(f"sum by (service) (increase(jvm_gc_pause_seconds_sum[{window_s}s]))") or []:
        res["total_jvm_gc_pause_seconds"][r["metric"].get("service", "unknown")] = float(r["value"][1])
    for r in pq(f'sum by (service) (increase(http_server_requests_seconds_count{{status=~"5.."}}[{window_s}s]))') or []:
        res["service_5xx_counts"][r["metric"].get("service", "unknown")] = float(r["value"][1])
    gw5 = pq(f'sum(increase(gateway_requests_total{{status_class="5xx"}}[{window_s}s])) or vector(0)') or []
    if gw5:
        res["service_5xx_counts"]["gateway-service"] = max(
            res["service_5xx_counts"].get("gateway-service", 0.0), float(gw5[0]["value"][1])
        )
    return res


def normalize_container_name(name):
    return re.sub(r"-\d{8}T\d{6}Z$", "", name)


def parse_cpu_peaks(docker_stats_csv):
    if not os.path.isfile(docker_stats_csv):
        return {}
    peaks = {}
    with open(docker_stats_csv, newline="") as f:
        for row in csv.DictReader(f):
            c = normalize_container_name(row.get("container", "").strip())
            if not c:
                continue
            try:
                cpu = float(row.get("cpu_pct", "0") or 0)
            except ValueError:
                cpu = 0.0
            if cpu > peaks.get(c, -1.0):
                peaks[c] = cpu
    return peaks


def parse_top3_cpu(docker_stats_csv):
    peaks = parse_cpu_peaks(docker_stats_csv)
    return sorted(peaks.items(), key=lambda kv: kv[1], reverse=True)[:3]


def parse_quiescence_seconds(audit_data):
    if not audit_data:
        return None, "NOT RUN"
    q = audit_data.get("quiescence", {})
    q_status = q.get("status", "UNKNOWN")
    detail = q.get("detail", "")
    m = re.search(r"(?:Quiesced in|Timed out after)\s+(\d+)s", detail)
    sec = float(m.group(1)) if m else None
    return sec, q_status


def extract_run_metrics(results_dir, prom_url=None):
    results_dir = os.path.abspath(results_dir)
    invalid_path = os.path.join(results_dir, "INVALID")
    is_invalid = os.path.isfile(invalid_path)
    invalid_reason = open(invalid_path).read().strip() if is_invalid else ""

    run_meta = load_json(os.path.join(results_dir, "run-meta.json")) or {}
    flash = (
        load_json(os.path.join(results_dir, "flash-sale-summary.json"))
        or load_json(os.path.join(results_dir, "sustained-hold-summary.json"))
        or {}
    )
    journey = (
        load_json(os.path.join(results_dir, "checkout-journey-summary.json"))
        or load_json(os.path.join(results_dir, "sustained-journey-summary.json"))
        or {}
    )
    audit = load_json(os.path.join(results_dir, "audit.json"))
    prom = load_json(os.path.join(results_dir, "prometheus-metrics.json"))

    if not prom:
        prom = query_prom_window(prom_url, run_meta) if prom_url else None
    if not prom:
        prom = parse_report_prom_fallback(os.path.join(results_dir, "report.md"))

    fm = flash.get("metrics", {})
    fmeta = flash.get("_meta", {})
    jm = journey.get("metrics", {})
    jmeta = journey.get("_meta", {})

    def mval(mdict, key, field):
        v = mdict.get(key, {}).get("values", {}).get(field)
        return float(v) if v is not None else None

    p1_reqs = mval(fm, "plateau1_reqs", "count")
    p2_reqs = mval(fm, "plateau2_reqs", "count")
    p1_dur = float(fmeta.get("plateau1_duration_s") or 0)
    p2_dur = float(fmeta.get("plateau2_duration_s") or 0)

    j_iters = mval(jm, "iterations", "count")
    j_dur = float(jmeta.get("journey_duration_s") or 0)

    q_sec, q_status = parse_quiescence_seconds(audit)
    audit_overall = audit.get("overall", "NOT RUN") if audit else "NOT RUN"

    return {
        "run_id": run_meta.get("run_id") or os.path.basename(results_dir),
        "invalid": is_invalid,
        "invalid_reason": invalid_reason,
        "plateau1_rps": (p1_reqs / p1_dur) if (p1_reqs is not None and p1_dur > 0) else None,
        "plateau2_rps": (p2_reqs / p2_dur) if (p2_reqs is not None and p2_dur > 0) else None,
        "journey_jps": (j_iters / j_dur) if (j_iters is not None and j_dur > 0) else None,
        "flash_hold_p99_ms": mval(fm, "http_req_duration{name:hold}", "p(99)"),
        "journey_hold_p99_ms": mval(jm, "http_req_duration{name:hold}", "p(99)"),
        "checkout_p99_ms": mval(jm, "http_req_duration{name:checkout}", "p(99)"),
        "saga_p95_ms": mval(jm, "saga_latency", "p(95)"),
        "flash_429_count": mval(fm, "hold_rate_limited", "count"),
        "flash_unexpected": mval(fm, "hold_unexpected", "count"),
        "journey_unexpected": mval(jm, "unexpected_statuses", "count"),
        "journey_timeouts": mval(jm, "journeys_timed_out", "count"),
        "service_5xx_counts": prom.get("service_5xx_counts", {}) or {},
        "peak_consumer_lag": prom.get("peak_consumer_lag", {}) or {},
        "peak_outbox_oldest_age_s": prom.get("peak_outbox_oldest_age_s", {}) or {},
        "peak_hikari_pending": prom.get("peak_hikari_pending", {}) or {},
        "total_jvm_gc_pause_seconds": prom.get("total_jvm_gc_pause_seconds", {}) or {},
        "cpu_peaks": parse_cpu_peaks(os.path.join(results_dir, "docker-stats.csv")),
        "top3_cpu": parse_top3_cpu(os.path.join(results_dir, "docker-stats.csv")),
        "audit_overall": audit_overall,
        "quiescence_status": q_status,
        "quiescence_seconds": q_sec,
    }


def fmt_num(val, unit="", decimals=2):
    if val is None:
        return "n/a"
    return f"{val:.{decimals}f}{unit}"


def fmt_median_range(vals, unit="", decimals=2):
    clean = [v for v in vals if v is not None]
    if not clean:
        return "n/a", "n/a"
    med = statistics.median(clean)
    lo, hi = min(clean), max(clean)
    return fmt_num(med, unit, decimals), f"{fmt_num(lo, unit, decimals)} .. {fmt_num(hi, unit, decimals)}"


def render_comparison_markdown(baseline_dir, after_dirs, prom_url=None):
    base = extract_run_metrics(baseline_dir, prom_url=prom_url)
    all_afters = [extract_run_metrics(d, prom_url=prom_url) for d in after_dirs]
    excluded = [a for a in all_afters if a.get("invalid")]
    afters = [a for a in all_afters if not a.get("invalid")]

    lines = []
    lines.append("# Load Test Comparison: Baseline vs After")
    lines.append("")
    base_note = " (**INVALID — NOT COUNTED**)" if base.get("invalid") else ""
    lines.append(f"- **Baseline**: `{base['run_id']}`{base_note}")
    lines.append(f"- **After Runs ({len(afters)} valid)**: " + (", ".join(f"`{a['run_id']}`" for a in afters) if afters else "_none_"))
    if excluded:
        lines.append(
            "- **Excluded INVALID Runs (NOT COUNTED)**: "
            + "; ".join(f"`{e['run_id']}` ({e['invalid_reason']})" for e in excluded)
        )
    lines.append("")
    lines.append("| Metric | Baseline | After (Median) | After (Range) |")
    lines.append("| :--- | ---: | ---: | ---: |")

    def add_scalar_row(label, key, unit="", decimals=2):
        bval = base.get(key)
        med_s, rng_s = fmt_median_range([a.get(key) for a in afters], unit=unit, decimals=decimals)
        lines.append(f"| {label} | {fmt_num(bval, unit, decimals)} | {med_s} | {rng_s} |")

    add_scalar_row("Flash Sale Plateau 1 Throughput", "plateau1_rps", " req/s", 2)
    add_scalar_row("Flash Sale Plateau 2 Throughput", "plateau2_rps", " req/s", 2)
    add_scalar_row("Checkout Journey Throughput", "journey_jps", " j/s", 2)
    add_scalar_row("Flash Sale Hold p99", "flash_hold_p99_ms", " ms", 2)
    add_scalar_row("Journey Hold p99", "journey_hold_p99_ms", " ms", 2)
    add_scalar_row("Journey Checkout p99", "checkout_p99_ms", " ms", 2)
    add_scalar_row("Journey Saga p95", "saga_p95_ms", " ms", 2)
    add_scalar_row("Flash Sale 429 Count", "flash_429_count", "", 0)
    add_scalar_row("Flash Sale Unexpected Count", "flash_unexpected", "", 0)
    add_scalar_row("Journey Unexpected Count", "journey_unexpected", "", 0)
    add_scalar_row("Journey Timed Out Count", "journey_timeouts", "", 0)

    def add_dict_rows(prefix, dict_key, unit="", decimals=1):
        keys = set(base.get(dict_key, {}).keys())
        for a in afters:
            keys.update(a.get(dict_key, {}).keys())
        for k in sorted(keys):
            bval = base.get(dict_key, {}).get(k)
            avals = [a.get(dict_key, {}).get(k) for a in afters]
            med_s, rng_s = fmt_median_range(avals, unit=unit, decimals=decimals)
            lines.append(f"| {prefix} (`{k}`) | {fmt_num(bval, unit, decimals)} | {med_s} | {rng_s} |")

    add_dict_rows("5xx Count", "service_5xx_counts", "", 1)
    add_dict_rows("Peak Consumer Lag", "peak_consumer_lag", "", 1)
    add_dict_rows("Peak Outbox Oldest Age", "peak_outbox_oldest_age_s", " s", 3)
    add_dict_rows("Peak HikariCP Pending", "peak_hikari_pending", "", 1)
    add_dict_rows("Total JVM GC Pause", "total_jvm_gc_pause_seconds", " s", 3)

    # Top 3 CPU consumers summary row
    def fmt_top3(items):
        if not items:
            return "n/a"
        return "; ".join(f"{c}: {v:.1f}%" for c, v in items)

    b_top3 = fmt_top3(base["top3_cpu"])
    # For afters, aggregate peak CPU per container across all runs and rank top 3 by median peak CPU
    all_cpu_containers = {}
    for a in afters:
        for c, v in a.get("cpu_peaks", {}).items():
            all_cpu_containers.setdefault(c, []).append(v)
    top3_after_ranked = sorted(
        all_cpu_containers.items(), key=lambda kv: statistics.median(kv[1]), reverse=True
    )[:3]
    a_top3_med = "; ".join(f"{c}: {statistics.median(vs):.1f}%" for c, vs in top3_after_ranked) if top3_after_ranked else "n/a"
    a_top3_rng = "; ".join(f"{c}: {min(vs):.1f}..{max(vs):.1f}%" for c, vs in top3_after_ranked) if top3_after_ranked else "n/a"
    lines.append(f"| Peak CPU (Top 3 Containers) | {b_top3} | {a_top3_med} | {a_top3_rng} |")

    # Audit & Quiescence
    q_med, q_rng = fmt_median_range([a["quiescence_seconds"] for a in afters], unit=" s", decimals=0)
    b_q = (
        f"{base['quiescence_seconds']:.0f} s ({base['quiescence_status']})"
        if base["quiescence_seconds"] is not None
        else base["quiescence_status"]
    )
    lines.append(f"| Time to Quiescence | {b_q} | {q_med} | {q_rng} |")

    after_audits = ", ".join(a["audit_overall"] for a in afters)
    pass_cnt = sum(1 for a in afters if a["audit_overall"] == "PASS")
    lines.append(f"| Audit Result | **{base['audit_overall']}** | {pass_cnt}/{len(afters)} PASS | {after_audits} |")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Compare baseline vs after load test result directories")
    parser.add_argument("--baseline", required=True, help="Baseline results directory")
    parser.add_argument("--after", required=True, nargs="+", help="One or more after results directories")
    parser.add_argument("--prometheus-url", default=None, help="Optional Prometheus API URL fallback")
    parser.add_argument("--output", default=None, help="Optional file path to write Markdown output")
    args = parser.parse_args()

    md = render_comparison_markdown(args.baseline, args.after, prom_url=args.prometheus_url)
    if args.output:
        with open(args.output, "w") as f:
            f.write(md)
    print(md)


if __name__ == "__main__":
    main()
