#!/usr/bin/env python3
"""
scripts/loadtest-tail-analysis.py — 10-Second Windowed Tail-Latency Correlation Analysis

Python 3 standard library only.
Reads a results directory (run-meta.json, k6 summary JSON for the measured window
excluding warm-up, and docker-stats.csv) and queries Prometheus with range queries
at a 10-second step over the measured window for:
  - Server-side p99 of http_server_requests for reservations on inventory-service & gateway-service
  - JVM GC pause max and GC pause sum rate per Java service
  - Container CPU% of inventory-service, gateway-service, and kafka (from docker-stats.csv)
  - HikariCP pending connections (inventory-service, order-service, payment-service)
  - p99 of ticketing_redis_lua_duration_seconds
  - Outbox oldest pending age (seconds)
  - Kafka consumer group lag

Lists the 10 worst p99 windows side by side with every signal, ranks signals by
Pearson correlation (r, n) with window p99, and writes a plain data-supported
coincidence summary (with reminder that correlation is not causation).
"""

import argparse
import csv
import datetime
import glob
import json
import math
import os
import sys
import urllib.parse
import urllib.request


STEP_SECONDS = 10


def safe_float(val):
    try:
        v = float(val)
        return None if (math.isnan(v) or math.isinf(v)) else v
    except (TypeError, ValueError):
        return None


def pearson_correlation(xs, ys):
    """
    Computes Pearson r over paired non-None values (xs, ys).
    Returns (r, n) where r is float in [-1, 1] or None if n < 3 or zero variance.
    """
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None and not math.isnan(x) and not math.isnan(y)]
    n = len(pairs)
    if n < 3:
        return None, n
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    dx = math.sqrt(sum((p[0] - mx) ** 2 for p in pairs))
    dy = math.sqrt(sum((p[1] - my) ** 2 for p in pairs))
    if dx == 0.0 or dy == 0.0:
        return None, n
    return max(-1.0, min(1.0, num / (dx * dy))), n


def prom_query_range(prom_url, expr, start_epoch, end_epoch, step_s=STEP_SECONDS):
    params = {
        "query": expr,
        "start": str(int(start_epoch)),
        "end": str(int(end_epoch)),
        "step": f"{int(step_s)}s",
    }
    url = f"{prom_url.rstrip('/')}/api/v1/query_range?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=8) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if payload.get("status") != "success":
            return None
        return payload.get("data", {}).get("result", [])
    except Exception:
        return None


def align_docker_cpu(docker_stats_csv, timestamps, containers=("inventory-service", "gateway-service", "kafka"), step_s=STEP_SECONDS):
    """Maps docker-stats.csv CPU% samples onto the nearest 10s window timestamps."""
    aligned = {c: {t: None for t in timestamps} for c in containers}
    if not os.path.isfile(docker_stats_csv) or not timestamps:
        return aligned
    raw = {c: [] for c in containers}
    with open(docker_stats_csv, newline="") as f:
        for row in csv.DictReader(f):
            c = (row.get("container") or "").strip()
            if c not in raw:
                continue
            ts_s = (row.get("timestamp_utc") or "").strip()
            cpu = safe_float(row.get("cpu_pct"))
            if not ts_s or cpu is None:
                continue
            try:
                ep = int(datetime.datetime.fromisoformat(ts_s.replace("Z", "+00:00")).timestamp())
            except ValueError:
                continue
            raw[c].append((ep, cpu))

    for c, samples in raw.items():
        if not samples:
            continue
        for t in timestamps:
            in_win = [cpu for ep, cpu in samples if (t - step_s) < ep <= (t + step_s // 2)]
            if in_win:
                aligned[c][t] = round(max(in_win), 1)
    return aligned


def analyze_windows(windows, target_key="inv_hold_p99_ms"):
    """
    Pure analysis function over a list of window dicts:
      {
        "epoch": int,
        "inv_hold_p99_ms": float|None,
        "gw_hold_p99_ms": float|None,
        "signals": {signal_name: float|None, ...}
      }
    Computes:
      - 10 worst p99 windows (by target_key, falling back to gw_hold_p99_ms)
      - Pearson correlation ranking of every signal vs window p99
      - Data-supported coincidence summary for p99 > 300ms windows
    """
    # Choose primary p99 target with data
    has_target = any(w.get(target_key) is not None for w in windows)
    eff_target = target_key if has_target else "gw_hold_p99_ms"

    valid_p99_windows = [w for w in windows if w.get(eff_target) is not None]
    worst10 = sorted(valid_p99_windows, key=lambda w: w[eff_target], reverse=True)[:10]

    signal_names = sorted({k for w in windows for k in (w.get("signals") or {}).keys()})
    p99_series = [w.get(eff_target) for w in windows]

    correlations = []
    for sig in signal_names:
        sig_series = [(w.get("signals") or {}).get(sig) for w in windows]
        r, n = pearson_correlation(p99_series, sig_series)
        correlations.append({
            "signal": sig,
            "pearson_r": round(r, 4) if r is not None else None,
            "abs_r": abs(r) if r is not None else -1.0,
            "n": n,
        })
    correlations.sort(key=lambda item: item["abs_r"], reverse=True)

    # Coincidence checks on windows exceeding the 300 ms hold SLO
    high_windows = [w for w in valid_p99_windows if w[eff_target] > 300.0]
    m = len(high_windows)
    coincidences = []
    if m > 0:
        checks = [
            ("gc_pause_max_ms", 200.0, "JVM GC pause max > 200 ms"),
            ("hikari_pending_inventory", 0.0, "inventory HikariCP pending > 0"),
            ("cpu_inventory_pct", 200.0, "inventory-service CPU > 200%"),
            ("cpu_gateway_pct", 200.0, "gateway-service CPU > 200%"),
            ("redis_lua_p99_ms", 50.0, "Redis Lua p99 > 50 ms"),
            ("outbox_oldest_age_s", 5.0, "outbox oldest pending age > 5 s"),
        ]
        for sig_key, thresh, desc in checks:
            present = [w for w in high_windows if (w.get("signals") or {}).get(sig_key) is not None]
            if not present:
                continue
            hits = sum(1 for w in present if w["signals"][sig_key] > thresh)
            coincidences.append(f"{desc} in {hits}/{len(present)} high-p99 windows ({hits * 100.0 / len(present):.0f}%)")

    if m == 0:
        statement = f"No 10-second windows exceeded 300 ms on `{eff_target}` across {len(valid_p99_windows)} measured windows."
    elif coincidences:
        statement = (
            f"Across {len(valid_p99_windows)} measured 10s windows, {m} window(s) exceeded 300 ms on `{eff_target}`; "
            f"those high-p99 windows coincide with: " + "; ".join(coincidences) + "."
        )
    else:
        statement = f"Across {len(valid_p99_windows)} measured 10s windows, {m} window(s) exceeded 300 ms on `{eff_target}`."

    statement += " Correlation is not causation: signals that move together under shared-host load may share an underlying cause (e.g. CPU saturation)."

    return {
        "target_metric": eff_target,
        "total_windows": len(windows),
        "valid_p99_windows": len(valid_p99_windows),
        "high_p99_windows_gt_300ms": m,
        "worst_10_windows": worst10,
        "correlations": [{k: v for k, v in c.items() if k != "abs_r"} for c in correlations],
        "statement": statement,
    }


def collect_and_analyze(results_dir, prom_url="http://127.0.0.1:9090", enable_live=True):
    results_dir = os.path.abspath(results_dir)
    run_meta = {}
    mp = os.path.join(results_dir, "run-meta.json")
    if os.path.isfile(mp):
        with open(mp) as f:
            run_meta = json.load(f)

    # Prefer measured_start_epoch / measured_end_epoch from summary _meta (excludes warm-up!)
    start_e = int(run_meta.get("start_epoch") or 0)
    end_e = int(run_meta.get("end_epoch") or 0)
    run_id = run_meta.get("run_id") or os.path.basename(results_dir)

    for sp in sorted(glob.glob(os.path.join(results_dir, "*-summary.json"))):
        try:
            with open(sp) as f:
                smeta = json.load(f).get("_meta", {})
            if smeta.get("measured_start_epoch") and smeta.get("measured_end_epoch"):
                start_e = int(smeta["measured_start_epoch"])
                end_e = int(smeta["measured_end_epoch"])
                run_id = smeta.get("run_id") or run_id
                break
        except Exception:
            continue

    if not enable_live or start_e <= 0 or end_e <= start_e:
        res = analyze_windows([])
        res["run_id"] = run_id
        res["measured_window"] = {"start_epoch": start_e, "end_epoch": end_e, "step_s": STEP_SECONDS}
        res["prometheus_available"] = False
        return res

    # Round timestamps to 10s grid
    t0 = (start_e // STEP_SECONDS) * STEP_SECONDS
    t1 = ((end_e + STEP_SECONDS - 1) // STEP_SECONDS) * STEP_SECONDS
    timestamps = list(range(t0, t1 + 1, STEP_SECONDS))

    def fetch_single_series(expr, scale=1.0):
        out = {t: None for t in timestamps}
        res = prom_query_range(prom_url, expr, t0, t1, STEP_SECONDS)
        if not res:
            return out
        for ts, val in res[0].get("values", []):
            ep = int(round(float(ts)))
            v = safe_float(val)
            if ep in out and v is not None:
                out[ep] = round(v * scale, 3)
        return out

    def fetch_by_label(expr, label_key, scale=1.0):
        res = prom_query_range(prom_url, expr, t0, t1, STEP_SECONDS)
        by_lbl = {}
        if not res:
            return by_lbl
        for item in res:
            lbl = item.get("metric", {}).get(label_key, "unknown")
            series = by_lbl.setdefault(lbl, {t: None for t in timestamps})
            for ts, val in item.get("values", []):
                ep = int(round(float(ts)))
                v = safe_float(val)
                if ep in series and v is not None:
                    series[ep] = round(v * scale, 3)
        return by_lbl

    inv_p99 = fetch_single_series(
        'histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="inventory-service",uri="/api/v1/reservations",method="POST"}[30s])))',
        scale=1000.0,
    )
    gw_p99 = fetch_single_series(
        'histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="gateway-service",uri="/api/v1/reservations",method="POST"}[30s])))',
        scale=1000.0,
    )
    if all(v is None for v in gw_p99.values()):
        gw_p99 = fetch_single_series(
            'histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service="gateway-service",method="POST"}[30s])))',
            scale=1000.0,
        )

    gc_max_by_svc = fetch_by_label("max by (service) (jvm_gc_pause_seconds_max)", "service", scale=1000.0)
    gc_rate_by_svc = fetch_by_label("sum by (service) (rate(jvm_gc_pause_seconds_sum[30s]))", "service", scale=1000.0)
    hikari_by_svc = fetch_by_label("max by (service) (hikaricp_connections_pending)", "service", scale=1.0)
    redis_lua_p99 = fetch_single_series(
        "histogram_quantile(0.99, sum by (le) (rate(ticketing_redis_lua_duration_seconds_bucket[30s])))",
        scale=1000.0,
    )
    if all(v is None for v in redis_lua_p99.values()):
        redis_lua_p99 = fetch_single_series("max(ticketing_redis_lua_duration_seconds_max)", scale=1000.0)
    outbox_age = fetch_single_series("max(outbox_oldest_pending_age_seconds)", scale=1.0)
    consumer_lag = fetch_single_series("max(sum by (consumergroup) (clamp_min(kafka_consumergroup_lag, 0)))", scale=1.0)

    docker_cpu = align_docker_cpu(os.path.join(results_dir, "docker-stats.csv"), timestamps)

    windows = []
    for t in timestamps:
        gc_max_vals = [s[t] for s in gc_max_by_svc.values() if s.get(t) is not None]
        sigs = {
            "gw_hold_p99_ms": gw_p99.get(t),
            "gc_pause_max_ms": round(max(gc_max_vals), 2) if gc_max_vals else None,
            "gc_max_inventory_ms": gc_max_by_svc.get("inventory-service", {}).get(t),
            "gc_max_gateway_ms": gc_max_by_svc.get("gateway-service", {}).get(t),
            "gc_rate_inventory_ms_s": gc_rate_by_svc.get("inventory-service", {}).get(t),
            "gc_rate_gateway_ms_s": gc_rate_by_svc.get("gateway-service", {}).get(t),
            "cpu_inventory_pct": docker_cpu["inventory-service"].get(t),
            "cpu_gateway_pct": docker_cpu["gateway-service"].get(t),
            "cpu_kafka_pct": docker_cpu["kafka"].get(t),
            "hikari_pending_inventory": hikari_by_svc.get("inventory-service", {}).get(t),
            "hikari_pending_order": hikari_by_svc.get("order-service", {}).get(t),
            "redis_lua_p99_ms": redis_lua_p99.get(t),
            "outbox_oldest_age_s": outbox_age.get(t),
            "consumer_lag_max": consumer_lag.get(t),
        }
        windows.append({
            "epoch": t,
            "timestamp_utc": datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc).strftime("%H:%M:%SZ"),
            "inv_hold_p99_ms": inv_p99.get(t),
            "gw_hold_p99_ms": gw_p99.get(t),
            "signals": sigs,
        })

    analysis = analyze_windows(windows)
    analysis["run_id"] = run_id
    analysis["measured_window"] = {"start_epoch": t0, "end_epoch": t1, "step_s": STEP_SECONDS}
    analysis["prometheus_available"] = True
    return analysis


def render_tail_analysis_markdown(analysis):
    lines = []
    lines.append(f"### Tail-Latency Analysis (`{analysis.get('run_id', 'unknown')}`)")
    lines.append("")
    mw = analysis.get("measured_window", {})
    lines.append(
        f"- **Measured Window (warm-up excluded)**: `step={mw.get('step_s', 10)}s`, "
        f"`{analysis['valid_p99_windows']}/{analysis['total_windows']}` windows with p99 data, "
        f"`{analysis['high_p99_windows_gt_300ms']}` windows > 300 ms (target: `{analysis['target_metric']}`)"
    )
    lines.append("")

    if analysis["worst_10_windows"]:
        lines.append("#### 10 Worst Hold p99 Windows (10s Step)")
        lines.append("")
        lines.append("| Window UTC | Inv p99 (ms) | GW p99 (ms) | GC Max (ms) | Inv CPU% | GW CPU% | Kafka CPU% | Hikari Pend (inv) | Redis Lua p99 (ms) | Outbox Age (s) | Lag Max |")
        lines.append("| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")

        def fv(v, d=1):
            return f"{v:.{d}f}" if v is not None else "n/a"

        for w in analysis["worst_10_windows"]:
            s = w.get("signals", {})
            lines.append(
                f"| `{w.get('timestamp_utc', w.get('epoch'))}` | {fv(w.get('inv_hold_p99_ms'), 2)} | "
                f"{fv(w.get('gw_hold_p99_ms'), 2)} | {fv(s.get('gc_pause_max_ms'), 1)} | "
                f"{fv(s.get('cpu_inventory_pct'), 1)} | {fv(s.get('cpu_gateway_pct'), 1)} | "
                f"{fv(s.get('cpu_kafka_pct'), 1)} | {fv(s.get('hikari_pending_inventory'), 1)} | "
                f"{fv(s.get('redis_lua_p99_ms'), 2)} | {fv(s.get('outbox_oldest_age_s'), 2)} | "
                f"{fv(s.get('consumer_lag_max'), 1)} |"
            )
        lines.append("")

    if analysis["correlations"]:
        lines.append("#### Signal Ranking by Pearson Correlation with Window p99")
        lines.append("")
        lines.append("| Rank | Signal | Pearson `r` | Paired Windows (`n`) |")
        lines.append("| ---: | :--- | ---: | ---: |")
        for idx, c in enumerate(analysis["correlations"], 1):
            r_str = f"{c['pearson_r']:+.4f}" if c["pearson_r"] is not None else "n/a (zero variance / <3 pts)"
            lines.append(f"| {idx} | `{c['signal']}` | `{r_str}` | {c['n']} |")
        lines.append("")

    lines.append(f"**Data Summary**: {analysis['statement']}")
    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Analyze 10s tail-latency correlations over measured window")
    parser.add_argument("results_dir", help="Results directory")
    parser.add_argument("--prometheus-url", default=os.environ.get("PROMETHEUS_URL", "http://127.0.0.1:9090"))
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()

    analysis = collect_and_analyze(args.results_dir, prom_url=args.prometheus_url, enable_live=not args.offline)
    with open(os.path.join(args.results_dir, "tail-analysis.json"), "w") as f:
        json.dump(analysis, f, indent=2)
        f.write("\n")
    md = render_tail_analysis_markdown(analysis)
    with open(os.path.join(args.results_dir, "tail-analysis.md"), "w") as f:
        f.write(md)
    print(md)


if __name__ == "__main__":
    main()
