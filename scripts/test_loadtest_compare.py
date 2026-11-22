#!/usr/bin/env python3
"""
Unit tests for scripts/loadtest-compare.py using synthetic result directories.
Runnable with: python3 -m unittest scripts/test_loadtest_compare.py
"""

import importlib.util
import json
import os
import tempfile
import unittest

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadtest-compare.py")
spec = importlib.util.spec_from_file_location("loadtest_compare", SCRIPT_PATH)
loadtest_compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loadtest_compare)


def write_synthetic_run(rdir, run_id, p2_reqs, hold_p99, chk_p99, saga_p95,
                        gw_5xx, pay_lag, inv_hikari, gw_gc, inv_cpu, audit_overall, q_sec):
    os.makedirs(rdir, exist_ok=True)
    with open(os.path.join(rdir, "run-meta.json"), "w") as f:
        json.dump({"run_id": run_id, "start_epoch": 1000, "end_epoch": 1350}, f)
    with open(os.path.join(rdir, "flash-sale-summary.json"), "w") as f:
        json.dump({
            "_meta": {"plateau1_duration_s": 60, "plateau2_duration_s": 60},
            "metrics": {
                "plateau1_reqs": {"values": {"count": 9000}},
                "plateau2_reqs": {"values": {"count": p2_reqs}},
                "http_req_duration{name:hold}": {"values": {"p(99)": hold_p99}},
                "hold_rate_limited": {"values": {"count": 0}},
                "hold_unexpected": {"values": {"count": int(gw_5xx)}},
            },
        }, f)
    with open(os.path.join(rdir, "checkout-journey-summary.json"), "w") as f:
        json.dump({
            "_meta": {"journey_duration_s": 180},
            "metrics": {
                "iterations": {"values": {"count": 4500}},
                "http_req_duration{name:hold}": {"values": {"p(99)": hold_p99 * 0.5}},
                "http_req_duration{name:checkout}": {"values": {"p(99)": chk_p99}},
                "saga_latency": {"values": {"p(95)": saga_p95}},
                "unexpected_statuses": {"values": {"count": 0}},
                "journeys_timed_out": {"values": {"count": 0}},
            },
        }, f)
    with open(os.path.join(rdir, "prometheus-metrics.json"), "w") as f:
        json.dump({
            "available": True,
            "service_5xx_counts": {"gateway-service": gw_5xx, "inventory-service": 0.0},
            "peak_consumer_lag": {"payment-service-order-cg": pay_lag},
            "peak_hikari_pending": {"inventory-service": inv_hikari, "order-service": 2.0},
            "total_jvm_gc_pause_seconds": {"gateway-service": gw_gc, "inventory-service": 0.4},
        }, f)
    with open(os.path.join(rdir, "docker-stats.csv"), "w") as f:
        f.write("timestamp_utc,container,cpu_pct,mem_usage,mem_limit,mem_pct\n")
        f.write(f"2026-11-22T12:00:05Z,inventory-service,{inv_cpu},300MiB,768MiB,39.0\n")
        f.write("2026-11-22T12:00:05Z,gateway-service,180.0,280MiB,768MiB,36.5\n")
        f.write("2026-11-22T12:00:05Z,kafka,150.0,900MiB,7.75GiB,11.3\n")
    with open(os.path.join(rdir, "audit.json"), "w") as f:
        q_detail = f"Quiesced in {q_sec}s" if audit_overall == "PASS" else f"Timed out after {q_sec}s"
        json.dump({
            "overall": audit_overall,
            "quiescence": {"status": audit_overall, "detail": q_detail},
            "items": [],
        }, f)


class TestLoadtestCompare(unittest.TestCase):
    def test_computes_median_and_range_across_three_after_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = os.path.join(tmp, "baseline")
            a1 = os.path.join(tmp, "after1")
            a2 = os.path.join(tmp, "after2")
            a3 = os.path.join(tmp, "after3")

            write_synthetic_run(base, "BASE", 17820, 4215.5, 1953.7, 31285.4, 130.0, 1478.0, 179.0, 44.36, 315.2, "FAIL", 125)
            write_synthetic_run(a1, "RUN1", 23400, 210.0, 180.0, 1800.0, 0.0, 10.0, 4.0, 1.20, 220.0, "PASS", 4)
            write_synthetic_run(a2, "RUN2", 24000, 240.0, 200.0, 2100.0, 0.0, 15.0, 6.0, 1.50, 240.0, "PASS", 6)
            write_synthetic_run(a3, "RUN3", 23700, 270.0, 250.0, 2400.0, 1.0, 25.0, 9.0, 1.80, 260.0, "PASS", 8)

            md = loadtest_compare.render_comparison_markdown(base, [a1, a2, a3])

            self.assertIn("| Flash Sale Plateau 2 Throughput | 297.00 req/s | 395.00 req/s | 390.00 req/s .. 400.00 req/s |", md)
            self.assertIn("| Flash Sale Hold p99 | 4215.50 ms | 240.00 ms | 210.00 ms .. 270.00 ms |", md)
            self.assertIn("| Journey Checkout p99 | 1953.70 ms | 200.00 ms | 180.00 ms .. 250.00 ms |", md)
            self.assertIn("| Journey Saga p95 | 31285.40 ms | 2100.00 ms | 1800.00 ms .. 2400.00 ms |", md)
            self.assertIn("| Peak Consumer Lag (`payment-service-order-cg`) | 1478.0 | 15.0 | 10.0 .. 25.0 |", md)
            self.assertIn("| Peak HikariCP Pending (`inventory-service`) | 179.0 | 6.0 | 4.0 .. 9.0 |", md)
            self.assertIn("| Total JVM GC Pause (`gateway-service`) | 44.360 s | 1.500 s | 1.200 s .. 1.800 s |", md)
            self.assertIn("| Time to Quiescence | 125 s (FAIL) | 6 s | 4 s .. 8 s |", md)
            self.assertIn("| Audit Result | **FAIL** | 3/3 PASS | PASS, PASS, PASS |", md)

    def test_falls_back_to_report_md_when_prometheus_json_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = os.path.join(tmp, "legacy_base")
            after = os.path.join(tmp, "new_after")
            write_synthetic_run(base, "LEGACY", 18000, 500.0, 300.0, 4000.0, 10.0, 100.0, 50.0, 5.0, 200.0, "PASS", 5)
            os.remove(os.path.join(base, "prometheus-metrics.json"))
            with open(os.path.join(base, "report.md"), "w") as f:
                f.write("- **Gateway 5xx Count (window)**: `130.30`\n")
                f.write("| Peak Consumer Lag | `payment-service-order-cg` | 1478.0 |\n")

            write_synthetic_run(after, "AFTER", 24000, 200.0, 150.0, 1500.0, 0.0, 12.0, 3.0, 1.1, 190.0, "PASS", 3)

            md = loadtest_compare.render_comparison_markdown(base, [after])
            self.assertIn("| 5xx Count (`gateway-service`) | 130.3 | 0.0 | 0.0 .. 0.0 |", md)
            self.assertIn("| Peak Consumer Lag (`payment-service-order-cg`) | 1478.0 | 12.0 | 12.0 .. 12.0 |", md)
            self.assertIn("| Peak HikariCP Pending (`inventory-service`) | n/a | 3.0 | 3.0 .. 3.0 |", md)

    def test_excludes_invalid_after_runs_from_median_and_range(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = os.path.join(tmp, "base")
            a1 = os.path.join(tmp, "a1")
            a2_invalid = os.path.join(tmp, "a2")
            write_synthetic_run(base, "BASE", 18000, 4200.0, 1900.0, 31000.0, 130.0, 1478.0, 179.0, 44.36, 315.0, "FAIL", 125)
            write_synthetic_run(a1, "VALID1", 24000, 200.0, 250.0, 2000.0, 0.0, 10.0, 2.0, 1.2, 220.0, "PASS", 5)
            write_synthetic_run(a2_invalid, "INVALID1", 1000, 480000.0, 15000.0, 45000.0, 500.0, 999.0, 200.0, 489.0, 9999.0, "FAIL", 99)
            with open(os.path.join(a2_invalid, "INVALID"), "w") as f:
                f.write("Host sleep gap 487s > 15s\n")

            md = loadtest_compare.render_comparison_markdown(base, [a1, a2_invalid])
            self.assertIn("Excluded INVALID Runs (NOT COUNTED)", md)
            self.assertIn("INVALID1", md)
            self.assertIn("| Flash Sale Hold p99 | 4200.00 ms | 200.00 ms | 200.00 ms .. 200.00 ms |", md)


if __name__ == "__main__":
    unittest.main()
