#!/usr/bin/env python3
"""
Unit tests for scripts/loadtest-report.py using synthetic result files.
Runnable with: python3 -m unittest scripts/test_loadtest_report.py
"""

import importlib.util
import json
import os
import tempfile
import unittest

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadtest-report.py")
spec = importlib.util.spec_from_file_location("loadtest_report", SCRIPT_PATH)
loadtest_report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loadtest_report)


def make_flash_summary(p99_ms=42.5, p99_ok=True, unexp=0, unexp_ok=True):
    return {
        "_meta": {
            "scenario": "flash-sale",
            "run_id": "20261121T190000Z",
            "load_scale": 1.0,
            "user_pool_size": 5000,
            "seats_per_event": 5000,
            "pre_allocated_vus": 150,
            "max_vus": 500,
            "stages": [
                {"name": "ramp1", "duration_s": 10, "target_rps": 150},
                {"name": "plateau1", "duration_s": 60, "target_rps": 150},
                {"name": "ramp2", "duration_s": 15, "target_rps": 400},
                {"name": "plateau2", "duration_s": 60, "target_rps": 400},
                {"name": "rampdown", "duration_s": 10, "target_rps": 0},
            ],
            "plateau1_duration_s": 60,
            "plateau2_duration_s": 60,
            "plateau1_target_rps": 150,
            "plateau2_target_rps": 400,
        },
        "state": {"testRunDurationMs": 155000},
        "metrics": {
            "http_reqs": {"type": "counter", "values": {"count": 38000, "rate": 245.16}},
            "http_req_duration{name:hold}": {
                "type": "trend",
                "values": {"med": 8.1, "p(90)": 20.0, "p(95)": 28.0, "p(99)": p99_ms, "max": 95.0},
                "thresholds": {"p(99)<300": {"ok": p99_ok}},
            },
            "hold_duration": {
                "type": "trend",
                "values": {"med": 8.1, "p(90)": 20.0, "p(95)": 28.0, "p(99)": p99_ms, "max": 95.0},
            },
            "hold_created": {"type": "counter", "values": {"count": 5000}},
            "hold_conflict": {"type": "counter", "values": {"count": 33000 - unexp}},
            "hold_rate_limited": {"type": "counter", "values": {"count": 0}},
            "hold_rate_limited_rate": {
                "type": "rate",
                "values": {"rate": 0.0},
                "thresholds": {"rate<0.01": {"ok": True}},
            },
            "hold_unexpected": {
                "type": "counter",
                "values": {"count": unexp},
                "thresholds": {"count==0": {"ok": unexp_ok}},
            },
            "hold_5xx": {
                "type": "rate",
                "values": {"rate": 0.0},
                "thresholds": {"rate<0.001": {"ok": True}},
            },
            "plateau1_reqs": {"type": "counter", "values": {"count": 8970}},
            "plateau2_reqs": {"type": "counter", "values": {"count": 23940}},
            "dropped_iterations": {"type": "counter", "values": {"count": 0}},
        },
    }


def make_journey_summary(chk_p99=110.0, chk_ok=True):
    return {
        "_meta": {
            "scenario": "checkout-journey",
            "run_id": "20261121T190000Z",
            "load_scale": 1.0,
            "user_pool_size": 5000,
            "seats_per_event": 5000,
            "journey_rate_target": 25,
            "journey_duration_s": 180,
            "pre_allocated_vus": 100,
            "max_vus": 300,
            "poll_interval_ms": 250,
            "poll_timeout_ms": 30000,
        },
        "state": {"testRunDurationMs": 182000},
        "metrics": {
            "iterations": {"type": "counter", "values": {"count": 4500}},
            "http_reqs": {"type": "counter", "values": {"count": 22500, "rate": 123.6}},
            "http_req_duration{name:hold}": {
                "type": "trend",
                "values": {"med": 12.0, "p(90)": 25.0, "p(95)": 35.0, "p(99)": 80.0, "max": 150.0},
                "thresholds": {"p(99)<300": {"ok": True}},
            },
            "http_req_duration{name:checkout}": {
                "type": "trend",
                "values": {"med": 18.0, "p(90)": 40.0, "p(95)": 65.0, "p(99)": chk_p99, "max": 220.0},
                "thresholds": {"p(99)<500": {"ok": chk_ok}},
            },
            "order_materialization_latency": {
                "type": "trend",
                "values": {"med": 120.0, "p(90)": 300.0, "p(95)": 450.0, "p(99)": 900.0, "max": 1200.0},
            },
            "checkout_duration": {
                "type": "trend",
                "values": {"med": 18.0, "p(90)": 40.0, "p(95)": 65.0, "p(99)": chk_p99, "max": 220.0},
            },
            "saga_latency": {
                "type": "trend",
                "values": {"med": 650.0, "p(90)": 1100.0, "p(95)": 1450.0, "p(99)": 2200.0, "max": 3100.0},
                "thresholds": {"p(95)<5000": {"ok": True}},
            },
            "hold_created": {"type": "counter", "values": {"count": 4500}},
            "journey_confirmed": {"type": "counter", "values": {"count": 4050}},
            "journey_payment_failed": {"type": "counter", "values": {"count": 225}},
            "journey_abandoned": {"type": "counter", "values": {"count": 225}},
            "journeys_timed_out": {
                "type": "counter",
                "values": {"count": 0},
                "thresholds": {"count==0": {"ok": True}},
            },
            "unexpected_statuses": {
                "type": "counter",
                "values": {"count": 0},
                "thresholds": {"count==0": {"ok": True}},
            },
            "order_not_ready_retries": {"type": "counter", "values": {"count": 12}},
            "dropped_iterations": {"type": "counter", "values": {"count": 0}},
        },
    }


def make_audit(all_pass=True):
    items = [
        {"id": f"A{i}", "name": f"Invariant {i}", "status": "PASS", "detail": "ok"}
        for i in range(1, 10)
    ]
    if not all_pass:
        items[0]["status"] = "FAIL"
        items[0]["detail"] = "seats_with_multiple_active_items=1"
    return {
        "overall": "PASS" if all_pass else "FAIL",
        "quiescence": {"status": "PASS", "detail": "Quiesced in 2s"},
        "items": items,
    }


def write_common_files(rdir):
    with open(os.path.join(rdir, "run-meta.json"), "w") as f:
        json.dump({
            "run_id": "20261121T190000Z",
            "start_epoch": 1700000000,
            "end_epoch": 1700000350,
            "load_scale": 1.0,
            "k6_image": "grafana/k6:0.54.0@sha256:test",
        }, f)
    with open(os.path.join(rdir, "docker-stats.csv"), "w") as f:
        f.write("timestamp_utc,container,cpu_pct,mem_usage,mem_limit,mem_pct\n")
        f.write("2026-11-21T19:00:05Z,gateway-service,45.2,210MiB,512MiB,41.0\n")
        f.write("2026-11-21T19:00:10Z,gateway-service,65.8,230MiB,512MiB,44.9\n")
        f.write("2026-11-21T19:00:05Z,inventory-service,55.0,260MiB,512MiB,50.8\n")
        f.write("2026-11-21T19:00:05Z,order-service,22.0,190MiB,512MiB,37.1\n")
        f.write("2026-11-21T19:00:05Z,kafka,31.5,400MiB,2GiB,19.5\n")
    for fname in ("containers-start.csv", "containers-end.csv"):
        with open(os.path.join(rdir, fname), "w") as f:
            f.write("container,restart_count,oom_killed,mem_limit_bytes,status\n")
            f.write("gateway-service,0,false,536870912,running\n")
            f.write("inventory-service,0,false,536870912,running\n")


class TestLoadtestReport(unittest.TestCase):
    def test_all_pass_renders_pass_and_publishes(self):
        with tempfile.TemporaryDirectory() as tmp:
            rdir = os.path.join(tmp, "20261121T190000Z")
            os.makedirs(rdir)
            write_common_files(rdir)
            with open(os.path.join(rdir, "flash-sale-summary.json"), "w") as f:
                json.dump(make_flash_summary(), f)
            with open(os.path.join(rdir, "checkout-journey-summary.json"), "w") as f:
                json.dump(make_journey_summary(), f)
            with open(os.path.join(rdir, "audit.json"), "w") as f:
                json.dump(make_audit(all_pass=True), f)

            pub_dest = os.path.join(tmp, "docs", "load-test-report.md")
            res = loadtest_report.generate_report(rdir, publish=True, publish_dest=pub_dest, enable_live=False)

            self.assertTrue(res["overall_ok"])
            self.assertEqual(res["overall_status"], "PASS")
            self.assertTrue(res["published"])
            self.assertTrue(os.path.isfile(pub_dest))

            with open(res["report_path"]) as f:
                content = f.read()
            self.assertIn("**Overall Verdict**: **PASS**", content)
            self.assertIn("**399.00**", content)  # 23940 / 60s computed plateau2 throughput
            self.assertIn("Single-node Docker on a laptop", content)
            self.assertIn("`gateway-service`", content)

    def test_failing_threshold_renders_fail_and_does_not_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            rdir = os.path.join(tmp, "20261121T190000Z")
            os.makedirs(rdir)
            write_common_files(rdir)
            with open(os.path.join(rdir, "flash-sale-summary.json"), "w") as f:
                json.dump(make_flash_summary(p99_ms=412.7, p99_ok=False), f)
            with open(os.path.join(rdir, "checkout-journey-summary.json"), "w") as f:
                json.dump(make_journey_summary(), f)
            with open(os.path.join(rdir, "audit.json"), "w") as f:
                json.dump(make_audit(all_pass=True), f)

            pub_dest = os.path.join(tmp, "docs", "load-test-report.md")
            res = loadtest_report.generate_report(rdir, publish=True, publish_dest=pub_dest, enable_live=False)

            self.assertFalse(res["overall_ok"])
            self.assertEqual(res["overall_status"], "FAIL")
            self.assertFalse(res["published"])
            self.assertFalse(os.path.exists(pub_dest))

            with open(res["report_path"]) as f:
                content = f.read()
            self.assertIn("**Overall Verdict**: **FAIL**", content)
            self.assertIn("| `http_req_duration{name:hold}` | `p(99)<300` | `412.70 ms` | **FAIL** |", content)

    def test_missing_audit_renders_not_run_and_does_not_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            rdir = os.path.join(tmp, "20261121T190000Z")
            os.makedirs(rdir)
            write_common_files(rdir)
            with open(os.path.join(rdir, "flash-sale-summary.json"), "w") as f:
                json.dump(make_flash_summary(), f)

            pub_dest = os.path.join(tmp, "docs", "load-test-report.md")
            res = loadtest_report.generate_report(rdir, publish=True, publish_dest=pub_dest, enable_live=False)

            self.assertFalse(res["overall_ok"])
            self.assertEqual(res["audit_status"], "NOT RUN")
            self.assertFalse(res["published"])
            self.assertFalse(os.path.exists(pub_dest))

            with open(res["report_path"]) as f:
                content = f.read()
            self.assertIn("**Post-Run Audit Verdict**: **NOT RUN**", content)
            self.assertIn("**Audit Status**: **NOT RUN**", content)

    def test_failing_audit_renders_fail_and_does_not_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            rdir = os.path.join(tmp, "20261121T190000Z")
            os.makedirs(rdir)
            write_common_files(rdir)
            with open(os.path.join(rdir, "flash-sale-summary.json"), "w") as f:
                json.dump(make_flash_summary(), f)
            with open(os.path.join(rdir, "audit.json"), "w") as f:
                json.dump(make_audit(all_pass=False), f)

            pub_dest = os.path.join(tmp, "docs", "load-test-report.md")
            res = loadtest_report.generate_report(rdir, publish=True, publish_dest=pub_dest, enable_live=False)

            self.assertFalse(res["overall_ok"])
            self.assertEqual(res["audit_status"], "FAIL")
            self.assertFalse(res["published"])
            self.assertFalse(os.path.exists(pub_dest))

            with open(res["report_path"]) as f:
                content = f.read()
            self.assertIn("| `A1` | **FAIL** |", content)

    def test_no_hardcoded_pass_in_markdown_templates(self):
        """Ensure report generator never hardcodes '**PASS**' in template strings."""
        with open(SCRIPT_PATH) as f:
            src = f.read()
        self.assertNotIn("**PASS**", src)
        self.assertNotIn("| PASS |", src)


if __name__ == "__main__":
    unittest.main()
