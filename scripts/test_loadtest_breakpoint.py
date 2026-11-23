#!/usr/bin/env python3
"""
Unit tests for scripts/loadtest-breakpoint.py using synthetic breakpoint summaries.
"""

import importlib.util
import json
import os
import tempfile
import unittest

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadtest-breakpoint.py")
spec = importlib.util.spec_from_file_location("loadtest_breakpoint", SCRIPT_PATH)
bp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bp)


def make_hold_bp_summary(step_specs, global_dropped=0):
    """step_specs: list of (target_rps, achieved_count, p99_ms, err_rate, unknown_cnt)."""
    steps = []
    metrics = {"dropped_iterations": {"type": "counter", "values": {"count": global_dropped}}}
    for idx, (target, cnt, p99, err, unk) in enumerate(step_specs, 1):
        tag = f"s{idx:02d}"
        steps.append({
            "index": idx,
            "tag": tag,
            "target_rps": target,
            "duration_s": 30,
            "start_epoch": 1700000000 + (idx - 1) * 30,
            "end_epoch": 1700000000 + idx * 30,
        })
        metrics[f"http_req_duration{{step:{tag},name:hold}}"] = {
            "type": "trend",
            "values": {"med": p99 * 0.2, "p(95)": p99 * 0.7, "p(99)": p99, "max": p99 * 1.2},
        }
        metrics[f"bp_step_reqs{{step:{tag}}}"] = {"type": "counter", "values": {"count": cnt}}
        metrics[f"bp_step_error{{step:{tag}}}"] = {"type": "rate", "values": {"rate": err}}
        metrics[f"bp_step_unknown{{step:{tag}}}"] = {"type": "counter", "values": {"count": unk}}
    return {
        "_meta": {
            "scenario": "breakpoint-hold",
            "run_id": "20261122T150000Z",
            "step_rps": 25,
            "step_seconds": 30,
            "steps": steps,
        },
        "metrics": metrics,
    }


class TestLoadtestBreakpoint(unittest.TestCase):
    def test_derives_breakpoint_and_70pct_sustainable_rate(self):
        # Steps 25, 50, 75, 100 pass; 125 fails p99 -> breakpoint = 100, 0.70*100=70 -> rounded down to 50
        s = make_hold_bp_summary([
            (25, 750, 40.0, 0.0, 0),
            (50, 1500, 90.0, 0.0, 0),
            (75, 2250, 180.0, 0.0, 0),
            (100, 3000, 260.0, 0.0, 0),
            (125, 3750, 410.0, 0.0, 0),
            (150, 4500, 210.0, 0.0, 0),  # passing later step must NOT override earlier failure
        ])
        res = bp.analyze_breakpoint_summary(s, enable_live=False)
        self.assertTrue(res["breakpoint_found"])
        self.assertEqual(res["breakpoint_step"], "s04")
        self.assertEqual(res["breakpoint_rate"], 100)
        self.assertEqual(res["sustainable_rate"], 50)

    def test_sustainable_rate_clamps_to_at_least_one_step(self):
        s = make_hold_bp_summary([
            (25, 750, 120.0, 0.0, 0),
            (50, 1500, 350.0, 0.0, 0),
        ])
        res = bp.analyze_breakpoint_summary(s, enable_live=False)
        self.assertEqual(res["breakpoint_rate"], 25)
        self.assertEqual(res["sustainable_rate"], 25)  # floor(17.5/25)*25=0 -> clamped to 25

    def test_no_breakpoint_found_does_not_write_profile(self):
        s = make_hold_bp_summary([
            (25, 750, 450.0, 0.0, 0),
            (50, 1500, 600.0, 0.01, 2),
        ])
        res = bp.analyze_breakpoint_summary(s, enable_live=False)
        self.assertFalse(res["breakpoint_found"])
        self.assertIsNone(res["sustainable_rate"])
        self.assertIn("NO BREAKPOINT FOUND", bp.render_breakpoint_markdown(res))

        with tempfile.TemporaryDirectory() as tmp:
            hdir = os.path.join(tmp, "h")
            jdir = os.path.join(tmp, "j")
            os.makedirs(hdir)
            os.makedirs(jdir)
            with open(os.path.join(hdir, "breakpoint.json"), "w") as f:
                json.dump(res, f)
            with open(os.path.join(jdir, "breakpoint.json"), "w") as f:
                json.dump(res, f)
            prof = os.path.join(tmp, "sustainable-profile.json")
            ok = bp.combine_sustainable_profile(hdir, jdir, prof)
            self.assertFalse(ok)
            self.assertFalse(os.path.exists(prof))

    def test_knee_detected_on_achieved_below_95pct_and_warmup_shown_separately(self):
        s = make_hold_bp_summary([
            (25, 750, 40.0, 0.0, 0),
            (50, 1500, 90.0, 0.0, 0),
            (75, 2250, 150.0, 0.0, 0),
            (100, 2700, 220.0, 0.0, 0),  # 90.0 req/s < 95.0 (95% of 100) -> knee at s04 (100 req/s)
            (125, 3100, 420.0, 0.0, 0),
        ])
        s["_meta"]["warmup_seconds"] = 90
        s["_meta"]["warmup_rate"] = 50
        s["_meta"]["seat_supply"] = "ample"
        s["metrics"]["warmup_reqs"] = {"type": "counter", "values": {"count": 4500}}
        s["metrics"]["warmup_hold_created"] = {"type": "counter", "values": {"count": 4500}}
        s["metrics"]["http_req_duration{step:warmup,name:warmup_hold}"] = {
            "type": "trend",
            "values": {"med": 12.0, "p(95)": 45.0, "p(99)": 210.0, "max": 480.0},
        }
        s["metrics"]["hold_duration{step:s01,outcome:created}"] = {
            "type": "trend",
            "values": {"med": 8.0, "p(95)": 25.0, "p(99)": 38.0, "max": 50.0},
        }
        s["metrics"]["hold_duration{outcome:created}"] = {
            "type": "trend",
            "values": {"med": 15.0, "p(95)": 90.0, "p(99)": 245.0, "max": 510.0},
        }

        res = bp.analyze_breakpoint_summary(s, enable_live=False)
        self.assertTrue(res["knee_found"])
        self.assertEqual(res["knee_step"], "s04")
        self.assertEqual(res["knee_rate"], 100)
        self.assertIn("< 95%", res["knee_derivation"])
        self.assertEqual(res["overall_created_p99_ms"], 245.0)
        self.assertEqual(res["warmup"]["requests"], 4500)
        md = bp.render_breakpoint_markdown(res)
        self.assertIn("**Knee**:", md)
        self.assertIn("Warm-up (informational, excluded from breakpoint/knee)", md)
        self.assertIn("245.00 ms", md)

    def test_knee_detected_on_dropped_iterations_above_1pct(self):
        s = make_hold_bp_summary([
            (25, 750, 40.0, 0.0, 0),
            (50, 1480, 90.0, 0.0, 0),  # 49.33 req/s >= 95% of 50, but 20 dropped > 1% of 1500 (15)
        ], global_dropped=20)
        s["metrics"]["dropped_iterations{scenario:step_s02}"] = {"type": "counter", "values": {"count": 20}}
        res = bp.analyze_breakpoint_summary(s, enable_live=False)
        self.assertTrue(res["knee_found"])
        self.assertEqual(res["knee_step"], "s02")
        self.assertEqual(res["knee_rate"], 50)
        self.assertIn("dropped 20", res["knee_derivation"])


if __name__ == "__main__":
    unittest.main()
