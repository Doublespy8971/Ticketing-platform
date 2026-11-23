#!/usr/bin/env python3
"""
Unit tests for scripts/loadtest-tail-analysis.py using synthetic 10s window series.
Runnable with: python3 -m unittest scripts/test_loadtest_tail_analysis.py
"""

import importlib.util
import os
import unittest

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadtest-tail-analysis.py")
spec = importlib.util.spec_from_file_location("loadtest_tail_analysis", SCRIPT_PATH)
ta = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ta)


class TestLoadtestTailAnalysis(unittest.TestCase):
    def test_correlating_and_non_correlating_signals_ranked_correctly(self):
        # 12 synthetic 10s windows where gc_pause_max_ms tracks p99 linearly,
        # cpu_inventory_pct is flat (undefined r), and redis_lua_p99_ms is uncorrelated.
        p99_vals = [50, 80, 120, 450, 90, 620, 110, 75, 510, 130, 880, 60]
        gc_vals = [10, 20, 35, 240, 25, 310, 30, 15, 260, 40, 450, 12]
        uncorr_vals = [5, 12, 8, 6, 15, 7, 11, 14, 9, 13, 6, 10]
        windows = []
        for idx, (p, g, u) in enumerate(zip(p99_vals, gc_vals, uncorr_vals)):
            windows.append({
                "epoch": 1700000000 + idx * 10,
                "inv_hold_p99_ms": float(p),
                "gw_hold_p99_ms": float(p + 15),
                "signals": {
                    "gc_pause_max_ms": float(g),
                    "cpu_inventory_pct": 120.0,  # constant -> r is None
                    "redis_lua_p99_ms": float(u),
                },
            })

        res = ta.analyze_windows(windows)
        self.assertEqual(res["target_metric"], "inv_hold_p99_ms")
        self.assertEqual(res["valid_p99_windows"], 12)
        self.assertEqual(res["high_p99_windows_gt_300ms"], 4)
        self.assertEqual(len(res["worst_10_windows"]), 10)
        self.assertEqual(res["worst_10_windows"][0]["inv_hold_p99_ms"], 880.0)

        corr_by_sig = {c["signal"]: c for c in res["correlations"]}
        self.assertEqual(res["correlations"][0]["signal"], "gc_pause_max_ms")
        self.assertGreater(corr_by_sig["gc_pause_max_ms"]["pearson_r"], 0.99)
        self.assertEqual(corr_by_sig["gc_pause_max_ms"]["n"], 12)
        self.assertIsNone(corr_by_sig["cpu_inventory_pct"]["pearson_r"])
        self.assertLess(abs(corr_by_sig["redis_lua_p99_ms"]["pearson_r"]), 0.6)
        self.assertIn("JVM GC pause max > 200 ms in 4/4 high-p99 windows (100%)", res["statement"])
        self.assertIn("Correlation is not causation", res["statement"])

        md = ta.render_tail_analysis_markdown(res)
        self.assertIn("10 Worst Hold p99 Windows", md)
        self.assertIn("`gc_pause_max_ms`", md)

    def test_windows_with_missing_data_and_gateway_fallback(self):
        # inv_hold_p99_ms is all None -> falls back to gw_hold_p99_ms; sparse signal (<3 points) -> r=None
        windows = [
            {"epoch": 1700000000, "inv_hold_p99_ms": None, "gw_hold_p99_ms": None, "signals": {"hikari_pending_inventory": None}},
            {"epoch": 1700000010, "inv_hold_p99_ms": None, "gw_hold_p99_ms": 120.0, "signals": {"hikari_pending_inventory": 0.0}},
            {"epoch": 1700000020, "inv_hold_p99_ms": None, "gw_hold_p99_ms": 210.0, "signals": {"hikari_pending_inventory": 2.0}},
            {"epoch": 1700000030, "inv_hold_p99_ms": None, "gw_hold_p99_ms": 180.0, "signals": {"hikari_pending_inventory": None}},
        ]
        res = ta.analyze_windows(windows)
        self.assertEqual(res["target_metric"], "gw_hold_p99_ms")
        self.assertEqual(res["valid_p99_windows"], 3)
        self.assertEqual(res["high_p99_windows_gt_300ms"], 0)
        self.assertEqual(len(res["worst_10_windows"]), 3)
        self.assertIsNone(res["correlations"][0]["pearson_r"])
        self.assertEqual(res["correlations"][0]["n"], 2)
        self.assertIn("No 10-second windows exceeded 300 ms", res["statement"])

    def test_empty_windows_handled_gracefully(self):
        res = ta.analyze_windows([])
        self.assertEqual(res["valid_p99_windows"], 0)
        self.assertEqual(res["worst_10_windows"], [])
        self.assertEqual(res["correlations"], [])
        md = ta.render_tail_analysis_markdown(res)
        self.assertIn("0/0` windows with p99 data", md)


if __name__ == "__main__":
    unittest.main()
