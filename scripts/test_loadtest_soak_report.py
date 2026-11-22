#!/usr/bin/env python3
"""
Unit tests for scripts/loadtest-soak-report.py drift detection using synthetic series:
  - flat series (no drift)
  - rising heap after GC (>20% positive linear regression slope)
  - rising p99 (>1.3x first full window)
  - monotonic growth over last 10 minutes (5 consecutive 2-min windows) in lag/outbox/Hikari
"""

import importlib.util
import os
import unittest

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadtest-soak-report.py")
spec = importlib.util.spec_from_file_location("loadtest_soak_report", SCRIPT_PATH)
soak = importlib.util.module_from_spec(spec)
spec.loader.exec_module(soak)


def make_flat_windows(n=10):
    windows = []
    for i in range(n):
        windows.append({
            "window_index": i + 1,
            "end_epoch": 1700000000 + (i + 1) * 120,
            "label": f"W{i+1:02d}",
            "hold_p99_ms": 50.0 + (i % 2) * 2.0,
            "checkout_p99_ms": 90.0 + (i % 2) * 3.0,
            "heap_old_gen_mib": {
                "gateway-service": 80.0 + (i % 3),
                "inventory-service": 95.0,
                "order-service": 70.0,
                "payment-service": 60.0,
            },
            "gc_pause_s": {"gateway-service": 0.1, "inventory-service": 0.1},
            "consumer_lag": {"payment-service-order-cg": 0.0},
            "outbox_age_s": {"inventory-service": 0.15},
            "hikari_pending": {"inventory-service": 0.0},
            "container_mem_mib": {"gateway-service": 300.0},
        })
    return windows


class TestLoadtestSoakDrift(unittest.TestCase):
    def test_flat_series_has_no_drift(self):
        windows = make_flat_windows(10)
        res = soak.detect_drift(windows)
        self.assertFalse(res["drift_detected"])
        self.assertEqual(res["findings"], [])

    def test_rising_heap_above_20pct_flags_drift(self):
        windows = make_flat_windows(10)
        # Grow inventory-service old-gen heap from 100 MiB to 145 MiB (+45%)
        for i, w in enumerate(windows):
            w["heap_old_gen_mib"]["inventory-service"] = 100.0 + i * 5.0
        res = soak.detect_drift(windows)
        self.assertTrue(res["drift_detected"])
        self.assertTrue(any("DRIFT(heap:inventory-service)" in f for f in res["findings"]))

    def test_rising_p99_above_1_3x_flags_drift(self):
        windows = make_flat_windows(10)
        windows[0]["hold_p99_ms"] = 50.0
        windows[-1]["hold_p99_ms"] = 72.0  # 1.44x > 1.3x
        res = soak.detect_drift(windows)
        self.assertTrue(res["drift_detected"])
        self.assertTrue(any("DRIFT(p99:hold)" in f for f in res["findings"]))

    def test_monotonic_lag_over_last_10m_flags_drift(self):
        windows = make_flat_windows(10)
        # Last 5 windows strictly increasing: 1, 3, 6, 10, 15
        for idx, val in zip(range(5, 10), [1.0, 3.0, 6.0, 10.0, 15.0]):
            windows[idx]["consumer_lag"]["payment-service-order-cg"] = val
        res = soak.detect_drift(windows)
        self.assertTrue(res["drift_detected"])
        self.assertTrue(any("DRIFT(consumer_lag:payment-service-order-cg)" in f for f in res["findings"]))


if __name__ == "__main__":
    unittest.main()
