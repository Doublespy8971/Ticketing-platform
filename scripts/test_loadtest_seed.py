#!/usr/bin/env python3
"""
Unit tests for scripts/loadtest-seed.py seat planning and 150,000 hard cap enforcement.
Runnable with: python3 -m unittest scripts/test_loadtest_seed.py
"""

import importlib.util
import os
import unittest

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadtest-seed.py")
spec = importlib.util.spec_from_file_location("loadtest_seed", SCRIPT_PATH)
seeder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seeder)


class TestLoadtestSeed(unittest.TestCase):
    def test_staircase_hard_cap_lowers_max_from_600_to_450_in_ample_mode(self):
        plan = seeder.compute_staircase_plan(
            start=25, step=25, max_rate=600, step_seconds=30,
            warmup_rate=50, warmup_seconds=90, seat_supply="ample",
        )
        self.assertTrue(plan["cap_applied"])
        self.assertEqual(plan["requested_max"], 600)
        self.assertEqual(plan["effective_max"], 450)
        self.assertLessEqual(plan["total_seats"], seeder.HARD_MAX_SEATS)
        self.assertIsNotNone(plan["cap_reason"])

    def test_staircase_scarce_mode_uses_single_event_pool(self):
        plan = seeder.compute_staircase_plan(
            start=25, step=25, max_rate=600, step_seconds=30,
            warmup_rate=50, warmup_seconds=90, seat_supply="scarce",
        )
        self.assertFalse(plan["cap_applied"])
        self.assertEqual(plan["effective_max"], 600)
        self.assertEqual(plan["measured_seats"], seeder.DEFAULT_SEATS_PER_EVENT)

    def test_steady_plan_respects_hard_cap(self):
        plan = seeder.compute_steady_plan(
            rate=100, duration_seconds=300, ramp_seconds=20, warmup_rate=50, warmup_seconds=90,
            seat_supply="ample",
        )
        self.assertLessEqual(plan["total_seats"], seeder.HARD_MAX_SEATS)
        self.assertEqual(plan["seat_supply"], "ample")
        self.assertGreaterEqual(plan["measured_seats"], 32000)


if __name__ == "__main__":
    unittest.main()
