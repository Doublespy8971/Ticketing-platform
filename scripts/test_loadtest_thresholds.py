#!/usr/bin/env python3
"""
Unit test proving loadtest/lib/thresholds.js matches the untouched threshold
definitions in loadtest/flash-sale.js and loadtest/checkout-journey.js
byte-for-byte (as canonical JSON), and that sustained/soak scenarios import
from ./lib/thresholds.js.
"""

import json
import os
import re
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def parse_js_thresholds_block(js_text, marker):
    idx = js_text.find(marker)
    if idx < 0:
        raise ValueError(f"Marker not found: {marker}")
    start = js_text.index("{", idx)
    depth = 0
    end = start
    for i in range(start, len(js_text)):
        ch = js_text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    raw = js_text[start : end + 1]
    # Strip // comments, convert single quotes to double quotes, remove trailing commas
    no_comments = re.sub(r"//.*", "", raw)
    double_quoted = no_comments.replace("'", '"')
    no_trailing = re.sub(r",\s*([}\]])", r"\1", double_quoted)
    return json.loads(no_trailing)


class TestLoadtestThresholds(unittest.TestCase):
    def test_shared_thresholds_match_existing_scenarios_identically(self):
        lib_txt = open(os.path.join(REPO_ROOT, "loadtest", "lib", "thresholds.js")).read()
        flash_txt = open(os.path.join(REPO_ROOT, "loadtest", "flash-sale.js")).read()
        journey_txt = open(os.path.join(REPO_ROOT, "loadtest", "checkout-journey.js")).read()

        shared_flash = parse_js_thresholds_block(lib_txt, "export const FLASH_SALE_THRESHOLDS")
        existing_flash = parse_js_thresholds_block(flash_txt, "thresholds:")
        self.assertEqual(
            json.dumps(shared_flash, sort_keys=True),
            json.dumps(existing_flash, sort_keys=True),
        )

        shared_journey = parse_js_thresholds_block(lib_txt, "export const CHECKOUT_JOURNEY_THRESHOLDS")
        existing_journey = parse_js_thresholds_block(journey_txt, "thresholds:")
        self.assertEqual(
            json.dumps(shared_journey, sort_keys=True),
            json.dumps(existing_journey, sort_keys=True),
        )

    def test_sustained_and_soak_scenarios_import_shared_thresholds(self):
        sh = open(os.path.join(REPO_ROOT, "loadtest", "sustained-hold.js")).read()
        sj = open(os.path.join(REPO_ROOT, "loadtest", "sustained-journey.js")).read()
        soak = open(os.path.join(REPO_ROOT, "loadtest", "soak.js")).read()

        self.assertIn("import { FLASH_SALE_THRESHOLDS } from './lib/thresholds.js';", sh)
        self.assertIn("thresholds: FLASH_SALE_THRESHOLDS", sh)
        self.assertIn("import { CHECKOUT_JOURNEY_THRESHOLDS } from './lib/thresholds.js';", sj)
        self.assertIn("thresholds: CHECKOUT_JOURNEY_THRESHOLDS", sj)
        self.assertIn("import { CHECKOUT_JOURNEY_THRESHOLDS } from './lib/thresholds.js';", soak)
        self.assertIn("thresholds: CHECKOUT_JOURNEY_THRESHOLDS", soak)


if __name__ == "__main__":
    unittest.main()
