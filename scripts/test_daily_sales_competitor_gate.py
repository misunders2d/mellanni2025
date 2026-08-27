#!/usr/bin/env python3
"""Regression tests for mandatory daily competitor evidence."""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


PREP = load_module("prepare_daily_competitor_check", ROOT / "prepare_daily_competitor_check.py")
REPORT = load_module("build_daily_sales_report_competitor_test", ROOT / "build_daily_sales_report.py")


class DailyCompetitorGateTest(unittest.TestCase):
    def test_requires_tmp_artifact_paths(self) -> None:
        blocked = Path("/media/misunderstood/DATA/projects/mellanni2025/reports/daily_sales/test.json")
        with self.assertRaisesRegex(SystemExit, "must stay under /tmp"):
            PREP.require_tmp(blocked)
        with self.assertRaisesRegex(SystemExit, "must stay under /tmp"):
            REPORT.require_tmp(blocked.parent)
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            subprocess.run(["git", "init", "-q", tmp], check=True)
            with self.assertRaisesRegex(SystemExit, "git worktree"):
                PREP.require_tmp(Path(tmp) / "competitor.json")

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.cache = self.root / "cache"
        self.cache.mkdir()
        self.config = {
            "marketplace": "Amazon.com",
            "products": [
                {"brand": "Amazon Basics", "asin": "B00Q7OARO2", "label": "Amazon set"},
                {"brand": "Utopia Bedding", "asin": "B00NX0WXQI", "label": "Utopia set"},
                {"brand": "CGK Unlimited", "asin": "B01M16WBW1", "label": "CGK set"},
            ],
        }
        self.config_path = self.root / "competitors.json"
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_cache(
        self,
        asin: str,
        brand: str,
        *,
        deal: bool = False,
        deal_type: str = "LIMITED_TIME_DEAL",
        cached_at: float | None = None,
        payload_asin: str | None = None,
        buy_box_shipping_cents: int = 0,
    ) -> None:
        csv_data = [None] * 36
        csv_data[0] = [1, 2500]
        csv_data[1] = [1, 2000]
        csv_data[4] = [1, 3999]
        csv_data[18] = [1, 1999, buy_box_shipping_cents]
        product = {
            "asin": payload_asin or asin,
            "brand": brand,
            "title": f"{brand} sheets",
            "_cached_at": cached_at if cached_at is not None else time.time(),
            "csv": csv_data,
            "deals": [{
                "dealType": deal_type,
                "badge": "Lightning Deal" if deal_type == "LIGHTNING_DEAL" else "Limited time deal",
                "accessType": "ALL",
            }] if deal else [],
        }
        (self.cache / f"{asin}.json").write_text(json.dumps(product), encoding="utf-8")

    def build(self) -> dict:
        for item in self.config["products"]:
            self.write_cache(item["asin"], item["brand"], deal=item["brand"] == "CGK Unlimited")
        return PREP.build_snapshot(self.config, self.cache, 60)

    def test_snapshot_requires_three_brands_and_surfaces_deal_limit(self) -> None:
        snapshot = self.build()
        self.assertEqual(snapshot["status"], "pass")
        self.assertEqual(len(snapshot["products"]), 3)
        cgk = next(item for item in snapshot["products"] if item["brand"] == "CGK Unlimited")
        self.assertEqual(cgk["current_price"], 19.99)
        self.assertEqual(cgk["current_price_source"], "buy_box")
        self.assertEqual(cgk["deals"][0]["badge"], "Limited time deal")
        self.assertFalse(cgk["best_or_lightning_deal"])
        self.assertEqual(cgk["duration_status"], "not_exposed_by_keepa")

    def test_default_cache_matches_live_keepa_extension(self) -> None:
        self.assertEqual(PREP.DEFAULT_CACHE, Path.home() / ".pi/agent/keepa-cache")

    def test_snapshot_includes_buy_box_shipping(self) -> None:
        for item in self.config["products"]:
            self.write_cache(item["asin"], item["brand"], buy_box_shipping_cents=150)
        snapshot = PREP.build_snapshot(self.config, self.cache, 60)
        self.assertTrue(all(item["current_price"] == 21.49 for item in snapshot["products"]))

    def test_snapshot_rejects_cache_payload_asin_mismatch(self) -> None:
        for item in self.config["products"]:
            self.write_cache(
                item["asin"],
                item["brand"],
                payload_asin="B000000000" if item["brand"] == "Amazon Basics" else item["asin"],
            )
        with self.assertRaisesRegex(ValueError, "payload ASIN mismatch"):
            PREP.build_snapshot(self.config, self.cache, 60)

    def test_snapshot_marks_lightning_deal_explicitly(self) -> None:
        for item in self.config["products"]:
            self.write_cache(
                item["asin"],
                item["brand"],
                deal=item["brand"] == "CGK Unlimited",
                deal_type="LIGHTNING_DEAL",
            )
        snapshot = PREP.build_snapshot(self.config, self.cache, 60)
        cgk = next(item for item in snapshot["products"] if item["brand"] == "CGK Unlimited")
        self.assertTrue(cgk["best_or_lightning_deal"])

    def test_snapshot_accepts_exact_60_minute_boundary(self) -> None:
        now = 10_000.0
        for item in self.config["products"]:
            self.write_cache(item["asin"], item["brand"], cached_at=now - 3_600)
        self.assertEqual(PREP.build_snapshot(self.config, self.cache, 60, now=now)["status"], "pass")

    def test_snapshot_rejects_over_60_minute_cache(self) -> None:
        now = 10_000.0
        for item in self.config["products"]:
            self.write_cache(item["asin"], item["brand"], cached_at=now - 3_601)
        with self.assertRaisesRegex(ValueError, "stale"):
            PREP.build_snapshot(self.config, self.cache, 60, now=now)

    def test_report_rejects_missing_required_brand(self) -> None:
        snapshot = self.build()
        snapshot["products"] = snapshot["products"][:-1]
        path = self.root / "competitor_check.json"
        path.write_text(json.dumps(snapshot), encoding="utf-8")
        checks = []
        products = REPORT.read_competitor_check(path, self.config_path, checks)
        self.assertEqual(products, [])
        self.assertEqual(checks[-1].status, "fail")
        self.assertIn("CGK Unlimited", checks[-1].details)

    def test_report_rejects_configured_asin_mismatch(self) -> None:
        snapshot = self.build()
        snapshot["products"][0]["asin"] = "B000000000"
        path = self.root / "competitor_check.json"
        path.write_text(json.dumps(snapshot), encoding="utf-8")
        checks = []
        products = REPORT.read_competitor_check(path, self.config_path, checks)
        self.assertEqual(products, [])
        self.assertEqual(checks[-1].status, "fail")
        self.assertIn("B00Q7OARO2", checks[-1].details)

    def test_report_rejects_stale_snapshot_even_if_status_passes(self) -> None:
        snapshot = self.build()
        snapshot["products"][0]["checked_at_utc"] = "2026-01-01T00:00:00+00:00"
        path = self.root / "competitor_check.json"
        path.write_text(json.dumps(snapshot), encoding="utf-8")
        checks = []
        products = REPORT.read_competitor_check(path, self.config_path, checks)
        self.assertEqual(products, [])
        self.assertEqual(checks[-1].status, "fail")

    def test_report_accepts_valid_snapshot(self) -> None:
        path = self.root / "competitor_check.json"
        path.write_text(json.dumps(self.build()), encoding="utf-8")
        checks = []
        products = REPORT.read_competitor_check(path, self.config_path, checks)
        self.assertEqual(len(products), 3)
        self.assertEqual(checks[-1].status, "pass")


if __name__ == "__main__":
    unittest.main()
