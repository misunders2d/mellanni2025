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
from datetime import datetime, timedelta, timezone
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
                {"brand": "Bedsure", "asin": "B07P5NBRL5", "label": "Bedsure set"},
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
        prime_exclusive_cents: int | None = None,
        lightning_deal_cents: int | None = None,
    ) -> None:
        csv_data = [None] * 36
        csv_data[0] = [1, 2500]
        csv_data[1] = [1, 2000]
        csv_data[4] = [1, 3999]
        csv_data[18] = [1, 1999, buy_box_shipping_cents]
        if prime_exclusive_cents is not None:
            csv_data[33] = [1, prime_exclusive_cents]
        if lightning_deal_cents is not None:
            csv_data[8] = [1, lightning_deal_cents]
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

    def test_snapshot_requires_configured_brands_and_surfaces_deal_limit(self) -> None:
        snapshot = self.build()
        self.assertEqual(snapshot["status"], "pass")
        self.assertEqual(len(snapshot["products"]), 4)
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

    def test_snapshot_prioritizes_prime_exclusive_and_lightning_deal(self) -> None:
        for item in self.config["products"]:
            if item["brand"] == "CGK Unlimited":
                self.write_cache(item["asin"], item["brand"], prime_exclusive_cents=1749)
            elif item["brand"] == "Utopia Bedding":
                self.write_cache(item["asin"], item["brand"], lightning_deal_cents=1599)
            else:
                self.write_cache(item["asin"], item["brand"])
        snapshot = PREP.build_snapshot(self.config, self.cache, 60)
        cgk = next(item for item in snapshot["products"] if item["brand"] == "CGK Unlimited")
        self.assertEqual(cgk["current_price"], 17.49)
        self.assertEqual(cgk["current_price_source"], "prime_exclusive")
        utopia = next(item for item in snapshot["products"] if item["brand"] == "Utopia Bedding")
        self.assertEqual(utopia["current_price"], 15.99)
        self.assertEqual(utopia["current_price_source"], "lightning_deal")
        amazon = next(item for item in snapshot["products"] if item["brand"] == "Amazon Basics")
        self.assertEqual(amazon["current_price"], 19.99)
        self.assertEqual(amazon["current_price_source"], "buy_box")

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
        self.assertIn("Bedsure", checks[-1].details)

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
        self.assertEqual(len(products), 4)
        self.assertEqual(checks[-1].status, "pass")

    def test_snapshot_target_date_surfaces_ended_historical_deal(self) -> None:
        # 2026-10-13 in Pacific is 2026-10-13 07:00:00 UTC to 2026-10-14 07:00:00 UTC
        # Deal: 2026-10-13 08:20 PDT (15:20 UTC) to 2026-10-13 20:20 PDT (10-14 03:20 UTC)
        # 1791904800 is 2026-10-13 15:20:00 UTC
        start_epoch = 1791904800
        end_epoch = start_epoch + 12 * 3600
        k_start = int(start_epoch // 60) - 21_564_000
        k_end = int(end_epoch // 60) - 21_564_000

        for item in self.config["products"]:
            if item["brand"] == "CGK Unlimited":
                csv_data = [None] * 36
                csv_data[0] = [1, 2500]
                csv_data[1] = [1, 2000]
                csv_data[4] = [1, 3999]
                csv_data[18] = [1, 2499, 0]
                # Deal active during 12h, then ends (-1)
                csv_data[8] = [k_start, 2122, k_end, -1]
                product = {
                    "asin": item["asin"],
                    "brand": item["brand"],
                    "title": f"{item['brand']} sheets",
                    "_cached_at": time.time(),
                    "csv": csv_data,
                    "deals": [],  # Live deals empty because deal already ended!
                }
                (self.cache / f"{item['asin']}.json").write_text(json.dumps(product), encoding="utf-8")
            else:
                self.write_cache(item["asin"], item["brand"])

        snapshot = PREP.build_snapshot(self.config, self.cache, 60, target_date="2026-10-13")
        cgk = next(p for p in snapshot["products"] if p["brand"] == "CGK Unlimited")
        # The ended deal is reported on the date, but never as today's live price.
        self.assertEqual(cgk["current_price"], 24.99)
        self.assertEqual(cgk["current_price_source"], "buy_box")
        self.assertEqual(cgk["date_deals"][0]["deal_price"], 21.22)
        self.assertTrue(cgk["best_or_lightning_deal"])
        self.assertEqual(cgk["duration_hours"], 12.0)
        self.assertEqual(cgk["duration_status"], "complete")
        self.assertEqual(cgk["deals"][0]["badge"], "Lightning Deal")

    def test_deal_duration_uses_real_elapsed_hours_across_dst(self) -> None:
        start = datetime.fromisoformat("2026-11-01T01:30:00-07:00").timestamp() // 60 - 21_564_000
        end = datetime.fromisoformat("2026-11-01T01:30:00-08:00").timestamp() // 60 - 21_564_000
        day_start, day_end = PREP.day_bounds("2026-11-01")
        intervals = PREP.deal_intervals([int(start), 1999, int(end), -1], day_start, day_end)
        self.assertEqual(intervals[0]["duration_hours"], 1.0)  # wall-clock subtraction would give 0.0

    def test_report_rejects_snapshot_prepared_for_another_date(self) -> None:
        path = self.root / "competitor_check.json"
        checked_at = "2026-10-22T18:00:00+00:00"
        now_ts = datetime.fromisoformat(checked_at).timestamp()
        snapshot = PREP.build_snapshot_from_mcp(self.config, self.mcp_payload(checked_at=checked_at), 60, target_date="2026-10-22", now=now_ts)
        path.write_text(json.dumps(snapshot), encoding="utf-8")
        checks = []
        self.assertEqual(REPORT.read_competitor_check(path, self.config_path, checks, now_utc=REPORT.pd.Timestamp(checked_at), target_date="2026-10-23"), [])
        self.assertEqual(checks[-1].status, "fail")

    def mcp_payload(self, *, checked_at: str | None = None) -> dict:
        checked = checked_at or datetime.now(timezone.utc).isoformat()
        products = []
        for item in self.config["products"]:
            products.append({
                "asin": item["asin"], "brand": item["brand"], "title": f"{item['brand']} sheets",
                "current_price": 14.39, "current_price_source": "buy_box", "list_price": None, "coupon": None,
                "active_deals": [{"type": "LIMITED_TIME_DEAL", "badge": "Limited time deal", "access": "ALL", "start": None, "end": None}] if item["brand"] == "Bedsure" else [],
                "date_deals": [{"type": "LIGHTNING_DEAL", "badge": "Lightning Deal", "deal_price": 12.99,
                                "start": "2026-10-22T08:00:00-07:00", "end": "2026-10-22T20:00:00-07:00",
                                "duration_hours": 12.0}] if item["brand"] == "CGK Unlimited" else [],
                "checked_at_utc": checked,
            })
        return {"status": "success", "date_pt": "2026-10-22", "products": products}

    def test_mcp_snapshot_builds_report_ready_products(self) -> None:
        checked_at = "2026-10-22T18:00:00+00:00"
        now_ts = datetime.fromisoformat(checked_at).timestamp()
        snapshot = PREP.build_snapshot_from_mcp(self.config, self.mcp_payload(checked_at=checked_at), 60, target_date="2026-10-22", now=now_ts)
        by_brand = {item["brand"]: item for item in snapshot["products"]}
        self.assertTrue(by_brand["CGK Unlimited"]["best_or_lightning_deal"])
        self.assertEqual(by_brand["CGK Unlimited"]["duration_status"], "complete")
        self.assertFalse(by_brand["Bedsure"]["best_or_lightning_deal"])
        self.assertEqual(by_brand["Bedsure"]["duration_status"], "not_exposed_by_keepa")
        self.assertEqual(by_brand["Amazon Basics"]["duration_status"], "not_applicable")
        path = self.root / "competitor_check.json"
        path.write_text(json.dumps(snapshot), encoding="utf-8")
        checks = []
        self.assertEqual(len(REPORT.read_competitor_check(path, self.config_path, checks, now_utc=REPORT.pd.Timestamp(checked_at))), 4)
        self.assertEqual(checks[-1].status, "pass")

    def test_mcp_snapshot_rejects_wrong_date_and_stale_evidence(self) -> None:
        with self.assertRaisesRegex(ValueError, "2026-10-21"):
            PREP.build_snapshot_from_mcp(self.config, self.mcp_payload(), 60, target_date="2026-10-21")
        now_ts = datetime.fromisoformat("2026-10-22T18:00:00+00:00").timestamp()
        stale = "2026-10-22T16:59:00+00:00"
        with self.assertRaisesRegex(ValueError, "stale"):
            PREP.build_snapshot_from_mcp(self.config, self.mcp_payload(checked_at=stale), 60, target_date="2026-10-22", now=now_ts)

    def test_mcp_snapshot_requires_history_for_past_target_date(self) -> None:
        checked_at = "2026-11-19T11:22:00+00:00"
        now_ts = datetime.fromisoformat(checked_at).timestamp()
        payload = self.mcp_payload(checked_at=checked_at)
        payload["date_pt"] = "2026-11-18"
        with self.assertRaisesRegex(ValueError, "--mcp-history"):
            PREP.build_snapshot_from_mcp(self.config, payload, 60, target_date="2026-11-18", now=now_ts)

    def test_report_blocks_d_plus_1_snapshot_without_target_date_price(self) -> None:
        checked_at = "2026-11-19T11:22:00+00:00"
        same_day = "2026-11-18T18:00:00+00:00"
        snap = PREP.build_snapshot_from_mcp(
            self.config,
            {**self.mcp_payload(checked_at=same_day), "date_pt": "2026-11-18"},
            60,
            target_date="2026-11-18",
            now=datetime.fromisoformat(same_day).timestamp(),
        )
        for p in snap["products"]:
            p["checked_at_utc"] = checked_at
        path = self.root / "competitor_check.json"
        path.write_text(json.dumps(snap), encoding="utf-8")
        checks = []
        self.assertEqual(REPORT.read_competitor_check(path, self.config_path, checks, now_utc=REPORT.pd.Timestamp(checked_at), target_date="2026-11-18"), [])
        self.assertEqual(checks[-1].status, "fail")

    def test_competitor_price_cell_formats_multi_point_intraday_dip(self) -> None:
        item = {
            "brand": "Utopia Bedding",
            "asin": "B00NX0WXQI",
            "current_price": 17.99,
            "current_price_source": "buy_box",
            "list_price": 29.99,
            "target_date_price": 17.99,
            "target_date_price_end": 17.99,
            "target_date_price_source": "buy_box",
            "target_date_price_points": [
                {"time_pt": "2026-11-02T20:49:00-08:00", "price": 17.99, "carried_in": True},
                {"time_pt": "2026-11-19T11:17:00-08:00", "price": 13.31, "carried_in": False},
                {"time_pt": "2026-11-19T12:23:00-08:00", "price": 12.64, "carried_in": False},
                {"time_pt": "2026-11-19T19:17:00-08:00", "price": 17.99, "carried_in": False},
            ],
            "checked_at_utc": "2026-11-20T11:32:00+00:00",
        }
        headline, notes = REPORT.competitor_price_cell(item, "2026-11-19")
        self.assertEqual(headline, "$12.64–$17.99")
        joined = " · ".join(notes)
        self.assertIn("$17.99 → $13.31 (11:17 AM) → $12.64 (12:23 PM) → $17.99 (7:17 PM)", joined)

    def test_mcp_history_isolates_target_date_prices_and_separates_d_plus_1_deals(self) -> None:
        checked = "2026-11-19T11:22:00+00:00"
        now_ts = datetime.fromisoformat(checked).timestamp()
        payload = {
            "status": "success",
            "date_pt": "2026-11-18",
            "products": [
                {"asin": "B00Q7OARO2", "brand": "Amazon Basics", "title": "Amazon set", "current_price": 15.22,
                 "current_price_source": "buy_box", "list_price": 18.99, "coupon": None,
                 "active_deals": [], "date_deals": [], "checked_at_utc": checked},
                {"asin": "B00NX0WXQI", "brand": "Utopia Bedding", "title": "Utopia set", "current_price": 17.99,
                 "current_price_source": "buy_box", "list_price": 29.99, "coupon": None,
                 "active_deals": [], "date_deals": [], "checked_at_utc": checked},
                {"asin": "B01M16WBW1", "brand": "CGK Unlimited", "title": "CGK set", "current_price": 17.49,
                 "current_price_source": "buy_box", "list_price": 39.99, "coupon": None,
                 "active_deals": [{"type": "SPECIAL_EVENT_SALE", "badge": "Black Friday Deal", "access": "ALL", "start": None, "end": None}],
                 "date_deals": [], "checked_at_utc": checked},
                {"asin": "B07P5NBRL5", "brand": "Bedsure", "title": "Bedsure set", "current_price": 13.67,
                 "current_price_source": "buy_box", "list_price": 19.99, "coupon": None,
                 "active_deals": [{"type": "SPECIAL_EVENT_SALE", "badge": "Black Friday Deal", "access": "ALL", "start": None, "end": None}],
                 "date_deals": [], "checked_at_utc": checked},
            ],
        }
        history = {
            "B00Q7OARO2": {"buy_box": {"status": "success", "history": [{"date": "2026-11-09T00:31:00+00:00", "price": 15.22, "carried_forward": True}]}},
            "B00NX0WXQI": {"buy_box": {"status": "success", "history": [{"date": "2026-11-03T04:49:00+00:00", "price": 17.99, "carried_forward": True}]}},
            "B01M16WBW1": {"buy_box": {"status": "success", "history": [
                {"date": "2026-08-29T07:27:00+00:00", "price": 24.99, "carried_forward": True},
                {"date": "2026-11-19T08:22:00+00:00", "price": 17.49, "carried_forward": False},
            ]}},
            "B07P5NBRL5": {"buy_box": {"status": "success", "history": [
                {"date": "2026-11-10T10:23:00+00:00", "price": 16.99, "carried_forward": True},
                {"date": "2026-11-19T07:17:00+00:00", "price": 19.99, "carried_forward": False},
            ]}},
        }
        snapshot = PREP.build_snapshot_from_mcp(self.config, payload, 60, target_date="2026-11-18", now=now_ts, mcp_history=history)
        by_brand = {item["brand"]: item for item in snapshot["products"]}
        self.assertEqual(by_brand["CGK Unlimited"]["target_date_price"], 24.99)
        self.assertEqual(by_brand["Bedsure"]["target_date_price"], 16.99)
        self.assertEqual(by_brand["Bedsure"]["target_date_price_end"], 19.99)

        cgk_price, cgk_notes = REPORT.competitor_price_cell(by_brand["CGK Unlimited"], "2026-11-18")
        beds_price, beds_notes = REPORT.competitor_price_cell(by_brand["Bedsure"], "2026-11-18")
        self.assertEqual(cgk_price, "$24.99")
        self.assertEqual(beds_price, "$16.99 → $19.99")
        self.assertTrue(any("At Thu Nov 19 check: $17.49" in note for note in cgk_notes))
        self.assertTrue(any("$19.99 from Nov 18 11:17 PM" in note for note in beds_notes))

        cgk_deal = REPORT.competitor_deal_text(by_brand["CGK Unlimited"], "2026-11-18")
        self.assertTrue(cgk_deal.startswith("No Lightning/Prime deal on Wed Nov 18"))
        self.assertNotIn("Active at check", cgk_deal)


if __name__ == "__main__":
    unittest.main()
