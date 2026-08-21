#!/usr/bin/env python3
"""Regression tests for required daily-report deal-calendar evidence."""
from __future__ import annotations

import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

MODULE_PATH = Path(__file__).with_name("build_daily_sales_report.py")
SPEC = importlib.util.spec_from_file_location("build_daily_sales_report", MODULE_PATH)
REPORT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = REPORT
assert SPEC.loader is not None
SPEC.loader.exec_module(REPORT)


class DealCalendarGateTest(unittest.TestCase):
    def write_evidence(self, dates: dict) -> Path:
        root = Path(self.temp.name)
        path = root / "deal_calendar_status.json"
        pacific = ZoneInfo("America/Los_Angeles")
        for day, item in dates.items():
            local_date = date.fromisoformat(day)
            item.setdefault("time_min", datetime.combine(local_date, datetime.min.time(), pacific).isoformat())
            item.setdefault("time_max", datetime.combine(local_date + timedelta(days=1), datetime.min.time(), pacific).isoformat())
        path.write_text(json.dumps({
            "source": "Google Calendar Events API",
            "calendar_name": "Lightning Deals",
            "calendar_id": "test-calendar-id",
            "dates": dates,
        }), encoding="utf-8")
        return path

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_orders_reject_both_canceled_spellings(self) -> None:
        path = Path(self.temp.name) / "orders.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["date_pt", "order-status", "sales-channel", "currency", "quantity", "item-price", "asin", "amazon-order-id"])
            writer.writerows([
                ["2026-08-20", "Canceled", "Amazon.com", "USD", 1, 10, "A", "1"],
                ["2026-08-20", "Cancelled", "Amazon.com", "USD", 1, 20, "A", "2"],
                ["2026-08-20", "Shipped", "Amazon.com", "USD", 1, 5, "A", "3"],
            ])
        checks = []
        dictionary = REPORT.pd.DataFrame([{"asin": "A", "collection": "Test"}])
        _, summary = REPORT.normalize_orders(path, "2026-08-20", dictionary, checks, "target")
        self.assertEqual(summary["sales"], 5.0)
        self.assertEqual(summary["units"], 1)
        self.assertEqual(summary["orders"], 1)

    def test_classifies_none_lightning_and_best_deal(self) -> None:
        path = self.write_evidence({
            "2026-08-11": {"checked": True, "events": []},
            "2026-08-04": {"checked": True, "events": [{
                "summary": "6 PC Iconic Sheets",
                "start": "2026-08-04T20:10:00+03:00",
                "end": "2026-08-05T08:10:00+03:00",
                "status": "confirmed",
            }]},
            "2026-08-15": {"checked": True, "events": [{
                "summary": "Iconic Fitted Sheets",
                "start": "2026-08-14T10:00:00+03:00",
                "end": "2026-08-28T09:59:00+03:00",
                "status": "confirmed",
            }]},
        })
        checks = []
        result = REPORT.read_deal_calendar(path, ["2026-08-11", "2026-08-04", "2026-08-15"], checks)
        self.assertTrue(all(c.status == "pass" for c in checks))
        self.assertEqual(result["2026-08-11"]["status"], "None")
        self.assertEqual(result["2026-08-04"]["status"], "Lightning Deal")
        self.assertEqual(result["2026-08-15"]["status"], "Best Deal")

    def test_accepts_date_only_all_day_best_deal(self) -> None:
        path = self.write_evidence({
            "2026-08-15": {"checked": True, "events": [{
                "summary": "All-day Best Deal",
                "start": "2026-08-14",
                "end": "2026-08-17",
                "status": "confirmed",
            }]},
        })
        checks = []
        result = REPORT.read_deal_calendar(path, ["2026-08-15"], checks)
        self.assertTrue(all(c.status == "pass" for c in checks))
        self.assertEqual(result["2026-08-15"]["status"], "Best Deal")

    def test_accepts_one_calendar_day_best_deal_across_spring_dst(self) -> None:
        path = self.write_evidence({
            "2026-03-08": {"checked": True, "events": [{
                "summary": "Spring DST all-day Best Deal",
                "start": "2026-03-08",
                "end": "2026-03-09",
                "status": "confirmed",
            }]},
        })
        checks = []
        result = REPORT.read_deal_calendar(path, ["2026-03-08"], checks)
        self.assertTrue(all(c.status == "pass" for c in checks))
        self.assertEqual(result["2026-03-08"]["status"], "Best Deal")

    def test_rejects_missing_date_check(self) -> None:
        path = self.write_evidence({"2026-08-11": {"checked": True, "events": []}})
        checks = []
        REPORT.read_deal_calendar(path, ["2026-08-11", "2026-08-04"], checks)
        self.assertTrue(any(c.name == "deal_calendar:2026-08-04" and c.status == "fail" for c in checks))

    def test_rejects_missing_calendar_id_even_when_empty(self) -> None:
        path = self.write_evidence({"2026-08-11": {"checked": True, "events": []}})
        data = json.loads(path.read_text(encoding="utf-8"))
        del data["calendar_id"]
        path.write_text(json.dumps(data), encoding="utf-8")
        checks = []
        result = REPORT.read_deal_calendar(path, ["2026-08-11"], checks)
        self.assertEqual(result["2026-08-11"]["status"], "Unverified")
        self.assertTrue(any(c.name == "deal_calendar_source" and c.status == "fail" for c in checks))

    def test_rejects_wrong_query_bounds_even_when_empty(self) -> None:
        path = self.write_evidence({
            "2026-08-11": {
                "checked": True,
                "time_min": "2026-08-11T01:00:00-07:00",
                "time_max": "2026-08-12T00:00:00-07:00",
                "events": [],
            },
        })
        checks = []
        result = REPORT.read_deal_calendar(path, ["2026-08-11"], checks)
        self.assertEqual(result["2026-08-11"]["status"], "Unverified")
        self.assertTrue(any(c.name == "deal_calendar:2026-08-11" and c.status == "fail" for c in checks))

    def test_accepts_exact_next_local_midnight_across_dst(self) -> None:
        path = self.write_evidence({
            "2026-03-08": {"checked": True, "events": []},
            "2026-11-01": {"checked": True, "events": []},
        })
        checks = []
        result = REPORT.read_deal_calendar(path, ["2026-03-08", "2026-11-01"], checks)
        self.assertTrue(all(c.status == "pass" for c in checks))
        self.assertEqual(result["2026-03-08"]["status"], "None")
        self.assertEqual(result["2026-11-01"]["status"], "None")

    def test_rejects_duration_between_lightning_and_best_deal(self) -> None:
        path = self.write_evidence({
            "2026-08-11": {"checked": True, "events": [{
                "summary": "Bad evidence",
                "start": "2026-08-11T08:00:00-07:00",
                "end": "2026-08-11T21:00:00-07:00",
                "status": "confirmed",
            }]},
        })
        checks = []
        REPORT.read_deal_calendar(path, ["2026-08-11"], checks)
        self.assertTrue(any(c.name == "deal_calendar:2026-08-11" and c.status == "fail" for c in checks))


if __name__ == "__main__":
    unittest.main()
