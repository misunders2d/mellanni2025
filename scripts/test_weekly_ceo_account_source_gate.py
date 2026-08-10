#!/usr/bin/env python3
"""Regression tests for independent weekly CEO headline conversion controls."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).with_name("build_weekly_ceo_report.py")
SPEC = importlib.util.spec_from_file_location("build_weekly_ceo_report", MODULE_PATH)
REPORT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = REPORT
assert SPEC.loader is not None
SPEC.loader.exec_module(REPORT)


class AccountHeadlineGateTest(unittest.TestCase):
    def setUp(self) -> None:
        current_end = date(2026, 8, 8)
        current_start = current_end - timedelta(days=6)
        prior_end = current_start - timedelta(days=1)
        prior_start = prior_end - timedelta(days=6)
        self.dates = {
            "week_start": current_start,
            "week_end": current_end,
            "prior_start": prior_start,
            "prior_end": prior_end,
        }
        days = [prior_start + timedelta(days=i) for i in range(14)]
        units = [0] * 14
        sessions = [0] * 14
        units[6], units[13] = 10833, 11254
        sessions[6], sessions[13] = 147169, 166175
        self.account = pd.DataFrame({
            "date": days,
            "sales": [0.0] * 14,
            "orders": [0] * 14,
            "units": units,
            "sessions": sessions,
            "unit_conversion": [0.0] * 12 + [0.0, 11254 / 166175],
        })
        self.trend = pd.DataFrame([
            {"week_end": prior_end, "units": 10833, "sessions": 147169, "unit_conversion": 10833 / 147169},
            {"week_end": current_end, "units": 11254, "sessions": 166175, "unit_conversion": 11254 / 166175},
        ])
        self.products = pd.DataFrame([{
            "sales": 91.94,
            "prior_sales": 0.0,
            "units": 11256,
            "prior_units": 10833,
            "sessions": 166175,
            "prior_sessions": 147169,
        }])

    def data(self):
        return {"account_by_date": self.account, "trend": self.trend, "products": self.products}

    def test_accepts_exact_account_headline_and_keeps_product_delta_separate(self):
        reconciliation, checks = REPORT.validate_account_source(self.data(), self.dates)
        self.assertTrue(all(c.status == "pass" for c in checks))
        current = reconciliation[reconciliation.period.eq("current")].set_index("metric")
        self.assertEqual(current.loc["units", "product_minus_account"], 2)
        self.assertEqual(current.loc["sessions", "product_minus_account"], 0)

    def test_rejects_old_sku_derived_session_denominator(self):
        self.trend.loc[self.trend.index[-1], "sessions"] = 159711
        self.trend.loc[self.trend.index[-1], "unit_conversion"] = 11256 / 159711
        _, checks = REPORT.validate_account_source(self.data(), self.dates)
        failed = {c.name for c in checks if c.status == "fail"}
        self.assertIn("account_headline_match:current", failed)

    def test_rejects_missing_account_source(self):
        data = self.data()
        data["account_by_date"] = pd.DataFrame()
        _, checks = REPORT.validate_account_source(data, self.dates)
        self.assertTrue(any(c.status == "fail" for c in checks))

    def test_rejects_conversion_difference_above_absolute_tolerance(self):
        self.trend.loc[self.trend.index[-1], "unit_conversion"] = 11254 / 166175 + 2e-12
        _, checks = REPORT.validate_account_source(self.data(), self.dates)
        self.assertTrue(any(c.name == "account_headline_match:current" and c.status == "fail" for c in checks))

    def test_rejects_fractional_headline_counts(self):
        self.trend["units"] = self.trend["units"].astype(float)
        self.trend.loc[self.trend.index[-1], "units"] = 11254.5
        _, checks = REPORT.validate_account_source(self.data(), self.dates)
        self.assertTrue(any(c.name == "account_headline_match:current" and c.status == "fail" for c in checks))

    def test_rejects_fractional_daily_counts_even_when_week_sum_is_integer(self):
        self.account["units"] = self.account["units"].astype(float)
        self.account.loc[self.account.index[-1], "units"] += 0.5
        self.account.loc[self.account.index[-2], "units"] -= 0.5
        _, checks = REPORT.validate_account_source(self.data(), self.dates)
        self.assertTrue(any(c.name == "account_headline_match:current" and c.status == "fail" for c in checks))

    def test_rejects_account_csv_without_matching_data_kiosk_provenance(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            raw = root / REPORT.ACCOUNT_RAW_FILE
            normalized = root / REPORT.ACCOUNT_NORMALIZED_FILE
            manifest_path = root / REPORT.ACCOUNT_MANIFEST_FILE
            raw_rows = []
            for row in self.account.itertuples(index=False):
                raw_rows.append({
                    "startDate": str(row.date),
                    "endDate": str(row.date),
                    "marketplaceId": "ATVPDKIKX0DER",
                    "sales": {"orderedProductSales": {"amount": row.sales}, "totalOrderItems": row.orders, "unitsOrdered": row.units},
                    "traffic": {"sessions": row.sessions, "unitSessionPercentage": row.unit_conversion * 100},
                })
            raw.write_text("\n".join(json.dumps(row) for row in raw_rows) + "\n", encoding="utf-8")
            self.account.to_csv(normalized, index=False)
            sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            manifest = {
                "source": "SP-API Data Kiosk",
                "dataset": "analytics_salesAndTraffic_2024_04_24.salesAndTrafficByDate",
                "aggregate_by": "DAY",
                "marketplace_id": "ATVPDKIKX0DER",
                "start_date": str(self.dates["prior_start"]),
                "end_date": str(self.dates["week_end"]),
                "query_id": "test-query",
                "document_id": "test-document",
                "raw_file": raw.name,
                "normalized_file": normalized.name,
                "raw_sha256": sha(raw),
                "normalized_sha256": sha(normalized),
                "row_count": 14,
            }
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            self.assertTrue(all(c.status == "pass" for c in REPORT.validate_account_provenance(root, self.dates)))
            raw_rows[0]["childAsin"] = "B000TEST00"
            raw_rows[0]["marketplaceId"] = "WRONG_MARKETPLACE"
            raw.write_text("\n".join(json.dumps(row) for row in raw_rows) + "\n", encoding="utf-8")
            manifest["raw_sha256"] = sha(raw)
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            self.assertTrue(any(c.status == "fail" for c in REPORT.validate_account_provenance(root, self.dates)))


if __name__ == "__main__":
    unittest.main()
