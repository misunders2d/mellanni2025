#!/usr/bin/env python3
"""Tests for prepare_weekly_rank_comparison.py, builder edge cases, and verifier checks."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent


def load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


RANK_CMP = load_module("prepare_weekly_rank_comparison", "prepare_weekly_rank_comparison.py")
BUILDER = load_module("build_weekly_ceo_report_mod", "build_weekly_ceo_report.py")


def write_tracked_page(path: Path, rows: list[dict], *, has_more: bool, truncated: bool = False) -> None:
    path.write_text(json.dumps({
        "data": {
            "rows": rows,
            "pagination": {"has_more": has_more, "truncated": truncated},
        }
    }), encoding="utf-8")


def write_sqp(path: Path, week_end: str, prior_end: str) -> None:
    rows = []
    for dt in (week_end, prior_end):
        rows.extend([
            {
                "reporting_date": dt,
                "search_query": "queen sheet set",
                "search_query_volume": 60000,
                "total_impressions": 10000,
                "brand_impressions": 2000,
                "total_clicks": 1000,
                "brand_clicks": 200,
                "total_purchases": 500,
                "brand_purchases": 40,
                "purchase_price_median": 30.0,
                "brand_purchase_price_median": 32.0,
            },
            {
                "reporting_date": dt,
                "search_query": "king size sheets",
                "search_query_volume": 30000,
                "total_impressions": 5000,
                "brand_impressions": 500,
                "total_clicks": 400,
                "brand_clicks": 25,
                "total_purchases": 200,
                "brand_purchases": 10,
                "purchase_price_median": 35.0,
                "brand_purchase_price_median": 38.0,
            },
        ])
    path.write_text(json.dumps({"rows": rows}), encoding="utf-8")


class PrepareWeeklyRankComparisonTest(unittest.TestCase):
    def _setup_base(self, root: Path, *, p2_has_more: bool = False, include_scope: bool = False) -> Path:
        report_dir = root / "weekly_conversion" / "2026-11-21"
        src = report_dir / "sources"
        (src / "rank-histories").mkdir(parents=True)
        (src / "h10").mkdir(parents=True)

        write_tracked_page(
            src / "tracked-B0D5HPLND5-p1.json",
            [{"asin": "B0D5HPLND5", "keywords_phrase": "Queen Sheet Set", "search_volume": 60000, "cpr": 120, "keyword_sales": 900, "sponsored_rank": "14"}],
            has_more=True,
        )
        write_tracked_page(
            src / "tracked-B0D5HPLND5-p2.json",
            [{"asin": "B0D5HPLND5", "keywords_phrase": "king size sheets", "search_volume": 30000, "cpr": 80, "keyword_sales": 400, "sponsored_rank": ">96"}],
            has_more=p2_has_more,
        )
        # Raw rank-histories containing fuzzy-matched distractors and exact rows
        (src / "rank-histories" / "hist1.json").write_text(json.dumps({
            "data": {
                "rows": [
                    {
                        "asin": "B0D5HPLND5",
                        "keywords_phrase": "queen sheet set cotton",
                        "organic_rank_history": [{"date_time": "2026-11-21T10:00:00-08:00", "rank": 99}],
                    },
                    {
                        "asin": "B0D5HPLND5",
                        "keywords_phrase": "queen sheet set",
                        "organic_rank_history": [
                            {"date_time": "2026-11-14T10:00:00-08:00", "rank": 32},
                            {"date_time": "2026-11-21T10:00:00-08:00", "rank": 27},
                        ],
                    },
                ]
            }
        }), encoding="utf-8")
        (src / "rank-histories" / "hist2.json").write_text(json.dumps({
            "data": {
                "rows": [
                    {
                        "asin": "B0D5HPLND5",
                        "keywords_phrase": "king size sheets",
                        "organic_rank_history": [
                            {"date_time": "2026-11-14T10:00:00-08:00", "rank": 36},
                            {"date_time": "2026-11-21T10:00:00-08:00", "rank": 40},
                        ],
                    },
                ]
            }
        }), encoding="utf-8")
        # Reverse-ASIN enrichment payload with intentional different organic_rank (must NOT overwrite tracked rank)
        (src / "h10" / "B00NLLUMOE.json").write_text(json.dumps({
            "data": {
                "keywords": [
                    {
                        "phrase": "queen sheet set",
                        "organic_rank": 1,
                        "sponsored_rank": 5,
                        "iq_score": 550.0,
                        "title_density": 12,
                        "competing_products": 3000,
                        "suggested_bid_usd": 1.85,
                        "search_volume_trend_30d_pct": 18,
                    }
                ]
            }
        }), encoding="utf-8")
        write_sqp(src / "sqp.json", "2026-11-21", "2026-11-14")
        if include_scope:
            (src / "keyword-scope.json").write_text(json.dumps({
                "asin": "B0D5HPLND5",
                "phrases": ["queen sheet set", "king size sheets"],
            }), encoding="utf-8")
        return report_dir

    def test_multipage_untouched_raw_and_reverse_asin_enrichment(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            report_dir = self._setup_base(Path(tmp), p2_has_more=False, include_scope=False)
            res = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"],
                text=True, capture_output=True, check=True,
            )
            summary = json.loads(res.stdout)
            self.assertEqual(summary["tracked_pairs"], 2)
            self.assertEqual(summary["paired"], 2)
            self.assertEqual(summary["enriched_rows"], 1)
            self.assertFalse(summary["scoped_mode"])

            h10 = pd.read_csv(report_dir / "h10" / "h10_keyword_candidates.csv").set_index("keyword")
            self.assertEqual(h10.loc["queen sheet set", "organic_rank"], 27)  # kept tracked rank, NOT reverse-ASIN 1
            self.assertEqual(h10.loc["queen sheet set", "sponsored_rank"], 14)  # tracker numeric preserved
            self.assertEqual(h10.loc["queen sheet set", "iq_score"], 550.0)
            self.assertEqual(h10.loc["queen sheet set", "title_density"], 12)
            self.assertEqual(h10.loc["queen sheet set", "cpc"], 1.85)
            self.assertTrue(pd.isna(h10.loc["king size sheets", "sponsored_rank"]))  # >96 -> null

            sqp_out = pd.read_csv(report_dir / "tracked_sqp_keywords.csv").set_index("keyword")
            self.assertAlmostEqual(sqp_out.loc["queen sheet set", "brand_ctr"], 200 / 2000)

    def test_scope_gate_accepts_partial_pages_when_scoped_and_rejects_missing(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            report_dir = self._setup_base(Path(tmp), p2_has_more=True, include_scope=True)
            res = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"],
                text=True, capture_output=True, check=True,
            )
            self.assertTrue(json.loads(res.stdout)["scoped_mode"])

            # Removing scope file must now fail because p2 still has has_more=True
            (report_dir / "sources" / "keyword-scope.json").unlink()
            res_fail = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"],
                text=True, capture_output=True,
            )
            self.assertNotEqual(res_fail.returncode, 0)
            self.assertIn("has_more=false", res_fail.stderr)

            # Scope requiring an unretrieved phrase must fail cleanly
            (report_dir / "sources" / "keyword-scope.json").write_text(json.dumps(["queen sheet set", "missing phrase"]), encoding="utf-8")
            res_missing = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"],
                text=True, capture_output=True,
            )
            self.assertNotEqual(res_missing.returncode, 0)
            self.assertIn("Missing scoped keyword", res_missing.stderr)

    def test_pagination_chain_mismatch_and_conflicting_raw_histories_fail(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            report_dir = self._setup_base(Path(tmp), p2_has_more=False, include_scope=False)
            src = report_dir / "sources"
            # Legacy rank-histories.json is ignored when rank-histories/ is missing
            (src / "rank-histories.json").write_text(json.dumps({
                "B0D5HPLND5|queen sheet set": {"organic_rank_history": [{"date_time": "2026-11-21T10:00:00-08:00", "rank": 99}]}
            }), encoding="utf-8")
            # Conflicting second raw history file must raise
            (src / "rank-histories" / "hist_conflict.json").write_text(json.dumps({
                "data": {"rows": [{
                    "asin": "B0D5HPLND5",
                    "keywords_phrase": "queen sheet set",
                    "organic_rank_history": [{"date_time": "2026-11-21T10:00:00-08:00", "rank": 99}],
                }]}
            }), encoding="utf-8")
            res_conflict = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"],
                text=True, capture_output=True,
            )
            self.assertNotEqual(res_conflict.returncode, 0)
            self.assertIn("Conflicting raw rank histories", res_conflict.stderr)
            (src / "rank-histories" / "hist_conflict.json").unlink()

            # Missing raw rank-histories/ must fail even if rank-histories.json exists
            import shutil
            shutil.rmtree(src / "rank-histories")
            res_no_dir = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"],
                text=True, capture_output=True,
            )
            self.assertNotEqual(res_no_dir.returncode, 0)
            self.assertIn("Missing raw rank-histories", res_no_dir.stderr)

            # cumulative_count mismatch in no-scope mode must raise
            (src / "tracked-B0D5HPLND5-p2.json").write_text(json.dumps({
                "data": {
                    "rows": [{"asin": "B0D5HPLND5", "keywords_phrase": "king size sheets", "search_volume": 30000, "cpr": 80, "keyword_sales": 400, "sponsored_rank": ">96"}],
                    "pagination": {"has_more": False, "truncated": False, "cumulative_count": 99},
                }
            }), encoding="utf-8")
            (src / "rank-histories").mkdir()
            (src / "rank-histories" / "hist_ok.json").write_text(json.dumps({
                "data": {"rows": [
                    {"asin": "B0D5HPLND5", "keywords_phrase": "queen sheet set", "organic_rank_history": [{"date_time": "2026-11-21T10:00:00-08:00", "rank": 27}]},
                    {"asin": "B0D5HPLND5", "keywords_phrase": "king size sheets", "organic_rank_history": [{"date_time": "2026-11-21T10:00:00-08:00", "rank": 40}]},
                ]}
            }), encoding="utf-8")
            res_chain = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"],
                text=True, capture_output=True,
            )
            self.assertNotEqual(res_chain.returncode, 0)
            self.assertIn("cumulative_count", res_chain.stderr)

    def test_html_hides_empty_optional_columns_and_shows_multi_asin(self) -> None:
        h10 = pd.DataFrame([
            {"source": "Helium 10 Keyword Tracker", "asin": "B00NLLUMOE", "keyword": "queen sheet set", "search_volume": 60000, "organic_rank": 22, "sponsored_rank": 5, "cpr": 120, "iq_score": None, "title_density": None, "competing_products": None, "keyword_sales": 900, "cpc": None, "trend": None, "notes": ""},
            {"source": "Helium 10 Keyword Tracker", "asin": "B00NQDGAP2", "keyword": "king sheets", "search_volume": 50000, "organic_rank": 15, "sponsored_rank": None, "cpr": 90, "iq_score": None, "title_density": None, "competing_products": None, "keyword_sales": 800, "cpc": None, "trend": None, "notes": ""},
        ])
        sqp = pd.DataFrame([
            {"keyword": "queen sheet set", "search_query_volume": 60000, "total_purchases": 500, "brand_purchases": 40, "est_total_keyword_sales": 15000.0, "est_brand_keyword_sales": 1280.0, "brand_ctr": 0.1, "brand_purchase_per_click": 0.2},
            {"keyword": "king sheets", "search_query_volume": 50000, "total_purchases": 400, "brand_purchases": 20, "est_total_keyword_sales": 12000.0, "est_brand_keyword_sales": 600.0, "brand_ctr": 0.08, "brand_purchase_per_click": 0.15},
        ])
        prior_h10 = pd.DataFrame([
            {"source": "Helium 10 Keyword Tracker", "asin": "B00NLLUMOE", "keyword": "queen sheet set", "organic_rank": 25, "keyword_sales": None},
            {"source": "Helium 10 Keyword Tracker", "asin": "B00NQDGAP2", "keyword": "king sheets", "organic_rank": 18, "keyword_sales": None},
        ])
        grouped = BUILDER.build_h10_keyword_table(h10, sqp, [], prior_h10)
        ctx = {
            "dates": {"week_end": "2026-11-21", "prior_start": "2026-11-08", "prior_end": "2026-11-14"},
            "metrics": {"gross_sales": 2000.0, "prior_gross_sales": 1800.0, "net_item_sales": 1900.0, "prior_net_item_sales": 1700.0, "units": 60, "prior_units": 55, "sessions": 1000, "prior_sessions": 950, "conversion": 0.06, "prior_conversion": 55 / 950, "item_promo_discount": 100.0, "all_orders_gross_item_sales": 2000.0},
            "products": pd.DataFrame([{"collection": "Sheets", "sales": 2000.0, "prior_sales": 1800.0, "sales_wow": 0.11, "sessions": 1000, "units": 60, "unit_conversion": 0.06, "conversion_pp": 0.002, "sales_delta": 200.0}]),
            "asins": pd.DataFrame([{"asin": "B00NLLUMOE", "collection": "Sheets", "size": "Queen", "color": "White", "sales": 1200.0, "sessions": 600, "units": 36, "conversion": 0.06, "conversion_change_pp": 0.0}]),
            "promos": pd.DataFrame([{"promo_label": "LD", "item_promo_discount": 100.0, "gross_item_sales": 500.0, "net_item_sales": 400.0, "orders": 10, "units": 12}]),
            "keywords": grouped,
            "trend": pd.DataFrame([{"week_end": "2026-11-14", "gross_sales": 1800.0, "sessions": 950, "units": 55, "unit_conversion": 55 / 950}, {"week_end": "2026-11-21", "gross_sales": 2000.0, "sessions": 1000, "units": 60, "unit_conversion": 0.06}]),
            "source_reconciliation": pd.DataFrame(),
        }
        html = BUILDER.render_html(ctx)
        self.assertIn("viewport", html)
        self.assertIn("width:50%", html)
        self.assertIn("overflow-x:auto", html)
        self.assertIn(">120<", html)
        self.assertNotIn(">120.0<", html)
        self.assertIn(">ASIN<", html)
        self.assertIn(">Spon. Rank<", html)
        self.assertIn(">CPR<", html)
        self.assertNotIn(">IQ<", html)
        self.assertNotIn(">Trend<", html)

    def test_write_scope_deterministic_and_verifier_recompute(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            report_dir = self._setup_base(Path(tmp), p2_has_more=False, include_scope=False)
            src = report_dir / "sources"
            # Add an untracked ASIN above tracked B0D5HPLND5, plus a tracked outside-top-N ASIN that must be excluded
            write_tracked_page(
                src / "tracked-B0OUTSIDE99.json",
                [{"asin": "B0OUTSIDE99", "keywords_phrase": "queen sheet set", "search_volume": 90000, "cpr": 150, "keyword_sales": 1500}],
                has_more=False,
            )
            (src / "asin.json").write_text(json.dumps({
                "rows": [
                    {"asin": "B0UNTRACK1", "sales": 2000.0},
                    {"asin": "B0D5HPLND5", "sales": 1000.0},
                    {"asin": "B0OUTSIDE99", "sales": 500.0},
                ]
            }), encoding="utf-8")
            res = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21", "--write-scope", "--top-selling-asins", "2", "--scope-limit", "2"],
                text=True, capture_output=True, check=True,
            )
            out = json.loads(res.stdout)
            self.assertEqual(out["pairs"], 2)
            self.assertEqual(out["top_selling_asins"], 2)
            self.assertEqual(out["untracked_top_selling_asins"], ["B0UNTRACK1"])
            scope = json.loads((src / "keyword-scope.json").read_text(encoding="utf-8"))
            self.assertEqual(scope["top_selling_asins"], 2)
            self.assertEqual(scope["tracked_asins_by_sales"], ["B0D5HPLND5"])
            self.assertEqual(scope["untracked_top_selling_asins"], ["B0UNTRACK1"])
            self.assertEqual(scope["pairs"], [
                {"asin": "B0D5HPLND5", "phrase": "queen sheet set"},
                {"asin": "B0D5HPLND5", "phrase": "king size sheets"},
            ])

            # Negative verifier test: omitting top_selling_asins/tracked_asins_by_sales or altering untracked_top_selling_asins must fail keyword_scope_recomputed_match
            load_module("verify_weekly_ceo_report_mod", "verify_weekly_ceo_report.py")
            # Build minimal report outputs so verify runs cleanly
            week_end = date(2026, 11, 21)
            prior_start = week_end - timedelta(days=13)
            days = [prior_start + timedelta(days=i) for i in range(14)]
            raw_rows = [{"startDate": str(d), "endDate": str(d), "marketplaceId": "ATVPDKIKX0DER", "sales": {"orderedProductSales": {"amount": 100.0}, "totalOrderItems": 5, "unitsOrdered": 5}, "traffic": {"sessions": 100, "unitSessionPercentage": 5.0}} for d in days]
            (report_dir / "source_data_kiosk_account_by_date_raw.jsonl").write_text("\n".join(json.dumps(r) for r in raw_rows) + "\n", encoding="utf-8")
            pd.DataFrame([{"date": str(d), "sales": 100.0, "orders": 5, "units": 5, "sessions": 100, "unit_conversion": 0.05} for d in days]).to_csv(report_dir / "source_data_kiosk_account_by_date.csv", index=False)
            import zipfile
            (report_dir / "source_data_kiosk_account_by_date_manifest.json").write_text("{}", encoding="utf-8")
            (report_dir / "independent_control_totals.csv").write_text("br_current_gross_sales,ao_current_gross_item_sales,ao_current_net_item_sales,ao_current_item_promo_discount\n1000,1000,900,100\n", encoding="utf-8")
            (report_dir / "product_performance.csv").write_text("collection,sales\nSheets,1000\n", encoding="utf-8")
            (report_dir / "asin_performance_size_color.csv").write_text("asin,sales,size\nB0D5HPLND5,1000,Queen\n", encoding="utf-8")
            (report_dir / "top_promo_discounts.csv").write_text("promo_label\nLD\n", encoding="utf-8")
            (report_dir / "verification_summary_2026-11-21.json").write_text(json.dumps({"metrics": {"gross_sales": 1000.0, "prior_gross_sales": 1000.0, "net_item_sales": 900.0, "item_promo_discount": 100.0, "sessions": 700.0, "prior_sessions": 700.0, "units": 35.0, "prior_units": 35.0, "conversion": 0.05, "prior_conversion": 0.05}}), encoding="utf-8")
            (report_dir / "email_body_2026-11-21.html").write_text("<html><th>Keyword</th></html>", encoding="utf-8")
            with zipfile.ZipFile(report_dir / "Mellanni_Weekly_CEO_Overview_2026-11-21.xlsx", "w") as zf:
                zf.writestr("ok.txt", "ok")
            (src / "net.json").write_text(json.dumps({"rows": [{"period": "current", "gross_item_sales": 1000.0, "net_item_sales": 900.0, "item_promo_discount": 100.0}]}), encoding="utf-8")
            subprocess.run([sys.executable, str(SCRIPTS_DIR / "prepare_weekly_rank_comparison.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"], check=True, capture_output=True)

            # Passing verifier run with valid top_selling_asins scope metadata
            subprocess.run([sys.executable, str(SCRIPTS_DIR / "verify_weekly_ceo_report.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"], capture_output=True, text=True)
            iv_ok = json.loads((report_dir / "independent_verification_2026-11-21.json").read_text())
            ok_chk = [c for c in iv_ok["checks"] if c["name"] == "keyword_scope_recomputed_match"][0]
            self.assertEqual(ok_chk["status"], "pass")

            # Corrupt scope by tampering untracked_top_selling_asins -> must fail
            tampered = dict(scope)
            tampered["untracked_top_selling_asins"] = []
            (src / "keyword-scope.json").write_text(json.dumps(tampered), encoding="utf-8")
            subprocess.run([sys.executable, str(SCRIPTS_DIR / "verify_weekly_ceo_report.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"], capture_output=True, text=True)
            iv_tamper = json.loads((report_dir / "independent_verification_2026-11-21.json").read_text())
            self.assertEqual([c for c in iv_tamper["checks"] if c["name"] == "keyword_scope_recomputed_match"][0]["status"], "fail")

            # Corrupt scope by removing metadata -> verifier must fail keyword_scope_recomputed_match
            (src / "keyword-scope.json").write_text(json.dumps({"pairs": scope["pairs"]}), encoding="utf-8")
            subprocess.run([sys.executable, str(SCRIPTS_DIR / "verify_weekly_ceo_report.py"), "--report-dir", str(report_dir), "--week-end", "2026-11-21"], capture_output=True, text=True)
            iv = json.loads((report_dir / "independent_verification_2026-11-21.json").read_text())
            match_chk = [c for c in iv["checks"] if c["name"] == "keyword_scope_recomputed_match"][0]
            self.assertEqual(match_chk["status"], "fail")

    def test_builder_opportunity_score_non_nan_when_title_density_all_null(self) -> None:
        h10 = pd.DataFrame([
            {
                "source": "Helium 10 Keyword Tracker",
                "asin": "B0D5HPLND5",
                "keyword": "queen sheet set",
                "search_volume": 60000,
                "organic_rank": 27,
                "sponsored_rank": None,
                "cpr": 120,
                "iq_score": None,
                "title_density": None,
                "competing_products": None,
                "keyword_sales": 900,
                "cpc": None,
                "trend": None,
                "notes": "",
            }
        ])
        sqp = pd.DataFrame([{"keyword": "queen sheet set", "search_query_volume": 60000, "total_purchases": 500, "brand_purchases": 40, "est_total_keyword_sales": 15000.0, "est_brand_keyword_sales": 1280.0, "brand_ctr": 0.1, "brand_purchase_per_click": 0.2}])
        checks: list = []
        grouped = BUILDER.build_h10_keyword_table(h10, sqp, checks)
        self.assertTrue(grouped["opportunity_score"].notna().all())

    def test_builder_and_verifier_end_to_end_new_checks(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            root = Path(tmp) / "run"
            src = root / "sources"
            (root / "h10").mkdir(parents=True)
            src.mkdir(parents=True)

            week_end = date(2026, 11, 21)
            week_start = week_end - timedelta(days=6)
            prior_end = week_start - timedelta(days=1)
            prior_start = prior_end - timedelta(days=6)
            days = [prior_start + timedelta(days=i) for i in range(14)]

            raw_rows = []
            acct_rows = []
            for idx, day in enumerate(days):
                units = 100 if idx < 7 else 120
                sessions = 1000 if idx < 7 else 1000
                sales = 3000.0 if idx < 7 else 3600.0
                raw_rows.append({
                    "startDate": str(day),
                    "endDate": str(day),
                    "marketplaceId": "ATVPDKIKX0DER",
                    "sales": {"orderedProductSales": {"amount": sales}, "totalOrderItems": units, "unitsOrdered": units},
                    "traffic": {"sessions": sessions, "unitSessionPercentage": (units / sessions) * 100},
                })
                acct_rows.append({
                    "date": str(day),
                    "sales": sales,
                    "orders": units,
                    "units": units,
                    "sessions": sessions,
                    "unit_conversion": units / sessions,
                })
            raw_bytes = ("\n".join(json.dumps(r) for r in raw_rows) + "\n").encode("utf-8")
            (root / BUILDER.ACCOUNT_RAW_FILE).write_bytes(raw_bytes)
            pd.DataFrame(acct_rows).to_csv(root / BUILDER.ACCOUNT_NORMALIZED_FILE, index=False)
            sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
            (root / BUILDER.ACCOUNT_MANIFEST_FILE).write_text(json.dumps({
                "source": "SP-API Data Kiosk",
                "dataset": "analytics_salesAndTraffic_2024_04_24.salesAndTrafficByDate",
                "aggregate_by": "DAY",
                "marketplace_id": "ATVPDKIKX0DER",
                "start_date": str(prior_start),
                "end_date": str(week_end),
                "query_id": "q1",
                "document_id": "d1",
                "raw_file": BUILDER.ACCOUNT_RAW_FILE.name,
                "normalized_file": BUILDER.ACCOUNT_NORMALIZED_FILE.name,
                "row_count": 14,
                "raw_sha256": sha(root / BUILDER.ACCOUNT_RAW_FILE),
                "provider_sha256": sha(root / BUILDER.ACCOUNT_RAW_FILE),
                "normalized_sha256": sha(root / BUILDER.ACCOUNT_NORMALIZED_FILE),
            }), encoding="utf-8")

            pd.DataFrame([{
                "min_date": str(prior_start),
                "max_date": str(week_end),
                "row_count": 10,
                "current_rows": 6,
                "prior_rows": 4,
                "negative_metric_rows": 0,
            }]).to_csv(root / "validation.csv", index=False)
            pd.DataFrame([
                {"week_start": str(prior_start), "week_end": str(prior_end), "gross_sales": 21000.0, "sessions": 7000, "units": 700, "unit_conversion": 700 / 7000},
                {"week_start": str(week_start), "week_end": str(week_end), "gross_sales": 25200.0, "sessions": 7000, "units": 840, "unit_conversion": 840 / 7000},
            ]).to_csv(root / "conversion_trend.csv", index=False)
            # Notice top collection CHANGED from Collection B (prior) -> Collection A (current)
            # Also top ASIN is B0CUSTOM01 (NOT hardcoded B00NLLUMOE)
            asin_rows = [
                {"asin": "B0CUSTOM01", "collection": "Collection A", "size": "Queen", "color": "White", "sales": 15200.0, "prior_sales": 6000.0, "sessions": 4000, "prior_sessions": 3000, "units": 500, "prior_units": 200, "conversion": 0.125, "prior_conversion": 200 / 3000, "conversion_change_pp": 0.125 - 200 / 3000},
                {"asin": "B0CUSTOM02", "collection": "Collection B", "size": "King", "color": "Gray", "sales": 10000.0, "prior_sales": 15000.0, "sessions": 3000, "prior_sessions": 4000, "units": 340, "prior_units": 500, "conversion": 340 / 3000, "prior_conversion": 0.125, "conversion_change_pp": 340 / 3000 - 0.125},
            ]
            pd.DataFrame(asin_rows).to_csv(root / "asin_performance_size_color.csv", index=False)
            (src / "asin.json").write_text(json.dumps({"rows": asin_rows}), encoding="utf-8")

            prod_rows = [
                {"collection": "Collection A", "sales": 15200.0, "prior_sales": 6000.0, "sessions": 4000, "prior_sessions": 3000, "units": 500, "prior_units": 200, "unit_conversion": 0.125, "prior_conversion": 200 / 3000, "conversion_pp": 0.125 - 200 / 3000, "sales_delta": 9200.0, "sales_wow": 9200.0 / 6000.0},
                {"collection": "Collection B", "sales": 10000.0, "prior_sales": 15000.0, "sessions": 3000, "prior_sessions": 4000, "units": 340, "prior_units": 500, "unit_conversion": 340 / 3000, "prior_conversion": 0.125, "conversion_pp": 340 / 3000 - 0.125, "sales_delta": -5000.0, "sales_wow": -5000.0 / 15000.0},
            ]
            pd.DataFrame(prod_rows).to_csv(root / "product_performance.csv", index=False)

            net_rows = [
                {"period": "prior", "orders": 680, "units": 700, "gross_item_sales": 21200.0, "item_promo_discount": 200.0, "net_item_sales": 21000.0, "shipping_promo_discount_excluded": 50.0},
                {"period": "current", "orders": 820, "units": 840, "gross_item_sales": 25400.0, "item_promo_discount": 400.0, "net_item_sales": 25000.0, "shipping_promo_discount_excluded": 75.0},
            ]
            pd.DataFrame(net_rows).to_csv(root / "net_sales_summary.csv", index=False)
            (src / "net.json").write_text(json.dumps({"rows": net_rows}), encoding="utf-8")
            pd.DataFrame([{"promo_label": "LD", "orders": 50, "units": 60, "gross_item_sales": 2000.0, "item_promo_discount": 400.0, "net_item_sales": 1600.0}]).to_csv(root / "top_promo_discounts.csv", index=False)

            pd.DataFrame([{"source": "Helium 10 Keyword Tracker", "asin": "B0CUSTOM01", "keyword": "queen sheet set", "search_volume": 60000, "organic_rank": 27, "sponsored_rank": 14, "cpr": 120, "iq_score": 550.0, "title_density": 12, "competing_products": 3000, "keyword_sales": 900, "cpc": 1.85, "trend": 18, "notes": ""}]).to_csv(root / "h10" / "h10_keyword_candidates.csv", index=False)
            pd.DataFrame([{"source": "Helium 10 Keyword Tracker", "asin": "B0CUSTOM01", "keyword": "queen sheet set", "organic_rank": 32, "keyword_sales": None}]).to_csv(root / "h10" / "h10_prior_keyword_candidates.csv", index=False)
            pd.DataFrame([{"keyword": "queen sheet set", "tracked_asins": "B0CUSTOM01", "current_position": 27, "prior_position": 32, "rank_movement": 5, "search_query_volume": 60000, "total_purchases": 500, "brand_purchases": 40, "est_total_keyword_sales": 15000.0, "est_brand_keyword_sales": 1280.0, "brand_ctr": 0.1, "brand_purchase_per_click": 0.2}]).to_csv(root / "tracked_sqp_keywords.csv", index=False)
            (src / "keyword-scope.json").write_text(json.dumps(["queen sheet set"]), encoding="utf-8")
            # Intentionally create independent_control_totals.csv as well
            pd.DataFrame([{"br_current_gross_sales": 25200.0, "br_current_sessions": 7000, "br_current_units": 840, "br_current_conversion": 840 / 7000, "ao_current_gross_item_sales": 25400.0, "ao_current_item_promo_discount": 400.0, "ao_current_net_item_sales": 25000.0}]).to_csv(root / "independent_control_totals.csv", index=False)

            build = subprocess.run([sys.executable, str(SCRIPTS_DIR / "build_weekly_ceo_report.py"), "--report-dir", str(root), "--week-end", "2026-11-21", "--strict"], text=True, capture_output=True, check=True)
            self.assertEqual(json.loads(build.stdout)["status"], "pass")
            html = (root / "email_body_2026-11-21.html").read_text(encoding="utf-8")
            self.assertIn("Top collection was Collection A", html)

            ver = subprocess.run([sys.executable, str(SCRIPTS_DIR / "verify_weekly_ceo_report.py"), "--report-dir", str(root), "--week-end", "2026-11-21"], text=True, capture_output=True, check=True)
            self.assertEqual(ver.stdout.strip(), "pass")


if __name__ == "__main__":
    unittest.main()
