#!/usr/bin/env python3
"""Synthetic regression checks for exclusive daily promo-associated sales."""
import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("daily_promos_report", Path(__file__).with_name("build_daily_sales_report.py"))
REPORT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = REPORT
SPEC.loader.exec_module(REPORT)


DRAFT_SPEC = importlib.util.spec_from_file_location("daily_promos_draft", Path(__file__).with_name("create_daily_sales_draft.py"))
DRAFT = importlib.util.module_from_spec(DRAFT_SPEC)
DRAFT_SPEC.loader.exec_module(DRAFT)


class PromotionsTest(unittest.TestCase):
    def row(self, ids="", sales="10", item="0", ship="0", order="O1", units="1", **extra):
        return {"promotion-ids": ids, "item-price": sales, "item-promotion-discount": item,
                "ship-promotion-discount": ship, "amazon-order-id": order, "quantity": units, **extra}

    def summarize(self, rows):
        checks = []
        result = REPORT.promotion_summary(REPORT.pd.DataFrame(rows), checks, "test")
        return result, checks

    def test_ranks_sales_not_discount_and_counts_stacked_lines_once(self):
        result, checks = self.summarize([
            self.row("A", "100", "1"), self.row("B", "20", "10"),
            self.row("B, A, A", "30", "2"), self.row("A,B", "5", "1"),
        ])
        self.assertTrue(all(c.status == "pass" for c in checks))
        self.assertEqual([g["promotion_ids"] for g in result["groups"]], [["A"], ["A", "B"], ["B"]])
        stacked = result["groups"][1]
        self.assertEqual(stacked["sales"], 35)
        self.assertEqual(stacked["orders"], 1)
        self.assertEqual(result["totals"], {"sales": 155, "item_discount": 14, "shipping_discount": 0, "net_sales": 141, "units": 4, "rows": 4, "orders": 1})

    def test_unidentified_shipping_only_and_no_recorded_promo_controls(self):
        result, _ = self.summarize([self.row(item="2"), self.row(ship="3"), self.row(), self.row("nan", "5", "1")])
        groups = {g["category"]: g for g in result["groups"]}
        self.assertEqual(groups["unidentified"]["label"], "Unidentified promotion")
        self.assertEqual(groups["unidentified"]["sales"], 25)
        self.assertEqual(groups["no_recorded_promotion"]["sales"], 10)
        self.assertEqual(result["totals"]["net_sales"], 32)
        self.assertEqual(result["totals"]["shipping_discount"], 3)
        html = REPORT.render_promotions("D1", "D2", {"D1": result, "D2": result})
        self.assertIn("No identified promotion ranking available", html)
        self.assertIn("not incremental lift", html)

    def test_draft_requires_both_dates_verified_promotions(self):
        result, _ = self.summarize([self.row("A")])
        verification = {"target_date_pt": "D1", "prior_date_pt": "D2", "promotions": {"D1": result, "D2": result}}
        DRAFT.validate_promotions(verification)
        for bad in ({}, {**verification, "promotions": {"D1": result}}, {**verification, "promotions": {"D1": result, "D2": {"status": "fail", "groups": result["groups"]}}}):
            with self.assertRaisesRegex(SystemExit, "promotion-sales ranking"):
                DRAFT.validate_promotions(bad)

    def test_missing_fields_block(self):
        for field in ("promotion-ids", "item-promotion-discount", "ship-promotion-discount"):
            row = self.row()
            del row[field]
            result, checks = self.summarize([row])
            self.assertEqual(result["status"], "fail")
            self.assertTrue(any(c.status == "fail" for c in checks))

    def test_invalid_numeric_or_order_evidence_blocks(self):
        for row in (self.row(item="bad"), self.row(item="nan"), self.row(ship="-1"), self.row(sales="Infinity"), self.row(units="1.5"), self.row(order="")):
            result, _ = self.summarize([row])
            self.assertEqual(result["status"], "fail")

    def test_pacific_date_channel_currency_and_cancellations_match_headline(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "orders.csv"
            base = self.row("A", "10", "2", "3", asin="X", **{"purchase-date": "2026-09-30T06:59:00Z", "sales-channel": "Amazon.com", "currency": "USD", "order-status": "Shipped"})
            rows = [base, {**base, "purchase-date": "2026-09-30T07:00:00Z"}, {**base, "sales-channel": "Amazon.ca"}, {**base, "currency": "CAD"}, {**base, "order-status": "Canceled"}, {**base, "order-status": "Cancelled"}]
            with path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(base))
                writer.writeheader()
                writer.writerows(rows)
            checks = []
            _, summary = REPORT.normalize_orders(path, "2026-09-29", REPORT.pd.DataFrame([{"asin": "X", "collection": "Test"}]), checks, "test")
            self.assertEqual(summary["sales"], 10)
            self.assertEqual(summary["promotions"]["totals"]["sales"], 10)
            self.assertEqual(summary["promotions"]["totals"]["rows"], 1)

    def test_html_escapes_labels_and_rolls_up_only_omitted_identified_groups(self):
        result, _ = self.summarize([self.row(f"Promo {i} <script>", str(20-i), "1") for i in range(12)] + [self.row()])
        html = REPORT.render_promotions("D1", "D2", {"D1": result, "D2": result})
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertIn("Other 2 identified groups", html)
        self.assertEqual(result["other_identified_totals"]["units"], 2)
        self.assertEqual(result["other_identified_totals"]["orders"], 1)  # Same order spans two omitted promos.
        self.assertIn("No recorded promotion", html)
        self.assertIn("All items — reconciliation total", html)
        DRAFT.validate_promotion_columns(html)
        self.assertEqual(html.count(">Units</th>"), 2)
        self.assertEqual(html.count(">Orders</th>"), 2)
        for missing in (html.replace(">Units</th>", ">Missing</th>"), html.replace(">Orders</th>", ">Missing</th>", 1)):
            with self.assertRaisesRegex(SystemExit, "must show Units and Orders"):
                DRAFT.validate_promotion_columns(missing)


if __name__ == "__main__":
    unittest.main()
