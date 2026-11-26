#!/usr/bin/env python3
"""Regression tests for daily sales Gmail draft assembly."""
from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("create_daily_sales_draft.py")
SPEC = importlib.util.spec_from_file_location("create_daily_sales_draft", MODULE_PATH)
DRAFT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = DRAFT
assert SPEC.loader is not None
SPEC.loader.exec_module(DRAFT)


class InlineChartTest(unittest.TestCase):
    def test_requires_tmp_report_directory(self) -> None:
        with self.assertRaisesRegex(SystemExit, "must stay under /tmp"):
            DRAFT.require_tmp(Path("/media/misunderstood/DATA/projects/mellanni2025/reports/daily_sales/test"))
        allowed = Path("/tmp/mellanni-reports/daily_sales/test")
        self.assertEqual(DRAFT.require_tmp(allowed), allowed)

    def test_rejects_daily_report_without_hourly_chart(self) -> None:
        with self.assertRaisesRegex(SystemExit, "missing the mandatory HTML hourly chart"):
            DRAFT.validate_hourly_chart("<p>No chart</p>")

    def test_rejects_images_in_html(self) -> None:
        chart = "<!-- DAILY_HOURLY_START --><p>Peak hour: $1.0k</p><!-- DAILY_HOURLY_END -->"
        for bad in ('<img src="cid:daily-hourly-sales">', '<img src="data:image/png;base64,AA">'):
            with self.assertRaisesRegex(SystemExit, "must not contain images"):
                DRAFT.validate_hourly_chart(chart + bad)

    def test_background_shorthand_becomes_background_color(self) -> None:
        out = DRAFT.gmail_safe_css('<div style="background:#111827;color:#fff"><td style="padding:2px;background: #fff">')
        self.assertEqual(out, '<div style="background-color:#111827;color:#fff"><td style="padding:2px;background-color:#fff">')
        with self.assertRaisesRegex(SystemExit, "not a plain color"):
            DRAFT.gmail_safe_css('<div style="background:url(x.png) #fff">')

    def test_accepts_html_table_chart(self) -> None:
        DRAFT.validate_hourly_chart("<!-- DAILY_HOURLY_START --><table><tr><td>Peak hour: $1.0k</td></tr></table><!-- DAILY_HOURLY_END -->")


if __name__ == "__main__":
    unittest.main()
