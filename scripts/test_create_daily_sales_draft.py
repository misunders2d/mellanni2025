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
    def test_rejects_daily_report_without_hourly_chart(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(SystemExit, "missing the mandatory hourly chart CID"):
                DRAFT.build_inline_attachments(Path(temp), "2026-08-17", "<p>No chart</p>")

    def test_builds_one_related_inline_attachment_for_hourly_chart(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report_dir = Path(temp)
            chart = report_dir / "hourly_sales_2026-08-17.png"
            chart.write_bytes(b"\x89PNG\r\n\x1a\nchart")
            html = '<img src="cid:daily-hourly-sales">'

            attachments = DRAFT.build_inline_attachments(report_dir, "2026-08-17", html)

            self.assertEqual(attachments, [{
                "filename": chart.name,
                "mime_type": "image/png",
                "local_path": str(chart),
                "inline": True,
                "content_id": "daily-hourly-sales",
            }])


if __name__ == "__main__":
    unittest.main()
