#!/usr/bin/env python3
"""Regression tests for daily sales Gmail draft assembly."""
from __future__ import annotations

import base64
import importlib.util
import json
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
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(SystemExit, "exactly once as cid:att0"):
                DRAFT.build_inline_chart(Path(temp), "2026-08-17", "<p>No chart</p>")

    def test_rejects_extra_cid(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(SystemExit, "no other CID"):
                DRAFT.build_inline_chart(Path(temp), "2026-08-17", '<img src="cid:att0"><img src="cid:other">')

    def test_builds_one_inline_attachment_for_hourly_chart(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report_dir = Path(temp)
            (report_dir / "hourly_sales_2026-08-17.png").write_bytes(b"\x89PNG\r\n\x1a\nchart")
            attachment = json.loads(DRAFT.build_inline_chart(report_dir, "2026-08-17", '<img src="cid:att0">'))
            self.assertEqual(attachment["filename"], "hourly_sales_2026-08-17.png")
            self.assertEqual(attachment["mimeType"], "image/png")
            self.assertIs(attachment["inline"], True)
            self.assertEqual(base64.b64decode(attachment["content"]), b"\x89PNG\r\n\x1a\nchart")

    def test_background_shorthand_becomes_background_color(self) -> None:
        out = DRAFT.gmail_safe_css('<div style="background:#111827;color:#fff"><td style="padding:2px;background: #fff">')
        self.assertEqual(out, '<div style="background-color:#111827;color:#fff"><td style="padding:2px;background-color:#fff">')
        with self.assertRaisesRegex(SystemExit, "not a plain color"):
            DRAFT.gmail_safe_css('<div style="background:url(x.png) #fff">')


if __name__ == "__main__":
    unittest.main()
