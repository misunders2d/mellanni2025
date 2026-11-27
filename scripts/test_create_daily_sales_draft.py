#!/usr/bin/env python3
"""Regression tests for daily sales Gmail draft assembly."""
from __future__ import annotations

import base64
from email.message import EmailMessage
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gwscli_gmail

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


class GwscliReadBackTest(unittest.TestCase):
    def _stored(self, html: str = '<img src="cid:att0">') -> dict:
        return {
            "id": "r123",
            "messageId": "m123",
            "threadId": "t123",
            "labelIds": ["DRAFT"],
            "viewUrl": "https://mail.google.com/mail/#all?compose=r123",
            "htmlBody": html,
        }

    def test_read_back_raw_mime_zero_attachments(self) -> None:
        msg = EmailMessage()
        msg.set_content("hello")
        raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii").rstrip("=")
        raw_proc = subprocess.CompletedProcess(["gwscli"], 0, stdout=json.dumps({"raw": raw_b64}), stderr="")
        with mock.patch.object(gwscli_gmail, "gwscli", return_value=self._stored("<p>ok</p>")), \
             mock.patch.object(gwscli_gmail, "_run_gwscli", return_value=raw_proc):
            res = gwscli_gmail.read_back("r123", expected_inline=0, expected_normal=0)
        self.assertEqual(res["attachment_verification"], "raw_mime")
        self.assertEqual(res["inline_attachment_count"], 0)
        self.assertEqual(res["attachment_count"], 0)
        self.assertEqual(res["label_ids"], ["DRAFT"])

    def test_read_back_confirms_non_body_parts_when_gwscli_rejects_raw_with_attachments(self) -> None:
        err_json = json.dumps({
            "content": [{"text": gwscli_gmail.RAW_UNSUPPORTED_WITH_ATTACHMENTS + ". Use PLAIN_TEXT or FULL_CONTENT format instead.", "type": "text"}],
            "isError": True,
        })
        raw_proc = subprocess.CompletedProcess(["gwscli"], 1, stdout="", stderr=err_json)
        with mock.patch.object(gwscli_gmail, "gwscli", return_value=self._stored()), \
             mock.patch.object(gwscli_gmail, "_run_gwscli", return_value=raw_proc):
            res = gwscli_gmail.read_back("r123", expected_inline=1, expected_normal=0, expected_cid='src="cid:att0"')
        self.assertEqual(res["attachment_verification"], "gwscli_non_body_parts_confirmed")
        self.assertEqual(res["inline_attachment_count"], 1)
        self.assertEqual(res["attachment_count"], 0)
        self.assertIn("attachment_verification_note", res)

    def test_read_back_fails_when_expected_cid_missing(self) -> None:
        with mock.patch.object(gwscli_gmail, "gwscli", return_value=self._stored("<p>no chart</p>")):
            with self.assertRaisesRegex(SystemExit, "missing expected"):
                gwscli_gmail.read_back("r123", expected_inline=1, expected_cid='src="cid:att0"')

    def test_read_back_detects_dropped_attachment_when_raw_succeeds_empty(self) -> None:
        msg = EmailMessage()
        msg.set_content("no attachment")
        raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii").rstrip("=")
        raw_proc = subprocess.CompletedProcess(["gwscli"], 0, stdout=json.dumps({"raw": raw_b64}), stderr="")
        with mock.patch.object(gwscli_gmail, "gwscli", return_value=self._stored()), \
             mock.patch.object(gwscli_gmail, "_run_gwscli", return_value=raw_proc):
            res = gwscli_gmail.read_back("r123", expected_inline=1, expected_cid='src="cid:att0"')
        self.assertEqual(res["inline_attachment_count"], 0)

    def test_read_back_fails_on_unrelated_raw_error(self) -> None:
        raw_proc = subprocess.CompletedProcess(["gwscli"], 1, stdout="", stderr="transient 500 error")
        with mock.patch.object(gwscli_gmail, "gwscli", return_value=self._stored()), \
             mock.patch.object(gwscli_gmail, "_run_gwscli", return_value=raw_proc):
            with self.assertRaisesRegex(SystemExit, "transient 500 error"):
                gwscli_gmail.read_back("r123", expected_inline=1, expected_cid='src="cid:att0"')


if __name__ == "__main__":
    unittest.main()
