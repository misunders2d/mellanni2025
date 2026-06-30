#!/usr/bin/env python3
"""Create Gmail draft for a generated weekly CEO report from full durable HTML.

Uses Pi's Google Workspace helper. Does not send email and does not print secrets.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

HELPER = Path("/home/misunderstood/.pi/agent/extensions/google_workspace_sa.py")
DEFAULT_RECIPIENTS = [
    "igor@mellanni.com",
    "margarita@mellanni.com",
    "masao@mellanni.com",
    "ruslan@mellanni.com",
]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--report-dir", type=Path, required=True)
    p.add_argument("--week-end", required=True)
    args = p.parse_args()
    report_dir = args.report_dir.resolve()
    html_path = report_dir / f"email_body_{args.week_end}.html"
    xlsx_path = report_dir / f"Mellanni_Weekly_CEO_Overview_{args.week_end}.xlsx"
    html = html_path.read_text(encoding="utf-8")

    required = [
        "Product / collection conversion change",
        "Top ASINs",
        "Top promo discounts",
        "Tracked SQP keyword standings",
    ]
    missing = [s for s in required if s not in html]
    if missing:
        raise SystemExit("HTML missing required visible sections: " + ", ".join(missing))
    if "See attached workbook for full Top ASINs table" in html:
        raise SystemExit("HTML still has shortened Top ASIN placeholder")
    if not xlsx_path.exists():
        raise SystemExit(f"Missing workbook: {xlsx_path}")

    asins = pd.read_csv(report_dir / "asin_performance_size_color.csv")
    promos = pd.read_csv(report_dir / "top_promo_discounts.csv")
    top_asin = str(asins.iloc[0]["asin"]) if len(asins) else None
    promo_label = None
    if len(promos):
        non_ld = promos[promos["promo_label"].astype(str).str.upper() != "LD"]
        promo_label = str((non_ld.iloc[0] if len(non_ld) else promos.iloc[0])["promo_label"])
    if top_asin and top_asin not in html:
        raise SystemExit(f"HTML missing top ASIN marker: {top_asin}")
    if promo_label and promo_label not in html:
        raise SystemExit(f"HTML missing top promo marker: {promo_label}")

    payload = {
        "confirm_write": True,
        "to": DEFAULT_RECIPIENTS,
        "draft_subject": f"Mellanni Weekly CEO Overview — Week Ending {args.week_end} (formerly Conversion and Keyword standings)",
        "body_html": html,
        "attachments": [
            {
                "local_path": str(xlsx_path),
                "filename": f"Mellanni_Weekly_CEO_Overview_{args.week_end}.xlsx",
                "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            }
        ],
    }
    proc = subprocess.run(
        [sys.executable, str(HELPER), "gmail_create_draft"],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        # Helper stdout is sanitized error JSON by design; stderr should normally be empty.
        print(proc.stdout.strip() or "draft creation failed")
        return proc.returncode
    result = json.loads(proc.stdout)
    print(json.dumps({
        "status": result.get("status"),
        "draft_id": result.get("draft_id"),
        "message_id": result.get("message_id"),
        "thread_id": result.get("thread_id"),
        "label_ids": result.get("label_ids"),
        "attachment_count": result.get("attachment_count"),
        "inline_attachment_count": result.get("inline_attachment_count"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
