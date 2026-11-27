#!/usr/bin/env python3
"""Create a real Gmail draft for a built Mellanni daily sales report.

Reads deterministic builder outputs from a temporary work directory under /tmp.
It never sends mail. Recipients come from an ignored local JSON config, not committed
code or verification artifacts. Use --dry-run to validate without creating a
new Gmail draft. The draft goes through gwscli (Google's official Workspace CLI)
under the company profile. The hourly line chart PNG is the only attachment, sent
inline (Gmail MCP `inline: true`) and referenced in the HTML as cid:att0.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

from gwscli_gmail import attachment_json, create_draft, fits, gmail_safe_css, read_back
from report_storage import require_tmp_artifact_path

CHART_CID = "att0"        # Gmail MCP names inline attachments by position: first attachment = cid:att0
DEFAULT_RECIPIENTS_CONFIG = Path(__file__).resolve().parents[1] / "config" / "daily_sales_recipients.json"


def require_tmp(path: Path) -> Path:
    return require_tmp_artifact_path(path, label="Daily report artifacts", error_type=SystemExit)


def build_inline_chart(report_dir: Path, target_date: str, html: str) -> str:
    """One inline PNG for the hourly line chart, as gwscli attachment JSON."""
    lowered = html.lower()
    if lowered.count(f'src="cid:{CHART_CID}"') != 1 or lowered.count("cid:") != 1:
        raise SystemExit(f"HTML must reference the hourly chart exactly once as cid:{CHART_CID} and contain no other CID")
    if "data:image" in lowered:
        raise SystemExit("HTML must not embed data:image URLs")
    chart_path = report_dir / f"hourly_sales_{target_date}.png"
    if not chart_path.is_file():
        raise SystemExit(f"Missing hourly chart image: {chart_path}")
    attachment = attachment_json(chart_path, "image/png", inline=True)
    if not fits(attachment):
        raise SystemExit(f"Hourly chart attachment is {len(attachment)} bytes; over gwscli's command-line limit")
    return attachment


def validate_promotions(verification: dict[str, Any]) -> None:
    for day in (verification.get("target_date_pt"), verification.get("prior_date_pt")):
        promo = verification.get("promotions", {}).get(day, {})
        if not day or promo.get("status") != "pass" or not promo.get("groups"):
            raise SystemExit("Daily report missing verified promotion-sales ranking")


def validate_promotion_columns(html: str) -> None:
    section = html.split("Promotion-associated sales", 1)[-1].split("Collection breakdown", 1)[0]
    if "Shipping discounts" in section:
        raise SystemExit("Shipping discounts do not belong in product promotion tables")
    for column in ("Units", "Orders", "Actual discount %"):
        if len(re.findall(r"<th\b[^>]*>" + column + r"</th>", section)) != 2:
            raise SystemExit("Daily promotion tables must show Units, Orders, and Actual discount % for both dates")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--report-dir", type=Path, required=True)
    p.add_argument("--target-date", required=True)
    p.add_argument("--dry-run", action="store_true", help="Validate payload only; do not create Gmail draft")
    p.add_argument("--recipients-config", type=Path, help="Ignored JSON file with {'to': [...]} recipients")
    return p.parse_args()


def load_recipients(path: Path | None) -> list[str]:
    env_path = os.environ.get("DAILY_SALES_RECIPIENTS_CONFIG")
    config_path = path or (Path(env_path) if env_path else DEFAULT_RECIPIENTS_CONFIG)
    if not config_path.exists():
        raise SystemExit(f"Missing recipients config: {config_path}. Create ignored JSON like {{\"to\": [\"person@example.com\"]}}")
    data = json.loads(config_path.read_text(encoding="utf-8"))
    to = data.get("to") or []
    if not isinstance(to, list) or not to or not all(isinstance(x, str) and "@" in x for x in to):
        raise SystemExit(f"Invalid recipients config: {config_path}")
    return to


def main() -> int:
    args = parse_args()
    report_dir = require_tmp(args.report_dir)
    verification_path = report_dir / f"verification_{args.target_date}.json"
    draft_status_path = report_dir / f"draft_status_{args.target_date}.json"
    html_path = report_dir / f"email_body_{args.target_date}.html"
    subject_path = report_dir / f"email_subject_{args.target_date}.txt"

    if not verification_path.exists():
        raise SystemExit(f"Missing verification JSON: {verification_path}")
    if not html_path.exists():
        raise SystemExit(f"Missing HTML body: {html_path}")
    if not subject_path.exists():
        raise SystemExit(f"Missing subject file: {subject_path}")

    verification: dict[str, Any] = json.loads(verification_path.read_text(encoding="utf-8"))
    if verification.get("status") != "pass":
        raise SystemExit(f"Refusing draft because verification status is {verification.get('status')!r}")
    if verification.get("email_sent") is True:
        raise SystemExit("Refusing draft because verification says email_sent=true")
    existing_draft_status = None
    if draft_status_path.exists():
        existing_draft_status = json.loads(draft_status_path.read_text(encoding="utf-8"))

    recipients = load_recipients(args.recipients_config)
    subject = subject_path.read_text(encoding="utf-8").strip()
    html = html_path.read_text(encoding="utf-8")
    required = [
        "Mellanni Daily Sales Report",
        "Prepared by Sergey's AI helper.",
        "Executive snapshot",
        "Hourly sales by Pacific hour",
        "Deal calendar check",
        "Competitor price and deal check",
        "Collection breakdown",
        "Promotion-associated sales",
    ]
    missing = [needle for needle in required if needle not in html]
    if missing:
        raise SystemExit("HTML missing required daily sections: " + ", ".join(missing))
    forbidden = ["H10 keyword standings", "Top ASINs", "Top promo discounts", "Full workbook attached"]
    present_forbidden = [needle for needle in forbidden if needle in html]
    if present_forbidden:
        raise SystemExit("HTML contains weekly-only sections: " + ", ".join(present_forbidden))
    if "data:image" in html.lower():
        raise SystemExit("HTML must not embed data:image URLs")
    validate_promotions(verification)
    validate_promotion_columns(html)
    attachment = build_inline_chart(report_dir, args.target_date, html)
    html = gmail_safe_css(html)
    if args.dry_run:
        print(json.dumps({
            "status": "dry_run_pass",
            "subject": subject,
            "recipient_count": len(recipients),
            "html": str(html_path),
            "hourly_chart": "inline_png",
            "inline_attachment_count": 1,
            "html_bytes": len(html.encode("utf-8")),
            "verification": str(verification_path),
            "existing_draft_id": (existing_draft_status or {}).get("draft_id"),
        }, indent=2))
        return 0

    if existing_draft_status:
        raise SystemExit(f"Draft already recorded ({existing_draft_status.get('draft_id')}); this helper will not create duplicates")

    result = create_draft(recipients, subject, html, [attachment])
    stored = read_back(
        result["id"],
        expected_inline=1,
        expected_normal=0,
        expected_cid=f'src="cid:{CHART_CID}"',
    )
    if "DRAFT" not in (stored.get("label_ids") or []) or stored["inline_attachment_count"] != 1:
        raise SystemExit(
            f"Gmail draft failed final verification (draft exists in Gmail id={result['id']}; inspect/supersede before retrying): {json.dumps(stored)}"
        )
    draft_status = {
        "status": "success",
        "draft_id": result["id"],
        "message_id": stored["message_id"],
        "thread_id": stored["thread_id"],
        "label_ids": stored["label_ids"],
        "view_url": stored.get("view_url"),
        "hourly_chart": "inline_png",
        "inline_attachment_count": stored["inline_attachment_count"],
        "attachment_verification": stored["attachment_verification"],
        "route": "gwscli",
        "email_sent": False,
    }
    if stored.get("attachment_verification_note"):
        draft_status["attachment_verification_note"] = stored["attachment_verification_note"]
    draft_status_path.write_text(json.dumps(draft_status, indent=2, default=str), encoding="utf-8")
    print(json.dumps(draft_status, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
