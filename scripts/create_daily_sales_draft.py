#!/usr/bin/env python3
"""Create a real Gmail draft for a built Mellanni daily sales report.

Reads deterministic builder outputs from a durable report directory. It never
sends mail. Recipients come from an ignored local JSON config, not committed
code or verification artifacts. Use --dry-run to validate without creating a
new Gmail draft.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

HELPER = Path("/home/misunderstood/.pi/agent/extensions/google_workspace_sa.py")
DEFAULT_RECIPIENTS_CONFIG = Path("config/daily_sales_recipients.json")


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
    report_dir = args.report_dir.resolve()
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
        "Deal calendar check",
        "Collection breakdown",
    ]
    missing = [needle for needle in required if needle not in html]
    if missing:
        raise SystemExit("HTML missing required daily sections: " + ", ".join(missing))
    forbidden = ["H10 keyword standings", "Top ASINs", "Top promo discounts", "Full workbook attached"]
    present_forbidden = [needle for needle in forbidden if needle in html]
    if present_forbidden:
        raise SystemExit("HTML contains weekly-only sections: " + ", ".join(present_forbidden))
    if "data:image" in html.lower() or "cid:" in html.lower():
        raise SystemExit("HTML must use Gmail-safe native tables/bars, not embedded/inline images")

    payload = {
        "confirm_write": True,
        "to": recipients,
        "draft_subject": subject,
        "body_html": html,
    }
    if args.dry_run:
        print(json.dumps({
            "status": "dry_run_pass",
            "subject": subject,
            "recipient_count": len(recipients),
            "html": str(html_path),
            "verification": str(verification_path),
            "existing_draft_id": (existing_draft_status or {}).get("draft_id"),
        }, indent=2))
        return 0

    if existing_draft_status:
        raise SystemExit(f"Draft already recorded ({existing_draft_status.get('draft_id')}); this helper will not create duplicates")

    proc = subprocess.run(
        [sys.executable, str(HELPER), "gmail_create_draft"],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        print(proc.stdout.strip() or "draft creation failed")
        return proc.returncode
    result = json.loads(proc.stdout)
    draft_status = {
        "status": result.get("status"),
        "draft_id": result.get("draft_id"),
        "message_id": result.get("message_id"),
        "thread_id": result.get("thread_id"),
        "label_ids": result.get("label_ids"),
        "attachment_count": result.get("attachment_count"),
        "inline_attachment_count": result.get("inline_attachment_count"),
        "email_sent": False,
    }
    draft_status_path.write_text(json.dumps(draft_status, indent=2, default=str), encoding="utf-8")
    print(json.dumps(draft_status, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
