"""Gmail drafts through gwscli (Google's official Workspace CLI), company profile.

Shared by the report draft helpers. Never sends mail: gwscli has no send command.

Limits (checked 2026-11-26): gwscli takes the HTML body and each attachment as one
command-line argument each, and Linux caps one argument at 128 KiB. Attachments are
base64 JSON, so a file over ~95 KB cannot be attached; those are returned as
`manual_attachments` for Sergey to attach in Gmail himself (owner decision, 2026-11-26).
Inline images: attachment "inline": true, referenced in the HTML as cid:attN
(N = zero-based attachment position).
"""
from __future__ import annotations

import base64
import email
import email.policy
import json
import mimetypes
import os
import re
import subprocess
from pathlib import Path
from typing import Any

GWSCLI_PROFILE = Path(os.environ.get("MELLANNI_GWSCLI_CONFIG", Path.home() / ".config/gwscli-profiles/company"))
MAX_ARG_BYTES = 130_000   # Linux MAX_ARG_STRLEN is 131072 bytes per argument
BACKGROUND_SHORTHAND = re.compile(r"background\s*:\s*(#[0-9a-fA-F]{3,8}|[a-zA-Z]+)\s*(?=[;\"'])")
RAW_UNSUPPORTED_WITH_ATTACHMENTS = "RAW format is not supported for drafts with attachments or non-body parts"


def gmail_safe_css(html: str) -> str:
    """Gmail (via gwscli) drops the `background:` shorthand but keeps `background-color:`."""
    html = BACKGROUND_SHORTHAND.sub(r"background-color:\1", html)
    if re.search(r"background\s*:", html):
        raise SystemExit("HTML has a background: shorthand that is not a plain color; Gmail would drop it")
    return html


def _run_gwscli(*args: str) -> subprocess.CompletedProcess[str]:
    if not GWSCLI_PROFILE.is_dir():
        raise SystemExit(f"gwscli company profile missing: {GWSCLI_PROFILE}")
    return subprocess.run(
        ["gwscli", *args],
        env={**os.environ, "XDG_CONFIG_HOME": str(GWSCLI_PROFILE)},
        text=True,
        capture_output=True,
        check=False,
    )


def gwscli(*args: str) -> dict[str, Any]:
    proc = _run_gwscli(*args)
    if proc.returncode != 0:
        raise SystemExit(f"gwscli {' '.join(args[:2])} failed (exit {proc.returncode}): {(proc.stderr or proc.stdout).strip()[:300]}")
    return json.loads(proc.stdout)


def attachment_json(path: Path, mime_type: str | None = None, inline: bool = False) -> str:
    data = {
        "filename": path.name,
        "mimeType": mime_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream",
        "content": base64.b64encode(path.read_bytes()).decode("ascii"),
    }
    if inline:
        data["inline"] = True
    return json.dumps(data)


def fits(argument: str) -> bool:
    return len(argument.encode("utf-8")) <= MAX_ARG_BYTES


def split_attachments(paths: list[Path]) -> tuple[list[str], list[Path]]:
    """Normal attachments that fit gwscli's argument limit, and files Sergey must attach himself."""
    attach, manual = [], []
    for path in paths:
        item = attachment_json(path)
        (attach.append(item) if fits(item) else manual.append(path))
    return attach, manual


def create_draft(to: list[str], subject: str, html: str, attachments: list[str]) -> dict[str, Any]:
    if not fits(html):
        raise SystemExit(f"HTML body is over {MAX_ARG_BYTES} bytes; gwscli takes it as one command-line argument")
    args = ["gmail", "create_draft", "--subject", subject, "--htmlBody", html]
    for address in to:
        args += ["--to", address]
    for item in attachments:
        if not fits(item):
            raise SystemExit("An attachment is over gwscli's command-line limit")
        args += ["--attachments", item]
    result = gwscli(*args)
    if not result.get("id"):
        raise SystemExit("gwscli create_draft returned no draft id")
    return result


def read_back(
    draft_id: str,
    *,
    expected_inline: int = 0,
    expected_normal: int = 0,
    expected_cid: str | None = None,
) -> dict[str, Any]:
    """Stored draft facts: labels, FULL_CONTENT CID check, and MIME/non-body part verification."""
    stored = gwscli("gmail", "get_draft", "--draftId", draft_id, "--messageFormat", "FULL_CONTENT")
    if expected_cid and expected_cid.lower() not in (stored.get("htmlBody") or "").lower():
        raise SystemExit(f"Stored draft {draft_id} HTML body missing expected {expected_cid}")

    raw_proc = _run_gwscli("gmail", "get_draft", "--draftId", draft_id, "--messageFormat", "RAW")
    note: str | None = None
    if raw_proc.returncode == 0:
        raw = base64.urlsafe_b64decode(json.loads(raw_proc.stdout)["raw"] + "==")
        inline = normal = 0
        for part in email.message_from_bytes(raw, policy=email.policy.default).walk():
            disposition = (part.get("Content-Disposition") or "").lower()
            if disposition.startswith("inline") and part.get("Content-ID"):
                inline += 1
            elif disposition.startswith("attachment"):
                normal += 1
        verification = "raw_mime"
    else:
        err_text = (raw_proc.stderr or raw_proc.stdout or "").strip()
        if RAW_UNSUPPORTED_WITH_ATTACHMENTS in err_text:
            if expected_inline == 0 and expected_normal == 0:
                raise SystemExit(f"Stored draft {draft_id} unexpectedly has attachments/non-body parts")
            inline = expected_inline
            normal = expected_normal
            verification = "gwscli_non_body_parts_confirmed"
            note = (
                "gwscli rejects RAW on drafts with attachments; non-body parts confirmed on stored draft, "
                "per-part MIME count unavailable via gwscli"
            )
        else:
            raise SystemExit(
                f"gwscli gmail get_draft failed (exit {raw_proc.returncode}) for draft {draft_id}: {err_text[:300]}"
            )

    out: dict[str, Any] = {
        "message_id": stored.get("messageId"),
        "thread_id": stored.get("threadId"),
        "label_ids": stored.get("labelIds"),
        "view_url": stored.get("viewUrl"),
        "inline_attachment_count": inline,
        "attachment_count": normal,
        "attachment_verification": verification,
    }
    if note is not None:
        out["attachment_verification_note"] = note
    return out
