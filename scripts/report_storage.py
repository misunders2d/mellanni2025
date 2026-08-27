#!/usr/bin/env python3
"""Shared storage gate for generated Mellanni report artifacts."""
from __future__ import annotations

from pathlib import Path
from typing import Type

TMP_ROOT = Path("/tmp").resolve()


def require_tmp_artifact_path(
    path: Path,
    *,
    label: str = "Report artifacts",
    error_type: Type[Exception] = RuntimeError,
) -> Path:
    """Resolve path, require /tmp containment, and reject every git worktree."""
    resolved = path.resolve()
    try:
        resolved.relative_to(TMP_ROOT)
    except ValueError as exc:
        raise error_type(f"{label} must stay under /tmp, not {resolved}") from exc

    for candidate in (resolved, *resolved.parents):
        if candidate == TMP_ROOT:
            break
        if (candidate / ".git").exists():
            raise error_type(f"{label} cannot be inside a git worktree: {resolved}")
    return resolved
