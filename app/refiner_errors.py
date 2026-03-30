"""Operator-facing Refiner failure text (logs + activity hints)."""

from __future__ import annotations

import errno
from typing import Any


def format_refiner_failure_for_operator(exc: BaseException) -> tuple[str, str | None]:
    """Return (summary line, optional technical reason line).

    Summary is safe for the Logs UI; the second line carries OS / library detail when useful.
    """
    if isinstance(exc, RuntimeError):
        text = str(exc).strip()
        if "Output file appeared" in text or "another writer" in text.lower():
            return (
                "Refiner could not finalize the processed file — something else created the output path first.",
                text or None,
            )
        if "work temp" in text.lower() or "could not be deleted" in text.lower():
            return (
                "Refiner placed the output file, but could not remove a temporary work file (check permissions).",
                text or None,
            )
        if "directory" in text.lower() and "output" in text.lower():
            return ("Refiner output path points to a folder, not a file.", text or None)
        if text:
            return (text, None)

    if isinstance(exc, OSError):
        win = getattr(exc, "winerror", None)
        if win == 17 or exc.errno == errno.EXDEV:
            return (
                "Refiner could not finalize the processed file in the destination folder.",
                f"{type(exc).__name__}: {exc}",
            )
        if exc.errno in (errno.ENOSPC, errno.EDQUOT):
            return (
                "Refiner could not write the output file — disk may be full.",
                f"{type(exc).__name__}: {exc}",
            )
        return (
            "Refiner could not complete file operations (read, write, or permissions).",
            f"{type(exc).__name__}: {exc}",
        )

    msg = str(exc).strip()
    if msg:
        return ("Refiner could not complete processing for this file.", msg)
    return ("Refiner could not complete processing for this file.", type(exc).__name__)


def failure_hint_from_exception(exc: BaseException) -> str:
    """Single stored/display string for per-file job log aggregation."""
    summary, detail = format_refiner_failure_for_operator(exc)
    if detail:
        return f"{summary}\n  Reason: {detail}"
    return summary


def failure_hint_from_message(message: str, *, default: str = "Processing failed.") -> str:
    """Normalize free-form failure text for activity / job log lines."""
    t = (message or "").strip()
    return t if t else default
