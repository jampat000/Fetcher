"""Operator-meaningful classification for Refiner failures and skips (retry vs wait vs manual vs permanent)."""

from __future__ import annotations

from enum import Enum
from typing import Any

from app.arr_failed_import_classify import FailedImportDisposition


class RefinerOutcomeClass(str, Enum):
    """How an operator should interpret a non-success Refiner file outcome."""

    RETRYABLE = "retryable"
    BLOCKED_WAITING = "blocked_waiting"
    MANUAL_ACTION = "manual_action"
    PERMANENT_FAILURE = "permanent_failure"


_REASON_CODE_BLOCKED: frozenset[str] = frozenset(
    {
        "skipped_queue_recheck",
        "skipped_final_readiness_gate",
        "skipped_readiness",
        "radarr_queue_active_download",
        "sonarr_queue_active_download",
        "radarr_queue_active_download_title",
        "sonarr_queue_active_download_title",
        "no_ready_sources",
    }
)

# Exposed for Refiner activity persistence: merge repeat "waiting" rows (same file + reason_code).
REFINER_BLOCKED_WAIT_REASON_CODES: frozenset[str] = _REASON_CODE_BLOCKED

_FAILURE_NEEDLE_PERMANENT: tuple[tuple[str, RefinerOutcomeClass], ...] = (
    ("no audio streams", RefinerOutcomeClass.PERMANENT_FAILURE),
    ("no audio track would remain", RefinerOutcomeClass.PERMANENT_FAILURE),
    ("refiner cannot produce a valid output", RefinerOutcomeClass.PERMANENT_FAILURE),
)

_FAILURE_NEEDLE_MANUAL: tuple[tuple[str, RefinerOutcomeClass], ...] = (
    ("output file already exists", RefinerOutcomeClass.MANUAL_ACTION),
    ("output path is a directory", RefinerOutcomeClass.MANUAL_ACTION),
    ("output path points to a folder", RefinerOutcomeClass.MANUAL_ACTION),
    ("created the output path first", RefinerOutcomeClass.MANUAL_ACTION),
    ("something else created the output", RefinerOutcomeClass.MANUAL_ACTION),
    ("disk may be full", RefinerOutcomeClass.MANUAL_ACTION),
    ("disk is full", RefinerOutcomeClass.MANUAL_ACTION),
    ("could not write the output file — disk", RefinerOutcomeClass.MANUAL_ACTION),
)

_FAILURE_NEEDLE_BLOCKED: tuple[tuple[str, RefinerOutcomeClass], ...] = (
    ("source file disappeared", RefinerOutcomeClass.BLOCKED_WAITING),
    ("source file is missing or not a regular file", RefinerOutcomeClass.BLOCKED_WAITING),
    ("not ready yet", RefinerOutcomeClass.BLOCKED_WAITING),
    ("still downloading", RefinerOutcomeClass.BLOCKED_WAITING),
    ("waiting for import", RefinerOutcomeClass.BLOCKED_WAITING),
    ("no eligible", RefinerOutcomeClass.BLOCKED_WAITING),
)

_FAILURE_NEEDLE_RETRYABLE: tuple[tuple[str, RefinerOutcomeClass], ...] = (
    ("unexpected error during processing (thread", RefinerOutcomeClass.RETRYABLE),
    ("timeout", RefinerOutcomeClass.RETRYABLE),
    ("cancellation", RefinerOutcomeClass.RETRYABLE),
    ("could not complete file operations", RefinerOutcomeClass.RETRYABLE),
    ("could not read or analyze the file", RefinerOutcomeClass.RETRYABLE),
    ("could not finalize the processed file in the destination", RefinerOutcomeClass.RETRYABLE),
    ("could not finalize the processed file", RefinerOutcomeClass.RETRYABLE),
    ("temporary work file", RefinerOutcomeClass.RETRYABLE),
    ("could not remove a temporary work file", RefinerOutcomeClass.RETRYABLE),
    ("refiner could not complete processing for this file", RefinerOutcomeClass.RETRYABLE),
    ("refiner placed the output file, but could not remove", RefinerOutcomeClass.RETRYABLE),
)


def map_failed_import_disposition_to_refiner_class(
    disp: FailedImportDisposition,
) -> RefinerOutcomeClass:
    """Bridge *arr failed-import disposition to Refiner-style operator classes."""
    if disp is FailedImportDisposition.PENDING_WAITING:
        return RefinerOutcomeClass.BLOCKED_WAITING
    if disp is FailedImportDisposition.TERMINAL_CLEANUP:
        return RefinerOutcomeClass.MANUAL_ACTION
    return RefinerOutcomeClass.RETRYABLE


def _classify_failure_text(low: str) -> RefinerOutcomeClass | None:
    for table in (
        _FAILURE_NEEDLE_PERMANENT,
        _FAILURE_NEEDLE_MANUAL,
        _FAILURE_NEEDLE_BLOCKED,
        _FAILURE_NEEDLE_RETRYABLE,
    ):
        for needle, c in table:
            if needle in low:
                return c
    return None


def classify_from_reason_code(reason_code: str | None) -> RefinerOutcomeClass | None:
    rc = (reason_code or "").strip().lower()
    if not rc:
        return None
    if rc in _REASON_CODE_BLOCKED:
        return RefinerOutcomeClass.BLOCKED_WAITING
    if rc == "radarr_wrong_content":
        return RefinerOutcomeClass.PERMANENT_FAILURE
    return None


def classify_refiner_activity_context(ctx: dict[str, Any], *, status: str) -> tuple[RefinerOutcomeClass, str, bool]:
    """
    From persisted activity_context (+ row status), return:
    (class, operator_hint_line, suggests_auto_retry).
    """
    st = (status or "").strip().lower()
    if st != "failed":
        return RefinerOutcomeClass.RETRYABLE, "", False

    rc = classify_from_reason_code(ctx.get("reason_code") if isinstance(ctx.get("reason_code"), str) else None)
    if rc is not None:
        if rc is RefinerOutcomeClass.PERMANENT_FAILURE and (
            ctx.get("wrong_content") is True
            or (str(ctx.get("reason_code") or "").strip().lower() == "radarr_wrong_content")
        ):
            return (
                rc,
                "Wrong content was handled automatically in Radarr; this file is not retried on schedule.",
                False,
            )
        auto = rc in (RefinerOutcomeClass.RETRYABLE, RefinerOutcomeClass.BLOCKED_WAITING)
        return rc, _hint_for_class(rc), auto

    ipb = ctx.get("import_promotion_block")
    if isinstance(ipb, dict) and ipb:
        return (
            RefinerOutcomeClass.MANUAL_ACTION,
            "Blocked by *arr import state — resolve the failed import or eligibility issue in Sonarr/Radarr.",
            False,
        )

    fr = (ctx.get("failure_reason") or "").strip()
    low = fr.casefold()
    hit = _classify_failure_text(low)
    if hit is not None:
        auto = hit in (RefinerOutcomeClass.RETRYABLE, RefinerOutcomeClass.BLOCKED_WAITING)
        return hit, _hint_for_class(hit), auto

    return (
        RefinerOutcomeClass.RETRYABLE,
        "Unclear failure — may clear on a later run; see details if this repeats.",
        True,
    )


def _hint_for_class(oc: RefinerOutcomeClass) -> str:
    if oc is RefinerOutcomeClass.RETRYABLE:
        return "Likely transient — the next scheduled run will try again automatically."
    if oc is RefinerOutcomeClass.BLOCKED_WAITING:
        return "Waiting on source or timing — not a hard error; should settle when conditions improve."
    if oc is RefinerOutcomeClass.MANUAL_ACTION:
        return "Manual action required — resolve the condition below; retry alone may not help."
    return "Non-retryable for this file — adjust rules or fix the source media, then try again."


def job_log_class_label(oc: RefinerOutcomeClass) -> str:
    if oc is RefinerOutcomeClass.RETRYABLE:
        return "retryable"
    if oc is RefinerOutcomeClass.BLOCKED_WAITING:
        return "waiting"
    if oc is RefinerOutcomeClass.MANUAL_ACTION:
        return "manual"
    return "permanent"


def format_per_file_job_log_line(file_name: str, failure_hint: str, *, reason_code: str = "") -> str:
    """Single line for JobRunLog aggregation (classification tag, then hint)."""
    hint = (failure_hint or "").strip() or "Processing failed."
    ctx: dict[str, Any] = {"failure_reason": hint}
    rc = (reason_code or "").strip()
    if rc:
        ctx["reason_code"] = rc
    oc, _, _ = classify_refiner_activity_context(ctx, status="failed")
    label = job_log_class_label(oc)
    one_line = hint.replace("\n", " — ").strip()
    return f"{file_name} [{label}] {one_line}"


def classify_failure_message(message: str) -> tuple[RefinerOutcomeClass, str]:
    """Classify a bare failure string (tests / helpers)."""
    ctx: dict[str, Any] = {"failure_reason": message}
    oc, hint, _ = classify_refiner_activity_context(ctx, status="failed")
    return oc, hint
