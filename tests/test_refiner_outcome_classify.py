"""Refiner failure classification: retryable vs waiting vs manual vs permanent."""

from __future__ import annotations

import json
from datetime import datetime

from app.arr_failed_import_classify import FailedImportDisposition
from app.models import RefinerActivity
from app.refiner_outcome_classify import (
    RefinerOutcomeClass,
    classify_failure_message,
    classify_refiner_activity_context,
    format_per_file_job_log_line,
    map_failed_import_disposition_to_refiner_class,
)
from app.web_common import refiner_activity_display_row, _humanize_refiner_batch_log_detail


def test_map_failed_import_disposition_bridge() -> None:
    assert map_failed_import_disposition_to_refiner_class(FailedImportDisposition.PENDING_WAITING) == (
        RefinerOutcomeClass.BLOCKED_WAITING
    )
    for disp in (
        FailedImportDisposition.CORRUPT,
        FailedImportDisposition.DOWNLOAD_FAILED,
        FailedImportDisposition.UNMATCHED,
        FailedImportDisposition.QUALITY,
    ):
        assert map_failed_import_disposition_to_refiner_class(disp) == RefinerOutcomeClass.MANUAL_ACTION
    assert map_failed_import_disposition_to_refiner_class(FailedImportDisposition.UNKNOWN) == RefinerOutcomeClass.RETRYABLE


def test_reason_code_readiness_is_blocked_waiting() -> None:
    ctx = {"reason_code": "radarr_queue_active_download", "failure_reason": ""}
    oc, hint, auto = classify_refiner_activity_context(ctx, status="failed")
    assert oc is RefinerOutcomeClass.BLOCKED_WAITING
    assert "Waiting on source" in hint or "timing" in hint
    assert auto is True


def test_reason_code_title_readiness_is_blocked_waiting() -> None:
    ctx = {"reason_code": "radarr_queue_active_download_title", "failure_reason": ""}
    oc, hint, auto = classify_refiner_activity_context(ctx, status="failed")
    assert oc is RefinerOutcomeClass.BLOCKED_WAITING
    assert "Waiting on source" in hint or "timing" in hint
    assert auto is True


def test_source_disappeared_blocked_waiting_not_hard_failure_semantics() -> None:
    oc, hint = classify_failure_message("Source file disappeared from the watch folder before remux could start.")
    assert oc is RefinerOutcomeClass.BLOCKED_WAITING
    assert "Waiting" in hint


def test_output_collision_manual_action() -> None:
    oc, hint = classify_failure_message(
        "Output file already exists — remove or rename it in the output folder, then retry."
    )
    assert oc is RefinerOutcomeClass.MANUAL_ACTION
    assert "Manual action" in hint


def test_no_audio_permanent() -> None:
    oc, hint = classify_failure_message("No audio streams — Refiner cannot produce a valid output.")
    assert oc is RefinerOutcomeClass.PERMANENT_FAILURE
    assert "Non-retryable" in hint or "rules" in hint


def test_thread_error_retryable() -> None:
    oc, hint = classify_failure_message(
        "Unexpected error during processing (thread error, cancellation, or timeout)."
    )
    assert oc is RefinerOutcomeClass.RETRYABLE


def test_job_log_line_prefixes_with_class() -> None:
    line = format_per_file_job_log_line(
        "a.mkv", "Output file already exists — remove or rename it in the output folder, then retry."
    )
    assert line.startswith("a.mkv [manual]")


def test_job_log_line_uses_reason_code_for_classification() -> None:
    line = format_per_file_job_log_line(
        "x.mkv",
        "Could not read or analyze the file.",
        reason_code="radarr_queue_active_download",
    )
    assert line.startswith("x.mkv [waiting]")


def test_activity_row_failed_includes_outcome_class_and_sub() -> None:
    r = RefinerActivity(
        file_name="bad.mkv",
        status="failed",
        size_before_bytes=100,
        size_after_bytes=100,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=json.dumps(
            {
                "v": 1,
                "failure_reason": "Source file disappeared from the watch folder before remux could start.",
                "audio_before": "",
                "audio_after": "",
                "subs_before": "",
                "subs_after": "",
            }
        ),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    assert row["refiner_outcome_class"] == "blocked_waiting"
    assert row["activity_outcome"] == "waiting"
    assert "Waiting on source" in (row.get("refiner_outcome_sub") or "")


def test_import_promotion_block_manual_action() -> None:
    ctx: dict = {
        "failure_reason": "",
        "import_promotion_block": {"arr_app": "radarr", "subtitle": "Not promoted"},
    }
    oc, hint, auto = classify_refiner_activity_context(ctx, status="failed")
    assert oc is RefinerOutcomeClass.MANUAL_ACTION
    assert "arr import" in hint.lower()
    assert auto is False


def test_activity_row_success_unchanged_no_failure_class() -> None:
    r = RefinerActivity(
        file_name="ok.mkv",
        status="success",
        size_before_bytes=100,
        size_after_bytes=90,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=json.dumps({"v": 1, "finalized": True}),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    assert row["refiner_outcome_label"] == "Completed"
    assert row.get("refiner_outcome_class") == ""


def test_humanize_batch_adds_guidance_when_errors() -> None:
    detail = "Refiner (scheduled): processed=0 unchanged=0 dry_run_items=0 errors=2"
    lines = _humanize_refiner_batch_log_detail(detail)
    assert lines is not None
    assert len(lines) == 2
    assert "per-file" in lines[1].lower()
