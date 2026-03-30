"""Refiner per-file activity merged into the shared Activity feed."""

from __future__ import annotations

import json
from datetime import datetime

from app.models import ActivityLog, RefinerActivity
from app.refiner_media_identity import provisional_media_title_before_probe
from app.web_common import activity_display_row, merge_activity_feed, refiner_activity_display_row


def _ctx(**kwargs: object) -> str:
    base = {
        "v": 1,
        "media_title": "",
        "refiner_title": "",
        "refiner_year": "",
        "audio_before": "",
        "audio_after": "",
        "subs_before": "",
        "subs_after": "",
        "commentary_removed": False,
        "failure_reason": "",
        "dry_run": False,
        "finalized": False,
        "source_removed": False,
    }
    base.update(kwargs)
    return json.dumps(base)


def test_activity_display_row_marks_log_type() -> None:
    e = ActivityLog(
        app="radarr",
        kind="missing",
        count=0,
        detail="",
        status="ok",
        created_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    row = activity_display_row(e, "UTC")
    assert row["activity_type"] == "log"
    assert row["type"] == "log"


def test_refiner_activity_display_row_success_with_context() -> None:
    r = RefinerActivity(
        file_name="Movie.Name.2023.mkv",
        status="success",
        size_before_bytes=9 * 1024 * 1024 * 1024,
        size_after_bytes=7 * 1024 * 1024 * 1024,
        audio_tracks_before=4,
        audio_tracks_after=1,
        subtitle_tracks_before=7,
        subtitle_tracks_after=2,
        processing_time_ms=120_000,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            media_title="Movie Name (2023)",
            audio_before="English 2.0 AAC · Japanese 2.0 AAC",
            audio_after="English 5.1 TrueHD",
            subs_before="English · French · German · Italian · Spanish · Dutch · Polish",
            subs_after="English · French",
            commentary_removed=True,
            finalized=True,
            source_removed=True,
        ),
    )
    now = datetime(2026, 1, 1, 12, 3, 0)
    row = refiner_activity_display_row(r, "UTC", now)
    assert row["activity_time_iso"].endswith("Z")
    assert row["activity_type"] == "refiner"
    assert row["refiner_media_title"] == "Movie Name 2023"
    assert row["refiner_outcome_label"] == "Completed"
    assert row["refiner_apply_mode"] == "applied"
    assert row["refiner_show_comparison"] is True
    assert row["refiner_primary_line"] == "Completed"
    labels = [x["label"] for x in row["refiner_compare_rows"]]
    assert "Audio" in labels and "Subtitles" in labels and "File size" in labels
    bullets = " ".join(row["refiner_summary_bullets"])
    assert "4 track(s) → 1 track(s)" in bullets or "Audio:" in bullets
    assert "7 track(s) → 2 track(s)" in bullets or "Subtitles:" in bullets
    assert "Commentary" in bullets
    assert row["refiner_detail_blocks"] == []


def test_refiner_activity_display_row_processing_state() -> None:
    """Processing rows store the same provisional title as insert-time (before ffprobe)."""
    fn = "in-progress.mkv"
    prov = provisional_media_title_before_probe(fn)
    r = RefinerActivity(
        file_name=fn,
        media_title=prov,
        status="processing",
        size_before_bytes=0,
        size_after_bytes=0,
        audio_tracks_before=0,
        audio_tracks_after=0,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context="",
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 5))
    assert row["refiner_outcome_label"] == "Processing"
    assert row["refiner_media_title"] == prov
    assert row["refiner_media_title"]
    assert row["refiner_source_file_line"] is None
    assert row["refiner_status_tone"] == "progress"
    assert row["refiner_show_comparison"] is False
    assert row["refiner_summary_bullets"]


def test_refiner_activity_display_row_queued_state() -> None:
    """Queued rows show FIFO backlog; no before/after affordance until the file is active."""
    fn = "waiting.mkv"
    prov = provisional_media_title_before_probe(fn)
    r = RefinerActivity(
        file_name=fn,
        media_title=prov,
        status="queued",
        size_before_bytes=0,
        size_after_bytes=0,
        audio_tracks_before=0,
        audio_tracks_after=0,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context="",
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 6))
    assert row["refiner_outcome_label"] == "Queued"
    assert row["refiner_show_comparison"] is False
    assert row["refiner_compare_rows"] == []
    assert row["activity_outcome"] == "queued"


def test_refiner_processing_empty_orm_still_resolves_display_title() -> None:
    """Legacy processing rows with empty media_title still get filename-based title (never blank)."""
    r = RefinerActivity(
        file_name="legacy.processing.mkv",
        media_title="",
        status="processing",
        size_before_bytes=0,
        size_after_bytes=0,
        audio_tracks_before=0,
        audio_tracks_after=0,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context="",
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    assert row["refiner_media_title"]
    assert row["refiner_media_title"] != "—"


def test_refiner_completed_filename_derived_beats_ffprobe_context() -> None:
    r = RefinerActivity(
        file_name="Ugly.Pack.1990.mkv",
        media_title="The Grifters (1990)",
        status="success",
        size_before_bytes=100,
        size_after_bytes=90,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            media_title="The Grifters (1990)",
            refiner_title="The Grifters",
            refiner_year="1990",
            finalized=True,
        ),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 2))
    assert row["refiner_media_title"] == "Ugly Pack 1990"
    assert provisional_media_title_before_probe(r.file_name) == "Ugly Pack 1990"


def test_merge_activity_feed_newest_first() -> None:
    t_new = datetime(2026, 1, 2, 12, 0, 0)
    t_old = datetime(2026, 1, 1, 12, 0, 0)
    log = ActivityLog(
        id=10,
        created_at=t_old,
        app="radarr",
        kind="missing",
        count=1,
        detail="Old",
        status="ok",
    )
    ref = RefinerActivity(
        id=5,
        created_at=t_new,
        file_name="new.mkv",
        status="skipped",
        size_before_bytes=100,
        size_after_bytes=100,
        audio_tracks_before=2,
        audio_tracks_after=2,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        activity_context=_ctx(
            audio_before="English 2.0 AAC",
            audio_after="English 2.0 AAC",
            subs_before="—",
            subs_after="None",
        ),
    )
    merged = merge_activity_feed([log], [ref], "UTC", t_new, limit=10)
    assert merged[0]["type"] == "refiner"
    assert merged[1]["type"] == "log"


def test_refiner_skipped_no_size_change_saved_zero() -> None:
    r = RefinerActivity(
        file_name="x.mkv",
        status="skipped",
        size_before_bytes=1024,
        size_after_bytes=1024,
        audio_tracks_before=2,
        audio_tracks_after=2,
        subtitle_tracks_before=1,
        subtitle_tracks_after=1,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="English 2.0 AAC",
            audio_after="English 2.0 AAC",
            subs_before="English",
            subs_after="English",
        ),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    assert row["refiner_outcome_label"] == "No changes required"
    assert row["refiner_show_comparison"] is False
    assert row["refiner_primary_line"] == "No changes required"
    assert "Remux not required" in " ".join(row["refiner_summary_bullets"])


def test_refiner_skipped_dry_run_with_projected_subtitle_change() -> None:
    r = RefinerActivity(
        file_name="dry-preview.mkv",
        status="skipped",
        size_before_bytes=1024,
        size_after_bytes=1024,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=3,
        subtitle_tracks_after=3,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="English AAC",
            audio_after="English AAC",
            subs_before="English · French · German",
            subs_after="English",
            dry_run=True,
        ),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    assert row["refiner_outcome_label"] == "Dry run"
    assert row["refiner_apply_mode"] == "preview"
    assert row["refiner_show_comparison"] is True
    assert "preview only" in (row["refiner_outcome_sub"] or "").lower()
    assert row["refiner_outcome_label"] != "No changes required"
    sub = next(x for x in row["refiner_compare_rows"] if x["label"] == "Subtitles")
    assert sub["before"] != sub["after"]


def test_refiner_display_filename_derived_beats_orm_when_no_probe_in_context() -> None:
    r = RefinerActivity(
        file_name="Ugly.Release.Name.1990.mkv",
        media_title="The Grifters (1990)",
        status="skipped",
        size_before_bytes=100,
        size_after_bytes=100,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    assert row["refiner_media_title"] == "Ugly Release Name 1990"
    assert row["refiner_source_file_line"] is None


def test_refiner_failed_includes_reason_block() -> None:
    r = RefinerActivity(
        file_name="bad.mkv",
        status="failed",
        size_before_bytes=1000,
        size_after_bytes=1000,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="English 2.0 AAC",
            audio_after="English 2.0 AAC",
            subs_before="—",
            subs_after="None",
            failure_reason="Output file already exists — remove or rename it in the output folder, then retry.",
        ),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    assert row["refiner_outcome_label"] == "Failed"
    assert any("Output file already exists" in b for b in row["refiner_summary_bullets"])
    assert row["refiner_show_comparison"] is True
    fs = next(x for x in row["refiner_compare_rows"] if x["label"] == "File size")
    assert fs["after"] == "—"
