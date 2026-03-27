"""Refiner per-file activity merged into the shared Activity feed."""

from __future__ import annotations

from datetime import datetime, timedelta

from app.models import ActivityLog, RefinerActivity
from app.web_common import activity_display_row, merge_activity_feed, refiner_activity_display_row


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


def test_refiner_activity_display_row_type_and_detail() -> None:
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
    )
    now = datetime(2026, 1, 1, 12, 3, 0)
    row = refiner_activity_display_row(r, "UTC", now)
    assert row["activity_type"] == "refiner"
    assert row["type"] == "refiner"
    assert row["app"] == "refiner"
    assert "Movie.Name.2023.mkv" == row["primary_label"]
    assert "GB" in row["detail_lines"][0]
    assert "No size change" not in row["detail_lines"][0]
    assert "Audio: 4 → 1" in row["detail_lines"][1]
    assert "Subtitles: 7 → 2" in row["detail_lines"][1]
    assert row["detail_lines"][3] == "Before"
    assert row["detail_lines"][-1].startswith("Removed:")


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
    )
    merged = merge_activity_feed([log], [ref], "UTC", t_new, limit=10)
    assert merged[0]["type"] == "refiner"
    assert merged[1]["type"] == "log"


def test_refiner_collapsed_line_no_size_change() -> None:
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
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    assert "No size change" in row["detail_lines"][0]
