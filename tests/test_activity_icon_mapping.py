"""Activity feed domain icons (Lucide names) and outcome class mapping."""

from __future__ import annotations

from datetime import datetime

from app.models import ActivityLog
from app.web_common import (
    _activity_log_domain_and_icon,
    _activity_log_outcome_class,
    activity_display_row,
    refiner_activity_display_row,
)


def test_domain_icon_sonarr_radarr_trimmer_service() -> None:
    assert _activity_log_domain_and_icon(
        ActivityLog(
            id=1,
            job_run_id=1,
            created_at=datetime(2026, 1, 1, 0, 0, 0),
            app="sonarr",
            kind="missing",
            count=1,
            detail="",
        )
    ) == ("sonarr", "tv")
    assert _activity_log_domain_and_icon(
        ActivityLog(
            id=2,
            job_run_id=1,
            created_at=datetime(2026, 1, 1, 0, 0, 0),
            app="radarr",
            kind="missing",
            count=1,
            detail="",
        )
    ) == ("radarr", "clapperboard")
    assert _activity_log_domain_and_icon(
        ActivityLog(
            id=3,
            job_run_id=1,
            created_at=datetime(2026, 1, 1, 0, 0, 0),
            app="trimmer",
            kind="trimmed",
            count=2,
            detail="",
        )
    ) == ("trimmer", "scissors")
    assert _activity_log_domain_and_icon(
        ActivityLog(
            id=4,
            job_run_id=1,
            created_at=datetime(2026, 1, 1, 0, 0, 0),
            app="service",
            kind="error",
            count=0,
            detail="",
        )
    ) == ("service", "server")


def test_outcome_class_search_and_trimmer() -> None:
    assert (
        _activity_log_outcome_class(
            ActivityLog(
                id=1,
                job_run_id=1,
                created_at=datetime(2026, 1, 1, 0, 0, 0),
                app="sonarr",
                kind="missing",
                count=0,
                detail="",
            )
        )
        == "skipped"
    )
    assert (
        _activity_log_outcome_class(
            ActivityLog(
                id=2,
                job_run_id=1,
                created_at=datetime(2026, 1, 1, 0, 0, 0),
                app="sonarr",
                kind="missing",
                count=3,
                detail="",
            )
        )
        == "success"
    )
    assert (
        _activity_log_outcome_class(
            ActivityLog(
                id=3,
                job_run_id=1,
                created_at=datetime(2026, 1, 1, 0, 0, 0),
                app="trimmer",
                kind="trimmed",
                count=0,
                detail="",
            )
        )
        == "skipped"
    )
    assert (
        _activity_log_outcome_class(
            ActivityLog(
                id=4,
                job_run_id=1,
                created_at=datetime(2026, 1, 1, 0, 0, 0),
                app="trimmer",
                kind="trimmed",
                count=1,
                detail="",
            )
        )
        == "success"
    )


def test_activity_display_row_trimmer_uses_trim_app_token() -> None:
    e = ActivityLog(
        id=9,
        job_run_id=1,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        app="trimmer",
        kind="trimmed",
        count=1,
        detail="",
    )
    row = activity_display_row(e, "UTC")
    assert row["app"] == "trimmer"
    assert row["activity_domain"] == "trimmer"


def test_activity_display_row_includes_visual_tokens() -> None:
    e = ActivityLog(
        id=1,
        job_run_id=1,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        app="sonarr",
        kind="missing",
        count=2,
        detail="A\nB",
    )
    row = activity_display_row(e, "UTC")
    assert row["activity_domain"] == "sonarr"
    assert row["activity_lucide"] == "tv"
    assert row["activity_outcome"] == "success"


def test_refiner_file_row_visual_tokens() -> None:
    from app.models import RefinerActivity

    r = RefinerActivity(
        id=1,
        file_name="x.mkv",
        status="success",
        size_before_bytes=100,
        size_after_bytes=100,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 13, 0, 0))
    assert row["activity_domain"] == "refiner"
    assert row["activity_lucide"] == "sliders-horizontal"
    assert row["activity_outcome"] == "success"
