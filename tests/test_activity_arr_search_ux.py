"""Activity display UX for Sonarr/Radarr missing and upgrade search rows."""

from __future__ import annotations

from datetime import datetime

import pytest

from app.models import ActivityLog
from app.web_common import _humanize_legacy_arr_search_detail_lines, activity_display_row


def test_humanize_legacy_missing_no_eligible_returns_none_when_count_positive() -> None:
    assert (
        _humanize_legacy_arr_search_detail_lines(
            "sonarr",
            "missing",
            "0 searches — no eligible missing items (candidates=0, ...)",
            1,
        )
        is None
    )


def test_humanize_legacy_missing_retry_delay_sonarr() -> None:
    lines = _humanize_legacy_arr_search_detail_lines(
        "sonarr",
        "missing",
        "0 searches — all items within retry delay (candidates=4, retry_delay_filtered=4, ...)",
        0,
    )
    assert lines is not None
    assert "No missing search was started" in lines[0]
    assert "4" in lines[0]
    assert "episodes" in lines[0]
    assert "Technical · TV" in lines[1]


def test_humanize_legacy_missing_retry_delay_radarr() -> None:
    lines = _humanize_legacy_arr_search_detail_lines(
        "radarr",
        "missing",
        "0 searches — all items within retry delay (candidates=2, retry_delay_filtered=2, ...)",
        0,
    )
    assert lines is not None
    assert "movies" in lines[0]
    assert "Technical · Movies" in lines[1]


def test_activity_display_row_missing_primary_label_tv_and_movies() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    son = ActivityLog(
        id=1,
        job_run_id=1,
        created_at=ts,
        app="sonarr",
        kind="missing",
        count=3,
        detail="A\nB",
    )
    rad = ActivityLog(
        id=2,
        job_run_id=1,
        created_at=ts,
        app="radarr",
        kind="missing",
        count=1,
        detail="M",
    )
    assert activity_display_row(son, "UTC")["primary_label"] == "TV · Missing search · 3 episodes"
    assert activity_display_row(rad, "UTC")["primary_label"] == "Movies · Missing search · 1 movie"


def test_activity_display_row_upgrade_zero_uses_product_label() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=1,
        created_at=ts,
        app="sonarr",
        kind="upgrade",
        count=0,
        detail="Manual upgrade search: suppressed by retry delay (no search triggered).",
    )
    row = activity_display_row(e, "UTC")
    assert row["primary_label"] == "TV · Upgrade search · No search started"
    assert "No upgrade search was started" in row["detail_lines"][0]
    assert "retry wait period" in row["detail_lines"][0].lower()


def test_activity_display_row_missing_legacy_humanized_in_ui() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=1,
        created_at=ts,
        app="radarr",
        kind="missing",
        count=0,
        detail=(
            "0 searches — no eligible missing items "
            "(candidates=0, retry_delay_filtered=0, cutoff_already_met_filtered=0, "
            "quality_profile_filtered=0, other_constraints_filtered=0)."
        ),
    )
    row = activity_display_row(e, "UTC")
    assert row["primary_label"] == "Movies · Missing search · No search started"
    assert "No missing search was started" in row["detail_lines"][0]
    assert "eligible missing" in row["detail_lines"][0]


@pytest.mark.parametrize(
    ("app", "kind", "detail", "needle"),
    (
        (
            "sonarr",
            "upgrade",
            "Manual upgrade search: no cutoff-unmet episodes found.",
            "episodes",
        ),
        (
            "radarr",
            "upgrade",
            "Manual upgrade search: no cutoff-unmet movies found.",
            "movies",
        ),
    ),
)
def test_activity_display_row_upgrade_no_candidates_parity(
    app: str, kind: str, detail: str, needle: str
) -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=1,
        created_at=ts,
        app=app,
        kind=kind,
        count=0,
        detail=detail,
    )
    row = activity_display_row(e, "UTC")
    assert needle in row["detail_lines"][0]
    assert "cutoff upgrade" in row["detail_lines"][0].lower()
