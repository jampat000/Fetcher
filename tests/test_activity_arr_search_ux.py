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


def test_humanize_legacy_missing_retry_delay_sonarr_no_internal_counts() -> None:
    lines = _humanize_legacy_arr_search_detail_lines(
        "sonarr",
        "missing",
        "0 searches — all items within retry delay (candidates=4, retry_delay_filtered=4, ...)",
        0,
    )
    assert lines is not None
    assert lines[0] == "All eligible items are still waiting for retry delay to expire."
    assert lines[1] == "Fetcher will try again automatically."
    assert "candidates" not in "\n".join(lines).lower()
    assert "Technical" not in "\n".join(lines)


def test_humanize_legacy_missing_retry_delay_radarr() -> None:
    lines = _humanize_legacy_arr_search_detail_lines(
        "radarr",
        "missing",
        "0 searches — all items within retry delay (candidates=2, retry_delay_filtered=2, ...)",
        0,
    )
    assert lines is not None
    assert "Fetcher will try again automatically." in lines[1]


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
    assert activity_display_row(son, "UTC")["primary_label"] == "TV · Missing search · 3 episodes searched"
    assert activity_display_row(rad, "UTC")["primary_label"] == "Movies · Missing search · 1 movie searched"
    son_row = activity_display_row(son, "UTC")
    assert son_row["detail_lines"][0] == "Started a missing search for 3 episodes."


def test_activity_display_row_upgrade_zero_uses_product_copy() -> None:
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
    assert row["detail_lines"][0] == "All eligible items are still waiting for retry delay to expire."
    assert row["detail_lines"][1] == "Fetcher will try again automatically."


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
    assert "No movies are eligible for a missing search right now." in row["detail_lines"][0]
    assert "Technical" not in "\n".join(row["detail_lines"])


def test_activity_display_row_scrubs_midrelease_monitored_missing_wording() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=1,
        created_at=ts,
        app="sonarr",
        kind="missing",
        count=0,
        detail=(
            "No missing search was started. All 48 monitored missing episodes are still inside "
            "Fetcher’s retry wait period.\n"
            "Technical · TV · candidates=48; skipped due to retry delay."
        ),
    )
    row = activity_display_row(e, "UTC")
    assert "48" not in "\n".join(row["detail_lines"])
    assert "monitored missing" not in "\n".join(row["detail_lines"]).lower()
    assert "Technical" not in "\n".join(row["detail_lines"])
    assert row["detail_lines"][0] == "All eligible items are still waiting for retry delay to expire."


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
    assert "eligible for an upgrade search" in row["detail_lines"][0].lower()
