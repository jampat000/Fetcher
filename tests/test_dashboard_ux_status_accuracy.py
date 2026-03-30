"""Dashboard presentation: global phase, per-app hints, log/activity non-empty messaging."""

from __future__ import annotations

from datetime import datetime

from app.dashboard_service import (
    _fetcher_phase_for_dashboard,
    automation_card_subtext,
)
from app.models import ActivityLog
from app.web_common import activity_display_row, user_visible_job_run_message


def test_global_phase_never_cooling_and_uses_active_when_scheduled() -> None:
    pid, label, detail = _fetcher_phase_for_dashboard(run_busy=False, job_intervals={"sonarr": 60})
    assert pid == "active"
    assert label == "Active"
    assert "cool" not in label.lower()
    assert "cooling" not in detail.lower()

    pid_i, label_i, _ = _fetcher_phase_for_dashboard(run_busy=False, job_intervals={})
    assert pid_i == "idle"

    pid_p, label_p, _ = _fetcher_phase_for_dashboard(run_busy=True, job_intervals={"sonarr": 60})
    assert pid_p == "processing"


def test_per_app_automation_subtext_independent() -> None:
    msg = (
        "Sonarr: Missing search — no searches started; "
        "3 in scope, all still waiting for their retry delay | "
        "Radarr: missing search for 2 movie(s)"
    )
    son = automation_card_subtext(app_key="sonarr", enabled=True, last_job_message=msg)
    rad = automation_card_subtext(app_key="radarr", enabled=True, last_job_message=msg)
    assert "retry-delay" in son.lower() or "retry delay" in son.lower()
    assert rad == ""
    assert "sonarr" not in rad.lower()


def test_per_app_partial_retry_mixed_with_dispatched_search() -> None:
    msg = (
        "Sonarr: missing search for 1 episode(s) | "
        "Sonarr: Upgrade search — no searches started; "
        "9 in scope, all still waiting for their retry delay (up to 10 per run)"
    )
    son = automation_card_subtext(app_key="sonarr", enabled=True, last_job_message=msg)
    assert "some items were skipped" in son.lower()


def test_automation_subtext_skips_fallback_when_app_has_run_evidence() -> None:
    """No per-app log line but card shows a last-run time → omit skipped-by-config fallback."""
    sub = automation_card_subtext(
        app_key="sonarr",
        enabled=True,
        last_job_message="Radarr: only radarr in summary",
        app_has_run_evidence=True,
    )
    assert sub == ""


def test_automation_subtext_fallback_when_no_run_evidence_and_no_line() -> None:
    sub = automation_card_subtext(
        app_key="sonarr",
        enabled=True,
        last_job_message="Radarr: missing search for 1",
        app_has_run_evidence=False,
    )
    assert "No line for this app" in sub


def test_automation_subtext_retry_delay_still_shown_with_run_evidence() -> None:
    msg = (
        "Sonarr: Missing search — no searches started; "
        "2 in scope, all still waiting for their retry delay"
    )
    sub = automation_card_subtext(
        app_key="sonarr", enabled=True, last_job_message=msg, app_has_run_evidence=True
    )
    assert "retry-delay" in sub.lower() or "retry delay" in sub.lower()


def test_user_visible_job_run_message_non_empty() -> None:
    assert user_visible_job_run_message(message="", ok=True, finished_at=datetime(2026, 1, 1, 12, 0, 0))
    assert user_visible_job_run_message(message="", ok=False, finished_at=datetime(2026, 1, 1, 12, 0, 0))
    assert user_visible_job_run_message(message=" ", ok=True, finished_at=None)
    assert "hello" in user_visible_job_run_message(message="hello", ok=True, finished_at=None).lower()


def test_activity_display_row_always_has_labels_and_detail() -> None:
    e = ActivityLog(
        app="radarr",
        kind="missing",
        count=0,
        detail="",
        status="ok",
        created_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    row = activity_display_row(e, "UTC")
    assert row.get("activity_type") == "log"
    assert row["primary_label"].strip()
    assert row["detail_lines"] and all(str(x).strip() for x in row["detail_lines"])

    e2 = ActivityLog(
        app="sonarr",
        kind="error",
        status="failed",
        count=0,
        detail="  ",
        created_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    row2 = activity_display_row(e2, "UTC")
    assert "error" in row2["primary_label"].lower() or "failed" in row2["primary_label"].lower()
    assert row2["detail_lines"] and all(str(x).strip() for x in row2["detail_lines"])
