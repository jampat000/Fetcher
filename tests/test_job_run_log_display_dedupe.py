"""Job run log list: hide orphan placeholder rows when a terminal row exists nearby in time."""

from __future__ import annotations

from datetime import datetime, timedelta

from app.models import JobRunLog
from app.web_common import (
    dedupe_job_run_logs_for_display,
    job_run_log_row_is_suppressible_placeholder,
    job_run_log_row_is_terminal_for_dedupe,
    user_visible_job_run_message,
)


def _row(
    *,
    started_at: datetime,
    finished_at: datetime | None,
    message: str,
    ok: bool = True,
) -> JobRunLog:
    return JobRunLog(started_at=started_at, finished_at=finished_at, message=message, ok=ok)


def test_placeholder_only_row_still_shown() -> None:
    t = datetime(2026, 3, 1, 10, 0, 0)
    r = _row(started_at=t, finished_at=None, message="", ok=False)
    assert job_run_log_row_is_suppressible_placeholder(r)
    assert not job_run_log_row_is_terminal_for_dedupe(r)
    out = dedupe_job_run_logs_for_display([r])
    assert len(out) == 1
    assert user_visible_job_run_message(message=r.message, ok=r.ok, finished_at=r.finished_at).startswith(
        "Run summary"
    )


def test_terminal_row_only_unchanged() -> None:
    t = datetime(2026, 3, 1, 10, 0, 1)
    r = _row(started_at=t, finished_at=t, message="Radarr: missing search for 3 movie(s)", ok=True)
    assert job_run_log_row_is_terminal_for_dedupe(r)
    assert not job_run_log_row_is_suppressible_placeholder(r)
    out = dedupe_job_run_logs_for_display([r])
    assert len(out) == 1
    assert out[0].message == r.message


def test_placeholder_suppressed_when_terminal_same_start_second() -> None:
    t = datetime(2026, 3, 1, 10, 0, 0)
    orphan = _row(started_at=t, finished_at=None, message="", ok=False)
    final = _row(
        started_at=t.replace(microsecond=500000),
        finished_at=t.replace(microsecond=500000),
        message=(
            "Sonarr: Missing search — no searches started; "
            "2 in scope, all still waiting for their retry delay"
        ),
        ok=True,
    )
    rows = [final, orphan]
    out = dedupe_job_run_logs_for_display(rows)
    assert len(out) == 1
    assert out[0] is final
    assert "retry delay" in (out[0].message or "").lower()


def test_placeholder_suppressed_when_terminal_within_tolerance_seconds() -> None:
    t0 = datetime(2026, 3, 1, 15, 48, 0)
    orphan = _row(started_at=t0, finished_at=None, message="", ok=False)
    final = _row(
        started_at=t0 + timedelta(seconds=4),
        finished_at=t0 + timedelta(seconds=4),
        message="Radarr: missing search for 1 movie(s)",
        ok=True,
    )
    out = dedupe_job_run_logs_for_display([final, orphan])
    assert len(out) == 1
    assert out[0].message == final.message


def test_placeholder_suppressed_adjacent_order_slight_timestamp_mismatch() -> None:
    """Same visible clock time can still differ in DB by 1–2 seconds."""
    orphan = _row(
        started_at=datetime(2026, 3, 1, 9, 48, 0, 100000),
        finished_at=None,
        message="",
        ok=False,
    )
    final = _row(
        started_at=datetime(2026, 3, 1, 9, 48, 2, 0),
        finished_at=datetime(2026, 3, 1, 9, 48, 2, 0),
        message=(
            "Radarr: Missing search — no searches started; "
            "1 in scope, all still waiting for their retry delay"
        ),
        ok=True,
    )
    out = dedupe_job_run_logs_for_display([final, orphan])
    assert len(out) == 1
    assert "retry delay" in (out[0].message or "").lower()


def test_placeholder_not_suppressed_when_terminal_beyond_tolerance() -> None:
    t0 = datetime(2026, 3, 1, 16, 0, 0)
    orphan = _row(started_at=t0, finished_at=None, message="", ok=False)
    final = _row(
        started_at=t0 + timedelta(seconds=6),
        finished_at=t0 + timedelta(seconds=6),
        message="Later run summary",
        ok=True,
    )
    out = dedupe_job_run_logs_for_display([final, orphan])
    assert len(out) == 2


def test_unrelated_placeholder_far_enough_no_false_positive() -> None:
    t1 = datetime(2026, 3, 1, 12, 0, 0)
    t2 = datetime(2026, 3, 1, 12, 0, 10)
    orphan = _row(started_at=t1, finished_at=None, message="", ok=False)
    other = _row(started_at=t2, finished_at=t2, message="Emby: dry-run matched 0 item(s)", ok=True)
    out = dedupe_job_run_logs_for_display([other, orphan])
    assert len(out) == 2


def test_two_terminal_rows_same_second_both_retained() -> None:
    t = datetime(2026, 3, 1, 11, 0, 0)
    a = _row(started_at=t, finished_at=t, message="Radarr: a", ok=True)
    b = _row(started_at=t.replace(microsecond=100), finished_at=t, message="Radarr: b", ok=True)
    out = dedupe_job_run_logs_for_display([a, b])
    assert len(out) == 2


def test_multiple_terminals_one_placeholder_all_reals_kept() -> None:
    t = datetime(2026, 3, 1, 17, 0, 0)
    ph = _row(started_at=t + timedelta(seconds=1), finished_at=None, message="", ok=False)
    a = _row(started_at=t, finished_at=t, message="First terminal", ok=True)
    b = _row(started_at=t + timedelta(seconds=2), finished_at=t, message="Second terminal", ok=True)
    out = dedupe_job_run_logs_for_display([b, ph, a])
    assert len(out) == 2
    assert {x.message for x in out} == {"First terminal", "Second terminal"}


def test_order_preserved_after_dedupe() -> None:
    t = datetime(2026, 3, 1, 13, 0, 0)
    newer = _row(
        started_at=t,
        finished_at=t,
        message="newer terminal",
        ok=True,
    )
    older_orphan = _row(started_at=t, finished_at=None, message="", ok=False)
    out = dedupe_job_run_logs_for_display([newer, older_orphan])
    assert len(out) == 1
    assert out[0].message == "newer terminal"


def test_in_progress_with_message_not_suppressed_even_if_terminal_same_second() -> None:
    """Non-empty message without finish is terminal for dedupe → never treated as stale placeholder."""
    t = datetime(2026, 3, 1, 14, 0, 0)
    partial = _row(started_at=t, finished_at=None, message="Partial line only", ok=True)
    final = _row(started_at=t.replace(microsecond=1), finished_at=t, message="Final summary", ok=True)
    out = dedupe_job_run_logs_for_display([final, partial])
    assert len(out) == 2
