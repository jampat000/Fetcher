"""ActivityLog Refiner batch rows (canonical app/kind only)."""

from __future__ import annotations

from datetime import datetime

from app.models import ActivityLog
from app.web_common import (
    _activity_log_outcome_class,
    _activity_primary_label,
    _humanize_refiner_batch_log_detail,
    activity_display_row,
)


def test_refiner_batch_primary_label_completed() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=None,
        created_at=ts,
        app="refiner",
        kind="refiner",
        status="ok",
        count=3,
        detail="Refiner (scheduled): processed=3 unchanged=0 dry_run_items=0 errors=0",
    )
    assert _activity_primary_label(e) == "Refiner completed"


def test_refiner_batch_primary_label_failed() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=None,
        created_at=ts,
        app="refiner",
        kind="refiner",
        status="failed",
        count=0,
        detail="Refiner (scheduled): processed=0 unchanged=0 dry_run_items=0 errors=1",
    )
    assert _activity_primary_label(e) == "Refiner failed"


def test_activity_display_row_refiner_batch() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=None,
        created_at=ts,
        app="refiner",
        kind="refiner",
        status="ok",
        count=2,
        detail="Refiner (manual): processed=2 unchanged=0 dry_run_items=0 errors=0",
    )
    row = activity_display_row(e, "UTC")
    assert row["app"] == "refiner"
    assert row["kind"] == "refiner"
    assert row["primary_label"] == "Refiner completed"


def test_humanize_refiner_batch_detail_readable() -> None:
    detail = "Refiner (scheduled): processed=3 unchanged=0 dry_run_items=0 errors=0"
    lines = _humanize_refiner_batch_log_detail(detail)
    assert lines is not None
    assert len(lines) == 1
    assert "3 refined" in lines[0]
    assert "scheduled" in lines[0]


def test_refiner_batch_primary_label_waiting_only_aggregate() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=None,
        created_at=ts,
        app="refiner",
        kind="refiner",
        status="ok",
        count=0,
        detail="Refiner (scheduled): processed=0 unchanged=0 dry_run_items=0 waiting=2 errors=0",
    )
    assert _activity_primary_label(e) == "Refiner waiting"
    assert _activity_log_outcome_class(e) == "waiting"


def test_refiner_batch_primary_label_mixed_refined_and_waiting_not_failed() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=None,
        created_at=ts,
        app="refiner",
        kind="refiner",
        status="ok",
        count=1,
        detail="Refiner (scheduled): processed=1 unchanged=0 dry_run_items=0 waiting=1 errors=0",
    )
    assert _activity_primary_label(e) == "Refiner completed"
    assert _activity_log_outcome_class(e) == "success"


def test_humanize_batch_includes_waiting_counts() -> None:
    detail = "Refiner (manual): processed=0 unchanged=0 dry_run_items=0 waiting=3 errors=0"
    lines = _humanize_refiner_batch_log_detail(detail)
    assert lines is not None
    assert "3 waiting on upstream" in lines[0]
