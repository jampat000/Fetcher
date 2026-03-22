"""Schedule CSV normalization and time dropdown helpers."""

from datetime import datetime
from zoneinfo import ZoneInfo

from app.schedule import in_window, normalize_schedule_days_csv, schedule_time_dropdown_choices


def test_normalize_schedule_days_long_names() -> None:
    assert normalize_schedule_days_csv("Monday,Tuesday,Wednesday") == "Mon,Tue,Wed"


def test_normalize_schedule_days_reorder() -> None:
    assert normalize_schedule_days_csv("Sun,Mon") == "Mon,Sun"


def test_normalize_schedule_days_empty_means_no_days() -> None:
    assert normalize_schedule_days_csv("") == ""
    assert normalize_schedule_days_csv("   ") == ""


def test_normalize_schedule_days_commas_only_still_full_week_legacy() -> None:
    """Comma-only input: no valid tokens → legacy full-week fallback."""
    assert normalize_schedule_days_csv(",,,") == "Mon,Tue,Wed,Thu,Fri,Sat,Sun"


def test_in_window_schedule_enabled_no_days_never_matches() -> None:
    now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=ZoneInfo("UTC"))  # Fri
    assert not in_window(
        schedule_enabled=True,
        schedule_days="",
        schedule_start="00:00",
        schedule_end="23:59",
        timezone="UTC",
        now=now,
    )


def test_schedule_time_dropdown_has_half_hours_and_eod() -> None:
    ch = schedule_time_dropdown_choices(step_minutes=30)
    keys = [v for v, _ in ch]
    assert "00:00" in keys
    assert "12:30" in keys
    assert "23:30" in keys
    assert "23:59" in keys
