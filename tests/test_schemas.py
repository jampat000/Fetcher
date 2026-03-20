"""Pydantic settings validation."""

from __future__ import annotations

from app.schemas import SettingsIn


def test_settings_in_coerces_zero_arr_intervals_to_sixty() -> None:
    s = SettingsIn(sonarr_interval_minutes=0, radarr_interval_minutes=0)
    assert s.sonarr_interval_minutes == 60
    assert s.radarr_interval_minutes == 60


def test_settings_in_coerces_negative_arr_intervals() -> None:
    s = SettingsIn(sonarr_interval_minutes=-1, radarr_interval_minutes=-5)
    assert s.sonarr_interval_minutes == 60
    assert s.radarr_interval_minutes == 60


def test_settings_in_preserves_positive_arr_intervals() -> None:
    s = SettingsIn(sonarr_interval_minutes=120, radarr_interval_minutes=90)
    assert s.sonarr_interval_minutes == 120
    assert s.radarr_interval_minutes == 90
