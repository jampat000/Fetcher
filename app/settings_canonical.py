"""Canonical reads for interval fields."""

from __future__ import annotations

from typing import Any

from app.arr_intervals import effective_arr_interval_minutes
from app.refiner_watch_config import clamp_refiner_interval_seconds


def sonarr_search_interval_minutes_read(settings: Any) -> int:
    return effective_arr_interval_minutes(
        getattr(settings, "sonarr_search_interval_minutes", None)
    )


def radarr_search_interval_minutes_read(settings: Any) -> int:
    return effective_arr_interval_minutes(
        getattr(settings, "radarr_search_interval_minutes", None)
    )


def trimmer_interval_minutes_read(settings: Any) -> int:
    v = getattr(settings, "trimmer_interval_minutes", None)
    try:
        return max(5, min(7 * 24 * 60, int(v or 60)))
    except (TypeError, ValueError):
        return 60


def movie_refiner_interval_seconds_read(settings: Any) -> int:
    return clamp_refiner_interval_seconds(
        getattr(settings, "movie_refiner_interval_seconds", None)
    )


def tv_refiner_interval_seconds_read(settings: Any) -> int:
    return clamp_refiner_interval_seconds(
        getattr(settings, "tv_refiner_interval_seconds", None)
    )


def _clamped_failed_import_cleanup_minutes(raw: object) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        v = 60
    return max(1, min(10080, v))


def sonarr_failed_import_cleanup_interval_minutes_read(settings: Any) -> int:
    return _clamped_failed_import_cleanup_minutes(
        getattr(settings, "sonarr_failed_import_cleanup_interval_minutes", 60)
    )


def radarr_failed_import_cleanup_interval_minutes_read(settings: Any) -> int:
    return _clamped_failed_import_cleanup_minutes(
        getattr(settings, "radarr_failed_import_cleanup_interval_minutes", 60)
    )
