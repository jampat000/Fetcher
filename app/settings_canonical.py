"""Canonical reads for interval fields (Phase 3).

Legacy ORM columns (``sonarr_interval_minutes``, ``failed_import_cleanup_interval_minutes``,
etc.) remain on ``AppSettings`` for one release as deprecated compatibility; active logic
uses the canonical getters below, which fall back to legacy values when canonical is missing
or invalid (e.g. older backup JSON).
"""

from __future__ import annotations

from typing import Any

from app.arr_intervals import effective_arr_interval_minutes
from app.refiner_watch_config import clamp_refiner_interval_seconds


def sonarr_search_interval_minutes_read(settings: Any) -> int:
    v = getattr(settings, "sonarr_search_interval_minutes", None)
    if v is not None:
        try:
            if int(v) >= 1:
                return effective_arr_interval_minutes(v)
        except (TypeError, ValueError):
            pass
    return effective_arr_interval_minutes(getattr(settings, "sonarr_interval_minutes", None))


def radarr_search_interval_minutes_read(settings: Any) -> int:
    v = getattr(settings, "radarr_search_interval_minutes", None)
    if v is not None:
        try:
            if int(v) >= 1:
                return effective_arr_interval_minutes(v)
        except (TypeError, ValueError):
            pass
    return effective_arr_interval_minutes(getattr(settings, "radarr_interval_minutes", None))


def trimmer_interval_minutes_read(settings: Any) -> int:
    v = getattr(settings, "trimmer_interval_minutes", None)
    if v is not None:
        try:
            iv = int(v)
            if iv >= 5:
                return max(5, min(7 * 24 * 60, iv))
        except (TypeError, ValueError):
            pass
    return max(5, int(getattr(settings, "emby_interval_minutes", 60) or 60))


def movie_refiner_interval_seconds_read(settings: Any) -> int:
    v = getattr(settings, "movie_refiner_interval_seconds", None)
    if v is not None:
        try:
            return clamp_refiner_interval_seconds(int(v))
        except (TypeError, ValueError):
            pass
    return clamp_refiner_interval_seconds(getattr(settings, "refiner_interval_seconds", None))


def tv_refiner_interval_seconds_read(settings: Any) -> int:
    v = getattr(settings, "tv_refiner_interval_seconds", None)
    if v is not None:
        try:
            return clamp_refiner_interval_seconds(int(v))
        except (TypeError, ValueError):
            pass
    return clamp_refiner_interval_seconds(
        getattr(settings, "sonarr_refiner_interval_seconds", None)
    )


def _clamped_failed_import_cleanup_minutes(raw: object) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        v = 60
    return max(1, min(10080, v))


def sonarr_failed_import_cleanup_interval_minutes_read(settings: Any) -> int:
    v = getattr(settings, "sonarr_failed_import_cleanup_interval_minutes", None)
    if v is not None:
        try:
            iv = int(v)
            if iv >= 1:
                return max(1, min(10080, iv))
        except (TypeError, ValueError):
            pass
    return _clamped_failed_import_cleanup_minutes(
        getattr(settings, "failed_import_cleanup_interval_minutes", 60)
    )


def radarr_failed_import_cleanup_interval_minutes_read(settings: Any) -> int:
    v = getattr(settings, "radarr_failed_import_cleanup_interval_minutes", None)
    if v is not None:
        try:
            iv = int(v)
            if iv >= 1:
                return max(1, min(10080, iv))
        except (TypeError, ValueError):
            pass
    return _clamped_failed_import_cleanup_minutes(
        getattr(settings, "failed_import_cleanup_interval_minutes", 60)
    )
