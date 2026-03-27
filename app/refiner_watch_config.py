"""Refiner watched-folder poll interval — stored and scheduled in seconds."""

from __future__ import annotations

STREAM_MANAGER_WATCH_INTERVAL_SEC_MIN = 5
STREAM_MANAGER_WATCH_INTERVAL_SEC_MAX = 7 * 24 * 3600  # 7 days
STREAM_MANAGER_WATCH_INTERVAL_SEC_DEFAULT = 60


def clamp_stream_manager_interval_seconds(raw: object) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        v = STREAM_MANAGER_WATCH_INTERVAL_SEC_DEFAULT
    return max(
        STREAM_MANAGER_WATCH_INTERVAL_SEC_MIN,
        min(STREAM_MANAGER_WATCH_INTERVAL_SEC_MAX, v),
    )
