"""Refiner watched-folder poll interval — stored and scheduled in seconds."""

from __future__ import annotations

REFINER_WATCH_INTERVAL_SEC_MIN = 5
REFINER_WATCH_INTERVAL_SEC_MAX = 7 * 24 * 3600  # 7 days
REFINER_WATCH_INTERVAL_SEC_DEFAULT = 60


def clamp_refiner_interval_seconds(raw: object) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        v = REFINER_WATCH_INTERVAL_SEC_DEFAULT
    return max(
        REFINER_WATCH_INTERVAL_SEC_MIN,
        min(REFINER_WATCH_INTERVAL_SEC_MAX, v),
    )
