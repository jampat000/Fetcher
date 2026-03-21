"""Sonarr/Radarr run-interval fallbacks (no separate global scheduler base in Settings UI)."""

from __future__ import annotations

# When per-app interval is unset/invalid in DB, use this (minutes). Kept in sync with model defaults.
ARR_INTERVAL_FALLBACK_MINUTES = 60


def effective_arr_interval_minutes(specific: object) -> int:
    """Per-app run interval; invalid or <1 uses ``ARR_INTERVAL_FALLBACK_MINUTES`` (min 5)."""
    try:
        v = int(specific) if specific is not None else 0
    except (TypeError, ValueError):
        v = 0
    fb = max(5, int(ARR_INTERVAL_FALLBACK_MINUTES))
    return max(5, v) if v > 0 else fb
