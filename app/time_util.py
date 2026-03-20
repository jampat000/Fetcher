"""Timezone helpers — avoid deprecated datetime.utcnow()."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now_naive() -> datetime:
    """UTC instant as naive datetime for SQLite DateTime columns without timezone=True."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
