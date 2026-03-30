"""Serialize concurrent mutation of the same *arr queue item (downloadId) across paths.

Refiner promotion (final output move) and failed-import queue deletion both touch the same logical
import; they must not run concurrently for the same ``(app, download_id)``.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

_locks_guard = threading.Lock()
_locks: dict[str, threading.Lock] = {}


def import_item_lock_key(app: str, download_id: str) -> str:
    a = (app or "").strip().lower()
    d = (download_id or "").strip()
    if a not in ("sonarr", "radarr"):
        raise ValueError("import_item_lock app must be sonarr or radarr")
    if not d:
        raise ValueError("import_item_lock requires non-empty download_id")
    return f"{a}:{d}"


def get_import_item_lock(key: str) -> threading.Lock:
    with _locks_guard:
        lk = _locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _locks[key] = lk
        return lk


@asynccontextmanager
async def hold_import_item_lock(app: str, download_id: str) -> AsyncIterator[None]:
    """Awaitable lock wrapper for async failed-import cleanup (same key as Refiner promotion)."""
    key = import_item_lock_key(app, download_id)
    lk = get_import_item_lock(key)
    await asyncio.to_thread(lk.acquire)
    try:
        yield
    finally:
        lk.release()
