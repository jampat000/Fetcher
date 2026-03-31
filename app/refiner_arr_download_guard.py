"""Sonarr/Radarr download-queue guard: skip Refiner while *arr still reports an active download for this path."""

from __future__ import annotations

import logging
from pathlib import Path

from app.arr_client import ArrClient
from app.refiner_promotion_gate import _fetch_queue_records, queue_row_contains_resolved_media_file

logger = logging.getLogger(__name__)


def queue_row_active_download_in_progress(q: dict) -> bool:
    """True when this queue row still represents an in-flight grab (not merely import pending)."""
    sl = q.get("sizeleft")
    try:
        if sl is not None and int(float(sl)) > 0:
            return True
    except (TypeError, ValueError):
        pass
    st = str(q.get("status") or "").strip().lower()
    if st in ("downloading", "queued", "delay"):
        return True
    tds = str(q.get("trackedDownloadState") or "").strip().lower()
    if tds in ("downloading", "queued"):
        return True
    return False


async def refiner_path_blocked_by_arr_active_download(
    media_file: Path,
    *,
    sonarr_client: ArrClient | None,
    radarr_client: ArrClient | None,
) -> tuple[bool, str]:
    """If either *arr queue contains an active download targeting this file, return (True, reason_code)."""
    try:
        resolved = media_file.resolve()
    except OSError:
        return False, ""

    for app, client in (("radarr", radarr_client), ("sonarr", sonarr_client)):
        if client is None:
            continue
        try:
            records = await _fetch_queue_records(client)
        except Exception:
            logger.warning(
                "Refiner: %s queue fetch failed while checking active download guard — not blocking",
                app,
                exc_info=True,
            )
            continue
        for q in records:
            if not isinstance(q, dict):
                continue
            if not queue_row_contains_resolved_media_file(resolved, q):
                continue
            if queue_row_active_download_in_progress(q):
                return True, f"{app}_queue_active_download"
    return False, ""
