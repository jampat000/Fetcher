"""
Pre-promotion gate: *arr queue says this download is already a terminal failed import
for the file under Refiner — do not copy/move into the trusted output folder.

Matching uses each queue row's ``downloadId`` (strong identity) and ``outputPath`` (path prefix);
no filename-only heuristics.

**Upgrade vs non-upgrade:** Legitimate upgrades still in progress (or without a terminal cleanup
signal on the matched queue row) do not produce a terminal label and are not blocked. Terminal
queue/history phrases such as "not an upgrade for existing …" are explicit non-upgrade failures;
other terminal signals (corrupt, hash mismatch, etc.) are also blocked — *arr has already rejected
the import, so output must stay clean.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from app.arr_client import ArrClient
from app.arr_failed_import_classify import (
    radarr_queue_terminal_cleanup_label,
    sonarr_queue_terminal_cleanup_label,
    terminal_cleanup_label_is_explicit_non_upgrade,
)
from app.import_item_lock import get_import_item_lock, import_item_lock_key
from app.radarr_failed_import_cleanup import (
    _flatten_radarr_queue_user_messages,
    _paginate_records,
    radarr_queue_item_is_pending_waiting_no_eligible,
)
from app.sonarr_failed_import_cleanup import user_visible_text_is_pending_waiting_no_eligible

logger = logging.getLogger(__name__)

AppName = Literal["sonarr", "radarr"]


def _flatten_sonarr_queue_user_messages(q: dict[str, Any]) -> str:
    parts: list[str] = []
    em = q.get("errorMessage")
    if isinstance(em, str) and em.strip():
        parts.append(em.strip())
    sm = q.get("statusMessages")
    if isinstance(sm, list):
        for item in sm:
            if isinstance(item, str) and item.strip():
                parts.append(item.strip())
            elif isinstance(item, dict):
                msgs = item.get("messages")
                if isinstance(msgs, list):
                    for m in msgs:
                        if isinstance(m, str) and m.strip():
                            parts.append(m.strip())
    return " ".join(parts)


def _queue_output_path_candidates(q: dict[str, Any]) -> list[Path]:
    raw = q.get("outputPath")
    if isinstance(raw, str) and raw.strip():
        try:
            return [Path(raw.strip()).expanduser().resolve()]
        except OSError:
            return []
    return []


def queue_row_contains_resolved_media_file(file_resolved: Path, q: dict[str, Any]) -> bool:
    """True when ``file_resolved`` equals or lives under this row's ``outputPath``."""
    try:
        f = file_resolved.resolve()
    except OSError:
        return False
    for base in _queue_output_path_candidates(q):
        try:
            if f == base:
                return True
            f.relative_to(base)
            return True
        except ValueError:
            continue
        except OSError:
            continue
    return False


def _terminal_signal_for_row(app: AppName, q: dict[str, Any]) -> str | None:
    if app == "radarr":
        blob = _flatten_radarr_queue_user_messages(q)
        if radarr_queue_item_is_pending_waiting_no_eligible(q):
            return None
        return radarr_queue_terminal_cleanup_label(blob)
    blob = _flatten_sonarr_queue_user_messages(q)
    if user_visible_text_is_pending_waiting_no_eligible(blob):
        return None
    return sonarr_queue_terminal_cleanup_label(blob)


async def _fetch_queue_records(client: ArrClient) -> list[dict[str, Any]]:
    return await _paginate_records(
        client.queue_page,
        page_size=200,
        label="refiner promotion gate (queue)",
        max_pages=200,
    )


@dataclass(frozen=True)
class PromotionGateSyncResult:
    """``allowed`` False means do not promote; ``held_locks`` always empty when not allowed."""

    allowed: bool
    held_locks: tuple[threading.Lock, ...]
    block_detail: dict[str, Any] | None


async def refiner_promotion_precheck(
    *,
    media_file: Path,
    sonarr_client: ArrClient | None,
    radarr_client: ArrClient | None,
) -> PromotionGateSyncResult:
    """Fresh queue fetch + classify. Acquires per-``downloadId`` locks (sorted) only when allowed."""

    try:
        media_resolved = media_file.resolve()
    except OSError as e:
        logger.info("Refiner promotion gate: could not resolve %s (%s) — allow", media_file, e)
        return PromotionGateSyncResult(True, (), None)

    blocking: dict[str, Any] | None = None
    lock_keys: list[tuple[AppName, str]] = []

    async def _scan(app: AppName, client: ArrClient | None) -> None:
        nonlocal blocking
        if client is None or blocking is not None:
            return
        try:
            records = await _fetch_queue_records(client)
        except Exception:
            logger.warning("Refiner promotion gate: %s queue fetch failed — allow (fail-open)", app, exc_info=True)
            return
        for q in records:
            if not isinstance(q, dict):
                continue
            if not queue_row_contains_resolved_media_file(media_resolved, q):
                continue
            sig = _terminal_signal_for_row(app, q)
            raw_did = q.get("downloadId")
            did = str(raw_did).strip() if raw_did is not None else ""
            if sig:
                blocking = {
                    "arr_app": app,
                    "import_state": sig,
                    "download_id": did,
                    "non_upgrade": terminal_cleanup_label_is_explicit_non_upgrade(sig),
                    "blocked_before_output_promotion": True,
                }
                logger.info(
                    "Refiner promotion gate: block promotion for %s — %s queue terminal signal=%s downloadId=%s",
                    media_file.name,
                    app,
                    sig,
                    did or "(missing)",
                )
                return
            if did:
                lock_keys.append((app, did))

    await _scan("radarr", radarr_client)
    await _scan("sonarr", sonarr_client)

    if blocking is not None:
        return PromotionGateSyncResult(False, (), blocking)

    unique_sorted = sorted({(a, d) for a, d in lock_keys if d})
    held: list[threading.Lock] = []
    try:
        for app, did in unique_sorted:
            key = import_item_lock_key(app, did)
            lk = get_import_item_lock(key)
            await asyncio.to_thread(lk.acquire)
            held.append(lk)
        return PromotionGateSyncResult(True, tuple(held), None)
    except Exception:
        for lk in reversed(held):
            try:
                lk.release()
            except RuntimeError:
                pass
        raise
