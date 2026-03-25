"""
Sonarr-only opt-in: remove queue items that match explicit import-failed history by downloadId.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient
from app.models import ActivityLog
from app.radarr_failed_import_cleanup import (
    classify_queue_matches_by_download_id,
    history_item_title,
    is_radarr_import_failed_record,
    parse_radarr_import_failed_reason,
)


async def run_sonarr_failed_import_queue_cleanup(
    client: ArrClient,
    *,
    session: AsyncSession,
    job_run_id: int | None,
    actions: list[str],
) -> None:
    queue = await client.queue_page(page=1, page_size=200)
    queue_records = queue.get("records") if isinstance(queue, dict) else []
    queue_records = queue_records if isinstance(queue_records, list) else []
    history = await client.history_page(page=1, page_size=250)
    history_records = history.get("records") if isinstance(history, dict) else []
    history_records = history_records if isinstance(history_records, list) else []

    processed_download_ids: set[str] = set()
    for rec in history_records:
        if not isinstance(rec, dict):
            continue
        if not is_radarr_import_failed_record(rec):
            continue
        raw_did = rec.get("downloadId")
        if raw_did is None:
            continue
        download_id = str(raw_did).strip()
        if not download_id or download_id in processed_download_ids:
            continue
        processed_download_ids.add(download_id)

        kind, qid = classify_queue_matches_by_download_id(download_id, queue_records)
        if kind == "none":
            continue
        if kind == "many":
            actions.append(
                "Sonarr: skipped failed-import queue remove (ambiguous downloadId match; multiple queue ids)"
            )
            continue
        assert qid is not None
        title = history_item_title(rec)
        reason = parse_radarr_import_failed_reason(rec)
        await client.delete_queue_item(queue_id=qid)
        detail_parts: list[str] = []
        if title:
            detail_parts.append(title)
        if reason:
            detail_parts.append(f"Reason: {reason}")
        detail_parts.append("Action: removed from download queue")
        session.add(
            ActivityLog(
                job_run_id=job_run_id,
                app="sonarr",
                kind="cleanup",
                count=1,
                status="ok",
                detail="\n".join(detail_parts),
            )
        )
        actions.append(f"Sonarr: removed failed import from queue — {title if title else f'queue id {qid}'}")
