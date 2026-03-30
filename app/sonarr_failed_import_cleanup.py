"""
Sonarr-only opt-in: delete terminal failed-import rows from Sonarr’s download queue.

Matches history/queue by ``downloadId``, classifies terminal vs waiting vs unknown, then calls
``DELETE /api/v3/queue/{id}`` (``blocklist=true`` first, ``false`` on fallback). No
``removeFromClient``. Activity rows only after a successful delete.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient
from app.arr_failed_import_classify import (
    FailedImportDisposition,
    import_failed_record_is_pending_waiting_no_eligible,
    sonarr_import_failed_history_disposition,
    sonarr_queue_terminal_cleanup_label,
    user_visible_text_is_pending_waiting_no_eligible,
)
from app.failed_import_activity import (
    failed_import_cleanup_action_success,
    format_failed_import_cleanup_activity_detail,
)
from app.http_status_hints import format_http_error_detail
from app.models import ActivityLog
from app.radarr_failed_import_cleanup import (
    _paginate_records,
    _queue_ids_for_download_id,
    _queue_row_id,
    classify_queue_matches_by_download_id,
    history_item_title,
    is_radarr_import_failed_record,
    parse_radarr_import_failed_reason,
)

logger = logging.getLogger(__name__)


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


async def run_sonarr_failed_import_queue_cleanup(
    client: ArrClient,
    *,
    session: AsyncSession,
    job_run_id: int | None,
    actions: list[str],
) -> None:
    logger.info("Sonarr failed-import cleanup: scan started")
    queue_records = await _paginate_records(
        client.queue_page,
        page_size=200,
        label="sonarr queue (failed-import cleanup)",
    )
    history_records = await _paginate_records(
        client.history_page,
        page_size=250,
        label="sonarr history (failed-import cleanup)",
    )
    logger.info(
        "Sonarr failed-import cleanup: inspected queue=%s history=%s",
        len(queue_records),
        len(history_records),
    )

    processed_download_ids: set[str] = set()
    removed_queue_ids: set[int] = set()
    eligible_from_history = 0
    eligible_from_queue = 0
    ineligible = 0

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

        disp = sonarr_import_failed_history_disposition(rec)
        if disp == FailedImportDisposition.PENDING_WAITING:
            logger.info(
                "Sonarr failed-import cleanup: skip downloadId=%s — pending waiting-to-import / no eligible files yet",
                download_id,
            )
            continue
        if disp == FailedImportDisposition.UNKNOWN:
            logger.info(
                "Sonarr failed-import cleanup: skip downloadId=%s — importFailed not classified as terminal (safe default)",
                download_id,
            )
            continue

        kind, qid = classify_queue_matches_by_download_id(download_id, queue_records)
        if kind == "none":
            logger.info(
                "Sonarr failed-import cleanup: ineligible (history has downloadId=%s; queue has no match)",
                download_id,
            )
            continue
        if kind == "many":
            qids = _queue_ids_for_download_id(download_id, queue_records)
            actions.append(
                f"Sonarr: Multiple queue rows matched one download; removing each ({len(qids)})."
            )
        else:
            assert qid is not None
            qids = [qid]

        title = history_item_title(rec)
        reason = parse_radarr_import_failed_reason(rec)
        for target_qid in qids:
            logger.info(
                "Sonarr failed-import cleanup: eligible queue id=%s via history importFailed (downloadId=%s)",
                target_qid,
                download_id,
            )
            try:
                await client.delete_queue_item(queue_id=target_qid, blocklist=True)
                blocklist_mode = "requested"
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Sonarr failed-import cleanup: delete failed for queue id=%s with blocklist=true: %s",
                    target_qid,
                    format_http_error_detail(exc),
                )
                try:
                    await client.delete_queue_item(queue_id=target_qid, blocklist=False)
                    blocklist_mode = "failed; removed queue without blocklist"
                except Exception as exc2:  # noqa: BLE001
                    suffix = f" ({title})" if title else ""
                    actions.append(
                        f"Sonarr: Failed import removal failed{suffix}: {format_http_error_detail(exc2)}"
                    )
                    logger.warning(
                        "Sonarr failed-import cleanup: delete fallback failed for queue id=%s: %s",
                        target_qid,
                        format_http_error_detail(exc2),
                    )
                    continue

            removed_queue_ids.add(target_qid)
            eligible_from_history += 1

            detail = format_failed_import_cleanup_activity_detail(
                "sonarr",
                blocklist_applied=blocklist_mode == "requested",
                title=title,
                reason=reason,
                queue_signal=None,
            )
            session.add(
                ActivityLog(
                    job_run_id=job_run_id,
                    app="sonarr",
                    kind="cleanup",
                    count=1,
                    status="ok",
                    detail=detail,
                )
            )
            label = title if title else f"queue id {target_qid}"
            actions.append(
                failed_import_cleanup_action_success(
                    "Sonarr",
                    blocklist_applied=blocklist_mode == "requested",
                    label=label,
                )
            )

        queue_records = [
            q
            for q in queue_records
            if not (isinstance(q, dict) and str(q.get("downloadId") or "").strip() == download_id)
        ]

    for q in list(queue_records):
        if not isinstance(q, dict):
            continue
        qid = _queue_row_id(q)
        if qid is None or qid in removed_queue_ids:
            continue
        q_blob = _flatten_sonarr_queue_user_messages(q)
        if user_visible_text_is_pending_waiting_no_eligible(q_blob):
            logger.info(
                "Sonarr failed-import cleanup: skip queue id=%s — pending waiting-to-import / no eligible files yet",
                qid,
            )
            continue
        q_signal = sonarr_queue_terminal_cleanup_label(q_blob)
        if not q_signal:
            ineligible += 1
            logger.info(
                "Sonarr failed-import cleanup: ineligible queue id=%s: no failed-import signal",
                qid,
            )
            continue

        label = q.get("title") if isinstance(q.get("title"), str) else ""
        logger.info(
            "Sonarr failed-import cleanup: eligible queue id=%s (%s) via queue signal: %s",
            qid,
            (label or "unknown"),
            q_signal,
        )
        try:
            await client.delete_queue_item(queue_id=qid, blocklist=True)
            blocklist_mode = "requested"
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Sonarr failed-import cleanup: queue-signal delete failed for queue id=%s with blocklist=true: %s",
                qid,
                format_http_error_detail(exc),
            )
            try:
                await client.delete_queue_item(queue_id=qid, blocklist=False)
                blocklist_mode = "failed; removed queue without blocklist"
            except Exception as exc2:  # noqa: BLE001
                suffix = f" ({label})" if label else ""
                actions.append(
                    f"Sonarr: Failed import removal failed{suffix}: {format_http_error_detail(exc2)}"
                )
                logger.warning(
                    "Sonarr failed-import cleanup: queue-signal delete fallback failed for queue id=%s: %s",
                    qid,
                    format_http_error_detail(exc2),
                )
                continue

        removed_queue_ids.add(qid)
        eligible_from_queue += 1
        detail = format_failed_import_cleanup_activity_detail(
            "sonarr",
            blocklist_applied=blocklist_mode == "requested",
            title=label,
            reason="",
            queue_signal=q_signal,
        )
        session.add(
            ActivityLog(
                job_run_id=job_run_id,
                app="sonarr",
                kind="cleanup",
                count=1,
                status="ok",
                detail=detail,
            )
        )
        lab = label if label else f"queue id {qid}"
        actions.append(
            failed_import_cleanup_action_success(
                "Sonarr",
                blocklist_applied=blocklist_mode == "requested",
                label=lab,
            )
        )

    logger.info(
        "Sonarr failed-import cleanup: scan complete; removed=%s (history=%s queue-signal=%s), ineligible=%s",
        len(removed_queue_ids),
        eligible_from_history,
        eligible_from_queue,
        ineligible,
    )
