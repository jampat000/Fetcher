"""
Radarr-only opt-in: delete terminal failed-import rows from Radarr’s download queue.

Matches history/queue by ``downloadId``, classifies terminal vs waiting vs unknown, then calls
``DELETE /api/v3/queue/{id}`` (``blocklist=true`` first, ``false`` on fallback). No
``removeFromClient``. Activity rows only after a successful delete.

See tests/test_radarr_failed_import_cleanup.py for intended behavior.
"""

from __future__ import annotations

import logging
from typing import Any, Literal, cast

from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient
from app.arr_failed_import_classify import (
    FailedImportDisposition,
    import_failed_record_is_pending_waiting_no_eligible,
    radarr_import_failed_history_disposition,
    radarr_queue_terminal_cleanup_label,
    user_visible_text_is_pending_waiting_no_eligible,
)
from app.failed_import_activity import (
    failed_import_cleanup_action_success,
    format_failed_import_cleanup_activity_detail,
)
from app.http_status_hints import format_http_error_detail
from app.models import ActivityLog

logger = logging.getLogger(__name__)

MatchKind = Literal["none", "one", "many"]


def is_radarr_import_failed_record(rec: dict[str, Any]) -> bool:
    et = rec.get("eventType")
    if isinstance(et, str):
        return et.strip().casefold() == "importfailed"
    # Radarr (and *arr family) serialize HistoryEventType as integer in some clients.
    if isinstance(et, int):
        return et == 9
    return False


def parse_radarr_import_failed_reason(rec: dict[str, Any]) -> str:
    """Return best-effort reason text; never raises."""
    try:
        for key in ("reason", "message", "downloadFailedMessage"):
            v = rec.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()[:2000]
        data = rec.get("data")
        if isinstance(data, dict):
            for key in ("message", "reason", "errorMessage", "exceptionMessage"):
                v = data.get(key)
                if isinstance(v, str) and v.strip():
                    return v.strip()[:2000]
    except Exception:
        pass
    return ""


def history_item_title(rec: dict[str, Any]) -> str:
    try:
        st = rec.get("sourceTitle")
        if isinstance(st, str) and st.strip():
            return st.strip()[:500]
        movie = rec.get("movie")
        if isinstance(movie, dict):
            t = movie.get("title")
            if isinstance(t, str) and t.strip():
                y = movie.get("year")
                if isinstance(y, int):
                    return f"{t.strip()} ({y})"[:500]
                return t.strip()[:500]
    except Exception:
        pass
    return ""


def _queue_row_id(q: dict[str, Any]) -> int | None:
    raw = q.get("id")
    if isinstance(raw, int) and raw > 0:
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return None


def _flatten_radarr_queue_user_messages(q: dict[str, Any]) -> str:
    """Human-facing error lines from a Radarr /queue record (statusMessages + errorMessage)."""
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


def radarr_queue_item_is_pending_waiting_no_eligible(q: dict[str, Any]) -> bool:
    """Queue statusMessages / errorMessage — same pending pattern as history."""
    return user_visible_text_is_pending_waiting_no_eligible(_flatten_radarr_queue_user_messages(q))


def is_radarr_queue_non_quality_upgrade_rejection(q: dict[str, Any]) -> bool:
    """
    Permanent import rejection: new file is not a quality (or preferred-word) upgrade vs existing.

    Used by tests and queue-only cleanup (terminal label table covers the same phrases).
    """
    blob = _flatten_radarr_queue_user_messages(q)
    lbl = radarr_queue_terminal_cleanup_label(blob)
    return lbl in ("not an upgrade vs existing file", "not a preferred-word upgrade")


def queue_item_label(q: dict[str, Any]) -> str:
    try:
        t = q.get("title")
        if isinstance(t, str) and t.strip():
            return t.strip()[:500]
        movie = q.get("movie")
        if isinstance(movie, dict):
            mt = movie.get("title")
            if isinstance(mt, str) and mt.strip():
                y = movie.get("year")
                if isinstance(y, int):
                    return f"{mt.strip()} ({y})"[:500]
                return mt.strip()[:500]
    except Exception:
        pass
    return ""


def classify_queue_matches_by_download_id(
    download_id: str,
    queue_records: list[dict[str, Any]],
) -> tuple[MatchKind, int | None]:
    """
    Match queue rows by exact ``downloadId`` string equality (after ``str()`` / strip on both sides).

    Returns:
        (``none``, None), (``one``, queue id), or (``many``, None) when multiple distinct queue ids match.
    """
    if not download_id:
        return "none", None
    ids: set[int] = set()
    for q in queue_records:
        qdid = q.get("downloadId")
        if qdid is None:
            continue
        if str(qdid).strip() != download_id:
            continue
        qid = q.get("id")
        i: int | None = None
        if isinstance(qid, int) and qid > 0:
            i = qid
        elif isinstance(qid, str) and qid.isdigit():
            i = int(qid)
        if i is not None:
            ids.add(i)
    if len(ids) == 0:
        return "none", None
    if len(ids) == 1:
        return "one", next(iter(ids))
    return "many", None


def _queue_ids_for_download_id(download_id: str, queue_records: list[dict[str, Any]]) -> list[int]:
    ids: set[int] = set()
    if not download_id:
        return []
    for q in queue_records:
        if str(q.get("downloadId") or "").strip() != download_id:
            continue
        qid = _queue_row_id(q)
        if qid is not None:
            ids.add(qid)
    return sorted(ids)


async def _paginate_records(
    fetch_page: Any,
    *,
    page_size: int,
    label: str,
    max_pages: int = 200,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        data = await fetch_page(page=page, page_size=page_size)
        if not isinstance(data, dict):
            break
        recs = data.get("records")
        if not isinstance(recs, list):
            break
        out.extend(cast(list[dict[str, Any]], recs))
        total = int(data.get("totalRecords") or 0)
        if total <= 0 or not recs or page * page_size >= total:
            break
        page += 1
    if page > max_pages:
        logger.warning("%s: page cap (%s) reached", label, max_pages)
    return out


async def run_radarr_failed_import_queue_cleanup(
    client: ArrClient,
    *,
    session: AsyncSession,
    job_run_id: int | None,
    actions: list[str],
) -> None:
    logger.info("Radarr failed-import cleanup: scan started")
    queue_records = await _paginate_records(
        client.queue_page,
        page_size=200,
        label="radarr queue (failed-import cleanup)",
    )
    history_records = await _paginate_records(
        client.history_page,
        page_size=250,
        label="radarr history (failed-import cleanup)",
    )
    logger.info(
        "Radarr failed-import cleanup: inspected queue=%s history=%s",
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
        if not download_id:
            continue
        if download_id in processed_download_ids:
            continue
        processed_download_ids.add(download_id)

        disp = radarr_import_failed_history_disposition(rec)
        if disp == FailedImportDisposition.PENDING_WAITING:
            logger.info(
                "Radarr failed-import cleanup: skip downloadId=%s — pending waiting-to-import / no eligible files yet",
                download_id,
            )
            continue
        if disp == FailedImportDisposition.UNKNOWN:
            logger.info(
                "Radarr failed-import cleanup: skip downloadId=%s — importFailed not classified as terminal (safe default)",
                download_id,
            )
            continue

        kind, qid = classify_queue_matches_by_download_id(download_id, queue_records)
        if kind == "none":
            logger.info(
                "Radarr failed-import cleanup: ineligible (history has downloadId=%s; queue has no match)",
                download_id,
            )
            continue

        qids: list[int]
        if kind == "many":
            qids = _queue_ids_for_download_id(download_id, queue_records)
            actions.append(
                f"Radarr: Multiple queue rows matched one download; removing each ({len(qids)})."
            )
        else:
            assert qid is not None
            qids = [qid]

        title = history_item_title(rec)
        reason = parse_radarr_import_failed_reason(rec)
        for target_qid in qids:
            logger.info(
                "Radarr failed-import cleanup: eligible queue id=%s via history importFailed (downloadId=%s)",
                target_qid,
                download_id,
            )
            try:
                await client.delete_queue_item(queue_id=target_qid, blocklist=True)
                blocklist_mode = "requested"
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Radarr failed-import cleanup: delete failed for queue id=%s with blocklist=true: %s",
                    target_qid,
                    format_http_error_detail(exc),
                )
                try:
                    await client.delete_queue_item(queue_id=target_qid, blocklist=False)
                    blocklist_mode = "failed; removed queue without blocklist"
                except Exception as exc2:  # noqa: BLE001
                    suffix = f" ({title})" if title else ""
                    actions.append(
                        f"Radarr: Failed import removal failed{suffix}: {format_http_error_detail(exc2)}"
                    )
                    logger.warning(
                        "Radarr failed-import cleanup: delete fallback failed for queue id=%s: %s",
                        target_qid,
                        format_http_error_detail(exc2),
                    )
                    continue

            removed_queue_ids.add(target_qid)
            eligible_from_history += 1

            detail = format_failed_import_cleanup_activity_detail(
                "radarr",
                blocklist_applied=blocklist_mode == "requested",
                title=title,
                reason=reason,
                queue_signal=None,
            )
            session.add(
                ActivityLog(
                    job_run_id=job_run_id,
                    app="radarr",
                    kind="cleanup",
                    count=1,
                    status="ok",
                    detail=detail,
                )
            )
            label = title if title else f"queue id {target_qid}"
            actions.append(
                failed_import_cleanup_action_success(
                    "Radarr",
                    blocklist_applied=blocklist_mode == "requested",
                    label=label,
                )
            )

        queue_records = [
            q
            for q in queue_records
            if not (
                isinstance(q, dict)
                and str(q.get("downloadId") or "").strip() == download_id
            )
        ]

    for q in list(queue_records):
        if not isinstance(q, dict):
            continue
        qid = _queue_row_id(q)
        if qid is None:
            continue
        if qid in removed_queue_ids:
            continue
        if radarr_queue_item_is_pending_waiting_no_eligible(q):
            logger.info(
                "Radarr failed-import cleanup: skip queue id=%s — pending waiting-to-import / no eligible files yet",
                qid,
            )
            continue
        q_blob = _flatten_radarr_queue_user_messages(q)
        q_signal = radarr_queue_terminal_cleanup_label(q_blob)
        if not q_signal:
            ineligible += 1
            logger.info(
                "Radarr failed-import cleanup: ineligible queue id=%s (%s): no failed-import signal",
                qid,
                queue_item_label(q) or "unknown",
            )
            continue

        label = queue_item_label(q)
        logger.info(
            "Radarr failed-import cleanup: eligible queue id=%s (%s) via queue signal: %s",
            qid,
            label or "unknown",
            q_signal,
        )
        try:
            await client.delete_queue_item(queue_id=qid, blocklist=True)
            blocklist_mode = "requested"
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Radarr failed-import cleanup: queue-signal delete failed for queue id=%s with blocklist=true: %s",
                qid,
                format_http_error_detail(exc),
            )
            try:
                await client.delete_queue_item(queue_id=qid, blocklist=False)
                blocklist_mode = "failed; removed queue without blocklist"
            except Exception as exc2:  # noqa: BLE001
                suffix = f" ({label})" if label else ""
                actions.append(
                    f"Radarr: Failed import removal failed{suffix}: {format_http_error_detail(exc2)}"
                )
                logger.warning(
                    "Radarr failed-import cleanup: queue-signal delete fallback failed for queue id=%s: %s",
                    qid,
                    format_http_error_detail(exc2),
                )
                continue

        removed_queue_ids.add(qid)
        eligible_from_queue += 1

        detail = format_failed_import_cleanup_activity_detail(
            "radarr",
            blocklist_applied=blocklist_mode == "requested",
            title=label,
            reason="",
            queue_signal=q_signal,
        )
        session.add(
            ActivityLog(
                job_run_id=job_run_id,
                app="radarr",
                kind="cleanup",
                count=1,
                status="ok",
                detail=detail,
            )
        )
        lab = label if label else f"queue id {qid}"
        actions.append(
            failed_import_cleanup_action_success(
                "Radarr",
                blocklist_applied=blocklist_mode == "requested",
                label=lab,
            )
        )

        queue_records = [
            x
            for x in queue_records
            if not (isinstance(x, dict) and _queue_row_id(x) == qid)
        ]

    logger.info(
        "Radarr failed-import cleanup: scan complete; removed=%s (history=%s queue-signal=%s), ineligible=%s",
        len(removed_queue_ids),
        eligible_from_history,
        eligible_from_queue,
        ineligible,
    )
