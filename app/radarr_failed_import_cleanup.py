"""
Radarr-only opt-in: remove queue items that match explicit import-failed history by downloadId.

See tests/test_radarr_failed_import_cleanup.py for intended behavior.
"""

from __future__ import annotations

import logging
from typing import Any, Literal, cast

from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient
from app.http_status_hints import format_http_error_detail
from app.log_sanitize import redact_sensitive_text
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
        if not download_id:
            continue
        if download_id in processed_download_ids:
            continue
        processed_download_ids.add(download_id)

        kind, qid = classify_queue_matches_by_download_id(download_id, queue_records)
        if kind == "none":
            continue
        if kind == "many":
            actions.append(
                "Radarr: skipped failed-import queue remove (ambiguous downloadId match; multiple queue ids)"
            )
            continue
        assert qid is not None
        title = history_item_title(rec)
        reason = parse_radarr_import_failed_reason(rec)
        try:
            await client.delete_queue_item(queue_id=qid)
        except Exception as exc:  # noqa: BLE001
            suffix = f" ({title})" if title else ""
            actions.append(
                f"Radarr: failed-import queue remove failed{suffix}: {format_http_error_detail(exc)}"
            )
            continue

        detail_parts: list[str] = []
        if title:
            detail_parts.append(title)
        if reason:
            detail_parts.append(f"Reason: {reason}")
        detail_parts.append("Action: removed from download queue")
        detail = redact_sensitive_text("\n".join(detail_parts))

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
        label = title if title else f"queue id {qid}"
        actions.append(f"Radarr: removed failed import from queue — {label}")

        queue_records = [
            q
            for q in queue_records
            if not (
                isinstance(q, dict)
                and str(q.get("downloadId") or "").strip() == download_id
            )
        ]
