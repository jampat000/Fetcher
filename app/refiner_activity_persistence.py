"""Refiner activity rows: DB insert/update/dedupe and job log text helpers."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import SessionLocal
from app.log_sanitize import redact_sensitive_text
from app.models import RefinerActivity
from app.refiner_activity_context import dumps_activity_context, parse_activity_context
from app.refiner_media_identity import provisional_media_title_before_probe
from app.time_util import utc_now_naive

logger = logging.getLogger(__name__)

_UPSTREAM_WAIT_REASON_CODES: frozenset[str] = frozenset(
    {
        "radarr_queue_active_download",
        "sonarr_queue_active_download",
        "radarr_queue_active_download_title",
        "sonarr_queue_active_download_title",
    }
)

_REFINER_JOB_LOG_MAX_CHARS = 400_000


def _refiner_job_log_text(body: str) -> str:
    t = (body or "").strip()
    if len(t) > _REFINER_JOB_LOG_MAX_CHARS:
        t = t[: _REFINER_JOB_LOG_MAX_CHARS - 28] + "\n… (message truncated)"
    return redact_sensitive_text(t)


async def _insert_refiner_processing_row(file_name: str) -> int | None:
    """Create a single activity row in ``processing`` state (updated when the file finishes)."""
    try:
        fn = str(file_name or "")[:512]
        prov = provisional_media_title_before_probe(fn)[:512]
        async with SessionLocal() as session:
            row = RefinerActivity(
                file_name=fn,
                media_title=prov,
                status="processing",
                size_before_bytes=0,
                size_after_bytes=0,
                audio_tracks_before=0,
                audio_tracks_after=0,
                subtitle_tracks_before=0,
                subtitle_tracks_after=0,
                processing_time_ms=None,
                activity_context="",
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return int(row.id)
    except Exception:
        logger.warning("Refiner: could not insert refiner_activity processing row", exc_info=True)
        return None


async def _dedupe_upstream_wait_row(
    session: AsyncSession,
    *,
    file_name: str,
    reason_code: str,
    failure_new: str,
    media_title: str,
    meta: dict[str, Any],
    exclude_id: int | None = None,
) -> bool:
    """If a prior failed row exists with the same file+reason+message, merge into it. Returns True if merged."""
    q = select(RefinerActivity).where(
        RefinerActivity.file_name == file_name,
        RefinerActivity.status == "failed",
    )
    if exclude_id is not None:
        q = q.where(RefinerActivity.id != exclude_id)
    prior = (await session.execute(q.order_by(RefinerActivity.id.desc()).limit(1))).scalars().first()
    if prior is None:
        return False
    prev_ctx = parse_activity_context(prior.activity_context)
    prev_rc = str(prev_ctx.get("reason_code") or "").strip().lower()
    prev_failure = str(prev_ctx.get("failure_reason") or "").strip()
    if prev_rc != reason_code or prev_failure != failure_new:
        return False
    repeats = int(prev_ctx.get("wait_repeat_count") or 1) + 1
    prev_ctx["wait_repeat_count"] = repeats
    prev_ctx["wait_last_seen_at"] = utc_now_naive().isoformat()
    ptm = meta.get("processing_time_ms")
    ptm_i = int(ptm) if ptm is not None else None
    await session.execute(
        update(RefinerActivity)
        .where(RefinerActivity.id == prior.id)
        .values(
            created_at=utc_now_naive(),
            media_title=media_title,
            status="failed",
            size_before_bytes=int(meta.get("size_before_bytes") or 0),
            size_after_bytes=int(meta.get("size_after_bytes") or 0),
            audio_tracks_before=int(meta.get("audio_tracks_before") or 0),
            audio_tracks_after=int(meta.get("audio_tracks_after") or 0),
            subtitle_tracks_before=int(meta.get("subtitle_tracks_before") or 0),
            subtitle_tracks_after=int(meta.get("subtitle_tracks_after") or 0),
            processing_time_ms=ptm_i,
            activity_context=dumps_activity_context(prev_ctx),
        )
    )
    return True


async def _update_refiner_activity_row(row_id: int, meta: dict[str, Any]) -> None:
    """Finalize a row created by ``_insert_refiner_processing_row`` (same logical job, no second row)."""
    try:
        fn = str(meta.get("file_name") or "")[:512]
        mt = str(meta.get("media_title") or "")[:512]
        st = str(meta.get("status") or "failed").strip().lower()
        if st not in ("success", "skipped", "failed"):
            st = "failed"
        ptm = meta.get("processing_time_ms")
        ptm_i = int(ptm) if ptm is not None else None
        ctx_raw = str(meta.get("activity_context") or "")[:120_000]
        ctx_new = parse_activity_context(ctx_raw)
        reason_code = str(ctx_new.get("reason_code") or "").strip().lower()
        failure_new = str(ctx_new.get("failure_reason") or "").strip()
        async with SessionLocal() as session:
            if st == "failed" and reason_code in _UPSTREAM_WAIT_REASON_CODES and fn:
                merged = await _dedupe_upstream_wait_row(
                    session,
                    file_name=fn,
                    reason_code=reason_code,
                    failure_new=failure_new,
                    media_title=mt,
                    meta=meta,
                    exclude_id=row_id,
                )
                if merged:
                    await session.execute(delete(RefinerActivity).where(RefinerActivity.id == row_id))
                    await session.commit()
                    return
            await session.execute(
                update(RefinerActivity)
                .where(RefinerActivity.id == row_id)
                .values(
                    file_name=fn,
                    media_title=mt,
                    status=st,
                    size_before_bytes=int(meta.get("size_before_bytes") or 0),
                    size_after_bytes=int(meta.get("size_after_bytes") or 0),
                    audio_tracks_before=int(meta.get("audio_tracks_before") or 0),
                    audio_tracks_after=int(meta.get("audio_tracks_after") or 0),
                    subtitle_tracks_before=int(meta.get("subtitle_tracks_before") or 0),
                    subtitle_tracks_after=int(meta.get("subtitle_tracks_after") or 0),
                    processing_time_ms=ptm_i,
                    activity_context=ctx_raw,
                )
            )
            await session.commit()
    except Exception:
        logger.warning("Refiner: could not update refiner_activity row id=%s", row_id, exc_info=True)


async def _persist_refiner_activity_safe(meta: dict[str, Any]) -> None:
    """Fail-safe: never raises; insert-only path when processing row could not be created."""
    try:
        fn = str(meta.get("file_name") or "")[:512]
        mt = str(meta.get("media_title") or "")[:512]
        st = str(meta.get("status") or "failed").strip().lower()
        if st not in ("success", "skipped", "failed"):
            st = "failed"
        ptm = meta.get("processing_time_ms")
        ptm_i = int(ptm) if ptm is not None else None
        ctx_raw = str(meta.get("activity_context") or "")[:120_000]
        ctx_new = parse_activity_context(ctx_raw)
        reason_code = str(ctx_new.get("reason_code") or "").strip().lower()
        failure_new = str(ctx_new.get("failure_reason") or "").strip()
        async with SessionLocal() as session:
            if st == "failed" and reason_code in _UPSTREAM_WAIT_REASON_CODES and fn:
                merged = await _dedupe_upstream_wait_row(
                    session,
                    file_name=fn,
                    reason_code=reason_code,
                    failure_new=failure_new,
                    media_title=mt,
                    meta=meta,
                )
                if merged:
                    await session.commit()
                    return
            session.add(
                RefinerActivity(
                    file_name=fn,
                    media_title=mt,
                    status=st,
                    size_before_bytes=int(meta.get("size_before_bytes") or 0),
                    size_after_bytes=int(meta.get("size_after_bytes") or 0),
                    audio_tracks_before=int(meta.get("audio_tracks_before") or 0),
                    audio_tracks_after=int(meta.get("audio_tracks_after") or 0),
                    subtitle_tracks_before=int(meta.get("subtitle_tracks_before") or 0),
                    subtitle_tracks_after=int(meta.get("subtitle_tracks_after") or 0),
                    processing_time_ms=ptm_i,
                    activity_context=ctx_raw,
                )
            )
            await session.commit()
    except Exception:
        logger.warning("Refiner: could not persist refiner_activity row", exc_info=True)


async def _close_all_processing_refiner_activity_rows(*, context: str) -> None:
    """Set every ``processing`` row to ``failed`` (no new DB columns; see callers for semantics)."""
    try:
        async with SessionLocal() as session:
            res = await session.execute(
                update(RefinerActivity)
                .where(RefinerActivity.status == "processing")
                .values(status="failed", processing_time_ms=None)
            )
            await session.commit()
            rc = getattr(res, "rowcount", None)
            if isinstance(rc, int) and rc > 0:
                logger.warning("Refiner: closed %s Processing activity row(s) — %s", rc, context)
    except Exception:
        logger.warning("Refiner: processing-row close failed (%s)", context, exc_info=True)


async def reconcile_refiner_processing_rows_on_worker_boot() -> None:
    """
    Called once when this process starts after the DB is migrated.

    Any ``refiner_activity`` row still in Processing could only belong to a prior crashed worker
    (this instance was not running to finish the job). This does not use wall-clock heuristics.
    """
    await _close_all_processing_refiner_activity_rows(
        context="worker boot — prior process instance could not complete these rows",
    )


async def _reconcile_interrupted_refiner_processing_rows_before_pass() -> None:
    """
    Start of each Refiner pass, under ``_refiner_lock``, **before** inserting new processing rows:

    No row can be “current pass” yet, so anything still Processing is leftover from an interrupted
    earlier pass. Long remuxes in the pass we are about to start are not affected (no rows exist
    for them until we insert below).
    """
    await _close_all_processing_refiner_activity_rows(
        context="new pass — closing rows left processing before this run inserts new ones",
    )
