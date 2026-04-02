"""
Radarr-only opt-in: delete failed-import rows from Radarr’s download queue by scenario.

Matches history/queue by ``downloadId`` or queue signals, applies per-scenario remove/blocklist
settings, then calls ``DELETE /api/v3/queue/{id}`` once per item with the chosen ``blocklist``
flag (no true-then-false fallback). ``removeFromClient`` is off by default; enable per app in
Fetcher Settings when the download client should drop the job so Radarr cannot re-track it.
Activity rows only after a successful delete.

History-driven cleanup skips when more than one distinct queue id shares the same ``downloadId``
(ambiguous match); queue-only cleanup can still remove individual rows by terminal queue messages.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal, cast

from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient
from app.arr_failed_import_classify import (
    FailedImportDisposition,
    import_failed_record_is_pending_waiting_no_eligible,
    is_radarr_download_failed_record,
    radarr_import_failed_history_disposition,
    radarr_queue_scenario_label,
    tracked_queue_download_state_is_failed,
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


@dataclass(frozen=True)
class RadarrCleanupPolicy:
    remove_corrupt: bool = False
    blocklist_corrupt: bool = False
    remove_download_failed: bool = False
    blocklist_download_failed: bool = False
    remove_import_failed: bool = False
    blocklist_import_failed: bool = False
    remove_unmatched: bool = False
    blocklist_unmatched: bool = False
    remove_quality: bool = False
    blocklist_quality: bool = False
    remove_from_client: bool = False


# Opt-in all scenarios with blocklist for tests and manual callers.
RADARR_CLEANUP_POLICY_ALL_ON = RadarrCleanupPolicy(
    remove_corrupt=True,
    blocklist_corrupt=True,
    remove_download_failed=True,
    blocklist_download_failed=True,
    remove_import_failed=True,
    blocklist_import_failed=True,
    remove_unmatched=True,
    blocklist_unmatched=True,
    remove_quality=True,
    blocklist_quality=True,
    remove_from_client=False,
)


def radarr_cleanup_policy_from_settings(settings: Any) -> RadarrCleanupPolicy:
    rfc = bool(getattr(settings, "radarr_failed_import_remove_from_client", False))
    return RadarrCleanupPolicy(
        remove_corrupt=bool(getattr(settings, "radarr_cleanup_corrupt", False)),
        blocklist_corrupt=bool(getattr(settings, "radarr_blocklist_corrupt", False)),
        remove_download_failed=bool(getattr(settings, "radarr_cleanup_download_failed", False)),
        blocklist_download_failed=bool(getattr(settings, "radarr_blocklist_download_failed", False)),
        remove_import_failed=bool(getattr(settings, "radarr_cleanup_import_failed", False)),
        blocklist_import_failed=bool(getattr(settings, "radarr_blocklist_import_failed", False)),
        remove_unmatched=bool(getattr(settings, "radarr_cleanup_unmatched", False)),
        blocklist_unmatched=bool(getattr(settings, "radarr_blocklist_unmatched", False)),
        remove_quality=bool(getattr(settings, "radarr_cleanup_quality", False)),
        blocklist_quality=bool(getattr(settings, "radarr_blocklist_quality", False)),
        remove_from_client=rfc,
    )


def _radarr_policy_for_scenario(
    policy: RadarrCleanupPolicy, scenario: FailedImportDisposition
) -> tuple[bool, bool]:
    if scenario is FailedImportDisposition.CORRUPT:
        return policy.remove_corrupt, policy.blocklist_corrupt
    if scenario is FailedImportDisposition.DOWNLOAD_FAILED:
        return policy.remove_download_failed, policy.blocklist_download_failed
    if scenario is FailedImportDisposition.IMPORT_FAILED:
        return policy.remove_import_failed, policy.blocklist_import_failed
    if scenario is FailedImportDisposition.UNMATCHED:
        return policy.remove_unmatched, policy.blocklist_unmatched
    if scenario is FailedImportDisposition.QUALITY:
        return policy.remove_quality, policy.blocklist_quality
    return False, False


async def _delete_queue_item_for_scenario(
    client: Any,
    *,
    queue_id: int,
    blocklist: bool,
    remove_from_client: bool,
    scenario: str,
    arr: str,
    actions: list[str],
    session: Any,
    job_run_id: int | None,
    title: str = "",
    reason: str = "",
    queue_signal: str | None = None,
) -> bool:
    """
    Delete one queue item with the given blocklist value. No fallback.
    Returns True on success, False on failure (failure is logged and
    appended to actions; an ActivityLog failure row is written).
    """
    app_key = arr.lower()
    try:
        await client.delete_queue_item(
            queue_id=queue_id,
            blocklist=blocklist,
            remove_from_client=remove_from_client,
        )
    except Exception as exc:
        hint = format_http_error_detail(exc)
        suffix = f" ({title})" if title else ""
        actions.append(f"{arr}: Failed import removal failed{suffix}: {hint}")
        session.add(
            ActivityLog(
                job_run_id=job_run_id,
                app=app_key,
                kind="cleanup",
                status="failed",
                count=0,
                detail=f"Failed import removal failed: {title or 'item'} — {hint}",
            )
        )
        logger.warning(
            "%s failed-import cleanup: delete failed queue_id=%s blocklist=%s scenario=%s: %s",
            arr,
            queue_id,
            blocklist,
            scenario,
            hint,
        )
        return False

    detail = format_failed_import_cleanup_activity_detail(
        app_key,
        blocklist_applied=blocklist,
        remove_from_client_applied=remove_from_client,
        title=title,
        reason=reason,
        queue_signal=queue_signal,
    )
    session.add(
        ActivityLog(
            job_run_id=job_run_id,
            app=app_key,
            kind="cleanup",
            count=1,
            status="ok",
            detail=detail,
        )
    )
    actions.append(
        failed_import_cleanup_action_success(
            arr,
            blocklist_applied=blocklist,
            label=title if title else f"queue id {queue_id}",
        )
    )
    return True


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

    Used by tests and queue-only cleanup (scenario QUALITY).
    """
    blob = _flatten_radarr_queue_user_messages(q)
    res = radarr_queue_scenario_label(blob)
    return res is not None and res[0] is FailedImportDisposition.QUALITY


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

    History-driven cleanup **skips** removals when kind is ``many`` (conservative policy); queue-only
    cleanup may still remove rows individually if their messages classify as terminal.
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
    policy: RadarrCleanupPolicy = RadarrCleanupPolicy(),
) -> None:
    logger.info("Radarr failed-import cleanup: scan started (policy=%r)", policy)

    if not any(
        [
            policy.remove_corrupt,
            policy.remove_download_failed,
            policy.remove_import_failed,
            policy.remove_unmatched,
            policy.remove_quality,
        ]
    ):
        logger.info("Radarr failed-import cleanup: all remove toggles off — skipping scan")
        return

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

    removed_queue_ids: set[int] = set()
    processed_download_ids: set[str] = set()
    eligible_from_history = 0
    eligible_from_queue = 0
    ineligible = 0

    # PASS 1: History-driven removals (importFailed + downloadFailed)
    for rec in history_records:
        if not isinstance(rec, dict):
            continue
        raw_did = rec.get("downloadId")
        if raw_did is None:
            continue
        download_id = str(raw_did).strip()
        if not download_id or download_id in processed_download_ids:
            continue

        scenario: FailedImportDisposition | None = None
        if is_radarr_download_failed_record(rec):
            scenario = FailedImportDisposition.DOWNLOAD_FAILED
        elif is_radarr_import_failed_record(rec):
            if import_failed_record_is_pending_waiting_no_eligible(rec):
                logger.info(
                    "Radarr failed-import cleanup: skip downloadId=%s — pending waiting",
                    download_id,
                )
                processed_download_ids.add(download_id)
                continue
            disp = radarr_import_failed_history_disposition(rec)
            if disp is FailedImportDisposition.UNKNOWN:
                scenario = FailedImportDisposition.IMPORT_FAILED
            else:
                scenario = disp
        else:
            continue

        processed_download_ids.add(download_id)

        remove, blocklist = _radarr_policy_for_scenario(policy, scenario)
        if not remove:
            logger.info(
                "Radarr failed-import cleanup: skip downloadId=%s — scenario=%s remove=False",
                download_id,
                scenario.value,
            )
            continue

        kind, qid = classify_queue_matches_by_download_id(download_id, queue_records)
        if kind == "none":
            logger.info(
                "Radarr failed-import cleanup: no queue match for downloadId=%s (history scenario=%s)",
                download_id,
                scenario.value,
            )
            continue

        if kind == "many":
            n = len(_queue_ids_for_download_id(download_id, queue_records))
            logger.info(
                "Radarr failed-import cleanup: skip downloadId=%s — %s queue rows match (ambiguous); no history-driven removals",
                download_id,
                n,
            )
            actions.append(
                f"Radarr: Skipped failed-import cleanup for downloadId={download_id} — "
                f"{n} queue rows match (ambiguous); removed none (conservative policy)."
            )
            continue

        assert qid is not None
        qids = [qid]

        title = history_item_title(rec)
        reason = parse_radarr_import_failed_reason(rec)
        for target_qid in qids:
            logger.info(
                "Radarr failed-import cleanup: eligible queue id=%s via history (downloadId=%s scenario=%s)",
                target_qid,
                download_id,
                scenario.value,
            )
            ok = await _delete_queue_item_for_scenario(
                client,
                queue_id=target_qid,
                blocklist=blocklist,
                remove_from_client=policy.remove_from_client,
                scenario=scenario.value,
                arr="Radarr",
                actions=actions,
                session=session,
                job_run_id=job_run_id,
                title=title,
                reason=reason,
                queue_signal=None,
            )
            if ok:
                removed_queue_ids.add(target_qid)
                eligible_from_history += 1

        queue_records = [
            q
            for q in queue_records
            if not (isinstance(q, dict) and str(q.get("downloadId") or "").strip() == download_id)
        ]

    # PASS 2: Queue-signal removals (no matching history pass above)
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
        scenario_label = radarr_queue_scenario_label(q_blob)
        if scenario_label is None and tracked_queue_download_state_is_failed(q):
            scenario_label = (
                FailedImportDisposition.DOWNLOAD_FAILED,
                "download client failure (tracked state)",
            )
        if scenario_label is None:
            ineligible += 1
            logger.info(
                "Radarr failed-import cleanup: ineligible queue id=%s (%s): no failed-import signal",
                qid,
                queue_item_label(q) or "unknown",
            )
            continue

        scenario, q_signal = scenario_label
        remove, blocklist = _radarr_policy_for_scenario(policy, scenario)
        if not remove:
            logger.info(
                "Radarr failed-import cleanup: skip queue id=%s — scenario=%s remove=False",
                qid,
                scenario.value,
            )
            continue

        label = queue_item_label(q)
        logger.info(
            "Radarr failed-import cleanup: eligible queue id=%s (%s) via queue signal: %s",
            qid,
            label or "unknown",
            q_signal,
        )
        ok = await _delete_queue_item_for_scenario(
            client,
            queue_id=qid,
            blocklist=blocklist,
            remove_from_client=policy.remove_from_client,
            scenario=scenario.value,
            arr="Radarr",
            actions=actions,
            session=session,
            job_run_id=job_run_id,
            title=label,
            reason="",
            queue_signal=q_signal,
        )
        if ok:
            removed_queue_ids.add(qid)
            eligible_from_queue += 1
            queue_records = [
                x
                for x in queue_records
                if not (isinstance(x, dict) and _queue_row_id(x) == qid)
            ]

    if ineligible > 0:
        logger.info(
            "Radarr failed-import cleanup: %d item(s) ineligible (no terminal signal) — skipped",
            ineligible,
        )

    logger.info(
        "Radarr failed-import cleanup: scan complete; removed=%s (history=%s queue-signal=%s), ineligible=%s",
        len(removed_queue_ids),
        eligible_from_history,
        eligible_from_queue,
        ineligible,
    )
