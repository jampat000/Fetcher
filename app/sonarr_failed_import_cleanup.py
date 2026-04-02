"""
Sonarr-only opt-in: delete failed-import rows from Sonarr’s download queue by scenario.

Same policy model as Radarr: per-scenario remove/blocklist, single delete per item, no blocklist fallback.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient
from app.arr_failed_import_classify import (
    FailedImportDisposition,
    import_failed_record_is_pending_waiting_no_eligible,
    is_sonarr_download_failed_record,
    sonarr_import_failed_history_disposition,
    sonarr_queue_scenario_label,
    user_visible_text_is_pending_waiting_no_eligible,
)
from app.radarr_failed_import_cleanup import (
    _delete_queue_item_for_scenario,
    _paginate_records,
    _queue_ids_for_download_id,
    _queue_row_id,
    classify_queue_matches_by_download_id,
    history_item_title,
    is_radarr_import_failed_record,
    parse_radarr_import_failed_reason,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SonarrCleanupPolicy:
    remove_corrupt: bool = False
    blocklist_corrupt: bool = False
    remove_download_failed: bool = False
    blocklist_download_failed: bool = False
    remove_unmatched: bool = False
    blocklist_unmatched: bool = False
    remove_quality: bool = False
    blocklist_quality: bool = False


SONARR_CLEANUP_POLICY_ALL_ON = SonarrCleanupPolicy(
    remove_corrupt=True,
    blocklist_corrupt=True,
    remove_download_failed=True,
    blocklist_download_failed=True,
    remove_unmatched=True,
    blocklist_unmatched=True,
    remove_quality=True,
    blocklist_quality=True,
)


def sonarr_cleanup_policy_from_settings(settings: Any) -> SonarrCleanupPolicy:
    if getattr(settings, "sonarr_remove_failed_imports", False):
        return SonarrCleanupPolicy(
            remove_corrupt=True,
            blocklist_corrupt=True,
            remove_download_failed=True,
            blocklist_download_failed=True,
            remove_unmatched=True,
            blocklist_unmatched=True,
            remove_quality=True,
            blocklist_quality=True,
        )
    return SonarrCleanupPolicy(
        remove_corrupt=bool(getattr(settings, "sonarr_cleanup_corrupt", False)),
        blocklist_corrupt=bool(getattr(settings, "sonarr_blocklist_corrupt", False)),
        remove_download_failed=bool(getattr(settings, "sonarr_cleanup_download_failed", False)),
        blocklist_download_failed=bool(getattr(settings, "sonarr_blocklist_download_failed", False)),
        remove_unmatched=bool(getattr(settings, "sonarr_cleanup_unmatched", False)),
        blocklist_unmatched=bool(getattr(settings, "sonarr_blocklist_unmatched", False)),
        remove_quality=bool(getattr(settings, "sonarr_cleanup_quality", False)),
        blocklist_quality=bool(getattr(settings, "sonarr_blocklist_quality", False)),
    )


def _sonarr_policy_for_scenario(
    policy: SonarrCleanupPolicy, scenario: FailedImportDisposition
) -> tuple[bool, bool]:
    if scenario is FailedImportDisposition.CORRUPT:
        return policy.remove_corrupt, policy.blocklist_corrupt
    if scenario is FailedImportDisposition.DOWNLOAD_FAILED:
        return policy.remove_download_failed, policy.blocklist_download_failed
    if scenario is FailedImportDisposition.UNMATCHED:
        return policy.remove_unmatched, policy.blocklist_unmatched
    if scenario is FailedImportDisposition.QUALITY:
        return policy.remove_quality, policy.blocklist_quality
    return False, False


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
    policy: SonarrCleanupPolicy = SonarrCleanupPolicy(),
) -> None:
    logger.info("Sonarr failed-import cleanup: scan started (policy=%r)", policy)

    if not any(
        [
            policy.remove_corrupt,
            policy.remove_download_failed,
            policy.remove_unmatched,
            policy.remove_quality,
        ]
    ):
        logger.info("Sonarr failed-import cleanup: all remove toggles off — skipping scan")
        return

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

    removed_queue_ids: set[int] = set()
    processed_download_ids: set[str] = set()
    eligible_from_history = 0
    eligible_from_queue = 0
    ineligible = 0

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
        if is_sonarr_download_failed_record(rec):
            scenario = FailedImportDisposition.DOWNLOAD_FAILED
        elif is_radarr_import_failed_record(rec):
            if import_failed_record_is_pending_waiting_no_eligible(rec):
                logger.info(
                    "Sonarr failed-import cleanup: skip downloadId=%s — pending waiting",
                    download_id,
                )
                processed_download_ids.add(download_id)
                continue
            disp = sonarr_import_failed_history_disposition(rec)
            if disp is FailedImportDisposition.UNKNOWN:
                logger.info(
                    "Sonarr failed-import cleanup: skip downloadId=%s — unknown disposition",
                    download_id,
                )
                processed_download_ids.add(download_id)
                continue
            scenario = disp
        else:
            continue

        processed_download_ids.add(download_id)

        remove, blocklist = _sonarr_policy_for_scenario(policy, scenario)
        if not remove:
            logger.info(
                "Sonarr failed-import cleanup: skip downloadId=%s — scenario=%s remove=False",
                download_id,
                scenario.value,
            )
            continue

        kind, qid = classify_queue_matches_by_download_id(download_id, queue_records)
        if kind == "none":
            logger.info(
                "Sonarr failed-import cleanup: no queue match for downloadId=%s (history scenario=%s)",
                download_id,
                scenario.value,
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
                "Sonarr failed-import cleanup: eligible queue id=%s via history (downloadId=%s scenario=%s)",
                target_qid,
                download_id,
                scenario.value,
            )
            ok = await _delete_queue_item_for_scenario(
                client,
                queue_id=target_qid,
                blocklist=blocklist,
                scenario=scenario.value,
                arr="Sonarr",
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
        scenario_label = sonarr_queue_scenario_label(q_blob)
        if scenario_label is None:
            ineligible += 1
            logger.info(
                "Sonarr failed-import cleanup: ineligible queue id=%s: no failed-import signal",
                qid,
            )
            continue

        scenario, q_signal = scenario_label
        remove, blocklist = _sonarr_policy_for_scenario(policy, scenario)
        if not remove:
            logger.info(
                "Sonarr failed-import cleanup: skip queue id=%s — scenario=%s remove=False",
                qid,
                scenario.value,
            )
            continue

        label = q.get("title") if isinstance(q.get("title"), str) else ""
        label = (label or "").strip()[:500]
        logger.info(
            "Sonarr failed-import cleanup: eligible queue id=%s (%s) via queue signal: %s",
            qid,
            label or "unknown",
            q_signal,
        )
        ok = await _delete_queue_item_for_scenario(
            client,
            queue_id=qid,
            blocklist=blocklist,
            scenario=scenario.value,
            arr="Sonarr",
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

    if ineligible > 0:
        logger.info(
            "Sonarr failed-import cleanup: %d item(s) ineligible (no terminal signal) — skipped",
            ineligible,
        )

    logger.info(
        "Sonarr failed-import cleanup: scan complete; removed=%s (history=%s queue-signal=%s), ineligible=%s",
        len(removed_queue_ids),
        eligible_from_history,
        eligible_from_queue,
        ineligible,
    )
