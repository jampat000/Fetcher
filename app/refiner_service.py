"""Refiner orchestration: watched-folder pass, scheduling hook, path helpers.

Per-file pipeline lives in ``app.refiner_pipeline``; activity DB rows in ``app.refiner_activity_persistence``;
watched-folder cleanup helpers in ``app.refiner_cleanup``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient, ArrConfig
from app.db import db_path
from app.models import AppSettings, JobRunLog
from app.refiner_activity_context import dumps_activity_context, parse_activity_context
from app.refiner_radarr_wrong_content_actions import execute_radarr_wrong_content_actions
from app.resolvers.api_keys import resolve_radarr_api_key
from app.refiner_cleanup import (
    _cleanup_refiner_source_sidecar_artifacts_after_success,
    _try_remove_empty_watch_subfolder,
)
from app.refiner_activity_persistence import (
    _UPSTREAM_WAIT_REASON_CODES,
    _insert_refiner_processing_row,
    _persist_refiner_activity_safe,
    _refiner_job_log_text,
    _reconcile_interrupted_refiner_processing_rows_before_pass,
    _update_refiner_activity_row,
    reconcile_refiner_processing_rows_on_worker_boot,
)
from app.refiner_pipeline import (
    _activity_snapshot,
    _failure_activity_meta,
    _file_size_bytes,
    _finalize_output_file,
    _process_one_refiner_file_sync,
    _readiness_skip_meta,
)
from app.refiner_mux import REFINER_FFMPEG_TIMEOUT_S
from app.refiner_outcome_classify import format_per_file_job_log_line
from app.refiner_rules import (
    RefinerRulesConfig,
    is_refiner_media_candidate,
    normalize_audio_preference_mode,
    normalize_lang,
    parse_subtitle_langs_csv,
)
from app.refiner_source_readiness import (
    RefinerQueueSnapshot,
    decide_refiner_readiness,
    fetch_refiner_queue_snapshot,
    ffprobe_failure_hint_is_read_analyze,
    log_refiner_readiness_diagnostic,
    upstream_analyze_path,
)
from app.schedule import in_window
from app.time_util import utc_now_naive

logger = logging.getLogger(__name__)

_refiner_lock = asyncio.Lock()


async def _movie_wrong_content_ctx_for_candidate(
    fp: Path, row: AppSettings, snap: RefinerQueueSnapshot
) -> dict[str, Any] | None:
    """Radarr movie queue association + catalog runtime for post-probe wrong-content scoring (movies only)."""
    if not row.radarr_enabled or not snap.authority_useful:
        return None
    _, _, _, ud = upstream_analyze_path(fp, snap)
    mid = ud.get("radarr_refiner_target_movie_id")
    if not isinstance(mid, int) or mid <= 0:
        return None
    url = (row.radarr_url or "").strip()
    key = resolve_radarr_api_key(row)
    if not (url and key):
        return None
    expected_rt: float | None = None
    try:
        client = ArrClient(ArrConfig(url, key), timeout_s=30.0)
        m = await client.get_movie(mid)
        if m:
            rt = m.get("runtime")
            if isinstance(rt, int) and rt > 0:
                expected_rt = float(rt)
    except Exception:
        logger.debug("Refiner: get_movie failed for wrong-content context (movie_id=%s)", mid, exc_info=True)
    title = str(ud.get("radarr_refiner_target_movie_title") or "").strip()
    y = ud.get("radarr_refiner_target_movie_year")
    qraw = ud.get("radarr_refiner_target_queue_id")
    qid: int | None
    if isinstance(qraw, int) and qraw > 0:
        qid = qraw
    elif isinstance(qraw, str) and qraw.isdigit():
        qid = int(qraw)
    else:
        qid = None
    return {
        "enabled": True,
        "movie_id": mid,
        "queue_id": qid,
        "target_title": title[:500],
        "target_year": int(y) if isinstance(y, int) else None,
        "expected_runtime_minutes": expected_rt,
    }


def _rules_config_from_settings(row: AppSettings) -> RefinerRulesConfig | None:
    if not row.refiner_enabled:
        return None
    slot = (row.refiner_default_audio_slot or "primary").strip().lower()
    if slot not in ("primary", "secondary"):
        slot = "primary"
    mode = (row.refiner_subtitle_mode or "remove_all").strip().lower()
    if mode not in ("remove_all", "keep_selected"):
        mode = "remove_all"
    pref = normalize_audio_preference_mode(row.refiner_audio_preference_mode)
    return RefinerRulesConfig(
        primary_audio_lang=row.refiner_primary_audio_lang or "",
        secondary_audio_lang=row.refiner_secondary_audio_lang or "",
        tertiary_audio_lang=row.refiner_tertiary_audio_lang or "",
        default_audio_slot=slot,  # type: ignore[arg-type]
        remove_commentary=bool(row.refiner_remove_commentary),
        subtitle_mode=mode,  # type: ignore[arg-type]
        subtitle_langs=parse_subtitle_langs_csv(row.refiner_subtitle_langs_csv or ""),
        preserve_forced_subs=bool(row.refiner_preserve_forced_subs),
        preserve_default_subs=bool(row.refiner_preserve_default_subs),
        audio_preference_mode=pref,  # type: ignore[arg-type]
    )


def _safe_resolve_folder(raw: str) -> Path | None:
    s = (raw or "").strip()
    if not s:
        return None
    p = Path(s).expanduser()
    try:
        return p.resolve()
    except OSError:
        return p


def _pipeline_from_settings(row: AppSettings) -> tuple[Path, Path, Path] | tuple[None, None, None]:
    watched = _safe_resolve_folder(row.refiner_watched_folder or "")
    output = _safe_resolve_folder(row.refiner_output_folder or "")
    if watched is None or output is None:
        return None, None, None
    work = _safe_resolve_folder(row.refiner_work_folder or "")
    if work is None:
        work = db_path().parent / "refiner-work"
    return watched, output, work


def _gather_watched_files(watched_folder: Path) -> list[Path]:
    if not watched_folder.exists() or not watched_folder.is_dir():
        return []
    out: list[Path] = []
    try:
        for p in watched_folder.rglob("*"):
            if is_refiner_media_candidate(p):
                out.append(p)
    except OSError:
        return []
    out.sort(key=lambda x: str(x).lower())
    return out


async def run_scheduled_refiner_pass(session: AsyncSession) -> dict[str, Any]:
    row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
    if not row or not row.refiner_enabled:
        return {"ran": False, "reason": "disabled"}
    tz = row.timezone or "UTC"
    if not in_window(
        schedule_enabled=row.refiner_schedule_enabled,
        schedule_days=row.refiner_schedule_days or "",
        schedule_start=row.refiner_schedule_start or "00:00",
        schedule_end=row.refiner_schedule_end or "23:59",
        timezone=tz,
    ):
        return {"ran": False, "reason": "outside_schedule"}
    return await run_refiner_pass(session, trigger="scheduled")


async def run_refiner_pass(
    session: AsyncSession, *, trigger: Literal["scheduled"] = "scheduled"
) -> dict[str, Any]:
    """Run Refiner over configured paths. Serialised with an internal lock."""
    async with _refiner_lock:
        t_start = utc_now_naive()
        row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
        if not row or not row.refiner_enabled:
            return {"ok": True, "ran": False, "reason": "disabled"}
        cfg = _rules_config_from_settings(row)
        if cfg is None:
            return {"ok": True, "ran": False, "reason": "disabled"}
        if not normalize_lang(cfg.primary_audio_lang):
            logger.warning("Refiner: primary audio language is required when Refiner is enabled.")
            session.add(
                JobRunLog(
                    started_at=t_start,
                    finished_at=utc_now_naive(),
                    ok=False,
                    message=_refiner_job_log_text(
                        "Refiner: primary audio language is required — choose a language in Refiner settings (Audio)."
                    ),
                    app="refiner",
                )
            )
            await session.commit()
            return {"ok": False, "ran": False, "error": "primary_lang_required"}
        watched_root, output_root, work_dir = _pipeline_from_settings(row)
        if watched_root is None or output_root is None or work_dir is None:
            logger.warning("Refiner: watched folder and output folder are required when enabled.")
            session.add(
                JobRunLog(
                    started_at=t_start,
                    finished_at=utc_now_naive(),
                    ok=False,
                    message=_refiner_job_log_text(
                        "Refiner: watched folder and output folder must both be configured when Refiner is enabled."
                    ),
                    app="refiner",
                )
            )
            await session.commit()
            return {"ok": False, "ran": False, "error": "folders_required"}
        if not watched_root.exists() or not watched_root.is_dir():
            logger.warning("Refiner: watched folder is not a readable directory: %s", watched_root)
            session.add(
                JobRunLog(
                    started_at=t_start,
                    finished_at=utc_now_naive(),
                    ok=False,
                    message=_refiner_job_log_text(
                        f"Refiner: watched folder is missing or not a directory: {watched_root}"
                    ),
                    app="refiner",
                )
            )
            await session.commit()
            return {"ok": False, "ran": False, "error": "watched_folder_invalid"}
        if not output_root.exists() or not output_root.is_dir():
            logger.warning("Refiner: output folder is not a directory: %s", output_root)
            session.add(
                JobRunLog(
                    started_at=t_start,
                    finished_at=utc_now_naive(),
                    ok=False,
                    message=_refiner_job_log_text(
                        f"Refiner: output folder is missing or not a directory: {output_root}"
                    ),
                    app="refiner",
                )
            )
            await session.commit()
            return {"ok": False, "ran": False, "error": "output_folder_invalid"}
        files = _gather_watched_files(watched_root)
        dry = bool(row.refiner_dry_run)
        await _reconcile_interrupted_refiner_processing_rows_before_pass()
        # Clean orphaned temp files from prior crashed/failed remux passes
        if not dry and work_dir.is_dir():
            import time as _time_mod
            _stale_cutoff = _time_mod.time() - REFINER_FFMPEG_TIMEOUT_S
            try:
                for _wf in work_dir.iterdir():
                    if _wf.is_file() and ".refiner." in _wf.name:
                        try:
                            if _wf.stat().st_mtime < _stale_cutoff:
                                _wf.unlink()
                                logger.info("Refiner: removed stale work file %s", _wf.name)
                        except OSError as _we:
                            logger.warning("Refiner: could not remove stale work file %s (%s)", _wf.name, _we)
            except OSError:
                pass
        elif dry and work_dir.is_dir():
            logger.info("Refiner: dry-run — skipping stale work-file cleanup in %s", work_dir)
        if not files:
            logger.info("Refiner: watched folder has no supported media files — nothing to do.")
            session.add(
                JobRunLog(
                    started_at=t_start,
                    finished_at=utc_now_naive(),
                    ok=True,
                    message=_refiner_job_log_text("Refiner: watched folder has no supported media files."),
                    app="refiner",
                )
            )
            await session.commit()
            return {"ok": True, "ran": False, "reason": "no_files"}
        row.refiner_current_pass_total = len(files)
        row.refiner_current_pass_done = 0
        await session.commit()
        ok_c = dry_c = err_c = wait_c = cleanup_c = 0
        noop_c = 0  # log field unchanged=0 (no in-place “skipped” live passes without pipeline finalize)
        failure_notes: list[str] = []
        waiting_notes: list[str] = []
        waiting_reason_codes: list[str] = []
        cleanup_notes: list[str] = []
        for fp in files:
            t_job = time.perf_counter()
            snap_pre = await fetch_refiner_queue_snapshot(row)
            d0 = await decide_refiner_readiness(fp, row, snapshot=snap_pre, gate_tag="initial")
            if not d0.proceed:
                meta0 = _readiness_skip_meta(fp, d0, t_job)
                await _persist_refiner_activity_safe(meta0)
                rc0 = str(meta0.get("_refiner_reason_code") or "").strip().lower()
                line0 = format_per_file_job_log_line(
                    fp.name,
                    str(meta0.get("failure_hint") or ""),
                    reason_code=rc0,
                )
                if rc0 in _UPSTREAM_WAIT_REASON_CODES:
                    wait_c += 1
                    waiting_notes.append(line0)
                    waiting_reason_codes.append(rc0)
                else:
                    err_c += 1
                    failure_notes.append(line0)
                row.refiner_current_pass_done += 1
                try:
                    await session.commit()
                except Exception:
                    pass
                continue

            act_id = await _insert_refiner_processing_row(fp.name)
            status: str = "error"
            meta: dict[str, Any] | None = None
            try:
                snap_final = await fetch_refiner_queue_snapshot(row)
                d1 = await decide_refiner_readiness(fp, row, snapshot=snap_final, gate_tag="final")
                if not d1.proceed:
                    meta = _readiness_skip_meta(fp, d1, t_job)
                else:
                    wc_ctx: dict[str, Any] | None = None
                    if not dry and row.radarr_enabled and snap_final.authority_useful:
                        wc_ctx = await _movie_wrong_content_ctx_for_candidate(fp, row, snap_final)
                    status, meta = await asyncio.to_thread(
                        _process_one_refiner_file_sync,
                        fp,
                        cfg,
                        dry,
                        watched_root,
                        output_root,
                        work_dir,
                        wc_ctx,
                    )
                    action_payload = (meta or {}).pop("_radarr_wrong_content_actions", None)
                    if (
                        action_payload
                        and str((meta or {}).get("_refiner_reason_code") or "").strip().lower()
                        == "radarr_wrong_content"
                    ):
                        auto = await execute_radarr_wrong_content_actions(
                            row,
                            queue_id=action_payload.get("queue_id"),
                            movie_id=int(action_payload["movie_id"]),
                            dry_run=bool(action_payload.get("dry_run")),
                        )
                        ctxd = parse_activity_context(str((meta or {}).get("activity_context") or ""))
                        ctxd.pop("v", None)
                        errs = auto.get("errors") if isinstance(auto.get("errors"), list) else []
                        err_s = ";".join(str(x) for x in errs)[:500]
                        ctxd["radarr_wrong_content_automation"] = {
                            "queue_delete_ok": bool(auto.get("queue_delete_ok")),
                            "queue_delete_attempted": bool(auto.get("queue_delete_attempted")),
                            "queue_blocklist_requested": bool(auto.get("queue_blocklist_requested")),
                            "movies_search_ok": bool(auto.get("movies_search_ok")),
                            "dry_run": bool(auto.get("dry_run")),
                            "error_summary": err_s,
                        }
                        meta["activity_context"] = dumps_activity_context(ctxd)
                    fh0 = str((meta or {}).get("failure_hint") or "")
                    if status == "error" and ffprobe_failure_hint_is_read_analyze(fh0):
                        snap_post = await fetch_refiner_queue_snapshot(row)
                        blocked, rc_up, msg_up, up_diag_post = upstream_analyze_path(fp, snap_post)
                        strict_post = bool(
                            snap_post.authority_configured and not snap_post.authority_useful
                        )
                        relabeled = bool(blocked and meta is not None)
                        if relabeled:
                            meta["failure_hint"] = msg_up
                            meta["activity_context"] = _activity_snapshot(
                                failure_reason=msg_up[:8000],
                                reason_code=rc_up,
                            )
                            meta["_refiner_reason_code"] = rc_up
                        log_refiner_readiness_diagnostic(
                            gate_tag="post_ffprobe",
                            path=fp,
                            snap=snap_post,
                            up_diag=up_diag_post,
                            strict_file_fallback=strict_post,
                            decision_proceed=not blocked,
                            decision_reason_code=rc_up if blocked else "",
                            file_gate_ok=None,
                            file_gate_detail="",
                            extra={
                                "ffprobe_ran_before_upstream_recheck": True,
                                "outcome_relabeled_to_queue_wait": relabeled,
                            },
                        )
            except Exception:
                logger.exception(
                    "Refiner: unexpected failure for %s (thread error, cancellation, or timeout)",
                    fp.name,
                )
                sb_e = 0
                try:
                    sb_e = await asyncio.to_thread(_file_size_bytes, fp)
                except Exception:
                    pass
                meta = _failure_activity_meta(
                    fp.name, size_before=sb_e, audio_before=0, subs_before=0, t0=t_job
                )
                meta["failure_hint"] = (
                    "Unexpected error during processing (thread, timeout, or cancellation). "
                    "See the Fetcher log file for the full traceback."
                )
                meta.setdefault("_refiner_reason_code", "")
            finally:
                if meta is None:
                    sb_f = 0
                    try:
                        sb_f = await asyncio.to_thread(_file_size_bytes, fp)
                    except Exception:
                        pass
                    meta = _failure_activity_meta(
                        fp.name, size_before=sb_f, audio_before=0, subs_before=0, t0=t_job
                    )
                if meta.get("failure_hint") and not str(meta.get("activity_context") or "").strip():
                    meta["activity_context"] = _activity_snapshot(
                        failure_reason=str(meta["failure_hint"]).strip()[:8000]
                    )
                if act_id is not None:
                    await _update_refiner_activity_row(act_id, meta)
                else:
                    await _persist_refiner_activity_safe(meta)
            if status == "ok":
                ok_c += 1
            elif status == "dry_run":
                dry_c += 1
            elif status == "cleanup_needed":
                hint = (meta or {}).get("failure_hint") or "Post-finalize cleanup did not fully complete."
                rc_note = str((meta or {}).get("_refiner_reason_code") or "").strip().lower()
                line_c = format_per_file_job_log_line(fp.name, str(hint), reason_code=rc_note)
                cleanup_c += 1
                cleanup_notes.append(line_c)
            else:
                hint = (meta or {}).get("failure_hint") or "Processing failed."
                rc_note = str((meta or {}).get("_refiner_reason_code") or "").strip().lower()
                line_e = format_per_file_job_log_line(fp.name, str(hint), reason_code=rc_note)
                if rc_note in _UPSTREAM_WAIT_REASON_CODES:
                    wait_c += 1
                    waiting_notes.append(line_e)
                    waiting_reason_codes.append(rc_note)
                else:
                    err_c += 1
                    failure_notes.append(line_e)
            row.refiner_current_pass_done += 1
            try:
                await session.commit()
            except Exception:
                pass
        row.refiner_current_pass_total = 0
        row.refiner_current_pass_done = 0
        row.refiner_last_run_at = utc_now_naive()
        row.updated_at = utc_now_naive()
        detail = (
            f"Refiner ({trigger}): processed={ok_c} unchanged={noop_c} "
            f"dry_run_items={dry_c} waiting={wait_c} cleanup_needed={cleanup_c} errors={err_c}"
        )
        if wait_c > 0:
            reason_bits = ",".join(sorted(set(rc for rc in waiting_reason_codes if rc)))
            if reason_bits:
                detail = f"{detail} wait_reasons={reason_bits}"
        job_lines = [detail]
        if waiting_notes:
            job_lines.append("Per-file upstream waits:")
            for note in waiting_notes[:25]:
                job_lines.append(f"  · {note.replace(chr(10), ' — ')}")
        if cleanup_notes:
            job_lines.append("Per-file cleanup needed:")
            for note in cleanup_notes[:25]:
                job_lines.append(f"  · {note.replace(chr(10), ' — ')}")
        if failure_notes:
            job_lines.append("Per-file failures:")
            for note in failure_notes[:25]:
                job_lines.append(f"  · {note.replace(chr(10), ' — ')}")
        job_message = _refiner_job_log_text("\n".join(job_lines))
        session.add(
            JobRunLog(
                started_at=t_start,
                finished_at=utc_now_naive(),
                ok=(err_c == 0 and cleanup_c == 0),
                message=job_message,
                app="refiner",
            )
        )
        await session.commit()
        try:
            from app.scheduler import notify_dashboard_changed

            notify_dashboard_changed()
        except Exception:
            pass
        return {
            "ok": (err_c == 0 and cleanup_c == 0),
            "ran": True,
            "remuxed": ok_c,
            "unchanged": noop_c,
            "dry_run_items": dry_c,
            "waiting": wait_c,
            "cleanup_needed": cleanup_c,
            "errors": err_c,
        }
