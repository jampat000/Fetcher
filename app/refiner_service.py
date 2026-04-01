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

from app.db import db_path
from app.models import ActivityLog, AppSettings, JobRunLog
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
from app.refiner_outcome_classify import format_per_file_job_log_line
from app.refiner_rules import (
    RefinerRulesConfig,
    is_refiner_media_candidate,
    normalize_audio_preference_mode,
    normalize_lang,
    parse_subtitle_langs_csv,
)
from app.refiner_source_readiness import (
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
                )
            )
            await session.commit()
            return {"ok": False, "ran": False, "error": "output_folder_invalid"}
        files = _gather_watched_files(watched_root)
        await _reconcile_interrupted_refiner_processing_rows_before_pass()
        # Clean orphaned temp files from prior crashed/failed remux passes
        if work_dir.is_dir():
            import time as _time_mod
            _stale_cutoff = _time_mod.time() - 3600  # 1 hour
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
        if not files:
            logger.info("Refiner: watched folder has no supported media files — nothing to do.")
            session.add(
                JobRunLog(
                    started_at=t_start,
                    finished_at=utc_now_naive(),
                    ok=True,
                    message=_refiner_job_log_text("Refiner: watched folder has no supported media files."),
                )
            )
            await session.commit()
            return {"ok": True, "ran": False, "reason": "no_files"}
        dry = bool(row.refiner_dry_run)
        ok_c = dry_c = err_c = wait_c = 0
        noop_c = 0  # log field unchanged=0 (no in-place “skipped” live passes without pipeline finalize)
        failure_notes: list[str] = []
        waiting_notes: list[str] = []
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
                else:
                    err_c += 1
                    failure_notes.append(line0)
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
                    status, meta = await asyncio.to_thread(
                        _process_one_refiner_file_sync, fp, cfg, dry, watched_root, output_root, work_dir
                    )
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
            else:
                hint = (meta or {}).get("failure_hint") or "Processing failed."
                rc_note = str((meta or {}).get("_refiner_reason_code") or "").strip().lower()
                line_e = format_per_file_job_log_line(fp.name, str(hint), reason_code=rc_note)
                if rc_note in _UPSTREAM_WAIT_REASON_CODES:
                    wait_c += 1
                    waiting_notes.append(line_e)
                else:
                    err_c += 1
                    failure_notes.append(line_e)
        row.refiner_last_run_at = utc_now_naive()
        row.updated_at = utc_now_naive()
        detail = (
            f"Refiner ({trigger}): processed={ok_c} unchanged={noop_c} "
            f"dry_run_items={dry_c} waiting={wait_c} errors={err_c}"
        )
        job_lines = [detail]
        if waiting_notes:
            job_lines.append("Per-file upstream waits:")
            for note in waiting_notes[:25]:
                job_lines.append(f"  · {note.replace(chr(10), ' — ')}")
        if failure_notes:
            job_lines.append("Per-file failures:")
            for note in failure_notes[:25]:
                job_lines.append(f"  · {note.replace(chr(10), ' — ')}")
        session.add(
            JobRunLog(
                started_at=t_start,
                finished_at=utc_now_naive(),
                ok=(err_c == 0),
                message=_refiner_job_log_text("\n".join(job_lines)),
            )
        )
        session.add(
            ActivityLog(
                app="refiner",
                kind="refiner",
                status="failed" if err_c > 0 else "ok",
                count=ok_c,
                detail=detail,
            )
        )
        await session.commit()
        return {
            "ok": err_c == 0,
            "ran": True,
            "remuxed": ok_c,
            "unchanged": noop_c,
            "dry_run_items": dry_c,
            "waiting": wait_c,
            "errors": err_c,
        }
