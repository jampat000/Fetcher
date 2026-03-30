from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import SessionLocal, db_path
from app.log_sanitize import redact_sensitive_text
from app.refiner_activity_context import dumps_activity_context
from app.refiner_errors import failure_hint_from_exception, format_refiner_failure_for_operator
from app.models import ActivityLog, AppSettings, JobRunLog, RefinerActivity
from app.schedule import in_window
from app.time_util import utc_now_naive
from app.refiner_media_identity import (
    MediaIdentity,
    provisional_media_title_before_probe,
    resolve_activity_card_title,
)
from app.refiner_mux import ffprobe_json, remux_to_temp_file
from app.refiner_rules import (
    RefinerRulesConfig,
    is_commentary_audio,
    is_remux_required,
    normalize_audio_preference_mode,
    normalize_lang,
    parse_subtitle_langs_csv,
    plan_remux,
    split_streams,
)
from app.refiner_track_display import (
    audio_after_line_from_plan,
    audio_before_line_from_probe,
    subtitle_after_line_from_plan,
    subtitle_before_line_from_probe,
)

logger = logging.getLogger(__name__)

_refiner_lock = asyncio.Lock()

# One in-flight pipeline per pass (FIFO). The inner loop awaits ``asyncio.to_thread(_process_one_refiner_file_sync)``
# for the full sync function — including finalize, source delete, and folder prune — before starting the next file.
# ``_refiner_lock`` serializes overlapping scheduler ticks so two passes never run concurrently.
REFINER_PASS_MAX_CONCURRENT_FILES = 1


def _activity_snapshot(
    *,
    ident: dict[str, str] | None = None,
    audio_before: str = "",
    audio_after: str = "",
    subs_before: str = "",
    subs_after: str = "",
    commentary_removed: bool = False,
    failure_reason: str = "",
    dry_run: bool = False,
    finalized: bool = False,
    source_removed: bool = False,
    folder_cleanup: str = "",
    pipeline_no_remux: bool = False,
    no_change_bullets: list[str] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "audio_before": audio_before,
        "audio_after": audio_after,
        "subs_before": subs_before,
        "subs_after": subs_after,
        "commentary_removed": bool(commentary_removed),
        "failure_reason": (failure_reason or "").strip()[:8000],
        "dry_run": bool(dry_run),
        "finalized": bool(finalized),
        "source_removed": bool(source_removed),
        "folder_cleanup": (folder_cleanup or "").strip()[:200],
        "pipeline_no_remux": bool(pipeline_no_remux),
    }
    if no_change_bullets:
        payload["no_change_bullets"] = [str(x).strip()[:500] for x in no_change_bullets if str(x).strip()][:8]
    idn = ident or {}
    for key in ("media_title", "refiner_title", "refiner_year", "trusted_title"):
        v = (idn.get(key) or "").strip()
        if v:
            payload[key] = v[:500] if key != "refiner_year" else v[:32]
    return dumps_activity_context(payload)
_MEDIA_EXTENSIONS = frozenset({".mkv", ".mp4", ".m4v", ".webm", ".avi"})
_REFINER_JOB_LOG_MAX_CHARS = 400_000


def _refiner_job_log_text(body: str) -> str:
    t = (body or "").strip()
    if len(t) > _REFINER_JOB_LOG_MAX_CHARS:
        t = t[: _REFINER_JOB_LOG_MAX_CHARS - 28] + "\n… (message truncated)"
    return redact_sensitive_text(t)


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
            if p.is_file() and p.suffix.lower() in _MEDIA_EXTENSIONS:
                out.append(p)
    except OSError:
        return []
    out.sort(key=lambda x: str(x).lower())
    return out


def _log_plan_outcome(*, path: Path, plan: Any, dry: bool) -> None:
    notes = getattr(plan, "audio_selection_notes", None) or []
    for line in notes:
        if dry:
            logger.info("Refiner: dry-run: audio selection (preview): %s", line)
        else:
            logger.info("Refiner: audio selection: %s", line)
    kept_a = ",".join(sorted({t.lang_label for t in plan.audio}))
    rem_a = ",".join(sorted({x for x in plan.removed_audio})) if plan.removed_audio else ""
    sub_parts: list[str] = []
    for t in plan.subtitles:
        lab = t.lang_label
        if t.forced:
            lab = f"{lab} (forced)"
        sub_parts.append(lab)
    kept_s = ", ".join(sub_parts) if sub_parts else "(none)"
    rem_s = ", ".join(sorted({x for x in plan.removed_subtitles})) if plan.removed_subtitles else ""
    name = path.name
    if dry:
        logger.info("Refiner: dry-run: no file changes applied for %s", name)
        logger.info("Refiner: would keep audio: %s", kept_a or "(none)")
        if rem_a:
            logger.info("Refiner: would remove audio: %s", rem_a)
        logger.info("Refiner: would keep subtitles: %s", kept_s)
        if rem_s:
            logger.info("Refiner: would remove subtitles: %s", rem_s)
        return
    logger.info("Refiner: cleaned streams for %s", name)
    logger.info("Refiner: kept audio: %s", kept_a)
    if rem_a:
        logger.info("Refiner: removed audio: %s", rem_a)
    logger.info("Refiner: kept subtitles: %s", kept_s)
    if rem_s:
        logger.info("Refiner: removed subtitles: %s", rem_s)


def _output_path_for_source(*, src: Path, watched_root: Path, output_root: Path) -> Path:
    rel = src.relative_to(watched_root)
    return output_root / rel


def _file_size_bytes(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return 0


def _finalize_output_file(src: Path, dst: Path) -> None:
    """Copy remux output from work/temp into the destination folder, then promote atomically on that volume.

    Stages into ``dst``'s parent (same drive as the final file) so promotion is always a same-volume
    ``os.rename``, which is reliable on Windows. Stream-copy from ``src`` supports work and output on
    different drives (no cross-volume rename/move of the finished bytes).
    """
    dst = dst.resolve()
    src = src.resolve()
    if dst.exists():
        if dst.is_dir():
            raise RuntimeError(f"Output path is a directory, not a file: {dst}")
        logger.error("Refiner finalize: output path already exists (refusing overwrite): %s", dst)
        raise RuntimeError(
            "Output file already exists at the destination — remove or rename it in the output folder, then retry."
        )
    dst.parent.mkdir(parents=True, exist_ok=True)
    partial = dst.with_name(f"{dst.name}.refiner-{uuid.uuid4().hex[:12]}.tmp")
    logger.info("Refiner finalize: src=%s dst=%s partial=%s", src, dst, partial)
    try:
        with open(partial, "wb") as out_f:
            with open(src, "rb") as in_f:
                shutil.copyfileobj(in_f, out_f, length=1024 * 1024)
            out_f.flush()
            os.fsync(out_f.fileno())
        try:
            shutil.copystat(src, partial, follow_symlinks=False)
        except OSError:
            logger.debug("Refiner finalize: copystat skipped for %s", partial, exc_info=True)
        if dst.exists():
            try:
                partial.unlink(missing_ok=True)
            except OSError:
                logger.warning("Refiner finalize: could not remove partial %s", partial, exc_info=True)
            logger.error("Refiner finalize: destination appeared before promote: %s", dst)
            raise RuntimeError(
                "Output file appeared while Refiner was working — another writer may have created it. "
                "Remove or rename the existing file in the output folder, then retry."
            ) from None
        try:
            os.replace(partial, dst)
        except FileExistsError:
            try:
                partial.unlink(missing_ok=True)
            except OSError:
                logger.warning("Refiner finalize: could not remove partial %s", partial, exc_info=True)
            raise RuntimeError(
                "Output file appeared while Refiner was working — another writer may have created it. "
                "Remove or rename the existing file in the output folder, then retry."
            ) from None
    except IsADirectoryError as e:
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            pass
        raise RuntimeError(f"Output path is a directory, not a file: {dst}") from e
    except Exception:
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            logger.warning("Refiner finalize: could not remove partial %s", partial, exc_info=True)
        raise
    try:
        src.unlink()
    except OSError as e:
        logger.warning(
            "Refiner finalize: output placed at %s but work temp %s could not be removed (%s). "
            "You may delete the work file manually.",
            dst,
            src,
            e,
        )
    logger.info("Refiner finalize: complete (destination finalized, work temp handled)")


def _try_remove_empty_watch_subfolder(*, source_parent: Path, watched_root: Path) -> str:
    """Remove the immediate parent of the source file only if it is empty and strictly inside watched_root.

    Does not walk up beyond one level. Returns a short token for activity context / support logs.
    """
    try:
        w = watched_root.resolve()
        parent = source_parent.resolve()
    except OSError as e:
        logger.info("Refiner folder cleanup: skipped (could not resolve paths: %s)", e)
        return "skipped_resolve"
    if parent == w:
        logger.info(
            "Refiner folder cleanup: skipped (source was directly under watch root: %s)",
            parent,
        )
        return "skipped_watch_root"
    try:
        parent.relative_to(w)
    except ValueError:
        logger.info(
            "Refiner folder cleanup: skipped (parent %s is not under watch root %s)",
            parent,
            w,
        )
        return "skipped_not_under_watch"
    if not parent.is_dir():
        logger.info("Refiner folder cleanup: skipped (not a directory: %s)", parent)
        return "skipped_not_dir"
    try:
        entries = list(parent.iterdir())
    except OSError as e:
        logger.warning("Refiner folder cleanup: skipped (could not list %s: %s)", parent, e)
        return "skipped_list_error"
    if entries:
        logger.info(
            "Refiner folder cleanup: skipped (folder not empty: %s has %s item(s))",
            parent,
            len(entries),
        )
        return "skipped_not_empty"
    try:
        parent.rmdir()
    except OSError as e:
        logger.warning("Refiner folder cleanup: failed to remove %s (%s)", parent, e)
        return "failed_rmdir"
    logger.info("Refiner folder cleanup: removed empty folder %s", parent)
    return "removed_empty_folder"


def _no_change_explanation_bullets(plan: Any, *, sbb_len: int, sba_len: int) -> list[str]:
    """Short operator-facing lines for no-remux outcomes (dry or live copy-only)."""
    bullets: list[str] = []
    notes = getattr(plan, "audio_selection_notes", None) or []
    for line in notes[:4]:
        t = (str(line) if line is not None else "").strip()
        if not t:
            continue
        bullets.append(t if t.endswith(".") else f"{t}.")
    if sbb_len == 0 and sba_len == 0:
        bullets.append("Subtitles: none present.")
    elif sbb_len > 0 and sbb_len == sba_len:
        bullets.append(f"Subtitles: {sbb_len} track(s) already match your rules.")
    if not bullets:
        bullets.append("Streams already match your current rules.")
    return bullets[:6]


async def _insert_refiner_pass_job_row(
    file_name: str, *, initial_status: Literal["queued", "processing"]
) -> int | None:
    """Insert one ``refiner_activity`` row. Passes use ``queued`` first; ``processing`` is for narrow call sites."""
    if initial_status not in ("queued", "processing"):
        initial_status = "queued"
    try:
        fn = str(file_name or "")[:512]
        prov = provisional_media_title_before_probe(fn)[:512]
        async with SessionLocal() as session:
            row = RefinerActivity(
                file_name=fn,
                media_title=prov,
                status=initial_status,
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
        logger.warning("Refiner: could not insert refiner_activity job row", exc_info=True)
        return None


async def _insert_refiner_processing_row(file_name: str) -> int | None:
    """Backward-compatible name: new passes insert ``queued`` rows first."""
    return await _insert_refiner_pass_job_row(file_name, initial_status="queued")


async def _set_refiner_pass_job_status(row_id: int | None, status: Literal["queued", "processing"]) -> None:
    """Move a pass job row between queued and active processing (no-op if ``row_id`` is None)."""
    if row_id is None:
        return
    if status not in ("queued", "processing"):
        return
    try:
        async with SessionLocal() as session:
            await session.execute(
                update(RefinerActivity)
                .where(RefinerActivity.id == int(row_id))
                .values(status=status)
            )
            await session.commit()
    except Exception:
        logger.warning("Refiner: could not set refiner_activity id=%s to %s", row_id, status, exc_info=True)


async def _update_refiner_activity_row(row_id: int, meta: dict[str, Any]) -> None:
    """Write terminal fields for a row created by ``_insert_refiner_pass_job_row`` (same job, no second row)."""
    try:
        fn = str(meta.get("file_name") or "")[:512]
        mt = str(meta.get("media_title") or "")[:512]
        st = str(meta.get("status") or "failed").strip().lower()
        if st not in ("success", "skipped", "failed"):
            st = "failed"
        ptm = meta.get("processing_time_ms")
        ptm_i = int(ptm) if ptm is not None else None
        async with SessionLocal() as session:
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
                    activity_context=str(meta.get("activity_context") or "")[:120_000],
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
        async with SessionLocal() as session:
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
                    activity_context=str(meta.get("activity_context") or "")[:120_000],
                )
            )
            await session.commit()
    except Exception:
        logger.warning("Refiner: could not persist refiner_activity row", exc_info=True)


async def _close_all_processing_refiner_activity_rows(*, context: str) -> None:
    """Set every in-flight pass row (``processing`` or ``queued``) to ``failed``."""
    try:
        async with SessionLocal() as session:
            res = await session.execute(
                update(RefinerActivity)
                .where(RefinerActivity.status.in_(("processing", "queued")))
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
    Start of each pass (under ``_refiner_lock``), before enqueueing new rows: mark any ``processing``
    or ``queued`` rows as ``failed``. Those states only belong to an interrupted prior pass — nothing
    in the new pass is enqueued yet.
    """
    await _close_all_processing_refiner_activity_rows(
        context="new pass — closing rows left processing before this run inserts new ones",
    )


def _failure_activity_meta(
    fname: str,
    *,
    size_before: int,
    audio_before: int,
    subs_before: int,
    t0: float,
) -> dict[str, Any]:
    """Activity row payload when processing aborts outside ``_process_one_refiner_file_sync``."""
    return {
        "file_name": fname,
        "media_title": provisional_media_title_before_probe(fname)[:512],
        "status": "failed",
        "size_before_bytes": int(size_before),
        "size_after_bytes": int(size_before),
        "audio_tracks_before": int(audio_before),
        "audio_tracks_after": int(audio_before),
        "subtitle_tracks_before": int(subs_before),
        "subtitle_tracks_after": int(subs_before),
        "processing_time_ms": int((time.perf_counter() - t0) * 1000),
    }


def _process_one_refiner_file_sync(
    path: Path,
    cfg: RefinerRulesConfig,
    dry: bool,
    watched_root: Path,
    output_root: Path,
    work_dir: Path,
) -> tuple[str, dict[str, Any]]:
    """One file end-to-end in the calling thread: probe → plan → remux or copy → finalize → delete/prune when applicable."""
    t0 = time.perf_counter()
    fname = path.name
    _provisional_mt = provisional_media_title_before_probe(fname)[:512]
    identity_snap: dict[str, str] = {}
    _media_title_col: list[str] = [""]

    def pack(
        act_status: str,
        sb: int,
        sa: int,
        ab: int,
        aa: int,
        sbb: int,
        sba: int,
        *,
        failure_hint: str | None = None,
        activity_context: str | None = None,
    ) -> dict[str, Any]:
        d: dict[str, Any] = {
            "file_name": fname,
            "media_title": (_media_title_col[0] or _provisional_mt)[:512],
            "status": act_status,
            "size_before_bytes": int(sb),
            "size_after_bytes": int(sa),
            "audio_tracks_before": int(ab),
            "audio_tracks_after": int(aa),
            "subtitle_tracks_before": int(sbb),
            "subtitle_tracks_after": int(sba),
            "processing_time_ms": int((time.perf_counter() - t0) * 1000),
        }
        if failure_hint:
            d["failure_hint"] = failure_hint
        if activity_context is not None and str(activity_context).strip() != "":
            d["activity_context"] = str(activity_context)[:120_000]
        elif failure_hint:
            d["activity_context"] = _activity_snapshot(
                ident=identity_snap,
                failure_reason=str(failure_hint).strip()[:8000],
            )
        else:
            d["activity_context"] = ""
        return d

    if not path.is_file():
        logger.warning("Refiner: source is missing or not a regular file — %s", path)
        return "error", pack(
            "failed",
            0,
            0,
            0,
            0,
            0,
            0,
            failure_hint="Source file is missing or not a regular file.",
            activity_context=_activity_snapshot(
                ident=identity_snap,
                failure_reason="Source file is missing or not a regular file.",
            ),
        )

    sb0 = _file_size_bytes(path)
    try:
        ffprobe_report = ffprobe_json(path)
    except Exception as e:
        logger.warning(
            "Refiner: could not read or probe media (ffprobe) for %s — %s",
            path.name,
            e,
        )
        fh = (
            failure_hint_from_exception(e)
            if isinstance(e, OSError)
            else f"Could not read or analyze the file. Reason: {e}"
        )
        return "error", pack(
            "failed",
            sb0,
            sb0,
            0,
            0,
            0,
            0,
            failure_hint=fh,
            activity_context=_activity_snapshot(ident=identity_snap, failure_reason=fh),
        )

    video, audio, subs = split_streams(ffprobe_report)
    ident = MediaIdentity.from_ffprobe(ffprobe_report)
    identity_snap.clear()
    identity_snap.update(ident.snapshot_identity_fields())
    _media_title_col[0] = resolve_activity_card_title(
        fname,
        {k: v for k, v in identity_snap.items() if k == "trusted_title"},
        orm_media_title="",
        ffprobe_media_title=ident.media_title,
        ffprobe_refiner_title=ident.refiner_title,
        ffprobe_year=ident.refiner_year,
    )
    sb = _file_size_bytes(path)
    ab_len = len(audio)
    sbb_len = len(subs)
    if not audio:
        logger.warning("Refiner: no audio streams in %s", path.name)
        subs_b_line = subtitle_before_line_from_probe(subs)
        fn0 = "No audio streams — Refiner cannot produce a valid output."
        return "error", pack(
            "failed",
            sb,
            sb,
            0,
            0,
            sbb_len,
            sbb_len,
            failure_hint=fn0,
            activity_context=_activity_snapshot(
                ident=identity_snap,
                subs_before=subs_b_line,
                failure_reason=fn0,
            ),
        )

    plan = plan_remux(video=video, audio=audio, subtitles=subs, config=cfg)
    ab_line = audio_before_line_from_probe(audio)
    subs_b_line = subtitle_before_line_from_probe(subs)
    if plan is None:
        logger.warning("Refiner: no audio would remain for %s — skipping", path.name)
        fn1 = "No audio track would remain after applying your rules."
        return "error", pack(
            "failed",
            sb,
            sb,
            ab_len,
            0,
            sbb_len,
            sbb_len,
            failure_hint=fn1,
            activity_context=_activity_snapshot(
                ident=identity_snap,
                audio_before=ab_line,
                subs_before=subs_b_line,
                failure_reason=fn1,
            ),
        )

    if not is_remux_required(plan, audio, subs):
        nc_bullets = _no_change_explanation_bullets(plan, sbb_len=sbb_len, sba_len=sbb_len)
        if dry:
            _log_plan_outcome(path=path, plan=plan, dry=True)
            return "dry_run", pack(
                "skipped",
                sb,
                sb,
                ab_len,
                ab_len,
                sbb_len,
                sbb_len,
                activity_context=_activity_snapshot(
                    ident=identity_snap,
                    audio_before=ab_line,
                    audio_after=ab_line,
                    subs_before=subs_b_line,
                    subs_after=subs_b_line,
                    dry_run=True,
                    no_change_bullets=nc_bullets,
                ),
            )

        destination = _output_path_for_source(src=path, watched_root=watched_root, output_root=output_root)
        if destination.exists():
            if destination.is_dir():
                logger.error("Refiner: output path is a directory (expected a file path) — %s", destination)
                fn_nd = "Output path is a directory, not a file — check folder layout under the output root."
                return "error", pack(
                    "failed",
                    sb,
                    sb,
                    ab_len,
                    ab_len,
                    sbb_len,
                    sbb_len,
                    failure_hint=fn_nd,
                    activity_context=_activity_snapshot(
                        ident=identity_snap,
                        audio_before=ab_line,
                        audio_after=ab_line,
                        subs_before=subs_b_line,
                        subs_after=subs_b_line,
                        failure_reason=fn_nd,
                    ),
                )
            logger.error("Refiner: output already exists, refusing overwrite: %s", destination)
            fn_ne = "Output file already exists — remove or rename it in the output folder, then retry."
            return "error", pack(
                "failed",
                sb,
                sb,
                ab_len,
                ab_len,
                sbb_len,
                sbb_len,
                failure_hint=fn_ne,
                activity_context=_activity_snapshot(
                    ident=identity_snap,
                    audio_before=ab_line,
                    audio_after=ab_line,
                    subs_before=subs_b_line,
                    subs_after=subs_b_line,
                    failure_reason=fn_ne,
                ),
            )

        _log_plan_outcome(path=path, plan=plan, dry=False)
        try:
            source_parent = path.parent
            _finalize_output_file(path, destination)
            fold = _try_remove_empty_watch_subfolder(source_parent=source_parent, watched_root=watched_root)
            sa = _file_size_bytes(destination)
            ok_ctx = _activity_snapshot(
                ident=identity_snap,
                audio_before=ab_line,
                audio_after=ab_line,
                subs_before=subs_b_line,
                subs_after=subs_b_line,
                finalized=True,
                source_removed=True,
                folder_cleanup=fold,
                pipeline_no_remux=True,
                no_change_bullets=nc_bullets,
            )
            return "ok", pack(
                "success", sb, sa, ab_len, ab_len, sbb_len, sbb_len, activity_context=ok_ctx
            )
        except Exception as e:
            fh = failure_hint_from_exception(e)
            summary, detail = format_refiner_failure_for_operator(e)
            logger.error("Refiner: no-remux pipeline finalize failed for %s — %s", path.name, summary)
            if detail:
                logger.error("Refiner: reason for %s — %s", path.name, detail)
            reason_body = summary if not detail else f"{summary}\n  {detail}"
            err_ctx = _activity_snapshot(
                ident=identity_snap,
                audio_before=ab_line,
                audio_after=ab_line,
                subs_before=subs_b_line,
                subs_after=subs_b_line,
                failure_reason=reason_body.strip()[:8000],
            )
            return "error", pack(
                "failed",
                sb,
                sb,
                ab_len,
                ab_len,
                sbb_len,
                sbb_len,
                failure_hint=fh,
                activity_context=err_ctx,
            )

    _log_plan_outcome(path=path, plan=plan, dry=dry)
    destination = _output_path_for_source(src=path, watched_root=watched_root, output_root=output_root)
    if dry:
        logger.info("Refiner: dry-run: source preserved, no file changes applied (%s)", path.name)
        logger.info("Refiner: dry-run: would output to %s", destination)
        subs_dry_after = subtitle_after_line_from_plan(plan, remove_all=cfg.subtitle_mode == "remove_all")
        return "dry_run", pack(
            "skipped",
            sb,
            sb,
            ab_len,
            ab_len,
            sbb_len,
            sbb_len,
            activity_context=_activity_snapshot(
                ident=identity_snap,
                audio_before=ab_line,
                audio_after=ab_line,
                subs_before=subs_b_line,
                subs_after=subs_dry_after,
                dry_run=True,
            ),
        )

    if not path.is_file():
        logger.warning("Refiner: source disappeared before remux — %s", path)
        fn2 = "Source file disappeared from the watch folder before remux could start."
        return "error", pack(
            "failed",
            sb,
            sb,
            ab_len,
            ab_len,
            sbb_len,
            sbb_len,
            failure_hint=fn2,
            activity_context=_activity_snapshot(
                ident=identity_snap,
                audio_before=ab_line,
                audio_after=ab_line,
                subs_before=subs_b_line,
                subs_after=subtitle_after_line_from_plan(plan, remove_all=cfg.subtitle_mode == "remove_all"),
                failure_reason=fn2,
            ),
        )

    if destination.exists():
        if destination.is_dir():
            logger.error("Refiner: output path is a directory (expected a file path) — %s", destination)
            fn3 = "Output path is a directory, not a file — check folder layout under the output root."
            return "error", pack(
                "failed",
                sb,
                sb,
                ab_len,
                ab_len,
                sbb_len,
                sbb_len,
                failure_hint=fn3,
                activity_context=_activity_snapshot(
                    ident=identity_snap,
                    audio_before=ab_line,
                    audio_after=ab_line,
                    subs_before=subs_b_line,
                    subs_after=subtitle_after_line_from_plan(plan, remove_all=cfg.subtitle_mode == "remove_all"),
                    failure_reason=fn3,
                ),
            )
        logger.error("Refiner: output already exists, refusing overwrite: %s", destination)
        logger.info(
            "Refiner: action — remove or rename the existing output file, then run Refiner again (%s)",
            destination,
        )
        fn4 = "Output file already exists — remove or rename it in the output folder, then retry."
        return "error", pack(
            "failed",
            sb,
            sb,
            ab_len,
            ab_len,
            sbb_len,
            sbb_len,
            failure_hint=fn4,
            activity_context=_activity_snapshot(
                ident=identity_snap,
                audio_before=ab_line,
                audio_after=ab_line,
                subs_before=subs_b_line,
                subs_after=subtitle_after_line_from_plan(plan, remove_all=cfg.subtitle_mode == "remove_all"),
                failure_reason=fn4,
            ),
        )

    temp_file: Path | None = None
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_file = remux_to_temp_file(src=path, work_dir=work_dir, plan=plan)
        _finalize_output_file(temp_file, destination)
        try:
            path.unlink()
        except OSError as u_err:
            logger.warning(
                "Refiner: wrote output to %s but could not delete the watched file (%s). "
                "Remove the original manually if you no longer need it: %s",
                destination,
                u_err,
                path,
            )
        fold = _try_remove_empty_watch_subfolder(source_parent=path.parent, watched_root=watched_root)
        logger.info("Refiner: output written to %s (watched file: %s)", destination, path.name)
        sa = _file_size_bytes(destination)
        aa = len(plan.audio)
        sba = len(plan.subtitles)
        subs_after = subtitle_after_line_from_plan(plan, remove_all=cfg.subtitle_mode == "remove_all")
        commentary_removed = bool(cfg.remove_commentary) and any(is_commentary_audio(s) for s in audio)
        ok_ctx = _activity_snapshot(
            ident=identity_snap,
            audio_before=ab_line,
            audio_after=audio_after_line_from_plan(plan),
            subs_before=subs_b_line,
            subs_after=subs_after,
            commentary_removed=commentary_removed,
            finalized=True,
            source_removed=True,
            folder_cleanup=fold,
        )
        return "ok", pack(
            "success", sb, sa, ab_len, aa, sbb_len, sba, activity_context=ok_ctx
        )
    except Exception as e:
        if temp_file is not None:
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except OSError:
                logger.warning("Refiner: could not clean work artifact %s", temp_file, exc_info=True)
        fh = failure_hint_from_exception(e)
        summary, detail = format_refiner_failure_for_operator(e)
        logger.error("Refiner: processing failed for %s — %s", path.name, summary)
        if detail:
            logger.error("Refiner: reason for %s — %s", path.name, detail)
        if isinstance(e, RuntimeError) and "Output file appeared" in str(e):
            logger.info(
                "Refiner: action — another file appeared at the output path while remuxing %s; "
                "remove or rename it in the output folder, then retry.",
                path.name,
            )
        reason_body = summary if not detail else f"{summary}\n  {detail}"
        err_ctx = _activity_snapshot(
            ident=identity_snap,
            audio_before=ab_line,
            audio_after=ab_line,
            subs_before=subs_b_line,
            subs_after=subtitle_after_line_from_plan(plan, remove_all=cfg.subtitle_mode == "remove_all"),
            failure_reason=reason_body.strip()[:8000],
        )
        return "error", pack(
            "failed",
            sb,
            sb,
            ab_len,
            ab_len,
            sbb_len,
            sbb_len,
            failure_hint=fh,
            activity_context=err_ctx,
        )


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
    """Run Refiner over the watch folder. Entire pass holds ``_refiner_lock``; files run strictly in order."""
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
        ok_c = dry_c = err_c = 0
        noop_c = 0  # log field unchanged=0 (no in-place “skipped” live passes without pipeline finalize)
        failure_notes: list[str] = []
        job_rows: list[tuple[Path, int | None]] = []
        for fp in files:
            act_id = await _insert_refiner_pass_job_row(fp.name, initial_status="queued")
            job_rows.append((fp, act_id))
        for fp, act_id in job_rows:
            t_job = time.perf_counter()
            await _set_refiner_pass_job_status(act_id, "processing")
            status: str = "error"
            meta: dict[str, Any] | None = None
            try:
                status, meta = await asyncio.to_thread(
                    _process_one_refiner_file_sync, fp, cfg, dry, watched_root, output_root, work_dir
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
                err_c += 1
                hint = (meta or {}).get("failure_hint") or "Processing failed."
                failure_notes.append(f"{fp.name}: {hint}")
        row.refiner_last_run_at = utc_now_naive()
        row.updated_at = utc_now_naive()
        detail = (
            f"Refiner ({trigger}): processed={ok_c} unchanged={noop_c} "
            f"dry_run_items={dry_c} errors={err_c}"
        )
        job_lines = [detail]
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
                status="ok" if err_c == 0 else "failed",
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
            "errors": err_c,
        }
