from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import db_path
from app.models import ActivityLog, AppSettings
from app.schedule import in_window
from app.time_util import utc_now_naive
from app.stream_manager_mux import ffprobe_json, remux_to_temp_file
from app.stream_manager_rules import (
    StreamManagerRulesConfig,
    is_remux_required,
    normalize_lang,
    parse_subtitle_langs_csv,
    plan_remux,
    split_streams,
)

logger = logging.getLogger(__name__)

_stream_manager_lock = asyncio.Lock()
_MEDIA_EXTENSIONS = frozenset({".mkv", ".mp4", ".m4v", ".webm", ".avi"})


def _rules_config_from_settings(row: AppSettings) -> StreamManagerRulesConfig | None:
    if not row.stream_manager_enabled:
        return None
    slot = (row.stream_manager_default_audio_slot or "primary").strip().lower()
    if slot not in ("primary", "secondary"):
        slot = "primary"
    mode = (row.stream_manager_subtitle_mode or "remove_all").strip().lower()
    if mode not in ("remove_all", "keep_selected"):
        mode = "remove_all"
    pref = (row.stream_manager_audio_preference_mode or "best_available").strip().lower()
    if pref not in (
        "best_available",
        "prefer_surround",
        "prefer_stereo",
        "prefer_lossless",
    ):
        pref = "best_available"
    return StreamManagerRulesConfig(
        primary_audio_lang=row.stream_manager_primary_audio_lang or "",
        secondary_audio_lang=row.stream_manager_secondary_audio_lang or "",
        default_audio_slot=slot,  # type: ignore[arg-type]
        remove_commentary=bool(row.stream_manager_remove_commentary),
        subtitle_mode=mode,  # type: ignore[arg-type]
        subtitle_langs=parse_subtitle_langs_csv(row.stream_manager_subtitle_langs_csv or ""),
        preserve_forced_subs=bool(row.stream_manager_preserve_forced_subs),
        preserve_default_subs=bool(row.stream_manager_preserve_default_subs),
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
    watched = _safe_resolve_folder(row.stream_manager_watched_folder or "")
    output = _safe_resolve_folder(row.stream_manager_output_folder or "")
    if watched is None or output is None:
        return None, None, None
    work = _safe_resolve_folder(row.stream_manager_work_folder or "")
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


def _process_one_sync(
    path: Path,
    cfg: StreamManagerRulesConfig,
    dry: bool,
    watched_root: Path,
    output_root: Path,
    work_dir: Path,
) -> tuple[str, dict[str, Any]]:
    """Per file: ffprobe analysis → rule planning from probe data → remux/validate/move (source delete only after success)."""
    meta: dict[str, Any] = {"path": str(path)}
    try:
        ffprobe_report = ffprobe_json(path)
    except Exception as e:
        logger.warning("Refiner: ffprobe failed for %s — %s", path.name, e)
        return "error", {**meta, "error": str(e)}
    video, audio, subs = split_streams(ffprobe_report)
    if not audio:
        logger.warning("Refiner: no audio streams in %s", path.name)
        return "error", {**meta, "error": "no audio in source"}
    plan = plan_remux(video=video, audio=audio, subtitles=subs, config=cfg)
    if plan is None:
        logger.warning("Refiner: no audio would remain for %s — skipping", path.name)
        return "error", {**meta, "error": "no audio would remain"}
    if not is_remux_required(plan, audio, subs):
        return "noop", meta
    _log_plan_outcome(path=path, plan=plan, dry=dry)
    destination = _output_path_for_source(src=path, watched_root=watched_root, output_root=output_root)
    if dry:
        logger.info("Refiner: dry-run: source preserved, no file changes applied (%s)", path.name)
        logger.info("Refiner: dry-run: would output to %s", destination)
        return "dry_run", meta
    if destination.exists():
        logger.error("Refiner: output already exists, refusing overwrite: %s", destination)
        return "error", {**meta, "error": "output_exists"}
    temp_file: Path | None = None
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_file = remux_to_temp_file(src=path, work_dir=work_dir, plan=plan)
        os.replace(temp_file, destination)
        path.unlink()
        logger.info("Refiner: processed file to output folder: %s", destination)
        logger.info("Refiner: source deleted after confirmed successful output: %s", path)
    except Exception as e:
        if temp_file is not None:
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except OSError:
                logger.warning("Refiner: could not clean work artifact %s", temp_file, exc_info=True)
        logger.error("Refiner: processing failed for %s — %s", path.name, e)
        return "error", {**meta, "error": str(e)}
    return "ok", meta


async def run_scheduled_stream_manager_pass(session: AsyncSession) -> dict[str, Any]:
    row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
    if not row or not row.stream_manager_enabled:
        return {"ran": False, "reason": "disabled"}
    tz = row.timezone or "UTC"
    if not in_window(
        schedule_enabled=row.stream_manager_schedule_enabled,
        schedule_days=row.stream_manager_schedule_days or "",
        schedule_start=row.stream_manager_schedule_start or "00:00",
        schedule_end=row.stream_manager_schedule_end or "23:59",
        timezone=tz,
    ):
        return {"ran": False, "reason": "outside_schedule"}
    return await run_stream_manager_pass(session, trigger="scheduled")


async def run_stream_manager_pass(
    session: AsyncSession, *, trigger: Literal["manual", "scheduled"]
) -> dict[str, Any]:
    """Run Refiner over configured paths. Serialised with an internal lock."""
    async with _stream_manager_lock:
        row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
        if not row or not row.stream_manager_enabled:
            return {"ok": True, "ran": False, "reason": "disabled"}
        cfg = _rules_config_from_settings(row)
        if cfg is None:
            return {"ok": True, "ran": False, "reason": "disabled"}
        if not normalize_lang(cfg.primary_audio_lang):
            logger.warning("Refiner: primary audio language is required when Refiner is enabled.")
            return {"ok": False, "ran": False, "error": "primary_lang_required"}
        watched_root, output_root, work_dir = _pipeline_from_settings(row)
        if watched_root is None or output_root is None or work_dir is None:
            logger.warning("Refiner: watched folder and output folder are required when enabled.")
            return {"ok": False, "ran": False, "error": "folders_required"}
        if not watched_root.exists() or not watched_root.is_dir():
            logger.warning("Refiner: watched folder is not a readable directory: %s", watched_root)
            return {"ok": False, "ran": False, "error": "watched_folder_invalid"}
        if not output_root.exists() or not output_root.is_dir():
            logger.warning("Refiner: output folder is not a directory: %s", output_root)
            return {"ok": False, "ran": False, "error": "output_folder_invalid"}
        files = _gather_watched_files(watched_root)
        if not files:
            logger.info("Refiner: watched folder has no supported media files — nothing to do.")
            return {"ok": True, "ran": False, "reason": "no_files"}
        dry = bool(row.stream_manager_dry_run)
        ok_c = noop_c = dry_c = err_c = 0
        for fp in files:
            status, _meta = await asyncio.to_thread(
                _process_one_sync, fp, cfg, dry, watched_root, output_root, work_dir
            )
            if status == "ok":
                ok_c += 1
            elif status == "noop":
                noop_c += 1
            elif status == "dry_run":
                dry_c += 1
            else:
                err_c += 1
        row.stream_manager_last_run_at = utc_now_naive()
        row.updated_at = utc_now_naive()
        detail = (
            f"Refiner ({trigger}): processed={ok_c} unchanged={noop_c} "
            f"dry_run_items={dry_c} errors={err_c}"
        )
        session.add(
            ActivityLog(
                app="stream_mgr",
                kind="stream",
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
