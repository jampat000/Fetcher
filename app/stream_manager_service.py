from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ActivityLog, AppSettings
from app.schedule import in_window
from app.time_util import utc_now_naive
from app.stream_manager_mux import ffprobe_json, remux_to_temp_then_replace
from app.stream_manager_rules import (
    StreamManagerRulesConfig,
    collect_media_files_under_path,
    is_remux_required,
    normalize_lang,
    parse_path_lines,
    parse_subtitle_langs_csv,
    plan_remux,
    split_streams,
)

logger = logging.getLogger(__name__)

_stream_manager_lock = asyncio.Lock()


def _rules_config_from_settings(row: AppSettings) -> StreamManagerRulesConfig | None:
    if not row.stream_manager_enabled:
        return None
    slot = (row.stream_manager_default_audio_slot or "primary").strip().lower()
    if slot not in ("primary", "secondary", "tertiary"):
        slot = "primary"
    mode = (row.stream_manager_subtitle_mode or "remove_all").strip().lower()
    if mode not in ("remove_all", "keep_selected"):
        mode = "remove_all"
    return StreamManagerRulesConfig(
        primary_audio_lang=row.stream_manager_primary_audio_lang or "",
        secondary_audio_lang=row.stream_manager_secondary_audio_lang or "",
        tertiary_audio_lang=row.stream_manager_tertiary_audio_lang or "",
        default_audio_slot=slot,  # type: ignore[arg-type]
        remove_commentary=bool(row.stream_manager_remove_commentary),
        subtitle_mode=mode,  # type: ignore[arg-type]
        subtitle_langs=parse_subtitle_langs_csv(row.stream_manager_subtitle_langs_csv or ""),
        preserve_forced_subs=bool(row.stream_manager_preserve_forced_subs),
        preserve_default_subs=bool(row.stream_manager_preserve_default_subs),
    )


def _gather_files(row: AppSettings) -> list[Path]:
    lines = parse_path_lines(row.stream_manager_paths or "")
    acc: list[str] = []
    for line in lines:
        acc.extend(collect_media_files_under_path(line))
    seen: set[str] = set()
    out: list[Path] = []
    for f in acc:
        if f not in seen:
            seen.add(f)
            out.append(Path(f))
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
        logger.info("Stream Manager: dry-run: no file changes applied for %s", name)
        logger.info("Stream Manager: would keep audio: %s", kept_a or "(none)")
        if rem_a:
            logger.info("Stream Manager: would remove audio: %s", rem_a)
        logger.info("Stream Manager: would keep subtitles: %s", kept_s)
        if rem_s:
            logger.info("Stream Manager: would remove subtitles: %s", rem_s)
        return
    logger.info("Stream Manager: cleaned streams for %s", name)
    logger.info("Stream Manager: kept audio: %s", kept_a)
    if rem_a:
        logger.info("Stream Manager: removed audio: %s", rem_a)
    logger.info("Stream Manager: kept subtitles: %s", kept_s)
    if rem_s:
        logger.info("Stream Manager: removed subtitles: %s", rem_s)


def _process_one_sync(path: Path, cfg: StreamManagerRulesConfig, dry: bool) -> tuple[str, dict[str, Any]]:
    meta: dict[str, Any] = {"path": str(path)}
    try:
        probe = ffprobe_json(path)
    except Exception as e:
        logger.warning("Stream Manager: ffprobe failed for %s — %s", path.name, e)
        return "error", {**meta, "error": str(e)}
    video, audio, subs = split_streams(probe)
    if not audio:
        logger.warning("Stream Manager: no audio streams in %s", path.name)
        return "error", {**meta, "error": "no audio in source"}
    plan = plan_remux(video=video, audio=audio, subtitles=subs, config=cfg)
    if plan is None:
        logger.warning("Stream Manager: no audio would remain for %s — skipping", path.name)
        return "error", {**meta, "error": "no audio would remain"}
    if not is_remux_required(plan, audio, subs):
        return "noop", meta
    _log_plan_outcome(path=path, plan=plan, dry=dry)
    if dry:
        return "dry_run", meta
    try:
        remux_to_temp_then_replace(path, plan)
    except Exception as e:
        logger.error("Stream Manager: remux failed for %s — %s", path.name, e)
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
    """Run Stream Manager over configured paths. Serialised with an internal lock."""
    async with _stream_manager_lock:
        row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
        if not row or not row.stream_manager_enabled:
            return {"ok": True, "ran": False, "reason": "disabled"}
        cfg = _rules_config_from_settings(row)
        if cfg is None:
            return {"ok": True, "ran": False, "reason": "disabled"}
        if not normalize_lang(cfg.primary_audio_lang):
            logger.warning("Stream Manager: primary audio language is required when Stream Manager is enabled.")
            return {"ok": False, "ran": False, "error": "primary_lang_required"}
        files = _gather_files(row)
        if not files:
            logger.info("Stream Manager: no media paths configured — nothing to do.")
            return {"ok": True, "ran": False, "reason": "no_paths"}
        dry = bool(row.stream_manager_dry_run)
        ok_c = noop_c = dry_c = err_c = 0
        for fp in files:
            status, _meta = await asyncio.to_thread(_process_one_sync, fp, cfg, dry)
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
            f"Stream Manager ({trigger}): remuxed={ok_c} unchanged={noop_c} "
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
