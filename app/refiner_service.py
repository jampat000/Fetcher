from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient, ArrConfig
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
from app.refiner_arr_download_guard import refiner_path_blocked_by_arr_active_download
from app.refiner_promotion_gate import PromotionGateSyncResult, refiner_promotion_precheck
from app.refiner_source_readiness import check_source_readiness, log_readiness_skip_throttled
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

# One in-flight pipeline per pass (FIFO). Each iteration awaits one ``asyncio.to_thread(...)`` wrapping the full
# sync file handler (probe → remux/plan → finalize/delete/prune) before the next file starts.
# Live (non-dry) runs pass ``enter_finalizing`` so the activity row becomes ``finalizing`` when promotion/delete/prune
# begins. ``_refiner_lock`` prevents concurrent passes from overlapping scheduler ticks.
REFINER_PASS_MAX_CONCURRENT_FILES = 1

# Refiner source folders are dedicated release directories on the media server (not mixed user data).
# Residue cleanup is allow-list delete only — never “delete everything except …”.
#
# Preserved: final video payloads and external subtitle sidecars processed separately from the main file.
_REFINER_SOURCE_PRESERVED_SUFFIXES: frozenset[str] = frozenset(
    {".mkv", ".mp4", ".srt", ".ass", ".ssa", ".sub", ".idx"}
)
# Deleted after successful live completion (same release folder + shallow sample/), plus multipart ``.rNN``.
_REFINER_RELEASE_RESIDUE_SUFFIXES: frozenset[str] = frozenset(
    {".par", ".par2", ".nfo", ".sfv", ".srr", ".rar", ".zip", ".7z", ".txt", ".url"}
)


@dataclass(frozen=True)
class RefinerPromotionBridge:
    """Live Refiner pass: asyncio loop + optional *arr clients for pre-promotion gate + locking."""

    loop: asyncio.AbstractEventLoop
    sonarr: ArrClient | None
    radarr: ArrClient | None


def _optional_arr_client(url: str, api_key: str, enabled: bool) -> ArrClient | None:
    if not enabled:
        return None
    u, k = (url or "").strip(), (api_key or "").strip()
    if not u or not k:
        return None
    return ArrClient(ArrConfig(base_url=u, api_key=k))


def _release_import_promotion_locks(locks: tuple[threading.Lock, ...]) -> None:
    for lk in reversed(locks):
        try:
            lk.release()
        except RuntimeError:
            pass


def _is_multipart_rar_suffix(suffix: str) -> bool:
    s = suffix.lower()
    if len(s) < 4 or not s.startswith(".r"):
        return False
    return s[2:].isdigit()


def _is_release_residue_file(p: Path) -> bool:
    """True if this path is an allow-listed junk artefact (not preserved video/subtitle payload)."""
    suf = p.suffix.lower()
    if suf in _REFINER_SOURCE_PRESERVED_SUFFIXES:
        return False
    if suf in _REFINER_RELEASE_RESIDUE_SUFFIXES:
        return True
    return _is_multipart_rar_suffix(suf)


def _remove_safe_release_residue(*, release_dir: Path, watched_root: Path) -> list[str]:
    """Remove known release junk from the dedicated source folder after a successful live job.

    Policy: delete only extensions in ``_REFINER_RELEASE_RESIDUE_SUFFIXES`` and multipart RAR parts
    (``.r00``+). Preserve ``_REFINER_SOURCE_PRESERVED_SUFFIXES`` (video + external subtitles).
    Scope: this directory and shallow ``sample`` / ``samples`` only; output folder is never touched.
    """
    removed: list[str] = []
    try:
        w = watched_root.resolve()
        rd = release_dir.resolve()
        rd.relative_to(w)
    except (OSError, ValueError):
        return removed
    if not rd.is_dir():
        return removed
    for sample_name in ("sample", "samples"):
        sp = rd / sample_name
        if not sp.is_dir():
            continue
        if sp.name.lower() != sample_name:
            continue
        for child in list(sp.iterdir()):
            if child.is_file() and _is_release_residue_file(child):
                try:
                    child.unlink()
                    removed.append(f"{sample_name}/{child.name}")
                except OSError:
                    pass
        try:
            if not any(sp.iterdir()):
                sp.rmdir()
                removed.append(f"{sample_name}/")
        except OSError:
            pass
    for p in list(rd.iterdir()):
        if p.is_file() and _is_release_residue_file(p):
            try:
                p.unlink()
                removed.append(p.name)
            except OSError:
                pass
    return removed


def _prune_empty_ancestors_under_watch(start_dir: Path, watched_root: Path, *, max_hops: int = 24) -> str:
    """Walk upward from ``start_dir`` removing empty directories until the watch root."""
    try:
        w = watched_root.resolve()
        cur = start_dir.resolve()
    except OSError:
        return ""
    removed_any = False
    for _ in range(max_hops):
        if cur == w:
            break
        try:
            cur.relative_to(w)
        except ValueError:
            break
        if not cur.is_dir():
            try:
                cur = cur.parent
            except (OSError, ValueError):
                break
            continue
        try:
            if any(cur.iterdir()):
                break
            cur.rmdir()
            removed_any = True
            nxt = cur.parent
            cur = nxt
        except OSError:
            break
    return "removed_empty_ancestors" if removed_any else ""


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
    import_promotion_block: dict[str, Any] | None = None,
    residue_files_removed: list[str] | None = None,
    source_path: str = "",
    reason_code: str = "",
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
        "source_path": (source_path or "").strip()[:4000],
        "reason_code": (reason_code or "").strip().lower()[:120],
    }
    if no_change_bullets:
        payload["no_change_bullets"] = [str(x).strip()[:500] for x in no_change_bullets if str(x).strip()][:8]
    if import_promotion_block:
        ipb = dict(import_promotion_block)
        for k in list(ipb.keys()):
            if isinstance(ipb[k], str):
                ipb[k] = str(ipb[k]).strip()[:2000]
        payload["import_promotion_block"] = ipb
    if residue_files_removed:
        payload["residue_files_removed"] = [str(x).strip()[:260] for x in residue_files_removed if str(x).strip()][
            :200
        ]
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


def _norm_source_path_for_identity(path: Path | str | None) -> str:
    raw = str(path or "").strip()
    if not raw:
        return ""
    p = Path(raw).expanduser()
    try:
        return str(p.resolve())
    except OSError:
        return str(p)


def _activity_context_dict(raw: str | None) -> dict[str, Any]:
    t = (raw or "").strip()
    if not t:
        return {}
    try:
        d = json.loads(t)
    except Exception:
        return {}
    return d if isinstance(d, dict) else {}


def _activity_context_with_identity(
    raw: str | None,
    *,
    source_path: str,
    reason_code: str,
) -> str:
    payload = _activity_context_dict(raw)
    payload["source_path"] = (source_path or "").strip()[:4000]
    payload["reason_code"] = (reason_code or "").strip().lower()[:120]
    return dumps_activity_context(payload)


async def _active_refiner_source_paths() -> set[str]:
    """Paths currently in queued/processing/finalizing states (in-flight only)."""
    out: set[str] = set()
    async with SessionLocal() as session:
        rows = (
            await session.execute(
                select(RefinerActivity.file_name, RefinerActivity.activity_context).where(
                    RefinerActivity.status.in_(("queued", "processing", "finalizing"))
                )
            )
        ).all()
    for _file_name, activity_context in rows:
        ctx = _activity_context_dict(activity_context)
        src = _norm_source_path_for_identity(ctx.get("source_path"))
        if src:
            out.add(src)
    return out


def _reason_code_from_hint(hint: str) -> str:
    s = (hint or "").strip().lower()
    if not s:
        return "failed_unknown"
    slug = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    if not slug:
        return "failed_unknown"
    return f"failed_{slug[:96]}"


async def _has_prior_refiner_failure(*, source_path: str, reason_code: str) -> bool:
    """True when a terminal failure row with same path+reason already exists."""
    src = _norm_source_path_for_identity(source_path)
    rc = (reason_code or "").strip().lower()
    if not src or not rc:
        return False
    async with SessionLocal() as session:
        rows = (
            await session.execute(
                select(RefinerActivity.activity_context).where(
                    RefinerActivity.status.in_(("failed", "skipped_terminal_failed"))
                )
            )
        ).all()
    for (raw_ctx,) in rows:
        ctx = _activity_context_dict(raw_ctx)
        if _norm_source_path_for_identity(ctx.get("source_path")) != src:
            continue
        if str(ctx.get("reason_code") or "").strip().lower() == rc:
            return True
    return False


def _parent_actionable_reason(raw: str) -> str:
    s = (raw or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not s:
        return ""
    line = next((ln.strip() for ln in s.splitlines() if ln.strip()), "")
    if not line:
        return ""
    low = line.lower()
    if low.startswith("traceback") or line.startswith("{") or line.startswith("["):
        return ""
    if len(line) > 180:
        line = line[:177].rstrip() + "..."
    return line


def _pick_primary_actionable_reason(reasons: list[str]) -> str:
    cleaned: list[str] = []
    for r in reasons:
        cr = _parent_actionable_reason(r)
        if cr:
            cleaned.append(cr)
    if not cleaned:
        return ""
    # Prefer most common reason; tie-break by most recent occurrence in this run.
    counts: dict[str, int] = {}
    last_idx: dict[str, int] = {}
    for idx, reason in enumerate(cleaned):
        counts[reason] = counts.get(reason, 0) + 1
        last_idx[reason] = idx
    return max(counts.keys(), key=lambda r: (counts[r], last_idx[r]))


def _reason_from_meta_for_parent(meta: dict[str, Any] | None) -> str:
    if not isinstance(meta, dict):
        return ""
    hint = _parent_actionable_reason(str(meta.get("failure_hint") or ""))
    if hint:
        return hint
    ctx = _activity_context_dict(str(meta.get("activity_context") or ""))
    reason = _parent_actionable_reason(str(ctx.get("failure_reason") or ""))
    if reason:
        return reason
    ipb = ctx.get("import_promotion_block") if isinstance(ctx.get("import_promotion_block"), dict) else {}
    return _parent_actionable_reason(str(ipb.get("subtitle") or ""))


def _refiner_run_parent_summary(*, processed: int, blocked: int, failed: int, primary_reason: str = "") -> str:
    # Current run only (no history aggregation).
    if processed == 0 and blocked == 0 and failed == 0:
        return "No new actions — all items already processed or blocked"
    if failed == 0 and blocked == 0:
        noun = "file" if processed == 1 else "files"
        return f"{processed} {noun} processed"
    reason = _parent_actionable_reason(primary_reason)
    if processed > 0:
        base = f"{processed} processed · {blocked + failed} blocked"
        return f"{base} — {reason}" if reason else base
    if blocked + failed > 0:
        noun = "item" if (blocked + failed) == 1 else "items"
        base = f"{blocked + failed} {noun} needs attention"
        return f"{base} — {reason}" if reason else base
    return "No new actions — all items already processed or blocked"


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
    file_name: str,
    *,
    initial_status: Literal["queued", "processing"],
    source_path: str = "",
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
                activity_context=_activity_snapshot(
                    source_path=source_path,
                    reason_code="in_flight",
                ),
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


async def _set_refiner_pass_job_status(
    row_id: int | None, status: Literal["queued", "processing", "finalizing"]
) -> None:
    """Update in-pass job status (queued → processing → finalizing). Terminal rows use ``_update_refiner_activity_row``."""
    if row_id is None:
        return
    if status not in ("queued", "processing", "finalizing"):
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
        if st not in ("success", "skipped", "failed", "skipped_terminal_failed"):
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
        if st not in ("success", "skipped", "failed", "skipped_terminal_failed"):
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
                .where(RefinerActivity.status.in_(("processing", "queued", "finalizing")))
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


def _deferred_readiness_skip_meta(
    fp: Path,
    *,
    reason_code: str,
    operator_note: str,
    t0: float,
) -> dict[str, Any]:
    """Terminal ``skipped`` row when a queued file fails a pre-process or *arr-queue readiness recheck."""
    sb = 0
    try:
        if fp.is_file():
            sb = _file_size_bytes(fp)
    except Exception:
        pass
    src = _norm_source_path_for_identity(fp)[:4000]
    note = (operator_note or "").strip()[:500] or reason_code
    ctx = _activity_snapshot(
        reason_code=reason_code,
        source_path=src,
        no_change_bullets=[note],
    )
    return {
        "file_name": fp.name,
        "media_title": provisional_media_title_before_probe(fp.name)[:512],
        "status": "skipped",
        "size_before_bytes": sb,
        "size_after_bytes": sb,
        "audio_tracks_before": 0,
        "audio_tracks_after": 0,
        "subtitle_tracks_before": 0,
        "subtitle_tracks_after": 0,
        "processing_time_ms": int((time.perf_counter() - t0) * 1000),
        "activity_context": ctx,
        "source_path": src,
    }


def _failure_activity_meta(
    fname: str,
    *,
    source_path: str = "",
    reason_code: str = "failed_unknown",
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
        "reason_code": (reason_code or "failed_unknown").strip().lower()[:120],
        "source_path": _norm_source_path_for_identity(source_path)[:4000],
    }


def _process_one_refiner_file_sync(
    path: Path,
    cfg: RefinerRulesConfig,
    dry: bool,
    watched_root: Path,
    output_root: Path,
    work_dir: Path,
    *,
    enter_finalizing: Callable[[], None] | None = None,
    promotion_bridge: RefinerPromotionBridge | None = None,
) -> tuple[str, dict[str, Any]]:
    """One file in the worker thread: probe → plan → remux/stream work, then finalize (move/delete/prune).

    ``enter_finalizing`` is invoked once when remux/planning is done and only file promotion / cleanup remains.
    Dry-run paths never call it. Callbacks often hop to the asyncio loop (see ``run_refiner_pass``).
    ``promotion_bridge`` enables the *arr pre-promotion gate (terminal failed-import block + per-downloadId locks).
    """
    t0 = time.perf_counter()
    _finalize_phase_done: list[bool] = [False]

    def _enter_finalize_phase() -> None:
        if _finalize_phase_done[0] or enter_finalizing is None:
            return
        _finalize_phase_done[0] = True
        enter_finalizing()
    fname = path.name

    def _sync_promotion_gate(watch_path: Path) -> PromotionGateSyncResult:
        if dry or promotion_bridge is None:
            return PromotionGateSyncResult(True, (), None)
        fut = asyncio.run_coroutine_threadsafe(
            refiner_promotion_precheck(
                media_file=watch_path,
                sonarr_client=promotion_bridge.sonarr,
                radarr_client=promotion_bridge.radarr,
            ),
            promotion_bridge.loop,
        )
        return fut.result(timeout=180)

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

    _gate0 = check_source_readiness(path)
    if not _gate0.ready:
        sb_gate = 0
        try:
            if path.is_file():
                sb_gate = _file_size_bytes(path)
        except Exception:
            pass
        return "skipped_readiness", pack(
            "skipped",
            sb_gate,
            sb_gate,
            0,
            0,
            0,
            0,
            activity_context=_activity_snapshot(
                reason_code="skipped_final_readiness_gate",
                source_path=_norm_source_path_for_identity(path)[:4000],
                no_change_bullets=[(_gate0.operator_message or _gate0.code).strip()[:500]],
            ),
        )

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
        source_parent = path.parent
        gate_nr = _sync_promotion_gate(path)
        if not gate_nr.allowed and gate_nr.block_detail is not None:
            ipb = dict(gate_nr.block_detail)
            ipb["subtitle"] = "Not promoted — item classified as a failed import"
            ipb["reason_code"] = "skipped_terminal_failed"
            blocked_ctx = _activity_snapshot(
                ident=identity_snap,
                audio_before=ab_line,
                audio_after=ab_line,
                subs_before=subs_b_line,
                subs_after=subs_b_line,
                import_promotion_block=ipb,
            )
            return "blocked_import", pack(
                "skipped_terminal_failed",
                sb,
                sb,
                ab_len,
                ab_len,
                sbb_len,
                sbb_len,
                activity_context=blocked_ctx,
            )
        try:
            try:
                _enter_finalize_phase()
                _finalize_output_file(path, destination)
                res_names = _remove_safe_release_residue(
                    release_dir=source_parent, watched_root=watched_root
                )
                fold = _prune_empty_ancestors_under_watch(source_parent, watched_root)
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
                    residue_files_removed=res_names or None,
                )
                return "ok", pack(
                    "success", sb, sa, ab_len, ab_len, sbb_len, sbb_len, activity_context=ok_ctx
                )
            except Exception as e:
                fh = failure_hint_from_exception(e)
                summary, detail_ex = format_refiner_failure_for_operator(e)
                logger.error("Refiner: no-remux pipeline finalize failed for %s — %s", path.name, summary)
                if detail_ex:
                    logger.error("Refiner: reason for %s — %s", path.name, detail_ex)
                reason_body = summary if not detail_ex else f"{summary}\n  {detail_ex}"
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
        finally:
            _release_import_promotion_locks(gate_nr.held_locks)

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

    gate_mx = _sync_promotion_gate(path)
    if not gate_mx.allowed and gate_mx.block_detail is not None:
        if temp_file is not None:
            try:
                if temp_file.is_file():
                    temp_file.unlink()
            except OSError:
                logger.warning(
                    "Refiner: could not remove temp file after import gate block: %s",
                    temp_file,
                    exc_info=True,
                )
        ipb = dict(gate_mx.block_detail)
        ipb["subtitle"] = "Not promoted — item classified as a failed import"
        ipb["reason_code"] = "skipped_terminal_failed"
        blocked_ctx = _activity_snapshot(
            ident=identity_snap,
            audio_before=ab_line,
            audio_after=ab_line,
            subs_before=subs_b_line,
            subs_after=subtitle_after_line_from_plan(plan, remove_all=cfg.subtitle_mode == "remove_all"),
            import_promotion_block=ipb,
        )
        return "blocked_import", pack(
            "skipped_terminal_failed",
            sb,
            sb,
            ab_len,
            ab_len,
            sbb_len,
            sbb_len,
            activity_context=blocked_ctx,
        )

    try:
        _enter_finalize_phase()
        _finalize_output_file(temp_file, destination)
        source_parent = path.parent
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
        res_names = _remove_safe_release_residue(
            release_dir=source_parent, watched_root=watched_root
        )
        fold = _prune_empty_ancestors_under_watch(source_parent, watched_root)
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
            residue_files_removed=res_names or None,
        )
        return "ok", pack(
            "success", sb, sa, ab_len, aa, sbb_len, sba, activity_context=ok_ctx
        )
    except Exception as e:
        fh = failure_hint_from_exception(e)
        summary, detail_ex = format_refiner_failure_for_operator(e)
        logger.error("Refiner: processing failed for %s — %s", path.name, summary)
        if detail_ex:
            logger.error("Refiner: reason for %s — %s", path.name, detail_ex)
        if isinstance(e, RuntimeError) and "Output file appeared" in str(e):
            logger.info(
                "Refiner: action — another file appeared at the output path while remuxing %s; "
                "remove or rename it in the output folder, then retry.",
                path.name,
            )
        reason_body = summary if not detail_ex else f"{summary}\n  {detail_ex}"
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
    finally:
        _release_import_promotion_locks(gate_mx.held_locks)


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
        candidates = _gather_watched_files(watched_root)
        await _reconcile_interrupted_refiner_processing_rows_before_pass()
        active_paths = await _active_refiner_source_paths()
        if active_paths:
            candidates = [
                fp for fp in candidates if _norm_source_path_for_identity(fp) not in active_paths
            ]
        if not candidates:
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

        files: list[Path] = []
        skipped_readiness = 0
        for fp in candidates:
            rr = await asyncio.to_thread(check_source_readiness, fp)
            if not rr.ready:
                skipped_readiness += 1
                log_readiness_skip_throttled(fp, rr)
                continue
            files.append(fp)

        if not files:
            n = len(candidates)
            msg = (
                f"Refiner: {n} media file(s) in watch folder but none ready yet "
                "(still downloading, locked, or unstable — will retry on the next run)."
            )
            logger.info(msg)
            session.add(
                JobRunLog(
                    started_at=t_start,
                    finished_at=utc_now_naive(),
                    ok=True,
                    message=_refiner_job_log_text(msg),
                )
            )
            await session.commit()
            return {
                "ok": True,
                "ran": True,
                "reason": "no_ready_sources",
                "skipped_not_ready": skipped_readiness,
                "remuxed": 0,
                "unchanged": 0,
                "dry_run_items": 0,
                "errors": 0,
            }
        dry = bool(row.refiner_dry_run)
        ok_c = dry_c = err_c = import_blocked_c = 0
        noop_c = 0  # log field unchanged=0 (no in-place “skipped” live passes without pipeline finalize)
        prom_bridge: RefinerPromotionBridge | None = None
        if not dry:
            prom_bridge = RefinerPromotionBridge(
                loop=asyncio.get_running_loop(),
                sonarr=_optional_arr_client(row.sonarr_url or "", row.sonarr_api_key or "", row.sonarr_enabled),
                radarr=_optional_arr_client(row.radarr_url or "", row.radarr_api_key or "", row.radarr_enabled),
            )
            if prom_bridge.sonarr is None and prom_bridge.radarr is None:
                prom_bridge = None
        failure_notes: list[str] = []
        blocked_reasons: list[str] = []
        job_rows: list[tuple[Path, int | None]] = []
        for fp in files:
            act_id = await _insert_refiner_pass_job_row(
                fp.name,
                initial_status="queued",
                source_path=_norm_source_path_for_identity(fp),
            )
            job_rows.append((fp, act_id))
        loop = asyncio.get_running_loop()

        def _sync_finalizing_notifier(row_id: int | None) -> Callable[[], None] | None:
            if row_id is None:
                return None

            def _notify() -> None:
                fut = asyncio.run_coroutine_threadsafe(
                    _set_refiner_pass_job_status(int(row_id), "finalizing"), loop
                )
                fut.result(timeout=180)

            return _notify

        for fp, act_id in job_rows:
            t_precheck = time.perf_counter()
            rr2 = await asyncio.to_thread(check_source_readiness, fp)
            if not rr2.ready:
                log_readiness_skip_throttled(fp, rr2)
                if act_id is not None:
                    await _update_refiner_activity_row(
                        int(act_id),
                        _deferred_readiness_skip_meta(
                            fp,
                            reason_code="skipped_queue_recheck",
                            operator_note=rr2.operator_message or rr2.code,
                            t0=t_precheck,
                        ),
                    )
                noop_c += 1
                continue
            if prom_bridge and (prom_bridge.sonarr is not None or prom_bridge.radarr is not None):
                blocked_ad, ad_rc = await refiner_path_blocked_by_arr_active_download(
                    fp,
                    sonarr_client=prom_bridge.sonarr,
                    radarr_client=prom_bridge.radarr,
                )
                if blocked_ad:
                    if act_id is not None:
                        await _update_refiner_activity_row(
                            int(act_id),
                            _deferred_readiness_skip_meta(
                                fp,
                                reason_code=ad_rc,
                                operator_note=(
                                    "Sonarr/Radarr download queue still reports an active download "
                                    "for this file — Refiner will retry on a later pass."
                                ),
                                t0=t_precheck,
                            ),
                        )
                    noop_c += 1
                    continue

            t_job = time.perf_counter()
            await _set_refiner_pass_job_status(act_id, "processing")
            status: str = "error"
            meta: dict[str, Any] | None = None
            suppress_duplicate_failure = False
            try:
                enter_fn = None if dry else _sync_finalizing_notifier(act_id)
                sync_call = partial(
                    _process_one_refiner_file_sync,
                    fp,
                    cfg,
                    dry,
                    watched_root,
                    output_root,
                    work_dir,
                    enter_finalizing=enter_fn,
                    promotion_bridge=prom_bridge,
                )
                status, meta = await asyncio.to_thread(sync_call)
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
                    fp.name,
                    source_path=_norm_source_path_for_identity(fp),
                    size_before=sb_e,
                    audio_before=0,
                    subs_before=0,
                    t0=t_job,
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
                        fp.name,
                        source_path=_norm_source_path_for_identity(fp),
                        size_before=sb_f,
                        audio_before=0,
                        subs_before=0,
                        t0=t_job,
                    )
                src_identity = _norm_source_path_for_identity(fp)
                if not str(meta.get("source_path") or "").strip():
                    meta["source_path"] = src_identity
                st_meta = str(meta.get("status") or "").strip().lower()
                if st_meta in ("failed", "skipped_terminal_failed"):
                    existing_rc = str(meta.get("reason_code") or "").strip().lower()
                    if not existing_rc:
                        hint = str(meta.get("failure_hint") or meta.get("failure_reason") or "").strip()
                        if st_meta == "skipped_terminal_failed":
                            rc = "skipped_terminal_failed"
                        else:
                            rc = _reason_code_from_hint(hint)
                        meta["reason_code"] = rc
                if st_meta in ("failed", "skipped_terminal_failed"):
                    meta["activity_context"] = _activity_context_with_identity(
                        meta.get("activity_context"),
                        source_path=str(meta.get("source_path") or "")[:4000],
                        reason_code=str(meta.get("reason_code") or "")[:120],
                    )
                if meta.get("failure_hint") and not str(meta.get("activity_context") or "").strip():
                    meta["activity_context"] = _activity_snapshot(
                        failure_reason=str(meta["failure_hint"]).strip()[:8000],
                        source_path=str(meta.get("source_path") or "")[:4000],
                        reason_code=str(meta.get("reason_code") or "")[:120],
                    )
                suppress_duplicate_failure = False
                if st_meta in ("failed", "skipped_terminal_failed"):
                    rc_check = str(meta.get("reason_code") or "").strip().lower()
                    if rc_check and src_identity and await _has_prior_refiner_failure(
                        source_path=src_identity, reason_code=rc_check
                    ):
                        suppress_duplicate_failure = True
                if suppress_duplicate_failure:
                    if act_id is not None:
                        async with SessionLocal() as cleanup_session:
                            await cleanup_session.execute(
                                delete(RefinerActivity).where(RefinerActivity.id == int(act_id))
                            )
                            await cleanup_session.commit()
                else:
                    if act_id is not None:
                        await _update_refiner_activity_row(act_id, meta)
                    else:
                        await _persist_refiner_activity_safe(meta)
            if status == "ok":
                ok_c += 1
            elif status == "dry_run":
                dry_c += 1
            elif status == "skipped_readiness":
                noop_c += 1
            elif status == "blocked_import":
                import_blocked_c += 1
                r = _reason_from_meta_for_parent(meta)
                if r:
                    blocked_reasons.append(r)
            else:
                if suppress_duplicate_failure:
                    continue
                err_c += 1
                hint = (meta or {}).get("failure_hint") or "Processing failed."
                failure_notes.append(f"{fp.name}: {hint}")
                r = _reason_from_meta_for_parent(meta)
                if r:
                    blocked_reasons.append(r)
        row.refiner_last_run_at = utc_now_naive()
        row.updated_at = utc_now_naive()
        primary_reason = _pick_primary_actionable_reason(blocked_reasons)
        detail = _refiner_run_parent_summary(
            processed=ok_c + dry_c,
            blocked=import_blocked_c,
            failed=err_c,
            primary_reason=primary_reason,
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
        # Activity feed: skip parent row when this pass did nothing observable (no processed, blocked, or failed).
        refiner_meaningful_activity = (
            (ok_c + dry_c) > 0 or import_blocked_c > 0 or err_c > 0
        )
        if refiner_meaningful_activity:
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
            "skipped_not_ready": 0,
        }
