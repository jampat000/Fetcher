"""Refiner per-file pipeline: probe, plan, remux, finalize, activity context snapshots."""

from __future__ import annotations

import logging
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

from app.refiner_activity_context import dumps_activity_context
from app.refiner_cleanup import (
    _cleanup_refiner_source_sidecar_artifacts_after_success,
    _try_remove_empty_watch_subfolder,
)
from app.refiner_errors import failure_hint_from_exception, format_refiner_failure_for_operator
from app.refiner_media_identity import (
    MediaIdentity,
    provisional_media_title_before_probe,
    resolve_activity_card_title,
)
from app.refiner_movie_wrong_content import evaluate_movie_wrong_content
from app.refiner_mux import ffprobe_json, remux_to_temp_file
from app.refiner_rules import (
    RefinerRulesConfig,
    is_commentary_audio,
    is_remux_required,
    plan_remux,
    split_streams,
)
from app.refiner_source_readiness import RefinerReadinessDecision, derive_title_fallback_candidate
from app.refiner_track_display import (
    audio_after_line_from_plan,
    audio_before_line_from_probe,
    subtitle_after_line_from_plan,
    subtitle_before_line_from_probe,
)

logger = logging.getLogger(__name__)


def _activity_snapshot(
    *,
    ident: dict[str, str] | None = None,
    audio_before: str = "",
    audio_after: str = "",
    subs_before: str = "",
    subs_after: str = "",
    commentary_removed: bool = False,
    failure_reason: str = "",
    reason_code: str = "",
    dry_run: bool = False,
    finalized: bool = False,
    source_removed: bool = False,
    folder_cleanup: str = "",
    pipeline_no_remux: bool = False,
    no_change_bullets: list[str] | None = None,
    wrong_content: bool = False,
    radarr_wrong_content_verdict: dict[str, Any] | None = None,
    radarr_wrong_content_automation: dict[str, Any] | None = None,
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
    rc = (reason_code or "").strip()
    if rc:
        payload["reason_code"] = rc[:128]
    if no_change_bullets:
        payload["no_change_bullets"] = [str(x).strip()[:500] for x in no_change_bullets if str(x).strip()][:8]
    if wrong_content:
        payload["wrong_content"] = True
    if isinstance(radarr_wrong_content_verdict, dict) and radarr_wrong_content_verdict:
        payload["radarr_wrong_content_verdict"] = radarr_wrong_content_verdict
    if isinstance(radarr_wrong_content_automation, dict) and radarr_wrong_content_automation:
        payload["radarr_wrong_content_automation"] = radarr_wrong_content_automation
    idn = ident or {}
    for key in ("media_title", "refiner_title", "refiner_year", "trusted_title"):
        v = (idn.get(key) or "").strip()
        if v:
            payload[key] = v[:500] if key != "refiner_year" else v[:32]
    return dumps_activity_context(payload)


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


def _readiness_skip_meta(fp: Path, decision: RefinerReadinessDecision, t0: float) -> dict[str, Any]:
    sb = _file_size_bytes(fp)
    msg = (decision.operator_message or "").strip() or "Source not ready yet."
    meta = _failure_activity_meta(fp.name, size_before=sb, audio_before=0, subs_before=0, t0=t0)
    title_raw, title_src = derive_title_fallback_candidate(fp)
    if title_src == "parent_folder" and title_raw.strip():
        meta["media_title"] = title_raw[:512]
    meta["failure_hint"] = msg
    meta["activity_context"] = _activity_snapshot(
        failure_reason=msg[:8000],
        reason_code=decision.reason_code,
    )
    meta["_refiner_reason_code"] = decision.reason_code
    return meta


def _process_one_refiner_file_sync(
    path: Path,
    cfg: RefinerRulesConfig,
    dry: bool,
    watched_root: Path,
    output_root: Path,
    work_dir: Path,
    movie_wrong_content_ctx: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Per file: ffprobe analysis → rule planning from probe data → remux/validate/move (source delete only after success)."""
    t0 = time.perf_counter()
    fname = path.name
    _provisional_mt = provisional_media_title_before_probe(fname)[:512]
    identity_snap: dict[str, str] = {}
    _media_title_col: list[str] = [""]
    _wc_failure_lines = (
        "Wrong content detected",
        "This file does not appear to match the selected movie. The release was failed and blocked in Radarr, and a new search was requested.",
    )

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
        if isinstance(e, OSError):
            fh = failure_hint_from_exception(e)
        else:
            detail = str(e).strip()
            compact = detail.replace(" ", "")
            if not detail or detail in ("{}", "[]", "None", "null") or compact in ("{}", "[]"):
                fh = "Could not read or analyze the file."
            else:
                fh = f"Could not read or analyze the file. Reason: {detail[:2000]}"
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
    if (
        movie_wrong_content_ctx
        and movie_wrong_content_ctx.get("enabled")
        and isinstance(movie_wrong_content_ctx.get("movie_id"), int)
        and int(movie_wrong_content_ctx["movie_id"]) > 0
    ):
        wc = evaluate_movie_wrong_content(
            path,
            ffprobe_report,
            video,
            target_title=str(movie_wrong_content_ctx.get("target_title") or ""),
            target_year=movie_wrong_content_ctx.get("target_year"),
            expected_runtime_minutes=movie_wrong_content_ctx.get("expected_runtime_minutes"),
        )
        if wc.wrong_content:
            fh_full = "\n".join(_wc_failure_lines)
            verdict_payload: dict[str, Any] = {
                "triggered_reason": (wc.triggered_reason or "")[:500],
                "score": int(wc.score),
                "hard_trigger": bool(wc.hard_trigger),
                "probed_runtime_minutes": wc.probed_runtime_minutes,
                "expected_runtime_minutes": wc.expected_runtime_minutes,
                "runtime_ratio": wc.runtime_ratio,
                "token_overlap_summary": (wc.token_overlap_summary or "")[:400],
            }
            wctx = _activity_snapshot(
                ident=identity_snap,
                failure_reason=fh_full[:8000],
                reason_code="radarr_wrong_content",
                wrong_content=True,
                radarr_wrong_content_verdict=verdict_payload,
            )
            meta0 = pack(
                "failed",
                sb,
                sb,
                len(audio),
                len(audio),
                len(subs),
                len(subs),
                failure_hint=_wc_failure_lines[0],
                activity_context=wctx,
            )
            meta0["_refiner_reason_code"] = "radarr_wrong_content"
            meta0["_radarr_wrong_content_actions"] = {
                "queue_id": movie_wrong_content_ctx.get("queue_id"),
                "movie_id": int(movie_wrong_content_ctx["movie_id"]),
                "dry_run": bool(dry),
            }
            return "error", meta0
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
            _cleanup_refiner_source_sidecar_artifacts_after_success(
                media_parent=source_parent, watched_root=watched_root
            )
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
        _cleanup_refiner_source_sidecar_artifacts_after_success(
            media_parent=path.parent, watched_root=watched_root
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
