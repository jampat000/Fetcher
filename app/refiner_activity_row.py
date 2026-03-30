"""Refiner per-file activity row: media title, outcome, before/after comparison, summary (spec-locked)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from app.display_helpers import _fmt_size_bytes_si
from app.models import RefinerActivity
from app.refiner_activity_context import parse_activity_context
from app.refiner_media_identity import (
    looks_like_internal_identifier,
    resolve_activity_card_title,
    should_show_raw_source_filename,
)

ApplyMode = Literal["applied", "preview", "none"]


def _norm_line(val: object) -> str:
    if not isinstance(val, str):
        return ""
    t = val.strip()
    if t in ("", "—", "-", "None", "n/a", "N/A"):
        return ""
    return t


def _display(val: object, *, empty: str = "—") -> str:
    n = _norm_line(val)
    return n if n else empty


def _ctx_lines_differ(ctx: dict[str, Any]) -> bool:
    ab = _norm_line(ctx.get("audio_before"))
    aa = _norm_line(ctx.get("audio_after"))
    if ab != aa:
        return True
    sb = _norm_line(ctx.get("subs_before"))
    sa = _norm_line(ctx.get("subs_after"))
    return sb != sa


def _metrics_differ(sb: int, sa: int, ab: int, aa: int, sbb: int, sba: int) -> bool:
    return sb != sa or ab != aa or sbb != sba


def _saved_sentence(sb: int, sa: int) -> str | None:
    if sb <= 0:
        return None
    if sa >= sb:
        return None
    delta = sb - sa
    pct = int(round(delta / sb * 100.0))
    return f"File size reduced by {_fmt_size_bytes_si(delta)} (~{pct}% smaller)."


def _compare_rows_audio_subs_size(
    *,
    ctx: dict[str, Any],
    sb: int,
    sa: int,
    failed: bool,
    include_audio_subs: bool,
    ab: int = 0,
    aa: int = 0,
    sbb: int = 0,
    sba: int = 0,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if include_audio_subs:
        ab_raw = _norm_line(ctx.get("audio_before"))
        aa_raw = _norm_line(ctx.get("audio_after"))
        ab_line = _display(ctx.get("audio_before"))
        aa_line = _display(ctx.get("audio_after"))
        if ab_raw or aa_raw or ab != aa:
            bf = ab_line if ab_raw else (f"{ab} track(s)" if ab else "—")
            af = aa_line if aa_raw else (f"{aa} track(s)" if aa else "—")
            rows.append({"label": "Audio", "before": bf, "after": af})
        sb_raw = _norm_line(ctx.get("subs_before"))
        sa_raw = _norm_line(ctx.get("subs_after"))
        sb_line = _display(ctx.get("subs_before"))
        sa_line = _display(ctx.get("subs_after"))
        if sb_raw or sa_raw or sbb != sba:
            rows.append({"label": "Subtitles", "before": sb_line, "after": sa_line})
    bsz = _fmt_size_bytes_si(sb)
    if failed:
        rows.append({"label": "File size", "before": bsz, "after": "—"})
    else:
        rows.append({"label": "File size", "before": bsz, "after": _fmt_size_bytes_si(sa)})
    return rows


def _success_summary_bullets(
    ctx: dict[str, Any], ab: int, aa: int, sbb: int, sba: int, sb: int, sa: int
) -> list[str]:
    out: list[str] = []
    nc_list = ctx.get("no_change_bullets")
    has_nc = isinstance(nc_list, list) and bool(nc_list)
    if ctx.get("pipeline_no_remux"):
        out.append("No remux required — streams already match your rules.")
        if has_nc:
            for line in nc_list:
                t = str(line).strip()
                if t and t not in out:
                    out.append(t)

    if not (ctx.get("pipeline_no_remux") and has_nc):
        if ab != aa:
            out.append(f"Audio: {ab} track(s) → {aa} track(s).")
        elif _norm_line(ctx.get("audio_before")) != _norm_line(ctx.get("audio_after")):
            out.append("Audio: layout updated.")
        else:
            out.append("Audio: unchanged.")

        if sba < sbb:
            out.append(f"Subtitles: {sbb} track(s) → {sba} track(s) (removed or merged per rules).")
        elif sba > sbb:
            out.append(f"Subtitles: {sbb} track(s) → {sba} track(s).")
        else:
            out.append("Subtitles: unchanged.")

    ss = _saved_sentence(sb, sa)
    if ss:
        out.append(ss)
    if ctx.get("commentary_removed"):
        out.append("Commentary tracks removed.")
    if ctx.get("finalized") and not ctx.get("dry_run"):
        if ctx.get("pipeline_no_remux"):
            out.append("Copied to output; source removed from watch folder.")
        else:
            out.append("Output written to the destination folder; source removed from the watch folder.")
    elif not ctx:
        out.append("Output written to the destination folder; source removed from the watch folder.")
    rf = ctx.get("residue_files_removed")
    if isinstance(rf, list) and rf:
        out.append(
            f"Removed {len(rf)} release residue file(s) from the source folder "
            "(par/nfo/sfv/archives/.txt/.url, etc.); subtitle sidecars and media payloads were preserved)."
        )
    fc = (ctx.get("folder_cleanup") or "").strip()
    if fc in ("removed_empty_folder", "removed_empty_ancestors"):
        out.append("Pruned empty folder(s) under the watch path after processing.")
    return out


def _failure_reason_display(ctx: dict[str, Any], st: str) -> str:
    raw = (ctx.get("failure_reason") or "").strip()
    if raw:
        return raw[:4000] if len(raw) > 4000 else raw
    if st == "failed":
        return "Processing did not complete."
    return ""


def build_refiner_activity_row_dict(r: RefinerActivity, tz: str, now: datetime) -> dict[str, Any]:
    del tz, now  # timestamps merged in web_common.refiner_activity_display_row
    sb = int(r.size_before_bytes or 0)
    sa = int(r.size_after_bytes or 0)
    ab = int(r.audio_tracks_before or 0)
    aa = int(r.audio_tracks_after or 0)
    sbb = int(r.subtitle_tracks_before or 0)
    sba = int(r.subtitle_tracks_after or 0)
    st = (r.status or "failed").strip().lower()
    ctx = parse_activity_context(getattr(r, "activity_context", None))
    fname = (r.file_name or "").strip() or "—"
    orm_mt = (getattr(r, "media_title", None) or "").strip()
    display_media_title = resolve_activity_card_title(
        fname if fname != "—" else "",
        ctx,
        orm_media_title=orm_mt,
        ffprobe_media_title=(ctx.get("media_title") or None),
        ffprobe_refiner_title=(ctx.get("refiner_title") or None),
        ffprobe_year=(ctx.get("refiner_year") or None),
    )
    if not display_media_title or looks_like_internal_identifier(display_media_title):
        if st in ("processing", "finalizing"):
            display_media_title = "Processing file..."
        else:
            display_media_title = "Detecting file details..."
    source_file_line: str | None = None
    raw_fn = fname if fname != "—" else ""
    if raw_fn and should_show_raw_source_filename(
        display_title=display_media_title,
        file_name=raw_fn,
        ctx=ctx,
        orm_media_title=orm_mt,
    ):
        source_file_line = raw_fn
    dry = bool(ctx.get("dry_run"))

    compare_rows: list[dict[str, str]] = []
    summary_bullets: list[str] = []
    technical_notes: list[str] = []
    outcome_label = "Failed"
    outcome_sub: str | None = None
    apply_mode: ApplyMode = "none"
    outcome_ui = "failed"
    tone = "fail"
    show_comparison = False

    if st == "queued":
        outcome_label = "Queued"
        outcome_sub = "Waiting — Refiner runs one file at a time (FIFO)."
        outcome_ui = "queued"
        tone = "queued"
        summary_bullets = ["This file will start after earlier items in this pass finish."]
    elif st == "processing":
        outcome_label = "Processing"
        outcome_sub = "Analyzing streams, planning the output, and remuxing or stream-copying to a work file if needed."
        outcome_ui = "processing"
        tone = "progress"
        summary_bullets = [
            "Remux and planning run here. Output placement and watch-folder cleanup come next (finalizing)."
        ]
    elif st == "finalizing":
        outcome_label = "Finalizing"
        outcome_sub = "Writing the output file, removing the source from the watch folder, and cleaning up empty folders if applicable."
        outcome_ui = "finalizing"
        tone = "finalizing"
        summary_bullets = [
            "No further remux work — this step is move/copy, delete source, and optional folder prune."
        ]
    elif st == "skipped_terminal_failed":
        outcome_label = "Skipped (failed import)"
        ipb = ctx.get("import_promotion_block") if isinstance(ctx.get("import_promotion_block"), dict) else {}
        outcome_sub = str(ipb.get("subtitle") or "").strip() or (
            "Not promoted — item classified as a failed import"
        )
        outcome_ui = "skipped_import"
        tone = "skip_import"
        apply_mode = "none"
        show_comparison = False
        summary_bullets = [
            outcome_sub,
            "Refiner did not write to the output folder: the matching *arr queue row shows a terminal failed-import state "
            "(fresh `/queue` fetch right before promotion).",
            "Upgrade-style grabs that are still waiting for eligible files are not blocked — those look like "
            "“waiting to import / no eligible” and are excluded from this gate.",
        ]
        av = str(ipb.get("arr_app") or "").strip().lower()
        if av in ("radarr", "sonarr"):
            summary_bullets.append(
                f"Matched {av.title()} by `downloadId` + `outputPath` containing this file — not by filename alone."
            )
        sig = str(ipb.get("import_state") or "").strip()
        if sig:
            summary_bullets.append(f"*arr signal: {sig}.")
        if ipb.get("non_upgrade") is True:
            summary_bullets.append("Classified as an explicit non-upgrade rejection vs an existing library file.")
    elif st == "success":
        outcome_label = "Completed"
        apply_mode = "applied"
        outcome_ui = "success"
        tone = "ok"
        show_comparison = True
        compare_rows = _compare_rows_audio_subs_size(
            ctx=ctx,
            sb=sb,
            sa=sa,
            failed=False,
            include_audio_subs=True,
            ab=ab,
            aa=aa,
            sbb=sbb,
            sba=sba,
        )
        summary_bullets = _success_summary_bullets(ctx, ab, aa, sbb, sba, sb, sa)
    elif st == "skipped":
        projected = _metrics_differ(sb, sa, ab, aa, sbb, sba) or _ctx_lines_differ(ctx)
        if dry and projected:
            outcome_label = "Dry run"
            outcome_sub = "Changes identified · preview only; no file changes applied."
            apply_mode = "preview"
            outcome_ui = "skipped"
            tone = "skip"
            show_comparison = True
            compare_rows = _compare_rows_audio_subs_size(
                ctx=ctx,
                sb=sb,
                sa=sa,
                failed=False,
                include_audio_subs=True,
                ab=ab,
                aa=aa,
                sbb=sbb,
                sba=sba,
            )
            summary_bullets = [
                "Dry run: source file was not modified.",
                "Before / After shows the remux Refiner would apply if dry run were off.",
            ]
        elif dry and not projected:
            outcome_label = "Dry run"
            outcome_sub = "No changes would be applied with current rules."
            apply_mode = "preview"
            outcome_ui = "skipped"
            tone = "skip"
            show_comparison = False
            nc = ctx.get("no_change_bullets")
            if isinstance(nc, list) and nc:
                summary_bullets = [str(x).strip() for x in nc if str(x).strip()]
                summary_bullets.insert(0, "Dry run: no file changes.")
            else:
                summary_bullets = ["Dry run: rules would leave this file unchanged."]
        else:
            outcome_label = "No changes required"
            outcome_sub = None
            apply_mode = "none"
            outcome_ui = "skipped"
            tone = "skip"
            show_comparison = False
            nc = ctx.get("no_change_bullets")
            if isinstance(nc, list) and nc:
                summary_bullets = [str(x).strip() for x in nc if str(x).strip()]
            else:
                summary_bullets = ["Remux not required; file already matches your rules."]
        if ctx.get("commentary_removed") and dry:
            technical_notes.append("Commentary would be affected per rules (see comparison).")
    else:
        outcome_label = "Failed"
        outcome_sub = None
        outcome_ui = "failed"
        tone = "fail"
        show_comparison = False
        reason = _failure_reason_display(ctx, st)
        if reason:
            first_ln = reason.splitlines()[0].strip()
            summary_bullets = [first_ln[:500] + ("…" if len(first_ln) > 500 else "")]
            if len(reason.splitlines()) > 1 or len(reason) > len(first_ln):
                technical_notes.append(reason)
        else:
            summary_bullets = ["Processing did not complete."]
        if ctx.get("commentary_removed"):
            technical_notes.append("Commentary was slated for removal before failure.")

    row: dict[str, Any] = {
        "activity_type": "refiner",
        "type": "refiner",
        "id": r.id,
        "app": "refiner",
        "kind": "refiner",
        "status": st,
        "count": "",
        "primary_label": display_media_title,
        "refiner_media_title": display_media_title,
        "refiner_source_file_line": source_file_line,
        "refiner_outcome_label": outcome_label,
        "refiner_outcome_sub": outcome_sub,
        "refiner_apply_mode": apply_mode,
        "refiner_show_comparison": show_comparison,
        "refiner_compare_rows": compare_rows,
        "refiner_summary_bullets": summary_bullets,
        "refiner_technical_notes": technical_notes,
        # Back-compat for tests / callers expecting the old keys:
        "refiner_primary_line": outcome_label,
        "refiner_summary_line": outcome_sub or (summary_bullets[0] if summary_bullets else ""),
        "refiner_detail_blocks": [],
        "detail_lines": [],
        "detail_preview": 99,
        "refiner_status": st,
        "refiner_status_tone": tone,
        "activity_domain": "refiner",
        "activity_lucide": "sliders-horizontal",
        "activity_outcome": outcome_ui,
        "refiner_file_title": display_media_title,
        "refiner_is_dry_run": dry,
    }
    return row
