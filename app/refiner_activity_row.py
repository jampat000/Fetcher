"""Refiner per-file activity row: primary line · summary · structured detail blocks (spec-locked)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.display_helpers import _fmt_size_bytes_si
from app.models import RefinerActivity
from app.refiner_activity_context import parse_activity_context


def _saved_line(sb: int, sa: int) -> str:
    if sb <= 0:
        return "Saved: 0 GB (0%)"
    if sa >= sb:
        return "Saved: 0 GB (0%)"
    delta = sb - sa
    pct = int(round(delta / sb * 100.0))
    return f"Saved: {_fmt_size_bytes_si(delta)} ({pct}%)"


def _file_size_block(sb: int, sa: int, *, failed: bool) -> str:
    b = _fmt_size_bytes_si(sb)
    if failed:
        return f"File size:\nBefore: {b}\nAfter: —\nSaved: —"
    a = _fmt_size_bytes_si(sa)
    return f"File size:\nBefore: {b}\nAfter: {a}\n{_saved_line(sb, sa)}"


def _summary_success(ab: int, aa: int, sbb: int, sba: int) -> str:
    audio_bit = "Audio unchanged" if ab == aa else "Audio refined"
    if sba < sbb:
        sub_bit = "Subtitles removed"
    elif sba == sbb:
        sub_bit = "Subtitles unchanged"
    else:
        sub_bit = "Subtitles updated"
    return f"{audio_bit} · {sub_bit}"


def _summary_from_counts_only(st: str, ab: int, aa: int, sbb: int, sba: int) -> str:
    if st == "skipped":
        return "No changes required"
    if st == "failed":
        return "Processing did not complete"
    return _summary_success(ab, aa, sbb, sba)


def _failure_reason_display(ctx: dict[str, Any], st: str) -> str:
    raw = (ctx.get("failure_reason") or "").strip()
    if raw:
        return raw[:4000] if len(raw) > 4000 else raw
    if st == "failed":
        return "Processing did not complete"
    return ""


def _short_failure_summary(ctx: dict[str, Any], st: str) -> str:
    full = _failure_reason_display(ctx, st)
    if not full:
        return "Processing did not complete"
    first = full.splitlines()[0].strip()
    return first[:140] + ("…" if len(first) > 140 else "")


def build_refiner_activity_row_dict(r: RefinerActivity, tz: str, now: datetime) -> dict[str, Any]:
    sb = int(r.size_before_bytes or 0)
    sa = int(r.size_after_bytes or 0)
    ab = int(r.audio_tracks_before or 0)
    aa = int(r.audio_tracks_after or 0)
    sbb = int(r.subtitle_tracks_before or 0)
    sba = int(r.subtitle_tracks_after or 0)
    st = (r.status or "failed").strip().lower()
    ctx = parse_activity_context(getattr(r, "activity_context", None))
    fname = (r.file_name or "").strip() or "—"

    detail_blocks: list[str] = []
    primary = "Refiner failed"
    summary = ""
    outcome_ui = "failed"
    tone = "fail"

    if st == "processing":
        primary = "Processing file"
        summary = "—"
        detail_blocks = [f"Title:\n{fname}", "Step:\nRefining"]
        outcome_ui = "processing"
        tone = "progress"
    elif st == "success":
        primary = "Refiner completed"
        summary = _summary_success(ab, aa, sbb, sba) if ctx else _summary_from_counts_only(st, ab, aa, sbb, sba)
        outcome_ui = "success"
        tone = "ok"
        ab_line = (ctx.get("audio_before") or "").strip()
        aa_line = (ctx.get("audio_after") or "").strip()
        if ab_line or aa_line:
            detail_blocks.append(f"Audio:\nBefore: {ab_line or '—'}\nAfter: {aa_line or '—'}")
        sb_line = (ctx.get("subs_before") or "").strip()
        sa_line = (ctx.get("subs_after") or "").strip()
        if sb_line or sa_line:
            detail_blocks.append(f"Subtitles:\nBefore: {sb_line or '—'}\nAfter: {sa_line or '—'}")
        if ctx.get("commentary_removed"):
            detail_blocks.append("Commentary:\nRemoved")
        detail_blocks.append(_file_size_block(sb, sa, failed=False))
        if ctx.get("finalized") and not ctx.get("dry_run"):
            detail_blocks.append("Output:\nFinalized to output folder\nSource removed")
        elif not ctx:
            detail_blocks.append("Output:\nFinalized to output folder\nSource removed")
    elif st == "skipped":
        primary = "Refiner skipped"
        summary = "No changes required"
        outcome_ui = "skipped"
        tone = "skip"
        ab_line = (ctx.get("audio_before") or "").strip()
        aa_line = (ctx.get("audio_after") or ab_line).strip() or ab_line
        sb_line = (ctx.get("subs_before") or "").strip()
        sa_line = (ctx.get("subs_after") or sb_line).strip() or sb_line
        if ctx and (ab_line or sb_line):
            detail_blocks.append(f"Audio:\nBefore: {ab_line or '—'}\nAfter: {aa_line or '—'}")
            detail_blocks.append(f"Subtitles:\nBefore: {sb_line or '—'}\nAfter: {sa_line or '—'}")
        detail_blocks.append(_file_size_block(sb, sa, failed=False))
        if ctx.get("dry_run"):
            detail_blocks.append("Output:\nNo file changes (dry run)")
    else:
        primary = "Refiner failed"
        summary = _short_failure_summary(ctx, st)
        outcome_ui = "failed"
        tone = "fail"
        ab_line = (ctx.get("audio_before") or "").strip()
        aa_line = (ctx.get("audio_after") or "").strip()
        if ab_line or aa_line:
            detail_blocks.append(f"Audio:\nBefore: {ab_line or '—'}\nAfter: {aa_line or '—'}")
        sb_line = (ctx.get("subs_before") or "").strip()
        sa_line = (ctx.get("subs_after") or "").strip()
        if sb_line or sa_line:
            detail_blocks.append(f"Subtitles:\nBefore: {sb_line or '—'}\nAfter: {sa_line or '—'}")
        if ctx.get("commentary_removed"):
            detail_blocks.append("Commentary:\nRemoved")
        detail_blocks.append(_file_size_block(sb, sa, failed=True))
        reason = _failure_reason_display(ctx, st)
        if reason:
            detail_blocks.append(f"Reason:\n{reason}")

    row: dict[str, Any] = {
        "activity_type": "refiner",
        "type": "refiner",
        "id": r.id,
        "app": "refiner",
        "kind": "refiner",
        "status": st,
        "count": "",
        "primary_label": fname,
        "refiner_primary_line": primary,
        "refiner_summary_line": summary,
        "refiner_detail_blocks": detail_blocks,
        "detail_lines": [],
        "detail_preview": 99,
        "refiner_status": st,
        "refiner_status_tone": tone,
        "activity_domain": "refiner",
        "activity_lucide": "sliders-horizontal",
        "activity_outcome": outcome_ui,
        "refiner_file_title": fname,
    }
    return row
