"""Presentation helpers for Refiner activity before/after (decision-summary panel, not raw debug tables)."""

from __future__ import annotations

from collections import Counter
from typing import Any, Literal

from app.display_helpers import _fmt_size_bytes_si

_JOIN_SEP = " · "

# Max distinct removed labels before "+ N more" (grouped rows, not raw bullets).
MAX_REMOVED_GROUPS_VISIBLE = 4

CompareVariant = Literal["changed", "unchanged", "empty", "failed"]


def is_absent_compare_token(val: object) -> bool:
    if not isinstance(val, str):
        return True
    t = val.strip()
    return t in ("", "—", "-", "None", "n/a", "N/A")


def split_joined_display_line(val: object) -> list[str]:
    """Split Refiner join_track_lines / subtitle blobs on the canonical middle-dot separator."""
    if not isinstance(val, str):
        return []
    t = val.strip()
    if not t or is_absent_compare_token(t):
        return []
    parts = [p.strip() for p in t.split(_JOIN_SEP) if p.strip()]
    if len(parts) <= 1 and _JOIN_SEP not in t and "·" in t:
        parts = [p.strip() for p in t.split("·") if p.strip()]
    return parts


def _multiset_subtract(before: list[str], after: list[str]) -> list[str]:
    rem = Counter(before) - Counter(after)
    return list(rem.elements())


def group_ordered_counts(items: list[str]) -> list[tuple[str, int]]:
    """First-seen order; merge identical stripped labels with running counts."""
    ordered: list[str] = []
    index: dict[str, int] = {}
    counts: list[int] = []
    for raw in items:
        label = raw.strip()
        if not label:
            continue
        if label not in index:
            index[label] = len(ordered)
            ordered.append(label)
            counts.append(0)
        counts[index[label]] += 1
    return [(ordered[i], counts[i]) for i in range(len(ordered))]


def format_grouped_removal_line(label: str, count: int) -> str:
    if count <= 1:
        return label
    return f"{label} ({count})"


def _is_synthetic_removed_placeholder(line: str) -> bool:
    t = line.lower()
    return "not in activity" in t or "languages not in activity" in t or "details not in activity" in t


def build_summarized_removed_items(
    raw_items: list[str],
    *,
    max_groups: int = MAX_REMOVED_GROUPS_VISIBLE,
) -> tuple[list[str], int, list[str]]:
    """Return (visible grouped lines, hidden track count, full grouped lines for expand).

    When the list is a single synthetic placeholder, grouping/summarization is skipped.
    """
    if not raw_items:
        return [], 0, []
    if len(raw_items) == 1 and _is_synthetic_removed_placeholder(raw_items[0]):
        return [raw_items[0].strip()], 0, []
    groups = group_ordered_counts(raw_items)
    full_lines = [format_grouped_removal_line(lab, c) for lab, c in groups]
    if len(groups) <= max_groups:
        return full_lines, 0, []
    visible_g = groups[:max_groups]
    visible = [format_grouped_removal_line(lab, c) for lab, c in visible_g]
    hidden_tracks = sum(c for _, c in groups[max_groups:])
    return visible, hidden_tracks, full_lines


def _fallback_track_line(raw_line: str, count: int, *, audio: bool) -> str:
    if not is_absent_compare_token(raw_line):
        return raw_line.strip()
    if count > 0:
        return f"{count} audio track(s)" if audio else f"{count} subtitle track(s)"
    return "—"


def compare_row_change_state(
    *,
    label: str,
    before: str,
    after: str,
    sb: int = 0,
    sa: int = 0,
) -> Literal["unchanged", "changed", "removed", "added", "unknown"]:
    if label == "File size":
        if sb <= 0:
            return "unknown"
        return "unchanged" if sa == sb else "changed"
    if label == "Subtitles":
        ae = is_absent_compare_token(after)
        be = is_absent_compare_token(before)
        if ae and not be:
            return "removed"
        if be and not ae:
            return "added"
        if before.strip() == after.strip():
            return "unchanged"
        return "changed"
    if label == "Audio":
        ae = is_absent_compare_token(after)
        be = is_absent_compare_token(before)
        if be and not ae:
            return "added"
        if ae and not be:
            return "removed"
        if before.strip() == after.strip():
            return "unchanged"
        return "changed"
    return "unknown"


def _size_delta_phrase(sb: int, sa: int) -> str | None:
    if sb <= 0 or sa <= 0 or sa >= sb:
        return None
    delta = sb - sa
    pct = int(round(delta / sb * 100.0))
    return f"Δ −{_fmt_size_bytes_si(delta)} (~{pct}%)"


def enrich_compare_row(
    row: dict[str, str],
    *,
    sb: int,
    sa: int,
) -> dict[str, Any]:
    label = row.get("label") or ""
    before = row.get("before") or ""
    after = row.get("after") or ""
    change = compare_row_change_state(label=label, before=before, after=after, sb=sb, sa=sa)
    size_delta: str | None = None
    if label == "File size":
        size_delta = _size_delta_phrase(sb, sa)
    out: dict[str, Any] = dict(row)
    out["change"] = change
    out["size_delta"] = size_delta
    return out


def build_refiner_compare_sections(
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
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    if include_audio_subs:
        ab_raw = ctx.get("audio_before")
        aa_raw = ctx.get("audio_after")
        ab_line = (str(ab_raw).strip() if isinstance(ab_raw, str) else "") or ""
        aa_line = (str(aa_raw).strip() if isinstance(aa_raw, str) else "") or ""
        ab_n = is_absent_compare_token(ab_line)
        aa_n = is_absent_compare_token(aa_line)
        before_items = split_joined_display_line(ab_line)
        after_items = split_joined_display_line(aa_line)

        if not ab_n or not aa_n or ab != aa:
            bf = _fallback_track_line(raw_line=ab_line, count=ab, audio=True)
            af = _fallback_track_line(raw_line=aa_line, count=aa, audio=True)
            removed = _multiset_subtract(before_items, after_items) if before_items else []
            if not before_items and ab > aa and ab > 0:
                removed = [f"{ab - aa} track(s) removed (details not in activity payload)"]

            variant: CompareVariant
            if (ab == aa and bf == af) or (before_items == after_items and bf == af):
                variant = "unchanged"
            elif aa == 0 and ab > 0 and not after_items:
                variant = "changed"
            else:
                variant = "changed" if (bf != af or removed) else "unchanged"

            primary_label = "Selected"
            if variant == "unchanged":
                primary_label = "Kept"
            primary_lines = after_items if after_items else ([af] if not aa_n else [])
            if not primary_lines:
                if aa == 0 and ab > 0:
                    primary_lines = ["All audio removed"]
                elif aa == 0:
                    primary_lines = ["No audio in output"]
                elif aa > 0:
                    primary_lines = [f"{aa} audio track(s)"]

            sec_heading: str | None = None
            sec_items: list[str] = []
            sec_more = 0
            sec_expand: list[str] = []
            if removed:
                n = len(removed)
                sec_heading = f"Removed ({n} track{'s' if n != 1 else ''})"
                sec_items, sec_more, sec_expand = build_summarized_removed_items(removed)
            elif variant == "changed" and ab > aa and not removed and before_items:
                n = ab - aa
                if n > 0:
                    sec_heading = f"Removed ({n} track{'s' if n != 1 else ''})"
                    raw_rm = _multiset_subtract(before_items, after_items) or before_items
                    sec_items, sec_more, sec_expand = build_summarized_removed_items(raw_rm)

            note: str | None = None
            if variant == "unchanged":
                note = "No changes needed."
            elif ab == aa and bf != af:
                note = "Layout updated (same track count)"

            sections.append(
                {
                    "kind": "audio",
                    "heading": "Audio",
                    "variant": variant,
                    "primary_label": primary_label,
                    "primary_lines": primary_lines,
                    "secondary_heading": sec_heading,
                    "secondary_items": sec_items,
                    "secondary_more_tracks": sec_more,
                    "secondary_expand_items": sec_expand,
                    "note": note,
                }
            )

        sb_raw = ctx.get("subs_before")
        sa_raw = ctx.get("subs_after")
        sb_line = (str(sb_raw).strip() if isinstance(sb_raw, str) else "") or ""
        sa_line = (str(sa_raw).strip() if isinstance(sa_raw, str) else "") or ""
        sb_empty = is_absent_compare_token(sb_line)
        sa_empty = is_absent_compare_token(sa_line)
        before_sub = split_joined_display_line(sb_line)
        after_sub = split_joined_display_line(sa_line)
        if sa_line.strip().lower() == "none":
            after_sub = []

        if not sb_empty or not sa_empty or sbb != sba:
            variant_s: CompareVariant = "unchanged"
            if sba < sbb or (before_sub and not after_sub and sbb > 0):
                variant_s = "changed"
            elif before_sub != after_sub:
                variant_s = "changed"

            removed_s = _multiset_subtract(before_sub, after_sub) if before_sub else []
            if not before_sub and sbb > sba:
                removed_s = [f"{sbb - sba} track(s) (languages not in activity payload)"]

            primary_label_s = "Kept" if after_sub else "Outcome"
            primary_lines_s: list[str]
            if after_sub:
                primary_lines_s = after_sub
            elif sba == 0 and sbb > 0:
                primary_lines_s = ["All subtitles removed"]
            elif sba == 0:
                primary_lines_s = ["No subtitles in output"]
            else:
                primary_lines_s = [f"{sba} subtitle track(s)"]

            sec_h: str | None = None
            sec_it: list[str] = []
            sec_more_s = 0
            sec_expand_s: list[str] = []
            if removed_s:
                n = len(removed_s)
                sec_h = f"Removed ({n} track{'s' if n != 1 else ''})"
                sec_it, sec_more_s, sec_expand_s = build_summarized_removed_items(removed_s)
            elif sba < sbb and not removed_s and before_sub:
                n = sbb - sba
                sec_h = f"Removed ({n} track{'s' if n != 1 else ''})"
                raw_rm = _multiset_subtract(before_sub, after_sub) or before_sub
                sec_it, sec_more_s, sec_expand_s = build_summarized_removed_items(raw_rm)

            note_s: str | None = None
            if variant_s == "unchanged":
                note_s = "No changes needed."

            sections.append(
                {
                    "kind": "subtitles",
                    "heading": "Subtitles",
                    "variant": variant_s,
                    "primary_label": primary_label_s,
                    "primary_lines": primary_lines_s,
                    "secondary_heading": sec_h,
                    "secondary_items": sec_it,
                    "secondary_more_tracks": sec_more_s,
                    "secondary_expand_items": sec_expand_s,
                    "note": note_s,
                }
            )

    bsz = _fmt_size_bytes_si(sb)
    if failed:
        sections.append(
            {
                "kind": "file_size",
                "heading": "File size",
                "variant": "failed",
                "primary_label": "Source",
                "primary_lines": [bsz],
                "secondary_heading": None,
                "secondary_items": [],
                "secondary_line": "No output size — processing did not complete",
                "note": None,
            }
        )
    else:
        final_sz = _fmt_size_bytes_si(sa)
        saved_line: str | None = None
        note_f: str | None = None
        if sb > 0 and sa > 0 and sa < sb:
            delta = sb - sa
            pct = int(round(delta / sb * 100.0))
            saved_line = f"↓ {_fmt_size_bytes_si(delta)} saved (~{pct}%)"
        elif sb > 0 and sa >= sb:
            note_f = "No smaller than source"

        variant_f: CompareVariant = "unchanged" if sb == sa and sb > 0 else "changed"
        if sa != sb:
            variant_f = "changed"

        sections.append(
            {
                "kind": "file_size",
                "heading": "File size",
                "variant": variant_f,
                "primary_label": "Final",
                "primary_lines": [final_sz],
                "secondary_heading": "Saved" if saved_line else None,
                "secondary_items": [saved_line] if saved_line else [],
                "secondary_line": None,
                "note": note_f,
            }
        )

    return sections
