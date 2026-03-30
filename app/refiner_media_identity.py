"""Media identity from ffprobe container tags + conservative filename display for Refiner activity."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_YEAR_IN_PARENS = re.compile(r"\(\s*(19\d{2}|20[0-3]\d)\s*\)\s*$")
_YEAR_IN_TEXT = re.compile(r"(19\d{2}|20[0-3]\d)")
_HEX_LONG = re.compile(r"^[a-f0-9]{16,}$", re.I)
_IDISH = re.compile(r"^(?:[a-f0-9]{8,}|[a-z0-9]{12,})$", re.I)

# Trailing release tokens (conservative): strip only when preceded by separator-like run.
_TRAILING_RELEASE = re.compile(
    r"(?i)\s*[-–—]\s*(web[- ]?dl|webrip|bluray|blu[- ]?ray|bdrip|dvdrip|hdtv|remux|hdr|sdr|"
    r"dv|atmos|ddp?\s*5\.1|aac\s*2\.0|x264|x265|hevc|h\.?\s*264|h\.?\s*265|10bit|8bit)\s*$"
)


def _format_tags_lower(probe: dict[str, Any]) -> dict[str, str]:
    fmt = probe.get("format")
    if not isinstance(fmt, dict):
        return {}
    tags = fmt.get("tags")
    if not isinstance(tags, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in tags.items():
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out[str(k).strip().lower()] = s
    return out


def _year_from_value(val: str) -> str | None:
    s = (val or "").strip()
    if not s:
        return None
    m = _YEAR_IN_TEXT.search(s)
    return m.group(1) if m else None


def _pick_title_tag(tags: dict[str, str]) -> str:
    for key in ("title", "show", "series"):
        v = (tags.get(key) or "").strip()
        if v:
            return v
    return ""


@dataclass(frozen=True)
class MediaIdentity:
    """Identity derived from ffprobe format tags (no external APIs)."""

    media_title: str | None  # single canonical display string when assembled
    refiner_title: str | None  # raw title-like tag
    refiner_year: str | None  # four-digit year when known from tags

    @classmethod
    def from_ffprobe(cls, probe: dict[str, Any]) -> MediaIdentity:
        tags = _format_tags_lower(probe)
        title = _pick_title_tag(tags)
        year: str | None = None
        for key in ("year", "date", "date_released", "creation_time"):
            y = _year_from_value(tags.get(key, ""))
            if y:
                year = y
                break
        if year is None:
            y2 = _year_from_value(tags.get("com.apple.quicktime.creationdate", ""))
            if y2:
                year = y2
        if title and _YEAR_IN_PARENS.search(title):
            return cls(media_title=title.strip()[:500] or None, refiner_title=title.strip()[:500] or None, refiner_year=year)
        if title and year:
            return cls(
                media_title=f"{title.strip()} ({year})"[:500],
                refiner_title=title.strip()[:500] or None,
                refiner_year=year,
            )
        if title:
            return cls(media_title=title.strip()[:500] or None, refiner_title=title.strip()[:500] or None, refiner_year=year)
        return cls(media_title=None, refiner_title=None, refiner_year=year)

    def snapshot_identity_fields(self) -> dict[str, str]:
        """Keys merged into Refiner activity_context JSON."""
        d: dict[str, str] = {}
        if self.media_title:
            d["media_title"] = self.media_title[:500]
        if self.refiner_title:
            d["refiner_title"] = self.refiner_title[:500]
        if self.refiner_year:
            d["refiner_year"] = self.refiner_year[:32]
        return d

    def persisted_media_title_column(self) -> str:
        """Single string stored on ``RefinerActivity.media_title`` (best-effort label)."""
        if self.media_title:
            return self.media_title[:512]
        if self.refiner_title:
            return self.refiner_title[:512]
        return ""


def conservative_filename_display(file_name: str) -> str:
    """Dots/underscores → spaces; trim obvious trailing release tokens once; collapse spaces."""
    raw = (file_name or "").strip()
    if not raw:
        return ""
    stem = Path(raw).stem
    s = stem.replace("_", " ").replace(".", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return raw
    prev = None
    while prev != s:
        prev = s
        s = _TRAILING_RELEASE.sub("", s).strip()
    return s or raw


def looks_like_internal_identifier(value: str) -> bool:
    s = (value or "").strip()
    if not s:
        return False
    if " " in s and not s.lower().startswith(("downloadid", "dlid")):
        return False
    compact = re.sub(r"[\s._-]+", "", s)
    if _HEX_LONG.fullmatch(compact):
        return True
    if compact.lower().startswith(("downloadid", "dlid")):
        return True
    if _IDISH.fullmatch(compact) and not _YEAR_IN_TEXT.search(compact):
        return True
    return False


def provisional_media_title_before_probe(file_name: str) -> str:
    """
    Title stored on ``processing`` rows before ffprobe — same rules as the activity card
    when only ``file_name`` is known (no container tags in context yet).
    """
    return resolve_activity_card_title(file_name, {}, orm_media_title="")


def _ffprobe_title_from_parts(
    *,
    media_title: str,
    refiner_title: str,
    refiner_year: str,
) -> str:
    mt = (media_title or "").strip()
    if mt:
        return mt[:500]
    rt = (refiner_title or "").strip()
    ry = (refiner_year or "").strip()
    if rt:
        if ry and not _YEAR_IN_PARENS.search(rt):
            return f"{rt} ({ry})"[:500]
        return rt[:500]
    return ""


def resolve_activity_card_title(
    file_name: str,
    ctx: dict[str, Any],
    *,
    orm_media_title: str = "",
    ffprobe_media_title: str | None = None,
    ffprobe_refiner_title: str | None = None,
    ffprobe_year: str | None = None,
) -> str:
    """Display title priority: trusted pipeline → filename-derived → ffprobe → ORM fallback → raw file_name."""
    trusted = (ctx.get("trusted_title") or "").strip()
    if trusted and not looks_like_internal_identifier(trusted):
        return trusted[:500]

    fn = (file_name or "").strip()
    if fn:
        derived = conservative_filename_display(fn)
        if derived and not looks_like_internal_identifier(derived):
            return derived[:500]

    pm = ffprobe_media_title if ffprobe_media_title is not None else (ctx.get("media_title") or "")
    pr = ffprobe_refiner_title if ffprobe_refiner_title is not None else (ctx.get("refiner_title") or "")
    py = ffprobe_year if ffprobe_year is not None else (ctx.get("refiner_year") or "")
    ff = _ffprobe_title_from_parts(media_title=str(pm), refiner_title=str(pr), refiner_year=str(py))
    if ff and not looks_like_internal_identifier(ff):
        return ff

    orm = (orm_media_title or "").strip()
    if orm and not looks_like_internal_identifier(orm):
        return orm[:500]

    return fn[:512] if fn else "—"


def should_show_raw_source_filename(
    *,
    display_title: str,
    file_name: str,
    ctx: dict[str, Any],
    orm_media_title: str = "",
) -> bool:
    """Muted file line when tag/canonical identity differs from raw name — not for filename-only provisionals."""
    raw = (file_name or "").strip()
    if not raw:
        return False
    if display_title.strip().casefold() == raw.casefold():
        return False
    if (ctx.get("trusted_title") or "").strip():
        return True
    derived = conservative_filename_display(raw)
    if derived and display_title.strip().casefold() == derived.casefold():
        return False
    if (ctx.get("media_title") or "").strip() or (ctx.get("refiner_title") or "").strip():
        return True
    orm = (orm_media_title or "").strip()
    if not orm:
        return False
    if derived and orm.casefold() == derived.casefold():
        return False
    if orm.casefold() == raw.casefold():
        return False
    return True
