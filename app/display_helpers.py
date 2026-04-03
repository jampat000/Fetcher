from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.form_helpers import _resolve_timezone_name


def to_12h(hhmm: str, default: str) -> str:
    try:
        dt = datetime.strptime((hhmm or "").strip(), "%H:%M")
        return dt.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return default


def normalize_hhmm(raw: str, default: str) -> str:
    v = (raw or "").strip()
    if not v:
        return default
    # Already 24h HH:MM
    try:
        dt = datetime.strptime(v, "%H:%M")
        return dt.strftime("%H:%M")
    except Exception:
        pass
    # 12h forms like 9:30 PM / 09:30pm
    for fmt in ("%I:%M %p", "%I:%M%p"):
        try:
            dt = datetime.strptime(v.upper(), fmt)
            return dt.strftime("%H:%M")
        except Exception:
            continue
    return default


def time_select_orphan(canonical_hhmm: str, choice_keys: set[str], *, fallback_display: str) -> tuple[str, str] | None:
    """If saved time is not on the dropdown grid, offer it as the first option."""
    if canonical_hhmm in choice_keys:
        return None
    return (canonical_hhmm, to_12h(canonical_hhmm, fallback_display))


def schedule_days_display(days_csv: str) -> str:
    """Format stored CSV weekdays for dashboard (commas → spaced hyphens)."""
    raw = (days_csv or "").strip()
    if not raw:
        return ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return " - ".join(parts)


def schedule_time_range_friendly(start_hhmm: str, end_hhmm: str) -> str:
    """Avoid a lone en-dash when pairing start/end for dashboard tiles."""
    s = (start_hhmm or "").strip() or "00:00"
    e = (end_hhmm or "").strip() or "23:59"
    if e == "24:00":
        e = "23:59"
    if s == "00:00" and e in ("23:59", "23:58"):
        return "All day"
    left = to_12h(s, "12:00 AM")
    right = to_12h(e, "11:59 PM")
    return f"{left} – {right}"


def _truncate_display(s: str, max_len: int = 220) -> str:
    t = (s or "").strip().replace("\n", " ")
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


def now_local(timezone: str) -> str:
    try:
        tz = ZoneInfo(_resolve_timezone_name(timezone))
    except Exception:
        tz = ZoneInfo("UTC")
    # Keep a stable-width display across tabs to avoid subtle layout jitter.
    return datetime.now(tz).strftime("%d-%m-%Y %I:%M %p")


def fmt_local(dt: datetime, tz_name: str) -> str:
    try:
        tz = ZoneInfo(_resolve_timezone_name(tz_name))
    except Exception:
        tz = ZoneInfo("UTC")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(tz).strftime("%d-%m-%Y %I:%M %p")


def fmt_local_run_log_split(dt: datetime, tz_name: str) -> tuple[str, str]:
    """Readable date + clock lines for job run history lists (e.g. Settings → Global).

    Returns ``(date_line, time_line)`` such as ``("Fri, 04 Apr 2026", "2:05 PM")``.
    """
    try:
        tz = ZoneInfo(_resolve_timezone_name(tz_name))
    except Exception:
        tz = ZoneInfo("UTC")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    local = dt.astimezone(tz)
    date_line = local.strftime("%a, %d %b %Y")
    clock = local.strftime("%I:%M %p")
    time_line = clock.lstrip("0").replace(" 0", " ")
    return date_line, time_line


def fmt_local_run_log_line(dt: datetime, tz_name: str) -> str:
    """One-line timestamp for job run rows (no line-break in the label)."""
    d, t = fmt_local_run_log_split(dt, tz_name)
    return f"{d} · {t}"


def _as_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _relative_phrase_past(dt_event: datetime, now: datetime) -> str:
    """Human-friendly elapsed time since dt_event (both datetimes naive UTC or aware)."""
    t0 = _as_utc_naive(dt_event)
    t1 = _as_utc_naive(now)
    secs = int((t1 - t0).total_seconds())
    if secs < 45:
        return "just now"
    mins = secs // 60
    if mins < 60:
        return f"{mins} minute{'s' if mins != 1 else ''} ago"
    hours = mins // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = secs // 86400
    if days < 14:
        return f"{days} day{'s' if days != 1 else ''} ago"
    return f"{t0.strftime('%d-%m-%Y %I:%M %p')} UTC"


def activity_relative_time(dt_event: datetime, now: datetime) -> str:
    """
    Compact relative labels for activity feeds (refreshed in-browser on a light interval).
    Switches to a stable date string for older events.
    """
    t0 = _as_utc_naive(dt_event)
    t1 = _as_utc_naive(now)
    secs = max(0, int((t1 - t0).total_seconds()))
    if secs < 10:
        return "just now"
    if secs < 60:
        return f"{secs}s ago"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m ago"
    hrs = secs // 3600
    if hrs < 48:
        return f"{hrs}h ago"
    days = secs // 86400
    if days < 14:
        return f"{days}d ago"
    return t0.strftime("%d %b %Y · %H:%M")


def _relative_phrase_until(dt_future: datetime, now: datetime) -> str:
    """Human-friendly time until dt_future (naive UTC recommended)."""
    t0 = _as_utc_naive(now)
    t1 = _as_utc_naive(dt_future)
    secs = int((t1 - t0).total_seconds())
    if secs <= 0:
        return "due now"
    if secs < 45:
        return "in under a minute"
    mins = secs // 60
    if mins < 60:
        return f"in {mins} minute{'s' if mins != 1 else ''}"
    hours = mins // 60
    if hours < 24:
        return f"in {hours} hour{'s' if hours != 1 else ''}"
    days = secs // 86400
    return f"in {days} day{'s' if days != 1 else ''}"


def _fmt_size_bytes_si(n: int) -> str:
    """Human-readable byte size (decimal GB/MB for display consistency with Refiner activity)."""
    v = max(0, int(n))
    gb = v / (1024**3)
    if gb >= 1.0:
        return f"{gb:.1f} GB"
    mb = v / (1024**2)
    if mb >= 1.0:
        return f"{mb:.1f} MB"
    kb = v / 1024.0
    if kb >= 1.0:
        return f"{kb:.0f} KB"
    return f"{v} B"
