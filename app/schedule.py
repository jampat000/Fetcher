from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Map common variants to DAY_NAMES tokens (fixes UI pills when DB has long names or odd casing).
_DAY_TOKEN_ALIASES: dict[str, str] = {
    "mon": "Mon",
    "monday": "Mon",
    "tue": "Tue",
    "tues": "Tue",
    "tuesday": "Tue",
    "wed": "Wed",
    "wednesday": "Wed",
    "thu": "Thu",
    "thur": "Thu",
    "thurs": "Thu",
    "thursday": "Thu",
    "fri": "Fri",
    "friday": "Fri",
    "sat": "Sat",
    "saturday": "Sat",
    "sun": "Sun",
    "sunday": "Sun",
}


def normalize_schedule_days_csv(raw: str) -> str:
    """
    Canonicalize stored weekday CSV to Mon,Tue,... order.
    Unknown tokens are dropped.
    If the string is empty/whitespace-only, return "" (no days — matches “uncheck all” in the UI).
    If the string had comma-separated tokens but none were valid weekdays, return all seven days (legacy typo recovery).
    """
    s = (raw or "").strip()
    if s == "":
        return ""
    parts = [p.strip() for p in s.split(",") if p.strip()]
    seen: set[str] = set()
    for p in parts:
        key = p.lower().rstrip(".")
        canon = _DAY_TOKEN_ALIASES.get(key) or (p if p in DAY_NAMES else None)
        if canon:
            seen.add(canon)
    ordered = [d for d in DAY_NAMES if d in seen]
    return ",".join(ordered) if ordered else ",".join(DAY_NAMES)


def schedule_time_dropdown_choices(*, step_minutes: int = 30) -> list[tuple[str, str]]:
    """HH:MM (24h) + 12h label for <select> options (end-of-day 23:59 included)."""
    base = datetime(2000, 1, 1, 0, 0, 0)
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for mins in range(0, 24 * 60, step_minutes):
        dt = base + timedelta(minutes=mins)
        hhmm = dt.strftime("%H:%M")
        seen.add(hhmm)
        lab = dt.strftime("%I:%M %p").lstrip("0")
        out.append((hhmm, lab))
    if "23:59" not in seen:
        eod = datetime.strptime("23:59", "%H:%M")
        out.append(("23:59", eod.strftime("%I:%M %p").lstrip("0")))
    return out


def _parse_hhmm(s: str, *, default: time) -> time:
    try:
        parts = (s or "").strip().split(":")
        if len(parts) != 2:
            return default
        h = int(parts[0])
        m = int(parts[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            return default
        return time(hour=h, minute=m)
    except Exception:
        return default


def _parse_days(s: str) -> set[str]:
    """Stored as Mon,Tue,Wed or "" (no days selected — schedule window never matches a weekday)."""
    st = (s or "").strip()
    if st == "":
        return set()
    tokens = [t.strip() for t in st.split(",") if t.strip()]
    out = {t for t in tokens if t in DAY_NAMES}
    return out if out else set(DAY_NAMES)


def in_window(
    *,
    schedule_enabled: bool,
    schedule_days: str,
    schedule_start: str,
    schedule_end: str,
    timezone: str = "UTC",
    now: datetime | None = None,
) -> bool:
    """
    Returns True if we're allowed to run *now*.
    Uses timezone (IANA) for day/time; defaults to UTC.

    If start <= end: window is same-day (e.g. 09:00-17:00)
    If start > end: window crosses midnight (e.g. 22:00-02:00)
    """
    if not schedule_enabled:
        return True

    try:
        tz = ZoneInfo(timezone) if timezone else ZoneInfo("UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    now = now or datetime.now(tz)
    if now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)
    day = DAY_NAMES[now.weekday()]
    allowed_days = _parse_days(schedule_days)
    if day not in allowed_days:
        return False

    start_t = _parse_hhmm(schedule_start, default=time(0, 0))
    end_t = _parse_hhmm(schedule_end, default=time(23, 59))
    cur_t = now.time().replace(second=0, microsecond=0)

    if start_t <= end_t:
        return start_t <= cur_t <= end_t
    # crosses midnight
    return cur_t >= start_t or cur_t <= end_t

