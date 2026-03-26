"""Shared helpers for server-rendered pages and settings persistence."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Sequence
from urllib.parse import quote

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.display_helpers import (
    _fmt_local,
    _truncate_display,
)
from app.models import ActivityLog, AppSettings, JobRunLog
from app.schedule import DAY_NAMES, normalize_schedule_days_csv
from app.scheduler import scheduler

logger = logging.getLogger(__name__)

# Activity list shows 5 title lines + “+N more”; full list is stored in ``ActivityLog.detail``.
ACTIVITY_DETAIL_PREVIEW_LINES = 5
_ACTIVITY_LOG_LEGACY_MORE = re.compile(r"^\+\d+ more$")


def activity_log_title_lines(detail: str) -> list[str]:
    """Split stored detail into display lines; drop legacy synthetic ``+N more`` rows."""
    lines: list[str] = []
    for raw in (detail or "").splitlines():
        s = raw.strip()
        if not s or _ACTIVITY_LOG_LEGACY_MORE.match(s):
            continue
        lines.append(s)
    return lines


def user_visible_job_run_message(
    *,
    message: str | None,
    ok: bool,
    finished_at: Any,
) -> str:
    """Never return blank strings for JobRunLog presentation."""
    t = _truncate_display((message or "").strip())
    if t:
        return t
    if finished_at is None:
        return "Run summary not available yet."
    return (
        "Run completed with no summary text."
        if ok
        else "Operation failed with no additional details."
    )


_RUN_SUMMARY_PENDING_DISPLAY = "Run summary not available yet."
# Orphan early-commit row vs final row may differ by more than one wall second in practice.
_JOB_RUN_LOG_PLACEHOLDER_NEARBY_SECONDS = 5.0


def job_run_log_started_at_bucket_key(started_at: datetime | None) -> tuple[datetime | None, ...]:
    """Group rows that share the same wall-clock start second (tests / diagnostics)."""
    if started_at is None:
        return (None,)
    return (started_at.replace(microsecond=0),)


def _job_run_log_started_at_delta_seconds(a: datetime | None, b: datetime | None) -> float | None:
    if a is None or b is None:
        return None
    return abs((a - b).total_seconds())


def job_run_log_row_is_terminal_for_dedupe(r: JobRunLog) -> bool:
    """True when the row is finalized or already carries a non-empty message (real summary)."""
    if r.finished_at is not None:
        return True
    if (r.message or "").strip():
        return True
    return False


def job_run_log_row_is_suppressible_placeholder(r: JobRunLog) -> bool:
    """Early-commit orphan: no finish time and no message → pending placeholder display only."""
    if job_run_log_row_is_terminal_for_dedupe(r):
        return False
    disp = user_visible_job_run_message(
        message=r.message, ok=bool(r.ok), finished_at=r.finished_at
    )
    return disp == _RUN_SUMMARY_PENDING_DISPLAY


def dedupe_job_run_logs_for_display(rows: Sequence[JobRunLog]) -> list[JobRunLog]:
    """Drop stale ``Run summary not available yet.`` rows when a terminal row exists nearby in time.

    Uses a short ``started_at`` proximity window so orphan early commits still match a final row
    when real-world timestamps differ by a few seconds. Read/render-time only.
    """
    seq = list(rows)

    def placeholder_has_nearby_terminal(p: JobRunLog) -> bool:
        if not job_run_log_row_is_suppressible_placeholder(p):
            return False
        for t in seq:
            if t is p:
                continue
            if not job_run_log_row_is_terminal_for_dedupe(t):
                continue
            delta = _job_run_log_started_at_delta_seconds(p.started_at, t.started_at)
            if delta is not None and delta <= _JOB_RUN_LOG_PLACEHOLDER_NEARBY_SECONDS:
                return True
        return False

    return [r for r in seq if not placeholder_has_nearby_terminal(r)]


def _activity_primary_label(e: ActivityLog) -> str:
    app = (getattr(e, "app", "") or "").strip().lower()
    kind = (getattr(e, "kind", "") or "").strip().lower()
    status = (getattr(e, "status", "") or "ok").strip().lower()
    count = int(getattr(e, "count", 0) or 0)
    if kind == "error" or status == "failed":
        return "Run error" if kind == "error" else "Run failed"
    if kind == "cleanup":
        return f"Radarr queue cleanup ({count} item{'s' if count != 1 else ''})"
    if kind == "trimmed":
        return f"{count} item{'s' if count != 1 else ''} matched Trimmer rules"
    if kind == "missing":
        unit = "episode" if app == "sonarr" else "movie"
        return f"Missing search for {count} {unit}{'s' if count != 1 else ''}"
    if kind == "upgrade":
        unit = "episode" if app == "sonarr" else "movie"
        return f"Upgrade search for {count} {unit}{'s' if count != 1 else ''}"
    return f"Activity ({kind or 'event'})"


def _activity_detail_fallback_line(e: ActivityLog) -> str:
    if (getattr(e, "status", "") or "").strip().lower() == "failed":
        return "Operation failed with no additional details."
    if int(getattr(e, "count", 0) or 0) > 0:
        return "No per-title detail was stored for this entry."
    return "No items were dispatched for this activity entry."


def activity_display_row(e: ActivityLog, tz: str) -> dict[str, Any]:
    raw_detail = (getattr(e, "detail", "") or "").strip()
    detail_lines = activity_log_title_lines(raw_detail)
    if not detail_lines:
        detail_lines = [_activity_detail_fallback_line(e)]
    return {
        "id": e.id,
        "time_local": _fmt_local(e.created_at, tz),
        "app": e.app,
        "kind": e.kind,
        "status": (getattr(e, "status", "") or "ok").strip().lower(),
        "count": e.count,
        "primary_label": _activity_primary_label(e),
        "detail_lines": detail_lines,
    }


def trimmer_settings_fragment(trimmer_section: str | None) -> str:
    key = (trimmer_section or "").strip().lower()
    ids = {
        "connection": "trimmer-connection",
        "schedule": "trimmer-schedule",
        "rules": "trimmer-rules",
        "people": "trimmer-people",
    }
    fid = ids.get(key)
    return f"#{fid}" if fid else ""


def trimmer_settings_redirect_url(*, saved: bool, reason: str | None = None, section: str | None = None) -> str:
    frag = trimmer_settings_fragment(section)
    if saved:
        return f"/trimmer/settings?saved=1{frag}"
    err = quote(str(reason or "error").replace("\n", " ").strip()[:240], safe="")
    return f"/trimmer/settings?save=fail&reason={err}{frag}"


def settings_looks_like_existing_fetcher_install(settings: AppSettings) -> bool:
    """True when Sonarr/Radarr/Emby were already configured — tailors setup step 0 for upgrades."""
    return bool(
        (settings.sonarr_url or "").strip()
        or (settings.radarr_url or "").strip()
        or (settings.emby_url or "").strip()
        or settings.sonarr_enabled
        or settings.radarr_enabled
        or settings.emby_enabled
    )


async def try_commit_and_reschedule(
    session: AsyncSession,
    *,
    targets: set[str] | None = None,
) -> bool:
    """Persist settings and refresh scheduler tick. False if SQLite could not commit (e.g. DB locked)."""
    try:
        await session.commit()
    except SQLAlchemyError:
        try:
            await session.rollback()
        except Exception:
            logger.exception("rollback after failed settings commit")
        return False
    try:
        await scheduler.reschedule(targets=targets)
    except Exception:
        logger.warning("scheduler.reschedule failed after settings commit", exc_info=True)
    return True


def is_setup_complete(settings: AppSettings) -> bool:
    """
    True when onboarding is complete enough that the Setup Wizard can be hidden.

    Source of truth is saved configuration state (not a one-time flag):
    - Auth must be configured (password set).
    - At least one integration must be configured (URL + API key), OR
      if any integration is enabled it must have its required fields present.
    """
    if not (settings.auth_password_hash or "").strip():
        return False

    def has_url_and_key(url: str | None, key: str | None) -> bool:
        return bool((url or "").strip() and (key or "").strip())

    son_cfg = has_url_and_key(settings.sonarr_url, settings.sonarr_api_key)
    rad_cfg = has_url_and_key(settings.radarr_url, settings.radarr_api_key)
    em_cfg = has_url_and_key(settings.emby_url, settings.emby_api_key)

    # If the user explicitly enabled an integration, its required fields must be present.
    if bool(settings.sonarr_enabled) and not son_cfg:
        return False
    if bool(settings.radarr_enabled) and not rad_cfg:
        return False
    if bool(settings.emby_enabled) and not em_cfg:
        return False

    # Otherwise, consider setup complete when *any* integration is configured.
    return bool(son_cfg or rad_cfg or em_cfg)


def movie_credit_types_summary(types: frozenset[str]) -> str:
    short = {
        "actor": "Cast",
        "director": "Director",
        "writer": "Writer",
        "producer": "Producer",
        "gueststar": "Guest",
    }
    order = ("actor", "director", "writer", "producer", "gueststar")
    parts = [short[k] for k in order if k in types]
    return "+".join(parts) if parts else "Cast"


def schedule_days_csv_from_named_day_checks(
    mon: int,
    tue: int,
    wed: int,
    thu: int,
    fri: int,
    sat: int,
    sun: int,
) -> str:
    """One checkbox per day (`name=prefix_Mon` value=1). Uncheck all → store "" (not full week)."""
    flags = (mon, tue, wed, thu, fri, sat, sun)
    parts = [DAY_NAMES[i] for i, v in enumerate(flags) if int(v or 0) != 0]
    if not parts:
        return ""
    return normalize_schedule_days_csv(",".join(parts))


def schedule_weekdays_selected_dict(days_csv: str) -> dict[str, bool]:
    """Per-day flags from DB column (raw). Empty stored value → all False."""
    n = normalize_schedule_days_csv((days_csv or "").strip())
    if not n.strip():
        return {d: False for d in DAY_NAMES}
    allowed = {p.strip() for p in n.split(",") if p.strip() in DAY_NAMES}
    return {d: (d in allowed) for d in DAY_NAMES}


def effective_emby_rules(settings: AppSettings) -> dict[str, int | bool]:
    global_rating = max(0, int(settings.emby_rule_watched_rating_below or 0))
    global_unwatched = max(0, int(settings.emby_rule_unwatched_days or 0))

    movie_rating = max(0, int(settings.emby_rule_movie_watched_rating_below or 0)) or global_rating
    movie_unwatched = max(0, int(settings.emby_rule_movie_unwatched_days or 0)) or global_unwatched
    tv_delete_watched = bool(settings.emby_rule_tv_delete_watched)
    tv_unwatched = max(0, int(settings.emby_rule_tv_unwatched_days or 0)) or global_unwatched

    return {
        "movie_rating_below": movie_rating,
        "movie_unwatched_days": movie_unwatched,
        "tv_delete_watched": tv_delete_watched,
        "tv_unwatched_days": tv_unwatched,
    }


# Total wizard screens (0 .. WIZARD_LAST_STEP_INDEX inclusive). Last index is the "done" page.
SETUP_WIZARD_STEPS = 6
WIZARD_LAST_STEP_INDEX = SETUP_WIZARD_STEPS - 1


def setup_wizard_step_title(step: int) -> str:
    return {
        0: "Account",
        1: "Sonarr",
        2: "Radarr",
        3: "Emby",
        4: "Schedule & timezone",
        5: "What's next",
    }.get(step, "Setup")
