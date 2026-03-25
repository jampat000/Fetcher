"""Shared helpers for server-rendered pages and settings persistence."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Sequence
from urllib.parse import quote

from app.arr_client import ArrClient, ArrConfig
from app.arr_intervals import effective_arr_interval_minutes
from app.resolvers.api_keys import resolve_radarr_api_key, resolve_sonarr_api_key
from app.service_logic import _radarr_missing_total_including_unreleased, _sonarr_missing_total_including_unreleased
from sqlalchemy import desc, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import _get_or_create_settings, fetch_latest_app_snapshots
from app.display_helpers import (
    _fmt_local,
    _relative_phrase_past,
    _relative_phrase_until,
    _truncate_display,
)
from app.models import ActivityLog, AppSettings, AppSnapshot, JobRunLog
from app.schedule import DAY_NAMES, normalize_schedule_days_csv
from app.scheduler import compute_job_intervals_minutes, scheduler
from app.time_util import utc_now_naive

logger = logging.getLogger(__name__)

# Live dashboard hero: HTTP timeout per Arr request; wall-clock caps so SSR/poll do not hang on huge libraries.
_DASHBOARD_LIVE_ARR_HTTP_TIMEOUT_S = 12.0
_DASHBOARD_SONARR_MISSING_WALL_S = 25.0
_DASHBOARD_RADARR_MOVIES_WALL_S = 20.0

# Activity list shows 5 title lines + “+N more”; full list is stored in ``ActivityLog.detail``.
ACTIVITY_DETAIL_PREVIEW_LINES = 5
_ACTIVITY_LOG_LEGACY_MORE = re.compile(r"^\+\d+ more$")

_JOB_RETRY_HINT_MARKERS = (
    "within retry delay",
    "all items within retry delay",
    "suppressed (retry delay)",
)


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
        return f"{count} item{'s' if count != 1 else ''} matched Emby Trimmer rules"
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


def _job_message_segments(message: str | None) -> list[str]:
    return [p.strip() for p in (message or "").split("|") if p.strip()]


def _job_segments_for_prefix(segments: list[str], prefix: str) -> list[str]:
    """Match ``Sonarr:`` / ``Emby:`` style segments case-insensitively."""
    root = (prefix or "").strip().lower().rstrip(":")
    pl = f"{root}:"
    return [s for s in segments if (s or "").strip().lower().startswith(pl)]


def _segment_suggests_retry_limitation(seg: str) -> bool:
    low = seg.lower()
    return any(m in low for m in _JOB_RETRY_HINT_MARKERS)


def _segment_suggests_items_dispatched(seg: str) -> bool:
    return bool(re.search(r"\bsearch for \d+\b", seg.lower()))


def automation_card_subtext(*, app_key: str, enabled: bool, last_job_message: str | None) -> str:
    """Per-dashboard-card hint from the last JobRunLog only (no cross-app inference)."""
    if not enabled:
        return ""
    lines = _job_segments_for_prefix(_job_message_segments(last_job_message), app_key)
    if not lines:
        return (
            "No line for this app in the last service run - it may have been skipped by schedule, "
            "manual scope, or configuration."
        )
    dispatched = any(_segment_suggests_items_dispatched(s) for s in lines)
    retry_hit = any(_segment_suggests_retry_limitation(s) for s in lines)
    if retry_hit and dispatched:
        return "Some items were skipped due to retry delay; other work still ran."
    if retry_hit:
        return (
            "All candidate items were still inside the retry-delay window - "
            "no searches were sent for this app on that run."
        )
    if any("skipped" in s.lower() for s in lines):
        return "Last run skipped this app - see the service log for the exact reason."
    return "Last service run reported normally for this app."


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


async def fetch_live_dashboard_queue_totals(settings: AppSettings) -> dict[str, int]:
    """Best-effort Arr totals for dashboard hero tiles (independent of scheduler runs).

    **Missing** counts use the same semantics as ``service_logic`` (**monitored** items without files,
    **including** unreleased / never-grabbed), not *only* ``/wanted/missing`` ``totalRecords`` (which
    excludes some unreleased rows).

    **Cutoff-unmet** (upgrade) tiles still use ``/wanted/cutoff`` ``totalRecords`` — that queue is
    defined by Arr for quality upgrades, not the “missing file” catalog walk.

    Per-app keys are omitted on failure/timeout so ``build_dashboard_status`` keeps snapshot fallback.
    """

    async def _sonarr_branch() -> dict[str, int]:
        son_url = (settings.sonarr_url or "").strip()
        son_key = resolve_sonarr_api_key(settings)
        if not (settings.sonarr_enabled and son_url and son_key):
            return {}
        local: dict[str, int] = {}
        client = ArrClient(
            ArrConfig(son_url, son_key), timeout_s=_DASHBOARD_LIVE_ARR_HTTP_TIMEOUT_S
        )
        try:
            son_missing = await asyncio.wait_for(
                _sonarr_missing_total_including_unreleased(client),
                timeout=_DASHBOARD_SONARR_MISSING_WALL_S,
            )
            local["sonarr_missing"] = int(son_missing)
        except TimeoutError:
            logger.debug(
                "Dashboard live Sonarr missing count (including unreleased) timed out",
                exc_info=True,
            )
        except Exception:
            logger.debug(
                "Dashboard live Sonarr missing count (including unreleased) failed",
                exc_info=True,
            )
        try:
            cutoff_raw = await client.wanted_cutoff_unmet(page=1, page_size=1)
            local["sonarr_upgrades"] = int(cutoff_raw.get("totalRecords") or 0)
        except Exception:
            logger.debug("Dashboard live Sonarr cutoff queue total failed", exc_info=True)
        return local

    async def _radarr_branch() -> dict[str, int]:
        rad_url = (settings.radarr_url or "").strip()
        rad_key = resolve_radarr_api_key(settings)
        if not (settings.radarr_enabled and rad_url and rad_key):
            return {}
        local: dict[str, int] = {}
        client = ArrClient(
            ArrConfig(rad_url, rad_key), timeout_s=_DASHBOARD_LIVE_ARR_HTTP_TIMEOUT_S
        )
        try:
            movies = await asyncio.wait_for(
                client.movies(),
                timeout=_DASHBOARD_RADARR_MOVIES_WALL_S,
            )
            local["radarr_missing"] = _radarr_missing_total_including_unreleased(movies)
        except TimeoutError:
            logger.debug("Dashboard live Radarr movies fetch timed out", exc_info=True)
        except Exception:
            logger.debug("Dashboard live Radarr missing count failed", exc_info=True)
        try:
            cutoff_raw = await client.wanted_cutoff_unmet(page=1, page_size=1)
            local["radarr_upgrades"] = int(cutoff_raw.get("totalRecords") or 0)
        except Exception:
            logger.debug("Dashboard live Radarr cutoff queue total failed", exc_info=True)
        return local

    son_o, rad_o = await asyncio.gather(_sonarr_branch(), _radarr_branch())
    out: dict[str, int] = {**son_o, **rad_o}
    return out


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


def _fetcher_phase_for_dashboard(
    *,
    run_busy: bool,
    job_intervals: dict[str, int],
) -> tuple[str, str, str]:
    """Return ``(phase_id, short_label, explanatory_sentence)`` for the global Automation strip.

    Global state stays high-level only — per-app retry delay is shown on each app card, not here.
    """
    if run_busy:
        return (
            "processing",
            "Processing",
            "Fetcher is running Sonarr, Radarr, or Trimmer work. Counts and last-run times refresh when it finishes.",
        )
    if not job_intervals:
        return (
            "idle",
            "Idle",
            "No automation jobs are scheduled. Enable Sonarr, Radarr, or Trimmer (URL + API key) in settings to start interval runs.",
        )
    return (
        "active",
        "Active",
        "Automation is scheduled for one or more apps. Each card below shows that app's latest status and timing.",
    )


async def build_dashboard_status(
    session: AsyncSession,
    tz: str,
    *,
    snapshots: dict[str, AppSnapshot | None] | None = None,
    include_live: bool = True,
) -> dict[str, Any]:
    """Shared JSON payload for dashboard live polling and server-rendered page."""
    now = utc_now_naive()
    snaps = snapshots if snapshots is not None else await fetch_latest_app_snapshots(session)
    last_run = (
        (await session.execute(select(JobRunLog).order_by(desc(JobRunLog.id)).limit(1))).scalars().first()
    )
    last_run_display: dict[str, Any] | None = None
    if last_run:
        rel_svc = _relative_phrase_past(last_run.started_at, now) if last_run.started_at else ""
        last_run_display = {
            "started_local": _fmt_local(last_run.started_at, tz),
            "finished_local": _fmt_local(last_run.finished_at, tz) if last_run.finished_at else "",
            "has_finished": last_run.finished_at is not None,
            "ok": bool(last_run.ok),
            "message": user_visible_job_run_message(
                message=last_run.message,
                ok=bool(last_run.ok),
                finished_at=last_run.finished_at,
            ),
            "relative": rel_svc,
        }
    next_runs = scheduler.next_runs_by_job()
    next_sonarr_dt = next_runs.get("sonarr")
    next_radarr_dt = next_runs.get("radarr")
    next_trimmer_dt = next_runs.get("trimmer")
    settings = await _get_or_create_settings(session)
    snaps = snapshots if snapshots is not None else await fetch_latest_app_snapshots(session)

    def _latest_snapshot_for(app_name: str) -> AppSnapshot | None:
        snap = snaps.get(app_name)
        return snap if isinstance(snap, AppSnapshot) else None

    def _last_from(settings_dt: Any, snap: AppSnapshot | None) -> dict[str, Any]:
        dt = snap.created_at if snap and getattr(snap, "created_at", None) else settings_dt
        if not dt:
            return {"time_local": "", "ok": None, "relative": ""}
        return {
            "time_local": _fmt_local(dt, tz),
            "ok": (bool(snap.ok) if snap is not None else None),
            "relative": _relative_phrase_past(dt, now),
        }

    # Snapshots capture per-app run outcomes (ok/failed) and are the primary source for per-app status.
    sonarr_snap = snaps.get("sonarr")
    radarr_snap = snaps.get("radarr")
    emby_snap = snaps.get("emby")

    latest_activity = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(1))).scalars().first()
    )
    latest_system_event: dict[str, Any] | None = None
    if latest_activity is not None:
        app_raw = (latest_activity.app or "").strip().lower()
        app_name = "System"
        if app_raw == "sonarr":
            app_name = "Sonarr"
        elif app_raw == "radarr":
            app_name = "Radarr"
        elif app_raw == "emby":
            app_name = "Trimmer"
        kind = (latest_activity.kind or "").strip().lower()
        event_name_map = {
            "missing": "Missing search",
            "upgrade": "Upgrade search",
            "trimmed": "Trimmer run",
            "cleanup": "Queue cleanup",
            "error": "Run error",
        }
        event_name = event_name_map.get(kind, kind.replace("_", " ").title() if kind else "Activity event")
        latest_system_event = {
            "context": f"{app_name} • {event_name}",
            "time_local": _fmt_local(latest_activity.created_at, tz),
            "ok": (latest_activity.status or "ok").strip().lower() != "failed",
            "relative": _relative_phrase_past(latest_activity.created_at, now),
        }
    last_sonarr = _last_from(settings.sonarr_last_run_at, _latest_snapshot_for("sonarr"))
    last_radarr = _last_from(settings.radarr_last_run_at, _latest_snapshot_for("radarr"))
    last_trimmer = _last_from(settings.emby_last_run_at, _latest_snapshot_for("emby"))

    # If scheduler next-run is unavailable in this process, keep useful per-app timing by estimating from last+interval.
    if not next_sonarr_dt and settings.sonarr_enabled and last_sonarr["time_local"]:
        dt = settings.sonarr_last_run_at
        if dt:
            next_sonarr_dt = dt + timedelta(
                minutes=effective_arr_interval_minutes(settings.sonarr_interval_minutes)
            )
    if not next_radarr_dt and settings.radarr_enabled and last_radarr["time_local"]:
        dt = settings.radarr_last_run_at
        if dt:
            next_radarr_dt = dt + timedelta(
                minutes=effective_arr_interval_minutes(settings.radarr_interval_minutes)
            )
    if not next_trimmer_dt and settings.emby_enabled and last_trimmer["time_local"]:
        dt = settings.emby_last_run_at
        if dt:
            next_trimmer_dt = dt + timedelta(minutes=max(5, int(settings.emby_interval_minutes or 60)))

    next_sonarr_local = _fmt_local(next_sonarr_dt, tz) if next_sonarr_dt else ""
    next_radarr_local = _fmt_local(next_radarr_dt, tz) if next_radarr_dt else ""
    next_trimmer_local = _fmt_local(next_trimmer_dt, tz) if next_trimmer_dt else ""
    next_sonarr_relative = _relative_phrase_until(next_sonarr_dt, now) if next_sonarr_dt else ""
    next_radarr_relative = _relative_phrase_until(next_radarr_dt, now) if next_radarr_dt else ""
    next_trimmer_relative = _relative_phrase_until(next_trimmer_dt, now) if next_trimmer_dt else ""

    job_intervals = compute_job_intervals_minutes(settings)
    job_msg = (last_run.message or "") if last_run else ""
    phase_id, phase_label, phase_detail = _fetcher_phase_for_dashboard(
        run_busy=scheduler.is_run_in_progress(),
        job_intervals=job_intervals,
    )
    sonarr_automation_sub = automation_card_subtext(
        app_key="sonarr", enabled=bool(settings.sonarr_enabled), last_job_message=job_msg
    )
    radarr_automation_sub = automation_card_subtext(
        app_key="radarr", enabled=bool(settings.radarr_enabled), last_job_message=job_msg
    )
    trimmer_automation_sub = automation_card_subtext(
        app_key="emby", enabled=bool(settings.emby_enabled), last_job_message=job_msg
    )
    sonarr_snap = snaps.get("sonarr")
    radarr_snap = snaps.get("radarr")
    emby_snap = snaps.get("emby")
    sonarr_missing = int(sonarr_snap.missing_total) if sonarr_snap else 0
    sonarr_upgrades = int(sonarr_snap.cutoff_unmet_total) if sonarr_snap else 0
    radarr_missing = int(radarr_snap.missing_total) if radarr_snap else 0
    radarr_upgrades = int(radarr_snap.cutoff_unmet_total) if radarr_snap else 0
    if include_live:
        live = await fetch_live_dashboard_queue_totals(settings)
        if "sonarr_missing" in live:
            sonarr_missing = live["sonarr_missing"]
        if "sonarr_upgrades" in live:
            sonarr_upgrades = live["sonarr_upgrades"]
        if "radarr_missing" in live:
            radarr_missing = live["radarr_missing"]
        if "radarr_upgrades" in live:
            radarr_upgrades = live["radarr_upgrades"]
    return {
        "last_run": last_run_display,
        "last_sonarr_run": last_sonarr,
        "last_radarr_run": last_radarr,
        "last_trimmer_run": last_trimmer,
        "latest_system_event": latest_system_event,
        "next_sonarr_tick_local": next_sonarr_local,
        "next_radarr_tick_local": next_radarr_local,
        "next_trimmer_tick_local": next_trimmer_local,
        "next_sonarr_relative": next_sonarr_relative,
        "next_radarr_relative": next_radarr_relative,
        "next_trimmer_relative": next_trimmer_relative,
        "fetcher_phase": phase_id,
        "fetcher_phase_label": phase_label,
        "fetcher_phase_detail": phase_detail,
        "sonarr_automation_sub": sonarr_automation_sub,
        "radarr_automation_sub": radarr_automation_sub,
        "trimmer_automation_sub": trimmer_automation_sub,
        "sonarr_missing": sonarr_missing,
        "sonarr_upgrades": sonarr_upgrades,
        "radarr_missing": radarr_missing,
        "radarr_upgrades": radarr_upgrades,
        "emby_matched": int(emby_snap.missing_total) if emby_snap else 0,
    }


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
