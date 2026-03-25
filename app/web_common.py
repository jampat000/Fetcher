"""Shared helpers for server-rendered pages and settings persistence."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import timedelta
from typing import Any
from urllib.parse import quote

from app.arr_client import ArrClient, ArrConfig
from app.arr_intervals import effective_arr_interval_minutes
from app.resolvers.api_keys import resolve_radarr_api_key, resolve_sonarr_api_key
from sqlalchemy import desc, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import _get_or_create_settings, fetch_latest_app_snapshots
from app.display_helpers import _fmt_local, _truncate_display
from app.models import ActivityLog, AppSettings, AppSnapshot, JobRunLog
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


def activity_display_row(e: ActivityLog, tz: str) -> dict[str, Any]:
    raw_detail = (getattr(e, "detail", "") or "").strip()
    return {
        "id": e.id,
        "time_local": _fmt_local(e.created_at, tz),
        "app": e.app,
        "kind": e.kind,
        "status": (getattr(e, "status", "") or "ok").strip().lower(),
        "count": e.count,
        "detail_lines": activity_log_title_lines(raw_detail),
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


async def fetch_live_dashboard_queue_totals(settings: AppSettings) -> dict[str, int]:
    """Best-effort *arr wanted queue sizes for dashboard hero tiles (no scheduler / snapshot lag).

    Uses a short HTTP timeout so a dead Sonarr/Radarr does not block dashboard status.
    Keys present only for apps where both missing + cutoff ``totalRecords`` were read successfully.
    """
    out: dict[str, int] = {}

    son_url = (settings.sonarr_url or "").strip()
    son_key = resolve_sonarr_api_key(settings)
    if settings.sonarr_enabled and son_url and son_key:
        try:
            client = ArrClient(ArrConfig(son_url, son_key), timeout_s=4.0)
            missing_raw, cutoff_raw = await asyncio.gather(
                client.wanted_missing(page=1, page_size=1),
                client.wanted_cutoff_unmet(page=1, page_size=1),
            )
            out["sonarr_missing"] = int(missing_raw.get("totalRecords") or 0)
            out["sonarr_upgrades"] = int(cutoff_raw.get("totalRecords") or 0)
        except Exception:
            logger.debug("Dashboard live Sonarr queue totals unavailable", exc_info=True)

    rad_url = (settings.radarr_url or "").strip()
    rad_key = resolve_radarr_api_key(settings)
    if settings.radarr_enabled and rad_url and rad_key:
        try:
            client = ArrClient(ArrConfig(rad_url, rad_key), timeout_s=4.0)
            missing_raw, cutoff_raw = await asyncio.gather(
                client.wanted_missing(page=1, page_size=1),
                client.wanted_cutoff_unmet(page=1, page_size=1),
            )
            out["radarr_missing"] = int(missing_raw.get("totalRecords") or 0)
            out["radarr_upgrades"] = int(cutoff_raw.get("totalRecords") or 0)
        except Exception:
            logger.debug("Dashboard live Radarr queue totals unavailable", exc_info=True)

    return out


async def build_dashboard_status(
    session: AsyncSession,
    tz: str,
    *,
    snapshots: dict[str, AppSnapshot | None] | None = None,
) -> dict[str, Any]:
    """Shared JSON payload for dashboard live polling and server-rendered page."""
    snaps = snapshots if snapshots is not None else await fetch_latest_app_snapshots(session)
    last_run = (
        (await session.execute(select(JobRunLog).order_by(desc(JobRunLog.id)).limit(1))).scalars().first()
    )
    last_run_display: dict[str, Any] | None = None
    if last_run:
        last_run_display = {
            "started_local": _fmt_local(last_run.started_at, tz),
            "finished_local": _fmt_local(last_run.finished_at, tz) if last_run.finished_at else "",
            "has_finished": last_run.finished_at is not None,
            "ok": bool(last_run.ok),
            "message": _truncate_display(last_run.message or ""),
        }
    next_runs = scheduler.next_runs_by_job()
    next_sonarr = next_runs.get("sonarr")
    next_radarr = next_runs.get("radarr")
    next_trimmer = next_runs.get("trimmer")
    next_sonarr_local = _fmt_local(next_sonarr, tz) if next_sonarr else ""
    next_radarr_local = _fmt_local(next_radarr, tz) if next_radarr else ""
    next_trimmer_local = _fmt_local(next_trimmer, tz) if next_trimmer else ""
    settings = await _get_or_create_settings(session)
    snaps = snapshots if snapshots is not None else await fetch_latest_app_snapshots(session)

    def _latest_snapshot_for(app_name: str) -> AppSnapshot | None:
        snap = snaps.get(app_name)
        return snap if isinstance(snap, AppSnapshot) else None

    def _last_from(settings_dt: Any, snap: AppSnapshot | None) -> dict[str, Any]:
        dt = snap.created_at if snap and getattr(snap, "created_at", None) else settings_dt
        if not dt:
            return {"time_local": "", "ok": None}
        return {"time_local": _fmt_local(dt, tz), "ok": (bool(snap.ok) if snap is not None else None)}

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
        }
    last_sonarr = _last_from(settings.sonarr_last_run_at, _latest_snapshot_for("sonarr"))
    last_radarr = _last_from(settings.radarr_last_run_at, _latest_snapshot_for("radarr"))
    last_trimmer = _last_from(settings.emby_last_run_at, _latest_snapshot_for("emby"))

    # If scheduler next-run is unavailable in this process, keep useful per-app timing by estimating from last+interval.
    if not next_sonarr and settings.sonarr_enabled and last_sonarr["time_local"]:
        dt = settings.sonarr_last_run_at
        if dt:
            next_sonarr_local = _fmt_local(dt + timedelta(minutes=effective_arr_interval_minutes(settings.sonarr_interval_minutes)), tz)
    if not next_radarr and settings.radarr_enabled and last_radarr["time_local"]:
        dt = settings.radarr_last_run_at
        if dt:
            next_radarr_local = _fmt_local(dt + timedelta(minutes=effective_arr_interval_minutes(settings.radarr_interval_minutes)), tz)
    if not next_trimmer and settings.emby_enabled and last_trimmer["time_local"]:
        dt = settings.emby_last_run_at
        if dt:
            next_trimmer_local = _fmt_local(dt + timedelta(minutes=max(5, int(settings.emby_interval_minutes or 60))), tz)
    sonarr_snap = snaps.get("sonarr")
    radarr_snap = snaps.get("radarr")
    emby_snap = snaps.get("emby")
    sonarr_missing = int(sonarr_snap.missing_total) if sonarr_snap else 0
    sonarr_upgrades = int(sonarr_snap.cutoff_unmet_total) if sonarr_snap else 0
    radarr_missing = int(radarr_snap.missing_total) if radarr_snap else 0
    radarr_upgrades = int(radarr_snap.cutoff_unmet_total) if radarr_snap else 0
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
