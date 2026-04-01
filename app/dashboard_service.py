"""Dashboard automation status and live Arr queue totals (non-HTTP service layer)."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import timedelta
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import ArrClient, ArrConfig
from app.arr_intervals import effective_arr_interval_minutes
from app.db import get_or_create_settings, fetch_latest_app_snapshots
from app.display_helpers import (
    fmt_local,
    _relative_phrase_past,
    _relative_phrase_until,
)
from app.models import ActivityLog, AppSettings, AppSnapshot, JobRunLog
from app.resolvers.api_keys import resolve_emby_api_key, resolve_radarr_api_key, resolve_sonarr_api_key
from app.scheduler import compute_job_intervals_minutes, scheduler
from app.service_logic import _radarr_missing_total_including_unreleased, _sonarr_missing_total_including_unreleased
from app.time_util import utc_now_naive
from app.web_common import user_visible_job_run_message

logger = logging.getLogger(__name__)

# Live dashboard hero: HTTP timeout per Arr request; wall-clock caps so SSR/poll do not hang on huge libraries.
_DASHBOARD_LIVE_ARR_HTTP_TIMEOUT_S = 12.0
_DASHBOARD_SONARR_MISSING_WALL_S = 25.0
_DASHBOARD_RADARR_MOVIES_WALL_S = 20.0

def trimmer_connection_status_display(
    settings: AppSettings,
    emby_snap: AppSnapshot | None,
) -> tuple[str, str]:
    """System row: media server backend type label and read-only connection status (Trimmer slot)."""
    key = resolve_emby_api_key(settings)
    url = (settings.emby_url or "").strip()
    configured = bool(url and key)
    backend_type = "Emby"
    if not configured:
        return (backend_type, "Not configured")
    if emby_snap is None:
        return (backend_type, "Configured")
    return (backend_type, "Connected" if emby_snap.ok else "Not connected")


_JOB_RETRY_HINT_MARKERS = (
    "within retry delay",
    "all items within retry delay",
    "suppressed (retry delay)",
)


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


def automation_card_subtext(
    *,
    app_key: str,
    enabled: bool,
    last_job_message: str | None,
    app_has_run_evidence: bool = False,
) -> str:
    """Per-dashboard-card hint from the last JobRunLog message plus per-app run timing on the card.

    When the card already shows a last-run time for this app, an empty message for that app is
    treated as unknown formatting — not as “never ran” — so we omit the skipped-by-config fallback.
    """
    if not enabled:
        return ""
    lines = _job_segments_for_prefix(_job_message_segments(last_job_message), app_key)
    if not lines:
        if app_has_run_evidence:
            return ""
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
    return ""


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


async def _fetch_live_arr_totals(session: AsyncSession, out: dict[str, Any], settings: AppSettings) -> None:
    live = await fetch_live_dashboard_queue_totals(settings)
    if "sonarr_missing" in live:
        out["sonarr_missing"] = live["sonarr_missing"]
    if "sonarr_upgrades" in live:
        out["sonarr_upgrades"] = live["sonarr_upgrades"]
    if "radarr_missing" in live:
        out["radarr_missing"] = live["radarr_missing"]
    if "radarr_upgrades" in live:
        out["radarr_upgrades"] = live["radarr_upgrades"]


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
            "started_local": fmt_local(last_run.started_at, tz),
            "finished_local": fmt_local(last_run.finished_at, tz) if last_run.finished_at else "",
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
    settings = await get_or_create_settings(session)
    snaps = snapshots if snapshots is not None else await fetch_latest_app_snapshots(session)

    def _latest_snapshot_for(app_name: str) -> AppSnapshot | None:
        snap = snaps.get(app_name)
        return snap if isinstance(snap, AppSnapshot) else None

    def _last_from(settings_dt: Any, snap: AppSnapshot | None) -> dict[str, Any]:
        dt = snap.created_at if snap and getattr(snap, "created_at", None) else settings_dt
        if not dt:
            return {"time_local": "", "ok": None, "relative": ""}
        return {
            "time_local": fmt_local(dt, tz),
            "ok": (bool(snap.ok) if snap is not None else None),
            "relative": _relative_phrase_past(dt, now),
        }

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
        elif app_raw == "trimmer":
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
            "time_local": fmt_local(latest_activity.created_at, tz),
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

    next_sonarr_local = fmt_local(next_sonarr_dt, tz) if next_sonarr_dt else ""
    next_radarr_local = fmt_local(next_radarr_dt, tz) if next_radarr_dt else ""
    next_trimmer_local = fmt_local(next_trimmer_dt, tz) if next_trimmer_dt else ""
    next_sonarr_relative = _relative_phrase_until(next_sonarr_dt, now) if next_sonarr_dt else ""
    next_radarr_relative = _relative_phrase_until(next_radarr_dt, now) if next_radarr_dt else ""
    next_trimmer_relative = _relative_phrase_until(next_trimmer_dt, now) if next_trimmer_dt else ""

    job_intervals = compute_job_intervals_minutes(settings)
    job_msg = (last_run.message or "") if last_run else ""
    phase_id, phase_label, phase_detail = _fetcher_phase_for_dashboard(
        run_busy=scheduler.is_run_in_progress(),
        job_intervals=job_intervals,
    )
    def _card_has_run(row: dict[str, Any]) -> bool:
        return bool((str(row.get("time_local") or "")).strip())

    sonarr_automation_sub = automation_card_subtext(
        app_key="sonarr",
        enabled=bool(settings.sonarr_enabled),
        last_job_message=job_msg,
        app_has_run_evidence=_card_has_run(last_sonarr),
    )
    radarr_automation_sub = automation_card_subtext(
        app_key="radarr",
        enabled=bool(settings.radarr_enabled),
        last_job_message=job_msg,
        app_has_run_evidence=_card_has_run(last_radarr),
    )
    trimmer_automation_sub = automation_card_subtext(
        app_key="emby",
        enabled=bool(settings.emby_enabled),
        last_job_message=job_msg,
        app_has_run_evidence=_card_has_run(last_trimmer),
    )
    sonarr_snap = snaps.get("sonarr")
    radarr_snap = snaps.get("radarr")
    emby_snap = snaps.get("emby")
    conn_type, conn_status = trimmer_connection_status_display(settings, emby_snap)
    sonarr_missing = int(sonarr_snap.missing_total) if sonarr_snap else 0
    sonarr_upgrades = int(sonarr_snap.cutoff_unmet_total) if sonarr_snap else 0
    radarr_missing = int(radarr_snap.missing_total) if radarr_snap else 0
    radarr_upgrades = int(radarr_snap.cutoff_unmet_total) if radarr_snap else 0
    if include_live:
        out = {
            "sonarr_missing": sonarr_missing,
            "sonarr_upgrades": sonarr_upgrades,
            "radarr_missing": radarr_missing,
            "radarr_upgrades": radarr_upgrades,
        }
        try:
            await asyncio.wait_for(_fetch_live_arr_totals(session, out, settings), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Dashboard: live Arr totals timed out after 10s — using cached values")
        sonarr_missing = out["sonarr_missing"]
        sonarr_upgrades = out["sonarr_upgrades"]
        radarr_missing = out["radarr_missing"]
        radarr_upgrades = out["radarr_upgrades"]
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
        "trimmer_connection_type": conn_type,
        "trimmer_connection_status": conn_status,
    }


# Re-export for tests and callers that need stable import paths for patching.
__all__ = [
    "automation_card_subtext",
    "build_dashboard_status",
    "fetch_live_dashboard_queue_totals",
    "trimmer_connection_status_display",
    "_fetcher_phase_for_dashboard",
]
