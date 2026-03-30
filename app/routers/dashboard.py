from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_csrf_token_for_template
from app.branding import APP_NAME, APP_TAGLINE
from app.db import _get_or_create_settings, fetch_latest_app_snapshots, get_session
from app.display_helpers import (
    _fmt_local,
    _now_local,
    _schedule_days_display,
    _schedule_time_range_friendly,
    _to_12h,
)
from app.emby_rules import (
    parse_genres_csv,
    parse_movie_people_credit_types_csv,
    parse_movie_people_phrases,
)
from app.models import ActivityLog, JobRunLog, RefinerActivity
from app.paths import is_safe_path, resolved_logs_dir
from app.time_util import utc_now_naive
from app.dashboard_service import build_dashboard_status
from app.ui_templates import templates
from app.web_common import (
    ACTIVITY_DETAIL_PREVIEW_LINES,
    dedupe_job_run_logs_for_display,
    is_setup_complete,
    merge_activity_feed,
    movie_credit_types_summary,
    user_visible_job_run_message,
)

from app.routers.deps import AUTH_DEPS

router = APIRouter(dependencies=AUTH_DEPS)


def _automation_view_for_template(settings: Any, dash_status: Mapping[str, Any]) -> dict[str, Any]:
    """Single dict for Automation card rows + footer (dashboard.html ``automation_view`` / ``av``)."""
    return {
        "fetcher_phase": dash_status["fetcher_phase"],
        "fetcher_phase_label": dash_status["fetcher_phase_label"],
        "fetcher_phase_detail": dash_status["fetcher_phase_detail"],
        "last_sonarr_run": dash_status["last_sonarr_run"],
        "last_radarr_run": dash_status["last_radarr_run"],
        "last_trimmer_run": dash_status["last_trimmer_run"],
        "next_sonarr_tick_local": dash_status["next_sonarr_tick_local"],
        "next_radarr_tick_local": dash_status["next_radarr_tick_local"],
        "next_trimmer_tick_local": dash_status["next_trimmer_tick_local"],
        "next_sonarr_relative": dash_status["next_sonarr_relative"],
        "next_radarr_relative": dash_status["next_radarr_relative"],
        "next_trimmer_relative": dash_status["next_trimmer_relative"],
        "sonarr_enabled": bool(settings.sonarr_enabled),
        "radarr_enabled": bool(settings.radarr_enabled),
        "refiner_enabled": bool(settings.refiner_enabled),
        "emby_enabled": bool(settings.emby_enabled),
        "refiner_last_run_at": settings.refiner_last_run_at,
        "sonarr_automation_sub": dash_status["sonarr_automation_sub"],
        "radarr_automation_sub": dash_status["radarr_automation_sub"],
        "trimmer_automation_sub": dash_status["trimmer_automation_sub"],
    }


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    tz = settings.timezone or "UTC"
    now = utc_now_naive()
    activity_logs = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(200)))
        .scalars().all()
    )
    refiner_logs = (
        (await session.execute(select(RefinerActivity).order_by(desc(RefinerActivity.id)).limit(200)))
        .scalars().all()
    )
    activity_display = merge_activity_feed(activity_logs, refiner_logs, tz, now, limit=200)
    show_setup_wizard = not is_setup_complete(settings)
    snapshots = await fetch_latest_app_snapshots(session)
    # Render dashboard quickly (no blocking live Arr totals). Live hero polling happens
    # client-side via ``/api/dashboard/status`` after the page is visible.
    dash_status = await build_dashboard_status(session, tz, snapshots=snapshots, include_live=False)
    hero_sonarr_missing = dash_status["sonarr_missing"]
    hero_sonarr_upgrades = dash_status["sonarr_upgrades"]
    hero_radarr_missing = dash_status["radarr_missing"]
    hero_radarr_upgrades = dash_status["radarr_upgrades"]
    latest_system_event = dash_status["latest_system_event"]
    last_sonarr_run = dash_status["last_sonarr_run"]
    last_radarr_run = dash_status["last_radarr_run"]
    last_trimmer_run = dash_status["last_trimmer_run"]
    next_sonarr_tick_local = dash_status["next_sonarr_tick_local"]
    next_radarr_tick_local = dash_status["next_radarr_tick_local"]
    next_trimmer_tick_local = dash_status["next_trimmer_tick_local"]
    next_sonarr_relative = dash_status["next_sonarr_relative"]
    next_radarr_relative = dash_status["next_radarr_relative"]
    next_trimmer_relative = dash_status["next_trimmer_relative"]
    fetcher_phase = dash_status["fetcher_phase"]
    fetcher_phase_label = dash_status["fetcher_phase_label"]
    fetcher_phase_detail = dash_status["fetcher_phase_detail"]
    sonarr_automation_sub = dash_status["sonarr_automation_sub"]
    radarr_automation_sub = dash_status["radarr_automation_sub"]
    trimmer_automation_sub = dash_status["trimmer_automation_sub"]
    sonarr_snap = snapshots.get("sonarr")
    radarr_snap = snapshots.get("radarr")
    emby_snap = snapshots.get("emby")
    emby_schedule_start_display = _to_12h(settings.emby_schedule_start or "00:00", "12:00 AM")
    emby_schedule_end_display = _to_12h(settings.emby_schedule_end or "23:59", "11:59 PM")
    sonarr_schedule_start_display = _to_12h(settings.sonarr_schedule_start or "00:00", "12:00 AM")
    sonarr_schedule_end_display = _to_12h(settings.sonarr_schedule_end or "23:59", "11:59 PM")
    radarr_schedule_start_display = _to_12h(settings.radarr_schedule_start or "00:00", "12:00 AM")
    radarr_schedule_end_display = _to_12h(settings.radarr_schedule_end or "23:59", "11:59 PM")
    sonarr_schedule_days_display = _schedule_days_display(settings.sonarr_schedule_days or "")
    sonarr_schedule_time_friendly = _schedule_time_range_friendly(
        settings.sonarr_schedule_start or "00:00",
        settings.sonarr_schedule_end or "23:59",
    )
    radarr_schedule_days_display = _schedule_days_display(settings.radarr_schedule_days or "")
    radarr_schedule_time_friendly = _schedule_time_range_friendly(
        settings.radarr_schedule_start or "00:00",
        settings.radarr_schedule_end or "23:59",
    )
    automation_view = _automation_view_for_template(settings, dash_status)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Dashboard",
            "subtitle": "Status overview and counts",
            "settings": settings,
            "show_setup_wizard": show_setup_wizard,
            "latest_system_event": latest_system_event,
            "last_sonarr_run": last_sonarr_run,
            "last_radarr_run": last_radarr_run,
            "last_trimmer_run": last_trimmer_run,
            "next_sonarr_tick_local": next_sonarr_tick_local,
            "next_radarr_tick_local": next_radarr_tick_local,
            "next_trimmer_tick_local": next_trimmer_tick_local,
            "next_sonarr_relative": next_sonarr_relative,
            "next_radarr_relative": next_radarr_relative,
            "next_trimmer_relative": next_trimmer_relative,
            "fetcher_phase": fetcher_phase,
            "fetcher_phase_label": fetcher_phase_label,
            "fetcher_phase_detail": fetcher_phase_detail,
            "sonarr_automation_sub": sonarr_automation_sub,
            "radarr_automation_sub": radarr_automation_sub,
            "trimmer_automation_sub": trimmer_automation_sub,
            "trimmer_connection_type": dash_status["trimmer_connection_type"],
            "trimmer_connection_status": dash_status["trimmer_connection_status"],
            "automation_view": automation_view,
            "emby_schedule_start_display": emby_schedule_start_display,
            "emby_schedule_end_display": emby_schedule_end_display,
            "sonarr_schedule_start_display": sonarr_schedule_start_display,
            "sonarr_schedule_end_display": sonarr_schedule_end_display,
            "radarr_schedule_start_display": radarr_schedule_start_display,
            "radarr_schedule_end_display": radarr_schedule_end_display,
            "sonarr_schedule_days_display": sonarr_schedule_days_display,
            "sonarr_schedule_time_friendly": sonarr_schedule_time_friendly,
            "radarr_schedule_days_display": radarr_schedule_days_display,
            "radarr_schedule_time_friendly": radarr_schedule_time_friendly,
            "hero_sonarr_missing": hero_sonarr_missing,
            "hero_sonarr_upgrades": hero_sonarr_upgrades,
            "hero_radarr_missing": hero_radarr_missing,
            "hero_radarr_upgrades": hero_radarr_upgrades,
            "activity": activity_display,
            "activity_detail_preview": ACTIVITY_DETAIL_PREVIEW_LINES,
            "sonarr": sonarr_snap,
            "radarr": radarr_snap,
            "emby": emby_snap,
            "selected_movie_genres": sorted(parse_genres_csv(settings.emby_rule_movie_genres_csv)),
            "selected_tv_genres": sorted(parse_genres_csv(settings.emby_rule_tv_genres_csv)),
            "movie_people_phrases": parse_movie_people_phrases(settings.emby_rule_movie_people_csv),
            "movie_people_credit_types": parse_movie_people_credit_types_csv(
                settings.emby_rule_movie_people_credit_types_csv
            ),
            "movie_people_credit_summary": movie_credit_types_summary(
                parse_movie_people_credit_types_csv(
                    settings.emby_rule_movie_people_credit_types_csv
                )
            ),
            "tv_people_phrases": parse_movie_people_phrases(settings.emby_rule_tv_people_csv),
            "tv_people_credit_summary": movie_credit_types_summary(
                parse_movie_people_credit_types_csv(
                    settings.emby_rule_tv_people_credit_types_csv
                )
            ),
            "now": now,
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )


@router.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    logs = (await session.execute(select(JobRunLog).order_by(desc(JobRunLog.id)).limit(200))).scalars().all()
    logs = dedupe_job_run_logs_for_display(logs)
    tz = settings.timezone or "UTC"
    logs_display = [
        {
            "started_local": _fmt_local(r.started_at, tz),
            "ok": r.ok,
            "message": user_visible_job_run_message(
                message=r.message, ok=bool(r.ok), finished_at=r.finished_at
            ),
        }
        for r in logs
    ]
    return templates.TemplateResponse(
        request,
        "logs.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Logs",
            "subtitle": "Service run history",
            "logs": logs_display,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
        },
    )


@router.get("/logs/file", response_class=PlainTextResponse)
async def logs_file(name: str, _request: Request) -> PlainTextResponse:
    """Read a log file only when it resolves under the designated logs directory."""
    logs_root = resolved_logs_dir()
    candidate = (logs_root / Path(name).name).resolve()
    if not is_safe_path(candidate, logs_root.resolve()):
        raise HTTPException(
            status_code=403,
            detail="That file path is not allowed — open logs only via links from the Logs page.",
        )
    if not candidate.is_file():
        return PlainTextResponse(
            "Log file not found — it may have been rotated or pruned. Return to Logs and pick another file.",
            status_code=404,
        )
    try:
        text = candidate.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return PlainTextResponse(
            "Could not read the log file from disk — check permissions or try again.",
            status_code=500,
        )
    return PlainTextResponse(text)


@router.get("/activity", response_class=HTMLResponse)
async def activity_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    tz = settings.timezone or "UTC"
    now = utc_now_naive()
    activity_logs = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(200)))
        .scalars().all()
    )
    refiner_logs = (
        (await session.execute(select(RefinerActivity).order_by(desc(RefinerActivity.id)).limit(200)))
        .scalars().all()
    )
    activity_display = merge_activity_feed(activity_logs, refiner_logs, tz, now, limit=200)
    return templates.TemplateResponse(
        request,
        "activity.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Activity",
            "subtitle": "What Fetcher grabbed",
            "activity": activity_display,
            "activity_detail_preview": ACTIVITY_DETAIL_PREVIEW_LINES,
            "now": now,
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
        },
    )
