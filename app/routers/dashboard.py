from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
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
from app.models import ActivityLog, JobRunLog
from app.time_util import utc_now_naive
from app.ui_templates import templates
from app.web_common import (
    ACTIVITY_DETAIL_PREVIEW_LINES,
    activity_display_row,
    build_dashboard_status,
    movie_credit_types_summary,
)

from app.routers.deps import AUTH_DEPS

router = APIRouter(dependencies=AUTH_DEPS)


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    activity = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(30)))
        .scalars().all()
    )
    tz = settings.timezone or "UTC"
    activity_display = [activity_display_row(e, tz) for e in activity]
    suggest_setup_wizard = not (
        (settings.sonarr_url or "").strip()
        or (settings.radarr_url or "").strip()
        or (settings.emby_url or "").strip()
    )
    snapshots = await fetch_latest_app_snapshots(session)
    dash_status = await build_dashboard_status(session, tz, snapshots=snapshots)
    last_run_display = dash_status["last_run"]
    next_tick_local = dash_status["next_scheduler_tick_local"]
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
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Dashboard",
            "subtitle": "Status overview and counts",
            "settings": settings,
            "suggest_setup_wizard": suggest_setup_wizard,
            "last_run": last_run_display,
            "next_scheduler_tick_local": next_tick_local,
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
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )


@router.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    logs = (await session.execute(select(JobRunLog).order_by(desc(JobRunLog.id)).limit(200))).scalars().all()
    tz = settings.timezone or "UTC"
    logs_display = [
        {"started_local": _fmt_local(r.started_at, tz), "ok": r.ok, "message": r.message}
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
        },
    )


@router.get("/activity", response_class=HTMLResponse)
async def activity_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    activity = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(200)))
        .scalars().all()
    )
    tz = settings.timezone or "UTC"
    activity_display = [activity_display_row(e, tz) for e in activity]
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
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )
