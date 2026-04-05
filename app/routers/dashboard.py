from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_csrf_token_for_template
from app.branding import APP_NAME, APP_TAGLINE
from app.db import get_or_create_settings, fetch_latest_app_snapshots, get_session
from app.display_helpers import (
    now_local,
    schedule_days_display,
    schedule_time_range_friendly,
    to_12h,
)
from app.emby_rules import (
    parse_genres_csv,
    parse_movie_people_credit_types_csv,
    parse_movie_people_phrases,
)
from app.models import ActivityLog, RefinerActivity
from app.paths import is_safe_path, resolved_logs_dir
from app.time_util import utc_now_naive
from app.dashboard_service import build_dashboard_status
from app.ui_templates import templates
from app.web_common import (
    ACTIVITY_DETAIL_PREVIEW_LINES,
    filter_activity_display_for_search,
    filter_activity_display_for_tab,
    is_setup_complete,
    merge_activity_feed,
    movie_credit_types_summary,
    normalize_activity_tab_query,
    sidebar_health_dots,
)

from app.routers.deps import AUTH_DEPS

router = APIRouter(dependencies=AUTH_DEPS)


def _automation_view_for_template(settings: Any, dash_status: Mapping[str, Any]) -> dict[str, Any]:
    """Single dict for Automation card rows + footer (dashboard.html ``automation_view`` / ``av``)."""
    def _fallback_display(*, enabled: bool, local: str, rel: str) -> dict[str, str]:
        if not enabled:
            return {"state": "disabled", "primary": "Off", "secondary": "Disabled in settings"}
        if local:
            return {"state": "scheduled", "primary": local, "secondary": rel or ""}
        return {"state": "enabled_unscheduled", "primary": "Always on", "secondary": "No schedule configured"}

    return {
        "fetcher_phase": dash_status["fetcher_phase"],
        "fetcher_phase_label": dash_status["fetcher_phase_label"],
        "fetcher_phase_detail": dash_status["fetcher_phase_detail"],
        "last_sonarr_run": dash_status["last_sonarr_run"],
        "last_radarr_run": dash_status["last_radarr_run"],
        "last_sonarr_cleanup_run": dash_status.get(
            "last_sonarr_cleanup_run",
            {"time_local": "", "ok": None, "relative": ""},
        ),
        "last_radarr_cleanup_run": dash_status.get(
            "last_radarr_cleanup_run",
            {"time_local": "", "ok": None, "relative": ""},
        ),
        "last_trimmer_run": dash_status["last_trimmer_run"],
        "last_refiner_run": dash_status.get(
            "last_refiner_run", {"time_local": "", "ok": None, "relative": "", "time_iso": ""}
        ),
        "next_sonarr_tick_local": dash_status["next_sonarr_tick_local"],
        "next_radarr_tick_local": dash_status["next_radarr_tick_local"],
        "next_trimmer_tick_local": dash_status["next_trimmer_tick_local"],
        "next_sonarr_relative": dash_status["next_sonarr_relative"],
        "next_radarr_relative": dash_status["next_radarr_relative"],
        "next_trimmer_relative": dash_status["next_trimmer_relative"],
        "next_refiner_tick_local": dash_status.get("next_refiner_tick_local", ""),
        "next_refiner_relative": dash_status.get("next_refiner_relative", ""),
        "sonarr_enabled": bool(settings.sonarr_enabled),
        "radarr_enabled": bool(settings.radarr_enabled),
        "refiner_enabled": bool(settings.refiner_enabled),
        "emby_enabled": bool(settings.emby_enabled),
        "next_sonarr_display": dash_status.get("next_sonarr_display")
        or _fallback_display(
            enabled=bool(settings.sonarr_enabled),
            local=str(dash_status.get("next_sonarr_tick_local") or ""),
            rel=str(dash_status.get("next_sonarr_relative") or ""),
        ),
        "next_sonarr_cleanup_display": dash_status.get("next_sonarr_cleanup_display")
        or {
            "state": "disabled",
            "primary": "Not scheduled",
            "secondary": "No failed-import cleanup actions enabled",
        },
        "next_radarr_display": dash_status.get("next_radarr_display")
        or _fallback_display(
            enabled=bool(settings.radarr_enabled),
            local=str(dash_status.get("next_radarr_tick_local") or ""),
            rel=str(dash_status.get("next_radarr_relative") or ""),
        ),
        "next_radarr_cleanup_display": dash_status.get("next_radarr_cleanup_display")
        or {
            "state": "disabled",
            "primary": "Not scheduled",
            "secondary": "No failed-import cleanup actions enabled",
        },
        "next_refiner_display": dash_status.get("next_refiner_display")
        or _fallback_display(
            enabled=bool(settings.refiner_enabled),
            local=str(dash_status.get("next_refiner_tick_local") or ""),
            rel=str(dash_status.get("next_refiner_relative") or ""),
        ),
        "next_trimmer_display": dash_status.get("next_trimmer_display")
        or _fallback_display(
            enabled=bool(settings.emby_enabled),
            local=str(dash_status.get("next_trimmer_tick_local") or ""),
            rel=str(dash_status.get("next_trimmer_relative") or ""),
        ),
        "sonarr_automation_sub": dash_status["sonarr_automation_sub"],
        "radarr_automation_sub": dash_status["radarr_automation_sub"],
        "trimmer_automation_sub": dash_status["trimmer_automation_sub"],
        "sonarr_sparkline": dash_status.get("sonarr_sparkline", []),
        "radarr_sparkline": dash_status.get("radarr_sparkline", []),
        "refiner_sparkline": dash_status.get("refiner_sparkline", []),
        "trimmer_sparkline": dash_status.get("trimmer_sparkline", []),
        "refiner_live_total": dash_status.get("refiner_live_total", 0),
        "refiner_live_done": dash_status.get("refiner_live_done", 0),
        "last_sonarr_refiner_run": dash_status.get(
            "last_sonarr_refiner_run",
            {
                "time_local": "",
                "ok": None,
                "relative": "",
                "time_iso": "",
                "outcome": "none",
            },
        ),
        "next_sonarr_refiner_display": dash_status.get("next_sonarr_refiner_display")
        or _fallback_display(
            enabled=bool(getattr(settings, "sonarr_refiner_enabled", False)),
            local=str(dash_status.get("next_sonarr_refiner_tick_local") or ""),
            rel=str(dash_status.get("next_sonarr_refiner_relative") or ""),
        ),
        "sonarr_refiner_sparkline": dash_status.get("sonarr_refiner_sparkline", []),
        "sonarr_refiner_live_total": dash_status.get("sonarr_refiner_live_total", 0),
        "sonarr_refiner_live_done": dash_status.get("sonarr_refiner_live_done", 0),
        "sonarr_refiner_enabled": bool(getattr(settings, "sonarr_refiner_enabled", False)),
        "sonarr_cleanup_ui_active": bool(dash_status.get("sonarr_cleanup_ui_active")),
        "radarr_cleanup_ui_active": bool(dash_status.get("radarr_cleanup_ui_active")),
    }


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await get_or_create_settings(session)
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
    emby_schedule_start_display = to_12h(settings.emby_schedule_start or "00:00", "12:00 AM")
    emby_schedule_end_display = to_12h(settings.emby_schedule_end or "23:59", "11:59 PM")
    sonarr_schedule_start_display = to_12h(settings.sonarr_schedule_start or "00:00", "12:00 AM")
    sonarr_schedule_end_display = to_12h(settings.sonarr_schedule_end or "23:59", "11:59 PM")
    radarr_schedule_start_display = to_12h(settings.radarr_schedule_start or "00:00", "12:00 AM")
    radarr_schedule_end_display = to_12h(settings.radarr_schedule_end or "23:59", "11:59 PM")
    sonarrschedule_days_display = schedule_days_display(settings.sonarr_schedule_days or "")
    sonarr_schedule_time_friendly = schedule_time_range_friendly(
        settings.sonarr_schedule_start or "00:00",
        settings.sonarr_schedule_end or "23:59",
    )
    radarrschedule_days_display = schedule_days_display(settings.radarr_schedule_days or "")
    radarr_schedule_time_friendly = schedule_time_range_friendly(
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
            "sonarrschedule_days_display": sonarrschedule_days_display,
            "sonarr_schedule_time_friendly": sonarr_schedule_time_friendly,
            "radarrschedule_days_display": radarrschedule_days_display,
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
            "now_local": now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
            "sidebar_health": sidebar_health_dots(snapshots, settings),
        },
    )


@router.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request) -> RedirectResponse:
    """Redirect legacy /logs URL to Settings → Global tab."""
    return RedirectResponse(url="/settings?tab=global", status_code=301)


@router.get("/logs/file", response_class=PlainTextResponse, dependencies=AUTH_DEPS)
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


@router.get("/logs/file/download", dependencies=AUTH_DEPS)
async def logs_file_download(name: str, _request: Request) -> Response:
    """Serve a log file as a download attachment."""
    logs_root = resolved_logs_dir()
    safe_name = Path(name).name
    candidate = (logs_root / safe_name).resolve()
    if not is_safe_path(candidate, logs_root.resolve()):
        raise HTTPException(
            status_code=403,
            detail="That file path is not allowed.",
        )
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail="Log file not found.")
    try:
        content = candidate.read_bytes()
    except OSError:
        raise HTTPException(
            status_code=500,
            detail="Could not read the log file from disk.",
        )
    return Response(
        content=content,
        media_type="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}"',
        },
    )


@router.get("/activity", response_class=HTMLResponse)
async def activity_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    app: str | None = Query(None),
    q: str | None = Query(None),
) -> HTMLResponse:
    settings = await get_or_create_settings(session)
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
    merged = merge_activity_feed(activity_logs, refiner_logs, tz, now, limit=200)
    tab_key = normalize_activity_tab_query(app)
    scoped = filter_activity_display_for_tab(merged, tab_key)
    activity_display = filter_activity_display_for_search(scoped, q)
    snaps_act = await fetch_latest_app_snapshots(session)
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
            "activity_tab": tab_key,
            "activity_search_q": (q or "") if q is not None else "",
            "now": now,
            "now_local": now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
            "sidebar_health": sidebar_health_dots(snaps_act, settings),
        },
    )
