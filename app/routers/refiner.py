"""Refiner settings — isolated from Trimmer service logic."""

from __future__ import annotations

import logging
import os
import sys

from typing import Annotated

from datetime import timedelta

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import case, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_csrf_token_for_template
from app.branding import APP_NAME, APP_TAGLINE
from app.db import get_or_create_settings, db_path, fetch_latest_app_snapshots, get_session
from app.display_helpers import (
    normalize_hhmm,
    now_local,
    time_select_orphan,
    activity_relative_time,
    _fmt_size_bytes_si,
)
from app.schedule import normalize_schedule_days_csv
from app.schedule import schedule_time_dropdown_choices
from app.refiner_readiness import (
    get_refiner_state,
    refiner_validate_settings_save_section,
    sonarr_refiner_validate_settings_save_section,
)
from app.refiner_watch_config import (
    REFINER_MINIMUM_AGE_SEC_DEFAULT,
    REFINER_WATCH_INTERVAL_SEC_DEFAULT,
    clamp_refiner_interval_seconds,
    clamp_refiner_minimum_age_seconds,
)
from app.refiner_lang_display import (
    STREAM_LANGUAGE_OPTIONS,
    refiner_lang_display,
    refiner_lang_display_or_blank,
)
from app.refiner_rules import (
    normalize_audio_preference_mode,
    parse_subtitle_langs_csv,
)
from app.time_util import utc_now_naive
from app.ui_templates import templates
from app.models import RefinerActivity
from app.web_common import (
    is_setup_complete,
    sidebar_health_dots,
    refiner_settings_redirect_url,
    schedule_days_csv_from_named_day_checks,
    schedule_weekdays_selected_dict,
    sonarr_refiner_settings_redirect_url,
    try_commit_and_reschedule,
)
from app.routers.deps import AUTH_DEPS, AUTH_FORM_DEPS

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=AUTH_DEPS)

# In-place JSON for Refiner settings (same transport pattern as Trimmer `x-fetcher-trimmer-settings-async`).
REFINER_SETTINGS_INPLACE_JSON_HEADER = "x-fetcher-refiner-settings-async"


def _refinerschedule_days_display(days_csv: str) -> str:
    norm = normalize_schedule_days_csv(days_csv or "")
    if not norm:
        return ""
    return " · ".join(p.strip() for p in norm.split(",") if p.strip())


def _refiner_schedule_window_24h(start: str, end: str) -> str:
    s = normalize_hhmm(start, "00:00")
    e = normalize_hhmm(end, "23:59")
    if s == "00:00" and e in ("23:59", "23:58"):
        return "All day"
    return f"{s}–{e}"


def _refiner_want_inplace_json(request: Request) -> bool:
    return (request.headers.get(REFINER_SETTINGS_INPLACE_JSON_HEADER) or "").strip() == "1"


def _refiner_ui_section(refiner_section: str | None) -> str:
    s = (refiner_section or "").strip().lower()
    if s in ("processing", "folders", "audio", "subtitles", "schedule"):
        return s
    return "processing"


def build_refiner_overview_config(settings, refiner_state) -> dict[str, object]:
    """Label/value strings for Refiner overview (configured state only; full paths)."""
    watched = (settings.refiner_watched_folder or "").strip()
    output = (settings.refiner_output_folder or "").strip()
    work = (settings.refiner_work_folder or "").strip()
    work_resolved = _refiner_default_work_folder_path()
    work_line = f"Custom · {work}" if work else f"Default · {work_resolved}"

    watched_disp = watched if watched else "—"
    output_disp = output if output else "—"

    pri = refiner_lang_display(settings.refiner_primary_audio_lang)
    sec = refiner_lang_display_or_blank(settings.refiner_secondary_audio_lang)
    ter = refiner_lang_display_or_blank(settings.refiner_tertiary_audio_lang)
    lang_bits = [x for x in (pri, sec, ter) if x and x != "—"]
    audio_langs = " · ".join(lang_bits) if lang_bits else "—"

    apm = normalize_audio_preference_mode(settings.refiner_audio_preference_mode)
    if apm == "quality_all_languages":
        audio_tracks_mode = "Keep best match only"
        audio_policy = "Highest quality"
    else:
        audio_tracks_mode = "Keep selected languages"
        audio_policy = (
            "Preferred languages (strict)"
            if apm == "preferred_langs_strict"
            else "Preferred languages, highest quality"
        )

    commentary = "Removed" if settings.refiner_remove_commentary else "Kept"

    sm = (settings.refiner_subtitle_mode or "").strip().lower()
    if sm == "keep_selected":
        subtitles_mode = "Selected"
        kept = parse_subtitle_langs_csv(settings.refiner_subtitle_langs_csv or "")
        slangs_bits = [refiner_lang_display_or_blank(x) for x in kept]
        slangs_bits = [x for x in slangs_bits if x]
        subtitle_languages = " · ".join(slangs_bits) if slangs_bits else "—"
        types_bits: list[str] = []
        if settings.refiner_preserve_default_subs:
            types_bits.append("Full")
        if settings.refiner_preserve_forced_subs:
            types_bits.append("Forced")
        subtitle_types = " · ".join(types_bits) if types_bits else "—"
    else:
        subtitles_mode = "Remove all"
        subtitle_languages = "—"
        subtitle_types = "—"

    dry = bool(settings.refiner_dry_run)
    source_cleanup = "Off" if dry else "File and folder removed after success"

    sched_on = bool(settings.refiner_schedule_enabled)
    schedule_window = "Yes" if sched_on else "No"
    if sched_on:
        days_part = _refinerschedule_days_display(settings.refiner_schedule_days or "")
        if not days_part:
            days_part = "—"
        win = _refiner_schedule_window_24h(
            settings.refiner_schedule_start or "00:00",
            settings.refiner_schedule_end or "23:59",
        )
        schedule_detail = f"{days_part} · {win}"
    else:
        schedule_detail = "—"

    readiness = (
        "Ready"
        if refiner_state.phase == "ready"
        else ("Not ready" if refiner_state.phase == "not_ready" else "—")
    )

    subtitles_collapsed = sm != "keep_selected"
    schedule_collapsed = not sched_on

    return {
        "readiness": readiness,
        "watched_folder": watched_disp,
        "output_folder": output_disp,
        "work_folder_line": work_line,
        "scan_interval": f"{int(settings.refiner_interval_seconds or 60)}s",
        "audio_tracks_mode": audio_tracks_mode,
        "audio_languages": audio_langs,
        "audio_policy": audio_policy,
        "commentary": commentary,
        "subtitles_mode": subtitles_mode,
        "subtitle_languages": subtitle_languages,
        "subtitle_types": subtitle_types,
        "source_cleanup": source_cleanup,
        "schedule_window": schedule_window,
        "schedule_detail": schedule_detail,
        "subtitles_collapsed": subtitles_collapsed,
        "schedule_collapsed": schedule_collapsed,
    }


def _refiner_default_work_folder_path() -> str:
    """Resolved path shown when temp/work folder is left empty (matches refiner_service)."""
    p = db_path().parent / "refiner-work"
    try:
        return str(p.resolve())
    except OSError:
        return str(p)


def _sonarr_refiner_default_work_folder_path() -> str:
    """Resolved path shown when Sonarr temp/work folder is
    left empty (matches refiner_service default)."""
    p = db_path().parent / "refiner-sonarr-work"
    try:
        return str(p.resolve())
    except OSError:
        return str(p)


@router.get("/refiner", response_class=HTMLResponse)
async def refiner_overview_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    tz = settings.timezone or "UTC"
    now = utc_now_naive()
    rs = get_refiner_state(settings)
    n_proc = (
        await session.execute(
            select(func.count()).select_from(RefinerActivity).where(RefinerActivity.status == "processing")
        )
    ).scalar_one()
    if int(n_proc or 0) > 0:
        refiner_recent_activity_summary = "Processing now"
    elif settings.refiner_last_run_at is not None:
        refiner_recent_activity_summary = f"Last scan {activity_relative_time(settings.refiner_last_run_at, now)}"
    else:
        refiner_recent_activity_summary = "No runs yet"
    cutoff_30d = utc_now_naive() - timedelta(days=30)
    _stats = (
        await session.execute(
            select(
                func.count().label("total"),
                func.sum(case((RefinerActivity.status == "success", 1), else_=0)).label("success_count"),
                func.sum(
                    case(
                        (
                            RefinerActivity.status == "success",
                            RefinerActivity.size_before_bytes - RefinerActivity.size_after_bytes,
                        ),
                        else_=0,
                    )
                ).label("bytes_saved"),
            ).where(RefinerActivity.created_at >= cutoff_30d)
        )
    ).one()
    total_30d = int(_stats.total or 0)
    success_30d = int(_stats.success_count or 0)
    bytes_saved_30d = max(0, int(_stats.bytes_saved or 0))
    success_rate_30d = f"{round(success_30d / total_30d * 100)}%" if total_30d > 0 else "—"
    bytes_saved_display = _fmt_size_bytes_si(bytes_saved_30d) if bytes_saved_30d > 0 else "—"
    snaps_ref_overview = await fetch_latest_app_snapshots(session)
    return templates.TemplateResponse(
        request,
        "refiner.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Refiner",
            "subtitle": "Refiner",
            "settings": settings,
            "timezone": tz,
            "now_local": now_local(tz),
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
            "refiner_state": rs,
            "refiner_overview": build_refiner_overview_config(settings, rs),
            "refiner_recent_activity_summary": refiner_recent_activity_summary,
            "refiner_stats_30d": {
                "total": total_30d,
                "success": success_30d,
                "bytes_saved": bytes_saved_display,
                "success_rate": success_rate_30d,
            },
            "sidebar_health": sidebar_health_dots(snaps_ref_overview, settings),
        },
    )


@router.get("/refiner/settings", response_class=HTMLResponse)
async def refiner_settings_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    tz = settings.timezone or "UTC"
    time_choices = schedule_time_dropdown_choices(step_minutes=30)
    time_choice_keys = {v for v, _ in time_choices}
    schedule_days_norm = normalize_schedule_days_csv(settings.refiner_schedule_days or "")
    schedule_start_hhmm = normalize_hhmm(settings.refiner_schedule_start, "00:00")
    schedule_end_hhmm = normalize_hhmm(settings.refiner_schedule_end, "23:59")
    snaps_ref_settings = await fetch_latest_app_snapshots(session)
    return templates.TemplateResponse(
        request,
        "refiner_settings.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Movies Settings",
            "subtitle": "Configure Movies Refiner workflow and schedule",
            "settings": settings,
            "timezone": tz,
            "now_local": now_local(tz),
            "schedule_time_choices": time_choices,
            "refiner_schedule_days_normalized": schedule_days_norm,
            "refiner_schedule_days_selected": schedule_weekdays_selected_dict(
                settings.refiner_schedule_days or ""
            ),
            "refiner_schedule_start_hhmm": schedule_start_hhmm,
            "refiner_schedule_end_hhmm": schedule_end_hhmm,
            "refiner_start_orphan": time_select_orphan(
                schedule_start_hhmm, time_choice_keys, fallback_display="12:00 AM"
            ),
            "refiner_end_orphan": time_select_orphan(
                schedule_end_hhmm, time_choice_keys, fallback_display="11:59 PM"
            ),
            "selected_stream_subtitle_langs": list(
                parse_subtitle_langs_csv(settings.refiner_subtitle_langs_csv or "")
            ),
            "stream_language_options": STREAM_LANGUAGE_OPTIONS,
            "refiner_default_work_folder_path": _refiner_default_work_folder_path(),
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
            "refiner_state": get_refiner_state(settings),
            "sidebar_health": sidebar_health_dots(snaps_ref_settings, settings),
        },
    )


@router.get("/refiner/sonarr/settings", response_class=HTMLResponse)
async def sonarr_refiner_settings_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    settings = await get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    tz = settings.timezone or "UTC"
    time_choices = schedule_time_dropdown_choices(step_minutes=30)
    time_choice_keys = {v for v, _ in time_choices}
    schedule_days_norm = normalize_schedule_days_csv(
        getattr(settings, "sonarr_refiner_schedule_days", "") or ""
    )
    schedule_start_hhmm = normalize_hhmm(
        getattr(settings, "sonarr_refiner_schedule_start", "00:00"),
        "00:00",
    )
    schedule_end_hhmm = normalize_hhmm(
        getattr(settings, "sonarr_refiner_schedule_end", "23:59"),
        "23:59",
    )
    snaps = await fetch_latest_app_snapshots(session)
    return templates.TemplateResponse(
        request,
        "refiner_sonarr_settings.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — TV Settings",
            "subtitle": "Configure TV Refiner workflow and schedule",
            "settings": settings,
            "timezone": tz,
            "now_local": now_local(tz),
            "schedule_time_choices": time_choices,
            "sonarr_refiner_schedule_days_normalized": schedule_days_norm,
            "sonarr_refiner_schedule_days_selected": schedule_weekdays_selected_dict(
                getattr(settings, "sonarr_refiner_schedule_days", "") or ""
            ),
            "sonarr_refiner_schedule_start_hhmm": schedule_start_hhmm,
            "sonarr_refiner_schedule_end_hhmm": schedule_end_hhmm,
            "sonarr_refiner_start_orphan": time_select_orphan(
                schedule_start_hhmm,
                time_choice_keys,
                fallback_display="12:00 AM",
            ),
            "sonarr_refiner_end_orphan": time_select_orphan(
                schedule_end_hhmm,
                time_choice_keys,
                fallback_display="11:59 PM",
            ),
            "sonarr_selected_stream_subtitle_langs": list(
                parse_subtitle_langs_csv(
                    getattr(settings, "sonarr_refiner_subtitle_langs_csv", "") or ""
                )
            ),
            "stream_language_options": STREAM_LANGUAGE_OPTIONS,
            "sonarr_refiner_default_work_folder_path": (
                _sonarr_refiner_default_work_folder_path()
            ),
            "csrf_token": await get_csrf_token_for_template(
                request, session
            ),
            "show_setup_wizard": show_setup_wizard,
            "sidebar_health": sidebar_health_dots(snaps, settings),
        },
    )


@router.post(
    "/refiner/sonarr/settings/save",
    dependencies=AUTH_FORM_DEPS,
    response_model=None,
)
async def sonarr_refiner_settings_save(
    request: Request,
    sonarr_refiner_enabled: bool = Form(False),
    sonarr_refiner_dry_run: bool = Form(False),
    sonarr_refiner_primary_audio_lang: str = Form(""),
    sonarr_refiner_secondary_audio_lang: str = Form(""),
    sonarr_refiner_tertiary_audio_lang: str = Form(""),
    sonarr_refiner_default_audio_slot: str = Form("primary"),
    sonarr_refiner_remove_commentary: bool = Form(False),
    sonarr_refiner_subtitle_mode: str = Form("remove_all"),
    sonarr_refiner_subtitle_langs: list[str] = Form(default=[]),
    sonarr_refiner_preserve_forced_subs: bool = Form(False),
    sonarr_refiner_preserve_default_subs: bool = Form(False),
    sonarr_refiner_audio_preference_mode: str = Form(
        "preferred_langs_quality"
    ),
    sonarr_refiner_watched_folder: str = Form(""),
    sonarr_refiner_output_folder: str = Form(""),
    sonarr_refiner_work_folder: str = Form(""),
    sonarr_refiner_interval_seconds: int = Form(
        REFINER_WATCH_INTERVAL_SEC_DEFAULT
    ),
    sonarr_refiner_minimum_age_seconds: int = Form(
        REFINER_MINIMUM_AGE_SEC_DEFAULT
    ),
    sonarr_refiner_schedule_enabled: bool = Form(False),
    sonarr_refiner_schedule_Mon: bool = Form(False),
    sonarr_refiner_schedule_Tue: bool = Form(False),
    sonarr_refiner_schedule_Wed: bool = Form(False),
    sonarr_refiner_schedule_Thu: bool = Form(False),
    sonarr_refiner_schedule_Fri: bool = Form(False),
    sonarr_refiner_schedule_Sat: bool = Form(False),
    sonarr_refiner_schedule_Sun: bool = Form(False),
    sonarr_refiner_schedule_start: str = Form("00:00"),
    sonarr_refiner_schedule_end: str = Form("23:59"),
    refiner_section: Annotated[str | None, Query()] = None,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse | JSONResponse:
    want_json = _refiner_want_inplace_json(request)
    ui_sec = _refiner_ui_section(refiner_section)

    def respond(
        *, saved: bool, reason: str | None = None, message: str | None = None
    ) -> RedirectResponse | JSONResponse:
        if want_json:
            out: dict[str, object] = {"ok": saved, "section": ui_sec}
            if not saved:
                out["reason"] = reason or "error"
                if message:
                    out["message"] = message
            return JSONResponse(out)
        if saved:
            return RedirectResponse(
                sonarr_refiner_settings_redirect_url(
                    saved=True, section=refiner_section
                ),
                status_code=303,
            )
        return RedirectResponse(
            sonarr_refiner_settings_redirect_url(
                saved=False, reason=reason, section=refiner_section
            ),
            status_code=303,
        )

    try:
        row = await get_or_create_settings(session)
        slot = (sonarr_refiner_default_audio_slot or "primary").strip().lower()
        if slot not in ("primary", "secondary"):
            slot = "primary"
        mode = (sonarr_refiner_subtitle_mode or "remove_all").strip().lower()
        if mode not in ("remove_all", "keep_selected"):
            mode = "remove_all"
        pref = normalize_audio_preference_mode(sonarr_refiner_audio_preference_mode)
        lang_set = sorted(
            {str(v).strip() for v in sonarr_refiner_subtitle_langs if str(v).strip()}
        )
        sim = clamp_refiner_interval_seconds(sonarr_refiner_interval_seconds)
        min_age = clamp_refiner_minimum_age_seconds(sonarr_refiner_minimum_age_seconds)
        watched_folder = (sonarr_refiner_watched_folder or "").strip()
        output_folder = (sonarr_refiner_output_folder or "").strip()
        primary_stripped = (sonarr_refiner_primary_audio_lang or "").strip()
        val_reason, val_msg = sonarr_refiner_validate_settings_save_section(
            ui_sec,
            enabled=sonarr_refiner_enabled,
            primary_lang=primary_stripped,
            watched_folder=watched_folder,
            output_folder=output_folder,
        )
        if val_reason:
            return respond(saved=False, reason=val_reason, message=val_msg)
        row.sonarr_refiner_enabled = sonarr_refiner_enabled
        row.sonarr_refiner_dry_run = sonarr_refiner_dry_run
        row.sonarr_refiner_primary_audio_lang = primary_stripped[:16]
        row.sonarr_refiner_secondary_audio_lang = (
            (sonarr_refiner_secondary_audio_lang or "").strip()[:16]
        )
        row.sonarr_refiner_tertiary_audio_lang = (
            (sonarr_refiner_tertiary_audio_lang or "").strip()[:16]
        )
        row.sonarr_refiner_default_audio_slot = slot
        row.sonarr_refiner_remove_commentary = sonarr_refiner_remove_commentary
        row.sonarr_refiner_subtitle_mode = mode
        row.sonarr_refiner_subtitle_langs_csv = ",".join(lang_set)
        row.sonarr_refiner_audio_preference_mode = pref
        row.sonarr_refiner_preserve_forced_subs = sonarr_refiner_preserve_forced_subs
        row.sonarr_refiner_preserve_default_subs = sonarr_refiner_preserve_default_subs
        row.sonarr_refiner_watched_folder = watched_folder[:8000]
        row.sonarr_refiner_output_folder = output_folder[:8000]
        row.sonarr_refiner_work_folder = (sonarr_refiner_work_folder or "").strip()[:8000]
        row.sonarr_refiner_interval_seconds = sim
        row.sonarr_refiner_minimum_age_seconds = min_age
        row.sonarr_refiner_schedule_enabled = sonarr_refiner_schedule_enabled
        row.sonarr_refiner_schedule_days = schedule_days_csv_from_named_day_checks(
            sonarr_refiner_schedule_Mon,
            sonarr_refiner_schedule_Tue,
            sonarr_refiner_schedule_Wed,
            sonarr_refiner_schedule_Thu,
            sonarr_refiner_schedule_Fri,
            sonarr_refiner_schedule_Sat,
            sonarr_refiner_schedule_Sun,
        )
        row.sonarr_refiner_schedule_start = normalize_hhmm(
            sonarr_refiner_schedule_start, "00:00"
        )
        row.sonarr_refiner_schedule_end = normalize_hhmm(
            sonarr_refiner_schedule_end, "23:59"
        )
        row.sonarr_refiner_schedule_days = normalize_schedule_days_csv(
            row.sonarr_refiner_schedule_days or ""
        )
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session, targets={"sonarr_refiner"}):
            return respond(saved=False, reason="db_busy")
        return respond(saved=True)
    except SQLAlchemyError:
        logger.exception("POST sonarr refiner save SQLAlchemyError")
        try:
            await session.rollback()
        except Exception:
            pass
        return respond(saved=False, reason="db_error")
    except Exception:
        logger.exception("POST sonarr refiner save failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").upper() == "DEBUG":
            raise
        return respond(saved=False, reason="error")


@router.post("/refiner/settings/save", dependencies=AUTH_FORM_DEPS, response_model=None)
async def refiner_settings_save(
    request: Request,
    refiner_enabled: bool = Form(False),
    refiner_dry_run: bool = Form(False),
    refiner_primary_audio_lang: str = Form(""),
    refiner_secondary_audio_lang: str = Form(""),
    refiner_tertiary_audio_lang: str = Form(""),
    refiner_default_audio_slot: str = Form("primary"),
    refiner_remove_commentary: bool = Form(False),
    refiner_subtitle_mode: str = Form("remove_all"),
    refiner_subtitle_langs: list[str] = Form(default=[]),
    refiner_preserve_forced_subs: bool = Form(False),
    refiner_preserve_default_subs: bool = Form(False),
    refiner_audio_preference_mode: str = Form("preferred_langs_quality"),
    refiner_watched_folder: str = Form(""),
    refiner_output_folder: str = Form(""),
    refiner_work_folder: str = Form(""),
    refiner_interval_seconds: int = Form(REFINER_WATCH_INTERVAL_SEC_DEFAULT),
    refiner_minimum_age_seconds: int = Form(REFINER_MINIMUM_AGE_SEC_DEFAULT),
    refiner_schedule_enabled: bool = Form(False),
    refiner_schedule_Mon: bool = Form(False),
    refiner_schedule_Tue: bool = Form(False),
    refiner_schedule_Wed: bool = Form(False),
    refiner_schedule_Thu: bool = Form(False),
    refiner_schedule_Fri: bool = Form(False),
    refiner_schedule_Sat: bool = Form(False),
    refiner_schedule_Sun: bool = Form(False),
    refiner_schedule_start: str = Form("00:00"),
    refiner_schedule_end: str = Form("23:59"),
    refiner_section: Annotated[str | None, Query()] = None,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse | JSONResponse:
    want_json = _refiner_want_inplace_json(request)
    ui_sec = _refiner_ui_section(refiner_section)

    def respond(
        *, saved: bool, reason: str | None = None, message: str | None = None
    ) -> RedirectResponse | JSONResponse:
        if want_json:
            out: dict[str, object] = {"ok": saved, "section": ui_sec}
            if not saved:
                out["reason"] = reason or "error"
                if message:
                    out["message"] = message
            return JSONResponse(out)
        if saved:
            return RedirectResponse(
                refiner_settings_redirect_url(saved=True, section=refiner_section), status_code=303
            )
        return RedirectResponse(
            refiner_settings_redirect_url(
                saved=False, reason=reason, section=refiner_section
            ),
            status_code=303,
        )

    try:
        row = await get_or_create_settings(session)
        slot = (refiner_default_audio_slot or "primary").strip().lower()
        if slot not in ("primary", "secondary"):
            slot = "primary"
        mode = (refiner_subtitle_mode or "remove_all").strip().lower()
        if mode not in ("remove_all", "keep_selected"):
            mode = "remove_all"
        pref = normalize_audio_preference_mode(refiner_audio_preference_mode)
        lang_set = sorted({str(v).strip() for v in refiner_subtitle_langs if str(v).strip()})
        sim = clamp_refiner_interval_seconds(refiner_interval_seconds)
        min_age_rad = clamp_refiner_minimum_age_seconds(refiner_minimum_age_seconds)
        watched_folder = (refiner_watched_folder or "").strip()
        output_folder = (refiner_output_folder or "").strip()
        primary_stripped = (refiner_primary_audio_lang or "").strip()
        val_reason, val_msg = refiner_validate_settings_save_section(
            ui_sec,
            enabled=refiner_enabled,
            primary_lang=primary_stripped,
            watched_folder=watched_folder,
            output_folder=output_folder,
        )
        if val_reason:
            return respond(saved=False, reason=val_reason, message=val_msg)
        row.refiner_enabled = refiner_enabled
        row.refiner_dry_run = refiner_dry_run
        row.refiner_primary_audio_lang = primary_stripped[:16]
        row.refiner_secondary_audio_lang = (refiner_secondary_audio_lang or "").strip()[:16]
        row.refiner_tertiary_audio_lang = (refiner_tertiary_audio_lang or "").strip()[:16]
        row.refiner_default_audio_slot = slot
        row.refiner_remove_commentary = refiner_remove_commentary
        row.refiner_subtitle_mode = mode
        row.refiner_subtitle_langs_csv = ",".join(lang_set)
        row.refiner_audio_preference_mode = pref
        row.refiner_preserve_forced_subs = refiner_preserve_forced_subs
        row.refiner_preserve_default_subs = refiner_preserve_default_subs
        row.refiner_watched_folder = watched_folder[:8000]
        row.refiner_output_folder = output_folder[:8000]
        row.refiner_work_folder = (refiner_work_folder or "").strip()[:8000]
        row.refiner_interval_seconds = sim
        row.refiner_minimum_age_seconds = min_age_rad
        row.refiner_schedule_enabled = refiner_schedule_enabled
        row.refiner_schedule_days = schedule_days_csv_from_named_day_checks(
            refiner_schedule_Mon,
            refiner_schedule_Tue,
            refiner_schedule_Wed,
            refiner_schedule_Thu,
            refiner_schedule_Fri,
            refiner_schedule_Sat,
            refiner_schedule_Sun,
        )
        row.refiner_schedule_start = normalize_hhmm(refiner_schedule_start, "00:00")
        row.refiner_schedule_end = normalize_hhmm(refiner_schedule_end, "23:59")
        # Canonicalize weekday CSV same as other schedules
        row.refiner_schedule_days = normalize_schedule_days_csv(row.refiner_schedule_days or "")
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session, targets={"refiner"}):
            return respond(saved=False, reason="db_busy")
        return respond(saved=True)
    except SQLAlchemyError:
        logger.exception("POST refiner save SQLAlchemyError")
        try:
            await session.rollback()
        except Exception:
            pass
        return respond(saved=False, reason="db_error")
    except Exception:
        logger.exception("POST refiner save failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").upper() == "DEBUG":
            raise
        return respond(saved=False, reason="error")


@router.get("/api/refiner/readiness-brief", response_model=None)
async def refiner_readiness_brief_api(session: AsyncSession = Depends(get_session)) -> JSONResponse:
    """JSON for Refiner banners after async save (enabled vs off, readiness list)."""
    row = await get_or_create_settings(session)
    state = get_refiner_state(row)
    issues = [{"anchor": a, "message": m} for a, m in state.issue_pairs]
    return JSONResponse({"enabled": state.enabled, "issues": issues})


