"""Refiner settings and manual run — isolated from Trimmer service logic."""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_csrf_token_for_template
from app.branding import APP_NAME, APP_TAGLINE
from app.db import _get_or_create_settings, get_session
from app.display_helpers import _now_local, _time_select_orphan
from app.display_helpers import _normalize_hhmm
from app.schedule import normalize_schedule_days_csv
from app.schedule import schedule_time_dropdown_choices
from app.stream_manager_service import run_stream_manager_pass
from app.stream_manager_rules import parse_subtitle_langs_csv
from app.time_util import utc_now_naive
from app.ui_templates import templates
from app.web_common import (
    is_setup_complete,
    schedule_days_csv_from_named_day_checks,
    schedule_weekdays_selected_dict,
    try_commit_and_reschedule,
)
from app.routers.deps import AUTH_DEPS, AUTH_FORM_DEPS

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=AUTH_DEPS)

_STREAM_LANGUAGE_OPTIONS: list[tuple[str, str]] = [
    ("eng", "English"),
    ("jpn", "Japanese"),
    ("spa", "Spanish"),
    ("fre", "French"),
    ("deu", "German"),
    ("ita", "Italian"),
    ("por", "Portuguese"),
    ("rus", "Russian"),
    ("zho", "Chinese"),
    ("kor", "Korean"),
    ("hin", "Hindi"),
    ("ara", "Arabic"),
    ("pol", "Polish"),
    ("tur", "Turkish"),
    ("swe", "Swedish"),
    ("dan", "Danish"),
    ("fin", "Finnish"),
    ("nld", "Dutch"),
    ("nor", "Norwegian"),
    ("hun", "Hungarian"),
    ("ces", "Czech"),
    ("ell", "Greek"),
    ("heb", "Hebrew"),
    ("tha", "Thai"),
    ("vie", "Vietnamese"),
    ("ukr", "Ukrainian"),
    ("ron", "Romanian"),
    ("ind", "Indonesian"),
    ("msa", "Malay"),
    ("und", "Undetermined"),
]


def _refiner_settings_redirect_url(*, saved: bool, reason: str | None = None) -> str:
    if saved:
        return "/refiner/settings?saved=1"
    if reason:
        return f"/refiner/settings?save=fail&reason={reason}"
    return "/refiner/settings?save=fail"


@router.get("/refiner", response_class=HTMLResponse)
async def refiner_overview_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    tz = settings.timezone or "UTC"
    return templates.TemplateResponse(
        request,
        "refiner.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Refiner",
            "subtitle": "Overview of Refiner status and run mode",
            "settings": settings,
            "timezone": tz,
            "now_local": _now_local(tz),
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
        },
    )


@router.get("/refiner/settings", response_class=HTMLResponse)
async def refiner_settings_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    tz = settings.timezone or "UTC"
    time_choices = schedule_time_dropdown_choices(step_minutes=30)
    time_choice_keys = {v for v, _ in time_choices}
    sm_days = normalize_schedule_days_csv(settings.stream_manager_schedule_days or "")
    sm_s = _normalize_hhmm(settings.stream_manager_schedule_start, "00:00")
    sm_e = _normalize_hhmm(settings.stream_manager_schedule_end, "23:59")
    return templates.TemplateResponse(
        request,
        "refiner_settings.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Refiner settings",
            "subtitle": "Configure Refiner workflow and schedule",
            "settings": settings,
            "timezone": tz,
            "now_local": _now_local(tz),
            "schedule_time_choices": time_choices,
            "stream_manager_schedule_days_normalized": sm_days,
            "stream_manager_schedule_days_selected": schedule_weekdays_selected_dict(
                settings.stream_manager_schedule_days or ""
            ),
            "stream_manager_schedule_start_hhmm": sm_s,
            "stream_manager_schedule_end_hhmm": sm_e,
            "stream_manager_start_orphan": _time_select_orphan(sm_s, time_choice_keys, fallback_display="12:00 AM"),
            "stream_manager_end_orphan": _time_select_orphan(sm_e, time_choice_keys, fallback_display="11:59 PM"),
            "selected_stream_subtitle_langs": list(
                parse_subtitle_langs_csv(settings.stream_manager_subtitle_langs_csv or "")
            ),
            "stream_language_options": _STREAM_LANGUAGE_OPTIONS,
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
        },
    )


@router.post("/refiner/settings/save", dependencies=AUTH_FORM_DEPS)
async def stream_manager_settings_save(
    stream_manager_enabled: bool = Form(False),
    stream_manager_dry_run: bool = Form(False),
    stream_manager_primary_audio_lang: str = Form(""),
    stream_manager_secondary_audio_lang: str = Form(""),
    stream_manager_default_audio_slot: str = Form("primary"),
    stream_manager_remove_commentary: bool = Form(False),
    stream_manager_subtitle_mode: str = Form("remove_all"),
    stream_manager_subtitle_langs: list[str] = Form(default=[]),
    stream_manager_preserve_forced_subs: bool = Form(False),
    stream_manager_preserve_default_subs: bool = Form(False),
    stream_manager_audio_preference_mode: str = Form("best_available"),
    stream_manager_watched_folder: str = Form(""),
    stream_manager_output_folder: str = Form(""),
    stream_manager_work_folder: str = Form(""),
    stream_manager_interval_minutes: int = Form(60),
    stream_manager_schedule_enabled: bool = Form(False),
    stream_manager_schedule_Mon: bool = Form(False),
    stream_manager_schedule_Tue: bool = Form(False),
    stream_manager_schedule_Wed: bool = Form(False),
    stream_manager_schedule_Thu: bool = Form(False),
    stream_manager_schedule_Fri: bool = Form(False),
    stream_manager_schedule_Sat: bool = Form(False),
    stream_manager_schedule_Sun: bool = Form(False),
    stream_manager_schedule_start: str = Form("00:00"),
    stream_manager_schedule_end: str = Form("23:59"),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    try:
        row = await _get_or_create_settings(session)
        if stream_manager_enabled and not (stream_manager_primary_audio_lang or "").strip():
            return RedirectResponse(
                _refiner_settings_redirect_url(saved=False, reason="primary_audio_required"),
                status_code=303,
            )
        slot = (stream_manager_default_audio_slot or "primary").strip().lower()
        if slot not in ("primary", "secondary"):
            slot = "primary"
        mode = (stream_manager_subtitle_mode or "remove_all").strip().lower()
        if mode not in ("remove_all", "keep_selected"):
            mode = "remove_all"
        pref = (stream_manager_audio_preference_mode or "best_available").strip().lower()
        if pref not in (
            "best_available",
            "prefer_surround",
            "prefer_stereo",
            "prefer_lossless",
        ):
            pref = "best_available"
        lang_set = sorted({str(v).strip() for v in stream_manager_subtitle_langs if str(v).strip()})
        sim = max(5, min(7 * 24 * 60, int(stream_manager_interval_minutes or 60)))
        watched_folder = (stream_manager_watched_folder or "").strip()
        output_folder = (stream_manager_output_folder or "").strip()
        if stream_manager_enabled and (not watched_folder or not output_folder):
            return RedirectResponse(
                _refiner_settings_redirect_url(saved=False, reason="watched_output_required"),
                status_code=303,
            )
        row.stream_manager_enabled = stream_manager_enabled
        row.stream_manager_dry_run = stream_manager_dry_run
        row.stream_manager_primary_audio_lang = (stream_manager_primary_audio_lang or "").strip()[:16]
        row.stream_manager_secondary_audio_lang = (stream_manager_secondary_audio_lang or "").strip()[:16]
        row.stream_manager_tertiary_audio_lang = ""
        row.stream_manager_default_audio_slot = slot
        row.stream_manager_remove_commentary = stream_manager_remove_commentary
        row.stream_manager_subtitle_mode = mode
        row.stream_manager_subtitle_langs_csv = ",".join(lang_set)
        row.stream_manager_audio_preference_mode = pref
        row.stream_manager_preserve_forced_subs = stream_manager_preserve_forced_subs
        row.stream_manager_preserve_default_subs = stream_manager_preserve_default_subs
        row.stream_manager_watched_folder = watched_folder[:8000]
        row.stream_manager_output_folder = output_folder[:8000]
        row.stream_manager_work_folder = (stream_manager_work_folder or "").strip()[:8000]
        row.stream_manager_interval_minutes = sim
        row.stream_manager_schedule_enabled = stream_manager_schedule_enabled
        row.stream_manager_schedule_days = schedule_days_csv_from_named_day_checks(
            stream_manager_schedule_Mon,
            stream_manager_schedule_Tue,
            stream_manager_schedule_Wed,
            stream_manager_schedule_Thu,
            stream_manager_schedule_Fri,
            stream_manager_schedule_Sat,
            stream_manager_schedule_Sun,
        )
        row.stream_manager_schedule_start = _normalize_hhmm(stream_manager_schedule_start, "00:00")
        row.stream_manager_schedule_end = _normalize_hhmm(stream_manager_schedule_end, "23:59")
        # Canonicalize weekday CSV same as other schedules
        row.stream_manager_schedule_days = normalize_schedule_days_csv(row.stream_manager_schedule_days or "")
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session, targets={"stream_manager"}):
            return RedirectResponse(
                _refiner_settings_redirect_url(saved=False, reason="db_busy"),
                status_code=303,
            )
        return RedirectResponse(
            _refiner_settings_redirect_url(saved=True), status_code=303
        )
    except SQLAlchemyError:
        logger.exception("POST refiner save SQLAlchemyError")
        try:
            await session.rollback()
        except Exception:
            pass
        return RedirectResponse(
            _refiner_settings_redirect_url(saved=False, reason="db_error"),
            status_code=303,
        )
    except Exception:
        logger.exception("POST refiner save failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").upper() == "DEBUG":
            raise
        return RedirectResponse(
            _refiner_settings_redirect_url(saved=False, reason="error"),
            status_code=303,
        )


@router.post("/refiner/settings/run", dependencies=AUTH_FORM_DEPS)
async def stream_manager_manual_run(
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await _get_or_create_settings(session)
    if not row.stream_manager_enabled:
        return RedirectResponse(
            _refiner_settings_redirect_url(saved=False, reason="refiner_disabled"),
            status_code=303,
        )
    result = await run_stream_manager_pass(session, trigger="manual")
    if not result.get("ran"):
        reason = str(result.get("error") or result.get("reason") or "nothing_to_do")
        return RedirectResponse(
            _refiner_settings_redirect_url(saved=False, reason=reason),
            status_code=303,
        )
    if not result.get("ok"):
        return RedirectResponse(
            _refiner_settings_redirect_url(saved=False, reason="refiner_errors"),
            status_code=303,
        )
    return RedirectResponse("/refiner?run=ok", status_code=303)
