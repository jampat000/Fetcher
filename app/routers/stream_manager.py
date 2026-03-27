"""Refiner settings — isolated from Trimmer service logic."""

from __future__ import annotations

import logging
import os

from typing import Annotated

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import desc, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_csrf_token_for_template
from app.branding import APP_NAME, APP_TAGLINE
from app.db import _get_or_create_settings, db_path, get_session
from app.display_helpers import _now_local, _time_select_orphan
from app.display_helpers import _normalize_hhmm
from app.schedule import normalize_schedule_days_csv
from app.schedule import schedule_time_dropdown_choices
from app.refiner_folder_picker import REFINER_PICK_FOLDER_FAIL_MESSAGE, refiner_pick_folder_subprocess
from app.refiner_readiness import (
    get_refiner_state,
    refiner_validate_settings_save_section,
)
from app.refiner_watch_config import (
    STREAM_MANAGER_WATCH_INTERVAL_SEC_DEFAULT,
    clamp_stream_manager_interval_seconds,
)
from app.stream_manager_rules import normalize_audio_preference_mode, parse_subtitle_langs_csv
from app.time_util import utc_now_naive
from app.ui_templates import templates
from app.models import RefinerActivity
from app.web_common import (
    ACTIVITY_DETAIL_PREVIEW_LINES,
    is_setup_complete,
    refiner_activity_display_row,
    refiner_settings_redirect_url,
    schedule_days_csv_from_named_day_checks,
    schedule_weekdays_selected_dict,
    try_commit_and_reschedule,
)
from app.routers.deps import AUTH_DEPS, AUTH_FORM_DEPS

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=AUTH_DEPS)

# In-place JSON for Refiner settings (same transport pattern as Trimmer `x-fetcher-trimmer-settings-async`).
REFINER_SETTINGS_INPLACE_JSON_HEADER = "x-fetcher-refiner-settings-async"


def _refiner_want_inplace_json(request: Request) -> bool:
    return (request.headers.get(REFINER_SETTINGS_INPLACE_JSON_HEADER) or "").strip() == "1"


def _refiner_ui_section(refiner_section: str | None) -> str:
    s = (refiner_section or "").strip().lower()
    if s in ("processing", "folders", "audio", "subtitles", "schedule"):
        return s
    return "processing"


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


def _refiner_default_work_folder_path() -> str:
    """Resolved path shown when temp/work folder is left empty (matches stream_manager_service)."""
    p = db_path().parent / "refiner-work"
    try:
        return str(p.resolve())
    except OSError:
        return str(p)


@router.get("/refiner", response_class=HTMLResponse)
async def refiner_overview_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    tz = settings.timezone or "UTC"
    now = utc_now_naive()
    refiner_recent = (
        (await session.execute(select(RefinerActivity).order_by(desc(RefinerActivity.id)).limit(3)))
        .scalars()
        .all()
    )
    refiner_recent_activity = [refiner_activity_display_row(r, tz, now) for r in refiner_recent]
    return templates.TemplateResponse(
        request,
        "refiner.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Refiner",
            "subtitle": "Refiner status and saved configuration",
            "settings": settings,
            "timezone": tz,
            "now_local": _now_local(tz),
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
            "refiner_recent_activity": refiner_recent_activity,
            "activity_detail_preview": ACTIVITY_DETAIL_PREVIEW_LINES,
            "refiner_state": get_refiner_state(settings),
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
            "refiner_default_work_folder_path": _refiner_default_work_folder_path(),
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
            "refiner_state": get_refiner_state(settings),
        },
    )


@router.post("/refiner/settings/save", dependencies=AUTH_FORM_DEPS, response_model=None)
async def stream_manager_settings_save(
    request: Request,
    stream_manager_enabled: bool = Form(False),
    stream_manager_dry_run: bool = Form(False),
    stream_manager_primary_audio_lang: str = Form(""),
    stream_manager_secondary_audio_lang: str = Form(""),
    stream_manager_tertiary_audio_lang: str = Form(""),
    stream_manager_default_audio_slot: str = Form("primary"),
    stream_manager_remove_commentary: bool = Form(False),
    stream_manager_subtitle_mode: str = Form("remove_all"),
    stream_manager_subtitle_langs: list[str] = Form(default=[]),
    stream_manager_preserve_forced_subs: bool = Form(False),
    stream_manager_preserve_default_subs: bool = Form(False),
    stream_manager_audio_preference_mode: str = Form("preferred_langs_quality"),
    stream_manager_watched_folder: str = Form(""),
    stream_manager_output_folder: str = Form(""),
    stream_manager_work_folder: str = Form(""),
    stream_manager_interval_seconds: int = Form(STREAM_MANAGER_WATCH_INTERVAL_SEC_DEFAULT),
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
        row = await _get_or_create_settings(session)
        slot = (stream_manager_default_audio_slot or "primary").strip().lower()
        if slot not in ("primary", "secondary"):
            slot = "primary"
        mode = (stream_manager_subtitle_mode or "remove_all").strip().lower()
        if mode not in ("remove_all", "keep_selected"):
            mode = "remove_all"
        pref = normalize_audio_preference_mode(stream_manager_audio_preference_mode)
        lang_set = sorted({str(v).strip() for v in stream_manager_subtitle_langs if str(v).strip()})
        sim = clamp_stream_manager_interval_seconds(stream_manager_interval_seconds)
        watched_folder = (stream_manager_watched_folder or "").strip()
        output_folder = (stream_manager_output_folder or "").strip()
        primary_stripped = (stream_manager_primary_audio_lang or "").strip()
        val_reason, val_msg = refiner_validate_settings_save_section(
            ui_sec,
            enabled=stream_manager_enabled,
            primary_lang=primary_stripped,
            watched_folder=watched_folder,
            output_folder=output_folder,
        )
        if val_reason:
            return respond(saved=False, reason=val_reason, message=val_msg)
        row.stream_manager_enabled = stream_manager_enabled
        row.stream_manager_dry_run = stream_manager_dry_run
        row.stream_manager_primary_audio_lang = primary_stripped[:16]
        row.stream_manager_secondary_audio_lang = (stream_manager_secondary_audio_lang or "").strip()[:16]
        row.stream_manager_tertiary_audio_lang = (stream_manager_tertiary_audio_lang or "").strip()[:16]
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
        row.stream_manager_interval_seconds = sim
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
    row = await _get_or_create_settings(session)
    state = get_refiner_state(row)
    issues = [{"anchor": a, "message": m} for a, m in state.issue_pairs]
    return JSONResponse({"enabled": state.enabled, "issues": issues})


@router.post("/api/refiner/pick-folder", response_model=None)
async def refiner_pick_folder_api() -> JSONResponse:
    """Open a native folder dialog (Windows/Linux desktop). Unavailable in Docker/headless/service."""
    try:
        body = await refiner_pick_folder_subprocess()
    except Exception:
        logger.exception("Refiner pick-folder: unexpected failure")
        body = {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
        }
    return JSONResponse(body)
