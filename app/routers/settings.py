from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from urllib.parse import quote

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import (
    get_csrf_token_for_template,
    hash_password,
    normalize_auth_ip_allowlist_input,
    require_csrf,
    verify_password,
)
from app.backup import export_json_bytes, import_settings_replace
from app.branding import APP_NAME, APP_TAGLINE
from app.constants import _TIMEZONE_CHOICES
from app.connection_test_service import ConnectionTestService
from app.db import _get_or_create_settings, fetch_latest_app_snapshots, get_session
from app.display_helpers import _fmt_local, _normalize_hhmm, _now_local, _time_select_orphan
from app.form_helpers import _normalize_base_url, _people_credit_types_csv_from_form, _resolve_timezone_name
from app.models import AppSnapshot
from app.resolvers.api_keys import resolve_radarr_api_key, resolve_sonarr_api_key
from app.schemas import SettingsIn
from app.schedule import normalize_schedule_days_csv, schedule_time_dropdown_choices
from app.security_utils import encrypt_secret_for_storage
from app.time_util import utc_now_naive
from app.ui_templates import templates
from app.web_common import (
    schedule_days_csv_from_named_day_checks,
    schedule_weekdays_selected_dict,
    settings_save_redirect_tab,
    try_commit_and_reschedule,
)

from app.routers.deps import AUTH_DEPS, AUTH_FORM_DEPS

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=AUTH_DEPS)


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    settings.sonarr_api_key = resolve_sonarr_api_key(settings)
    settings.radarr_api_key = resolve_radarr_api_key(settings)
    snaps = await fetch_latest_app_snapshots(session)
    sonarr_snap = snaps.get("sonarr")
    radarr_snap = snaps.get("radarr")
    tz = settings.timezone or "UTC"
    time_choices = schedule_time_dropdown_choices(step_minutes=30)
    time_choice_keys = {v for v, _ in time_choices}
    sn_days = normalize_schedule_days_csv(settings.sonarr_schedule_days or "")
    rd_days = normalize_schedule_days_csv(settings.radarr_schedule_days or "")
    ss = _normalize_hhmm(settings.sonarr_schedule_start, "00:00")
    se = _normalize_hhmm(settings.sonarr_schedule_end, "23:59")
    rs = _normalize_hhmm(settings.radarr_schedule_start, "00:00")
    re = _normalize_hhmm(settings.radarr_schedule_end, "23:59")
    sec_notice = (request.query_params.get("sec") or "").strip()
    response = templates.TemplateResponse(
        request,
        "settings.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Fetcher settings",
            "subtitle": "Configure connections, schedules, and limits",
            "settings": settings,
            "sec_notice": sec_notice,
            "sonarr": sonarr_snap,
            "radarr": radarr_snap,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "timezones": _TIMEZONE_CHOICES,
            "schedule_time_choices": time_choices,
            "sonarr_schedule_days_normalized": sn_days,
            "radarr_schedule_days_normalized": rd_days,
            "sonarr_schedule_days_selected": schedule_weekdays_selected_dict(
                settings.sonarr_schedule_days or ""
            ),
            "radarr_schedule_days_selected": schedule_weekdays_selected_dict(
                settings.radarr_schedule_days or ""
            ),
            "sonarr_schedule_start_hhmm": ss,
            "sonarr_schedule_end_hhmm": se,
            "radarr_schedule_start_hhmm": rs,
            "radarr_schedule_end_hhmm": re,
            "sonarr_start_orphan": _time_select_orphan(ss, time_choice_keys, fallback_display="12:00 AM"),
            "sonarr_end_orphan": _time_select_orphan(se, time_choice_keys, fallback_display="11:59 PM"),
            "radarr_start_orphan": _time_select_orphan(rs, time_choice_keys, fallback_display="12:00 AM"),
            "radarr_end_orphan": _time_select_orphan(re, time_choice_keys, fallback_display="11:59 PM"),
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )
    # Simple Browser / embedded WebViews often cache HTML; force reload of Settings.
    response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response


@router.get("/settings/backup/export")
async def settings_backup_export(session: AsyncSession = Depends(get_session)) -> Response:
    row = await _get_or_create_settings(session)
    body = export_json_bytes(row)
    d = datetime.now(timezone.utc).strftime("%d-%m-%Y")
    fname = f"fetcher-settings-backup-{d}.json"
    return Response(
        content=body,
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.post("/settings/auth/credentials", dependencies=AUTH_FORM_DEPS)
async def settings_auth_credentials(
    auth_form: str = Form(""),
    current_password: str = Form(""),
    new_username: str = Form(""),
    new_password: str = Form(""),
    confirm_new_password: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await _get_or_create_settings(session)
    cp = current_password or ""
    if not verify_password(password=cp, stored_hash=(row.auth_password_hash or "")):
        return RedirectResponse("/settings?sec=bad_current", status_code=303)

    form = (auth_form or "").strip().lower()
    if form == "username":
        nu = (new_username or "").strip()
        if not nu:
            return RedirectResponse("/settings?sec=user_empty", status_code=303)
        row.auth_username = nu
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session):
            return RedirectResponse("/settings?sec=save_fail", status_code=303)
        return RedirectResponse("/settings?sec=user_ok", status_code=303)

    if form == "password":
        np = (new_password or "").strip()
        cf = (confirm_new_password or "").strip()
        if not np or len(np) < 8:
            return RedirectResponse("/settings?sec=pass_short", status_code=303)
        if np != cf:
            return RedirectResponse("/settings?sec=pass_mismatch", status_code=303)
        row.auth_password_hash = hash_password(np)
        row.auth_refresh_token_hash = ""
        row.auth_refresh_expires_at = None
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session):
            return RedirectResponse("/settings?sec=save_fail", status_code=303)
        return RedirectResponse("/settings?sec=pass_ok", status_code=303)

    return RedirectResponse("/settings?sec=invalid", status_code=303)


@router.post("/settings/auth/access_control", dependencies=AUTH_FORM_DEPS)
async def settings_auth_access_control(
    auth_ip_allowlist: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await _get_or_create_settings(session)
    try:
        normalized = normalize_auth_ip_allowlist_input(auth_ip_allowlist)
    except ValueError:
        return RedirectResponse("/settings?save=fail&reason=invalid_ip&tab=security", status_code=303)
    row.auth_ip_allowlist = normalized
    row.updated_at = utc_now_naive()
    if not await try_commit_and_reschedule(session):
        return RedirectResponse("/settings?sec=save_fail&tab=security", status_code=303)
    return RedirectResponse("/settings?saved=1&tab=security", status_code=303)


@router.post("/settings/backup/import", dependencies=AUTH_FORM_DEPS)
async def settings_backup_import(
    session: AsyncSession = Depends(get_session),
    file: UploadFile = File(...),
    confirm: str = Form(""),
) -> RedirectResponse:
    if (confirm or "").strip() != "yes":
        return RedirectResponse("/settings?import=need_confirm", status_code=303)
    raw = await file.read()
    if not raw.strip():
        return RedirectResponse("/settings?import=empty", status_code=303)
    try:
        await import_settings_replace(session, raw)
    except ValueError as e:
        r = str(e)
        if len(r) > 180:
            r = r[:177] + "..."
        return RedirectResponse(f"/settings?import=fail&reason={quote(r, safe='')}", status_code=303)
    return RedirectResponse("/settings?import=ok", status_code=303)


@router.post("/settings", dependencies=AUTH_FORM_DEPS)
async def save_settings(
    sonarr_enabled: bool = Form(False),
    sonarr_url: str = Form(""),
    sonarr_api_key: str = Form(""),
    sonarr_search_missing: bool = Form(False),
    sonarr_search_upgrades: bool = Form(False),
    sonarr_max_items_per_run: int = Form(50),
    sonarr_interval_minutes: int = Form(60),
    sonarr_schedule_enabled: bool = Form(False),
    sonarr_schedule_Mon: int = Form(0),
    sonarr_schedule_Tue: int = Form(0),
    sonarr_schedule_Wed: int = Form(0),
    sonarr_schedule_Thu: int = Form(0),
    sonarr_schedule_Fri: int = Form(0),
    sonarr_schedule_Sat: int = Form(0),
    sonarr_schedule_Sun: int = Form(0),
    sonarr_schedule_start: str = Form("00:00"),
    sonarr_schedule_end: str = Form("23:59"),
    radarr_enabled: bool = Form(False),
    radarr_url: str = Form(""),
    radarr_api_key: str = Form(""),
    radarr_search_missing: bool = Form(False),
    radarr_search_upgrades: bool = Form(False),
    radarr_max_items_per_run: int = Form(50),
    radarr_interval_minutes: int = Form(60),
    radarr_schedule_enabled: bool = Form(False),
    radarr_schedule_Mon: int = Form(0),
    radarr_schedule_Tue: int = Form(0),
    radarr_schedule_Wed: int = Form(0),
    radarr_schedule_Thu: int = Form(0),
    radarr_schedule_Fri: int = Form(0),
    radarr_schedule_Sat: int = Form(0),
    radarr_schedule_Sun: int = Form(0),
    radarr_schedule_start: str = Form("00:00"),
    radarr_schedule_end: str = Form("23:59"),
    arr_search_cooldown_minutes: int = Form(1440),
    log_retention_days: int = Form(90),
    timezone: str = Form("UTC"),
    save_scope: str = Form("all"),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    tab_q = quote(settings_save_redirect_tab(save_scope), safe="")
    try:
        row = await _get_or_create_settings(session)
        data = SettingsIn(
            sonarr_enabled=sonarr_enabled,
            sonarr_url=_normalize_base_url(sonarr_url),
            sonarr_api_key=sonarr_api_key.strip(),
            sonarr_search_missing=sonarr_search_missing,
            sonarr_search_upgrades=sonarr_search_upgrades,
            sonarr_max_items_per_run=sonarr_max_items_per_run,
            sonarr_interval_minutes=sonarr_interval_minutes,
            # schedule fields are not in SettingsIn; set on ORM row below
            radarr_enabled=radarr_enabled,
            radarr_url=_normalize_base_url(radarr_url),
            radarr_api_key=radarr_api_key.strip(),
            radarr_search_missing=radarr_search_missing,
            radarr_search_upgrades=radarr_search_upgrades,
            radarr_max_items_per_run=radarr_max_items_per_run,
            radarr_interval_minutes=radarr_interval_minutes,
            arr_search_cooldown_minutes=arr_search_cooldown_minutes,
        )
        scope = (save_scope or "all").strip().lower()
        # Sonarr/Radarr: persist on app-specific save OR "Save Global" (same form posts all fields).
        if scope in ("all", "sonarr", "global"):
            row.sonarr_enabled = data.sonarr_enabled
            row.sonarr_url = data.sonarr_url
            row.sonarr_api_key = encrypt_secret_for_storage(data.sonarr_api_key)
            row.sonarr_search_missing = data.sonarr_search_missing
            row.sonarr_search_upgrades = data.sonarr_search_upgrades
            row.sonarr_max_items_per_run = data.sonarr_max_items_per_run
            row.sonarr_interval_minutes = data.sonarr_interval_minutes
            row.sonarr_schedule_enabled = sonarr_schedule_enabled
            row.sonarr_schedule_days = schedule_days_csv_from_named_day_checks(
                sonarr_schedule_Mon,
                sonarr_schedule_Tue,
                sonarr_schedule_Wed,
                sonarr_schedule_Thu,
                sonarr_schedule_Fri,
                sonarr_schedule_Sat,
                sonarr_schedule_Sun,
            )
            row.sonarr_schedule_start = _normalize_hhmm(sonarr_schedule_start, "00:00")
            row.sonarr_schedule_end = _normalize_hhmm(sonarr_schedule_end, "23:59")

        if scope in ("all", "radarr", "global"):
            row.radarr_enabled = data.radarr_enabled
            row.radarr_url = data.radarr_url
            row.radarr_api_key = encrypt_secret_for_storage(data.radarr_api_key)
            row.radarr_search_missing = data.radarr_search_missing
            row.radarr_search_upgrades = data.radarr_search_upgrades
            row.radarr_max_items_per_run = data.radarr_max_items_per_run
            row.radarr_interval_minutes = data.radarr_interval_minutes
            row.radarr_schedule_enabled = radarr_schedule_enabled
            row.radarr_schedule_days = schedule_days_csv_from_named_day_checks(
                radarr_schedule_Mon,
                radarr_schedule_Tue,
                radarr_schedule_Wed,
                radarr_schedule_Thu,
                radarr_schedule_Fri,
                radarr_schedule_Sat,
                radarr_schedule_Sun,
            )
            row.radarr_schedule_start = _normalize_hhmm(radarr_schedule_start, "00:00")
            row.radarr_schedule_end = _normalize_hhmm(radarr_schedule_end, "23:59")

        if scope in ("all", "global"):
            row.arr_search_cooldown_minutes = data.arr_search_cooldown_minutes
            row.log_retention_days = max(7, min(3650, int(log_retention_days or 90)))
            row.timezone = _resolve_timezone_name(timezone)

        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session):
            return RedirectResponse(f"/settings?save=fail&reason=db_busy&tab={tab_q}", status_code=303)
        return RedirectResponse(f"/settings?saved=1&tab={tab_q}", status_code=303)
    except SQLAlchemyError:
        logger.exception("POST /settings SQLAlchemyError")
        return RedirectResponse(f"/settings?save=fail&reason=db_error&tab={tab_q}", status_code=303)
    except ValueError:
        logger.exception("POST /settings ValueError")
        return RedirectResponse(f"/settings?save=fail&reason=invalid&tab={tab_q}", status_code=303)
    except Exception:
        logger.exception("POST /settings failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return RedirectResponse(f"/settings?save=fail&reason=error&tab={tab_q}", status_code=303)


@router.post("/test/sonarr", dependencies=AUTH_FORM_DEPS)
async def test_sonarr(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    # Keep route-owned side effects here: snapshot payload + redirect contract.
    # Any message or redirect changes must be covered by connection-testing regression tests.
    settings = await _get_or_create_settings(session)
    result = await ConnectionTestService().check_arr_health(
        url=settings.sonarr_url,
        api_key=resolve_sonarr_api_key(settings),
    )
    if result.ok:
        session.add(AppSnapshot(app="sonarr", ok=True, status_message="Connection test succeeded.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=sonarr_ok", status_code=303)
    session.add(
        AppSnapshot(
            app="sonarr",
            ok=False,
            status_message=f"Connection test failed: {ConnectionTestService.message_with_exception_prefix(result)}",
            missing_total=0,
            cutoff_unmet_total=0,
        )
    )
    await session.commit()
    return RedirectResponse("/settings?test=sonarr_fail", status_code=303)


@router.post("/test/radarr", dependencies=AUTH_FORM_DEPS)
async def test_radarr(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    # ConnectionTestService provides transport result primitives only; caller preserves UX contract.
    settings = await _get_or_create_settings(session)
    result = await ConnectionTestService().check_arr_health(
        url=settings.radarr_url,
        api_key=resolve_radarr_api_key(settings),
    )
    if result.ok:
        session.add(AppSnapshot(app="radarr", ok=True, status_message="Connection test succeeded.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=radarr_ok", status_code=303)
    session.add(
        AppSnapshot(
            app="radarr",
            ok=False,
            status_message=f"Connection test failed: {ConnectionTestService.message_with_exception_prefix(result)}",
            missing_total=0,
            cutoff_unmet_total=0,
        )
    )
    await session.commit()
    return RedirectResponse("/settings?test=radarr_fail", status_code=303)
