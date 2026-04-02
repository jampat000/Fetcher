from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from urllib.parse import quote

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
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
from app.db import get_or_create_settings, fetch_latest_app_snapshots, get_session
from app.display_helpers import fmt_local, normalize_hhmm, now_local, time_select_orphan
from app.form_helpers import _normalize_base_url, _people_credit_types_csv_from_form, _resolve_timezone_name
from app.models import AppSnapshot
from app.resolvers.api_keys import resolve_radarr_api_key, resolve_sonarr_api_key
from app.schemas import SettingsIn
from app.schedule import normalize_schedule_days_csv, schedule_time_dropdown_choices
from app.security_utils import encrypt_secret_for_storage
from app.time_util import utc_now_naive
from app.ui_templates import templates
from app.web_common import (
    is_setup_complete,
    schedule_days_csv_from_named_day_checks,
    schedule_weekdays_selected_dict,
    sidebar_health_dots,
    try_commit_and_reschedule,
)

from app.routers.deps import AUTH_DEPS, AUTH_FORM_DEPS

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=AUTH_DEPS)

# In-place POSTs (section Save + Arr connection Test) send this header for JSON instead of a 303 redirect.
SETTINGS_INPLACE_JSON_HEADER = "x-fetcher-settings-async"

# ``POST /settings`` (Fetcher settings forms only): exactly one of these scopes per request.
_SETTINGS_POST_SAVE_SCOPES = frozenset({"global", "sonarr", "radarr"})


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await get_or_create_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    template_sonarr_api_key = resolve_sonarr_api_key(settings)
    template_radarr_api_key = resolve_radarr_api_key(settings)
    snaps = await fetch_latest_app_snapshots(session)
    sonarr_snap = snaps.get("sonarr")
    radarr_snap = snaps.get("radarr")
    tz = settings.timezone or "UTC"
    time_choices = schedule_time_dropdown_choices(step_minutes=30)
    time_choice_keys = {v for v, _ in time_choices}
    sn_days = normalize_schedule_days_csv(settings.sonarr_schedule_days or "")
    rd_days = normalize_schedule_days_csv(settings.radarr_schedule_days or "")
    ss = normalize_hhmm(settings.sonarr_schedule_start, "00:00")
    se = normalize_hhmm(settings.sonarr_schedule_end, "23:59")
    rs = normalize_hhmm(settings.radarr_schedule_start, "00:00")
    re = normalize_hhmm(settings.radarr_schedule_end, "23:59")
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
            "now_local": now_local(tz),
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
            "sonarr_start_orphan": time_select_orphan(ss, time_choice_keys, fallback_display="12:00 AM"),
            "sonarr_end_orphan": time_select_orphan(se, time_choice_keys, fallback_display="11:59 PM"),
            "radarr_start_orphan": time_select_orphan(rs, time_choice_keys, fallback_display="12:00 AM"),
            "radarr_end_orphan": time_select_orphan(re, time_choice_keys, fallback_display="11:59 PM"),
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
            "template_sonarr_api_key": template_sonarr_api_key,
            "template_radarr_api_key": template_radarr_api_key,
            "sidebar_health": sidebar_health_dots(snaps),
        },
    )
    # Simple Browser / embedded WebViews often cache HTML; force reload of Settings.
    response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response


@router.post("/settings/backup/export", dependencies=AUTH_FORM_DEPS)
async def settings_backup_export(session: AsyncSession = Depends(get_session)) -> Response:
    row = await get_or_create_settings(session)
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
    row = await get_or_create_settings(session)
    cp = current_password or ""
    if not verify_password(password=cp, stored_hash=(row.auth_password_hash or "")):
        return RedirectResponse("/settings?sec=bad_current&tab=security", status_code=303)

    form = (auth_form or "").strip().lower()
    if form == "username":
        nu = (new_username or "").strip()
        if not nu:
            return RedirectResponse("/settings?sec=user_empty&tab=security", status_code=303)
        row.auth_username = nu
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session):
            return RedirectResponse("/settings?sec=save_fail&tab=security", status_code=303)
        return RedirectResponse("/settings?sec=user_ok&tab=security", status_code=303)

    if form == "password":
        np = (new_password or "").strip()
        cf = (confirm_new_password or "").strip()
        if not np or len(np) < 8:
            return RedirectResponse("/settings?sec=pass_short&tab=security", status_code=303)
        if np != cf:
            return RedirectResponse("/settings?sec=pass_mismatch&tab=security", status_code=303)
        row.auth_password_hash = hash_password(np)
        row.auth_refresh_token_hash = ""
        row.auth_refresh_expires_at = None
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session):
            return RedirectResponse("/settings?sec=save_fail&tab=security", status_code=303)
        return RedirectResponse("/settings?sec=pass_ok&tab=security", status_code=303)

    return RedirectResponse("/settings?sec=invalid&tab=security", status_code=303)


@router.post("/settings/auth/access_control", dependencies=AUTH_FORM_DEPS)
async def settings_auth_access_control(
    auth_ip_allowlist: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await get_or_create_settings(session)
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
        return RedirectResponse("/settings?import=need_confirm&tab=global", status_code=303)
    raw = await file.read()
    if not raw.strip():
        return RedirectResponse("/settings?import=empty&tab=global", status_code=303)
    try:
        await import_settings_replace(session, raw)
    except ValueError as e:
        r = str(e)
        if len(r) > 180:
            r = r[:177] + "..."
        return RedirectResponse(f"/settings?import=fail&reason={quote(r, safe='')}&tab=global", status_code=303)
    return RedirectResponse("/settings?import=ok&tab=global", status_code=303)


@router.post("/settings", dependencies=AUTH_FORM_DEPS, response_model=None)
async def save_settings(
    request: Request,
    sonarr_enabled: bool = Form(False),
    sonarr_url: str = Form(""),
    sonarr_api_key: str = Form(""),
    sonarr_search_missing: bool = Form(False),
    sonarr_search_upgrades: bool = Form(False),
    sonarr_cleanup_corrupt: bool = Form(False),
    sonarr_blocklist_corrupt: bool = Form(False),
    sonarr_cleanup_download_failed: bool = Form(False),
    sonarr_blocklist_download_failed: bool = Form(False),
    sonarr_cleanup_unmatched: bool = Form(False),
    sonarr_blocklist_unmatched: bool = Form(False),
    sonarr_cleanup_quality: bool = Form(False),
    sonarr_blocklist_quality: bool = Form(False),
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
    radarr_cleanup_corrupt: bool = Form(False),
    radarr_blocklist_corrupt: bool = Form(False),
    radarr_cleanup_download_failed: bool = Form(False),
    radarr_blocklist_download_failed: bool = Form(False),
    radarr_cleanup_unmatched: bool = Form(False),
    radarr_blocklist_unmatched: bool = Form(False),
    radarr_cleanup_quality: bool = Form(False),
    radarr_blocklist_quality: bool = Form(False),
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
    sonarr_retry_delay_minutes: int = Form(1440),
    radarr_retry_delay_minutes: int = Form(1440),
    failed_import_cleanup_interval_minutes: int = Form(60),
    log_retention_days: int = Form(90),
    timezone: str = Form("UTC"),
    save_scope: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse | JSONResponse:
    """Write **one** Fetcher settings section per request (``global`` | ``sonarr`` | ``radarr`` only)."""
    scope = (save_scope or "").strip().lower()
    # ``invalid_scope`` messages render on the Global tab only — always land there for that error.
    tab_key = "global" if scope not in _SETTINGS_POST_SAVE_SCOPES else scope
    tab_q = quote(tab_key, safe="")
    want_json = (request.headers.get(SETTINGS_INPLACE_JSON_HEADER) or "").strip() == "1"

    def respond(*, saved: bool, reason: str | None = None) -> RedirectResponse | JSONResponse:
        if want_json:
            payload: dict[str, str | bool] = {"ok": saved, "tab": tab_key}
            if not saved:
                payload["reason"] = reason or "error"
            return JSONResponse(payload)
        if saved:
            return RedirectResponse(f"/settings?saved=1&tab={tab_q}", status_code=303)
        r = reason or "error"
        return RedirectResponse(
            f"/settings?save=fail&reason={quote(r, safe='')}&tab={tab_q}",
            status_code=303,
        )

    if scope not in _SETTINGS_POST_SAVE_SCOPES:
        return respond(saved=False, reason="invalid_scope")
    if scope == "sonarr" and int(sonarr_retry_delay_minutes or 0) < 1:
        return respond(saved=False, reason="sonarr_retry_delay_min")
    if scope == "radarr" and int(radarr_retry_delay_minutes or 0) < 1:
        return respond(saved=False, reason="radarr_retry_delay_min")

    try:
        row = await get_or_create_settings(session)
        # Merge intervals from DB for the app not being saved so SettingsIn is well-formed without
        # applying Form defaults to the other app's interval.
        son_im = sonarr_interval_minutes
        rad_im = radarr_interval_minutes
        if scope != "sonarr":
            son_im = row.sonarr_interval_minutes if row.sonarr_interval_minutes is not None else 60
        if scope != "radarr":
            rad_im = row.radarr_interval_minutes if row.radarr_interval_minutes is not None else 60

        def _row_bool(attr: str) -> bool:
            return bool(getattr(row, attr, False))

        s_cc = sonarr_cleanup_corrupt if scope == "sonarr" else _row_bool("sonarr_cleanup_corrupt")
        s_bc = sonarr_blocklist_corrupt if scope == "sonarr" else _row_bool("sonarr_blocklist_corrupt")
        s_cdf = sonarr_cleanup_download_failed if scope == "sonarr" else _row_bool("sonarr_cleanup_download_failed")
        s_bdf = sonarr_blocklist_download_failed if scope == "sonarr" else _row_bool("sonarr_blocklist_download_failed")
        s_cu = sonarr_cleanup_unmatched if scope == "sonarr" else _row_bool("sonarr_cleanup_unmatched")
        s_bu = sonarr_blocklist_unmatched if scope == "sonarr" else _row_bool("sonarr_blocklist_unmatched")
        s_cq = sonarr_cleanup_quality if scope == "sonarr" else _row_bool("sonarr_cleanup_quality")
        s_bq = sonarr_blocklist_quality if scope == "sonarr" else _row_bool("sonarr_blocklist_quality")

        r_cc = radarr_cleanup_corrupt if scope == "radarr" else _row_bool("radarr_cleanup_corrupt")
        r_bc = radarr_blocklist_corrupt if scope == "radarr" else _row_bool("radarr_blocklist_corrupt")
        r_cdf = radarr_cleanup_download_failed if scope == "radarr" else _row_bool("radarr_cleanup_download_failed")
        r_bdf = radarr_blocklist_download_failed if scope == "radarr" else _row_bool("radarr_blocklist_download_failed")
        r_cu = radarr_cleanup_unmatched if scope == "radarr" else _row_bool("radarr_cleanup_unmatched")
        r_bu = radarr_blocklist_unmatched if scope == "radarr" else _row_bool("radarr_blocklist_unmatched")
        r_cq = radarr_cleanup_quality if scope == "radarr" else _row_bool("radarr_cleanup_quality")
        r_bq = radarr_blocklist_quality if scope == "radarr" else _row_bool("radarr_blocklist_quality")

        data = SettingsIn(
            sonarr_enabled=sonarr_enabled,
            sonarr_url=_normalize_base_url(sonarr_url),
            sonarr_api_key=sonarr_api_key.strip(),
            sonarr_search_missing=sonarr_search_missing,
            sonarr_search_upgrades=sonarr_search_upgrades,
            sonarr_remove_failed_imports=bool(row.sonarr_remove_failed_imports),
            sonarr_cleanup_corrupt=s_cc,
            sonarr_blocklist_corrupt=s_bc,
            sonarr_cleanup_download_failed=s_cdf,
            sonarr_blocklist_download_failed=s_bdf,
            sonarr_cleanup_unmatched=s_cu,
            sonarr_blocklist_unmatched=s_bu,
            sonarr_cleanup_quality=s_cq,
            sonarr_blocklist_quality=s_bq,
            sonarr_max_items_per_run=sonarr_max_items_per_run,
            sonarr_interval_minutes=son_im,
            # schedule fields are not in SettingsIn; set on ORM row below
            radarr_enabled=radarr_enabled,
            radarr_url=_normalize_base_url(radarr_url),
            radarr_api_key=radarr_api_key.strip(),
            radarr_search_missing=radarr_search_missing,
            radarr_search_upgrades=radarr_search_upgrades,
            radarr_remove_failed_imports=bool(row.radarr_remove_failed_imports),
            radarr_cleanup_corrupt=r_cc,
            radarr_blocklist_corrupt=r_bc,
            radarr_cleanup_download_failed=r_cdf,
            radarr_blocklist_download_failed=r_bdf,
            radarr_cleanup_unmatched=r_cu,
            radarr_blocklist_unmatched=r_bu,
            radarr_cleanup_quality=r_cq,
            radarr_blocklist_quality=r_bq,
            radarr_max_items_per_run=radarr_max_items_per_run,
            radarr_interval_minutes=rad_im,
            sonarr_retry_delay_minutes=sonarr_retry_delay_minutes,
            radarr_retry_delay_minutes=radarr_retry_delay_minutes,
            failed_import_cleanup_interval_minutes=max(1, min(10080, int(failed_import_cleanup_interval_minutes or 60))),
        )
        if scope == "sonarr":
            row.sonarr_enabled = data.sonarr_enabled
            row.sonarr_url = data.sonarr_url
            row.sonarr_api_key = encrypt_secret_for_storage(data.sonarr_api_key)
            row.sonarr_search_missing = data.sonarr_search_missing
            row.sonarr_search_upgrades = data.sonarr_search_upgrades
            row.sonarr_cleanup_corrupt = data.sonarr_cleanup_corrupt
            row.sonarr_blocklist_corrupt = data.sonarr_blocklist_corrupt
            row.sonarr_cleanup_download_failed = data.sonarr_cleanup_download_failed
            row.sonarr_blocklist_download_failed = data.sonarr_blocklist_download_failed
            row.sonarr_cleanup_unmatched = data.sonarr_cleanup_unmatched
            row.sonarr_blocklist_unmatched = data.sonarr_blocklist_unmatched
            row.sonarr_cleanup_quality = data.sonarr_cleanup_quality
            row.sonarr_blocklist_quality = data.sonarr_blocklist_quality
            row.sonarr_max_items_per_run = data.sonarr_max_items_per_run
            row.sonarr_interval_minutes = data.sonarr_interval_minutes
            row.sonarr_retry_delay_minutes = data.sonarr_retry_delay_minutes
            row.failed_import_cleanup_interval_minutes = data.failed_import_cleanup_interval_minutes
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
            row.sonarr_schedule_start = normalize_hhmm(sonarr_schedule_start, "00:00")
            row.sonarr_schedule_end = normalize_hhmm(sonarr_schedule_end, "23:59")

        if scope == "radarr":
            row.radarr_enabled = data.radarr_enabled
            row.radarr_url = data.radarr_url
            row.radarr_api_key = encrypt_secret_for_storage(data.radarr_api_key)
            row.radarr_search_missing = data.radarr_search_missing
            row.radarr_search_upgrades = data.radarr_search_upgrades
            row.radarr_cleanup_corrupt = data.radarr_cleanup_corrupt
            row.radarr_blocklist_corrupt = data.radarr_blocklist_corrupt
            row.radarr_cleanup_download_failed = data.radarr_cleanup_download_failed
            row.radarr_blocklist_download_failed = data.radarr_blocklist_download_failed
            row.radarr_cleanup_unmatched = data.radarr_cleanup_unmatched
            row.radarr_blocklist_unmatched = data.radarr_blocklist_unmatched
            row.radarr_cleanup_quality = data.radarr_cleanup_quality
            row.radarr_blocklist_quality = data.radarr_blocklist_quality
            row.radarr_max_items_per_run = data.radarr_max_items_per_run
            row.radarr_interval_minutes = data.radarr_interval_minutes
            row.radarr_retry_delay_minutes = data.radarr_retry_delay_minutes
            row.failed_import_cleanup_interval_minutes = data.failed_import_cleanup_interval_minutes
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
            row.radarr_schedule_start = normalize_hhmm(radarr_schedule_start, "00:00")
            row.radarr_schedule_end = normalize_hhmm(radarr_schedule_end, "23:59")

        if scope == "global":
            row.log_retention_days = max(7, min(3650, int(log_retention_days or 90)))
            row.timezone = _resolve_timezone_name(timezone)

        row.updated_at = utc_now_naive()
        if scope == "sonarr":
            sched_targets = {"sonarr"}
        elif scope == "radarr":
            sched_targets = {"radarr"}
        else:
            sched_targets = set()
        if not await try_commit_and_reschedule(session, targets=sched_targets):
            return respond(saved=False, reason="db_busy")
        return respond(saved=True)
    except SQLAlchemyError:
        logger.exception("POST /settings SQLAlchemyError")
        return respond(saved=False, reason="db_error")
    except ValueError:
        logger.exception("POST /settings ValueError")
        return respond(saved=False, reason="invalid")
    except Exception:
        logger.exception("POST /settings failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return respond(saved=False, reason="error")


@router.post("/test/sonarr", dependencies=AUTH_FORM_DEPS, response_model=None)
async def test_sonarr(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse | JSONResponse:
    """Health check against **saved** Sonarr URL/API key (``AppSettings`` is read-only here).

    Writes **AppSnapshot** only — dashboard / settings tiles use latest snapshot per app.
    Does **not** change configuration columns on ``AppSettings``.
    """
    want_json = (request.headers.get(SETTINGS_INPLACE_JSON_HEADER) or "").strip() == "1"
    settings = await get_or_create_settings(session)
    result = await ConnectionTestService().check_arr_health(
        url=settings.sonarr_url,
        api_key=resolve_sonarr_api_key(settings),
    )
    if result.ok:
        session.add(AppSnapshot(app="sonarr", ok=True, status_message="Connection test succeeded.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        if want_json:
            return JSONResponse({"ok": True, "tab": "sonarr", "test": "sonarr_ok"})
        return RedirectResponse("/settings?test=sonarr_ok&tab=sonarr", status_code=303)
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
    if want_json:
        return JSONResponse({"ok": False, "tab": "sonarr", "test": "sonarr_fail"})
    return RedirectResponse("/settings?test=sonarr_fail&tab=sonarr", status_code=303)


@router.post("/test/radarr", dependencies=AUTH_FORM_DEPS, response_model=None)
async def test_radarr(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse | JSONResponse:
    """Health check against **saved** Radarr URL/API key (``AppSettings`` is read-only here).

    Writes **AppSnapshot** only — dashboard / settings tiles use latest snapshot per app.
    Does **not** change configuration columns on ``AppSettings``.
    """
    want_json = (request.headers.get(SETTINGS_INPLACE_JSON_HEADER) or "").strip() == "1"
    settings = await get_or_create_settings(session)
    result = await ConnectionTestService().check_arr_health(
        url=settings.radarr_url,
        api_key=resolve_radarr_api_key(settings),
    )
    if result.ok:
        session.add(AppSnapshot(app="radarr", ok=True, status_message="Connection test succeeded.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        if want_json:
            return JSONResponse({"ok": True, "tab": "radarr", "test": "radarr_ok"})
        return RedirectResponse("/settings?test=radarr_ok&tab=radarr", status_code=303)
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
    if want_json:
        return JSONResponse({"ok": False, "tab": "radarr", "test": "radarr_fail"})
    return RedirectResponse("/settings?test=radarr_fail&tab=radarr", status_code=303)
