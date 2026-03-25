from __future__ import annotations

"""Setup wizard: each step POST persists via ``try_commit_and_reschedule`` then redirects (or JSON for async clients).

Browser JS may POST with ``X-Fetcher-Setup-Async: 1`` to get ``{ok, redirect}`` or error JSON and show the same
pending/success/failure strip as settings; navigation to the next step still uses one ``Location`` follow.
"""

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import attach_session_cookie, get_csrf_token_for_template, hash_password, require_csrf
from app.branding import APP_NAME, APP_TAGLINE
from app.constants import _TIMEZONE_CHOICES
from app.db import _get_or_create_settings, get_session
from app.form_helpers import _normalize_base_url, _resolve_timezone_name
from app.resolvers.api_keys import resolve_emby_api_key, resolve_radarr_api_key, resolve_sonarr_api_key
from app.security_utils import encrypt_secret_for_storage
from app.time_util import utc_now_naive
from app.display_helpers import _now_local
from app.ui_templates import templates
from app.web_common import (
    SETUP_WIZARD_STEPS,
    WIZARD_LAST_STEP_INDEX,
    settings_looks_like_existing_fetcher_install,
    setup_wizard_step_title,
    is_setup_complete,
    try_commit_and_reschedule,
)

router = APIRouter()

SETUP_INPLACE_JSON_HEADER = "x-fetcher-setup-async"


def _setup_wants_inplace_json(request: Request) -> bool:
    return (request.headers.get(SETUP_INPLACE_JSON_HEADER) or "").strip() == "1"


@router.get("/setup", response_class=RedirectResponse)
async def setup_wizard_entry(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    if not (settings.auth_password_hash or "").strip():
        return RedirectResponse("/setup/0", status_code=302)
    return RedirectResponse("/setup/1", status_code=302)


@router.get("/setup/{step}", response_class=HTMLResponse, response_model=None)
async def setup_wizard_page(
    step: int, request: Request, session: AsyncSession = Depends(get_session)
) -> HTMLResponse | RedirectResponse:
    settings = await _get_or_create_settings(session)
    settings.sonarr_api_key = resolve_sonarr_api_key(settings)
    settings.radarr_api_key = resolve_radarr_api_key(settings)
    settings.emby_api_key = resolve_emby_api_key(settings)
    if not (settings.auth_password_hash or "").strip():
        if step != 0:
            return RedirectResponse("/setup/0", status_code=302)
    elif step == 0:
        return RedirectResponse("/setup/1", status_code=302)

    if step < 0 or step > WIZARD_LAST_STEP_INDEX:
        if not (settings.auth_password_hash or "").strip():
            return RedirectResponse("/setup/0", status_code=302)
        return RedirectResponse("/setup/1", status_code=302)

    tz = settings.timezone or "UTC"
    show_setup_wizard = not is_setup_complete(settings)
    setup_error = (request.query_params.get("error") or "").strip()
    setup_save_fail = (request.query_params.get("save") or "").strip().lower() == "fail"
    if step == 0:
        setup_account_intro = (
            "upgrade" if settings_looks_like_existing_fetcher_install(settings) else "new"
        )
    else:
        setup_account_intro = ""
    return templates.TemplateResponse(
        request,
        "setup_wizard.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Setup (step {step + 1} of {SETUP_WIZARD_STEPS})",
            "subtitle": "Connect your apps",
            "settings": settings,
            "step": step,
            "setup_steps_total": SETUP_WIZARD_STEPS,
            "step_title": setup_wizard_step_title(step),
            "setup_step_labels": ["Account", "Sonarr", "Radarr", "Emby", "Schedule", "Next steps"],
            "timezone_choices": _TIMEZONE_CHOICES,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "setup_error": setup_error,
            "setup_save_fail": setup_save_fail,
            "setup_account_intro": setup_account_intro,
            "csrf_token": await get_csrf_token_for_template(request, session),
            "show_setup_wizard": show_setup_wizard,
        },
    )


@router.post("/setup/{step}", dependencies=[Depends(require_csrf)], response_model=None)
async def setup_wizard_save(
    request: Request,
    step: int,
    wizard_action: str = Form("continue"),
    setup_auth_username: str = Form("admin"),
    setup_auth_password: str = Form(""),
    sonarr_enabled: bool = Form(False),
    sonarr_url: str = Form(""),
    sonarr_api_key: str = Form(""),
    radarr_enabled: bool = Form(False),
    radarr_url: str = Form(""),
    radarr_api_key: str = Form(""),
    emby_enabled: bool = Form(False),
    emby_url: str = Form(""),
    emby_api_key: str = Form(""),
    emby_user_id: str = Form(""),
    sonarr_interval_minutes: int = Form(60),
    radarr_interval_minutes: int = Form(60),
    emby_interval_minutes: int = Form(60),
    timezone: str = Form("UTC"),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse | JSONResponse:
    want_json = _setup_wants_inplace_json(request)

    def respond_redirect(path: str, *, status: int = 303) -> RedirectResponse | JSONResponse:
        if want_json:
            return JSONResponse({"ok": True, "redirect": path})
        return RedirectResponse(path, status_code=status)

    def respond_cookie_then_redirect(
        path: str, *, secret: str, username: str, status: int = 303
    ) -> RedirectResponse | JSONResponse:
        if want_json:
            resp = JSONResponse({"ok": True, "redirect": path})
            attach_session_cookie(resp, secret=secret, username=username)
            return resp
        resp = RedirectResponse(path, status_code=status)
        attach_session_cookie(resp, secret=secret, username=username)
        return resp

    def respond_err(
        *,
        path: str,
        error: str | None = None,
        save_fail_reason: str | None = None,
    ) -> RedirectResponse | JSONResponse:
        if want_json:
            body: dict[str, str | bool] = {"ok": False}
            if error:
                body["error"] = error
            if save_fail_reason:
                body["reason"] = save_fail_reason
            return JSONResponse(body)
        q: list[str] = []
        if error:
            q.append(f"error={error}")
        if save_fail_reason:
            q.append("save=fail")
            q.append(f"reason={save_fail_reason}")
        suffix = ("?" + "&".join(q)) if q else ""
        return RedirectResponse(f"{path}{suffix}", status_code=303)

    row0 = await _get_or_create_settings(session)
    if not (row0.auth_password_hash or "").strip():
        if step != 0:
            return respond_redirect("/setup/0")
    else:
        if step == 0:
            return respond_redirect("/setup/1")

    if step < 0 or step > WIZARD_LAST_STEP_INDEX:
        if not (row0.auth_password_hash or "").strip():
            return respond_redirect("/setup/0")
        return respond_redirect("/setup/1")
    if step == WIZARD_LAST_STEP_INDEX:
        return respond_redirect("/?setup=complete")

    skip = (wizard_action or "").strip().lower() == "skip"
    if skip and step == 0:
        return respond_err(path="/setup/0", error="account_required")

    if not skip:
        row = await _get_or_create_settings(session)
        if step == 0:
            u = (setup_auth_username or "admin").strip() or "admin"
            pw = (setup_auth_password or "").strip()
            if len(pw) < 8:
                return respond_err(path="/setup/0", error="short_password")
            row.auth_username = u
            row.auth_password_hash = hash_password(pw)
            row.auth_refresh_token_hash = ""
            row.auth_refresh_expires_at = None
            row.updated_at = utc_now_naive()
            if not await try_commit_and_reschedule(session):
                return respond_err(path="/setup/0", save_fail_reason="db_busy")
            secret = (row.auth_session_secret or "").strip()
            expected_user = (row.auth_username or "admin").strip() or "admin"
            return respond_cookie_then_redirect("/setup/1", secret=secret, username=expected_user)
        if step == 1:
            row.sonarr_enabled = sonarr_enabled
            row.sonarr_url = _normalize_base_url(sonarr_url)
            row.sonarr_api_key = encrypt_secret_for_storage((sonarr_api_key or "").strip())
        elif step == 2:
            row.radarr_enabled = radarr_enabled
            row.radarr_url = _normalize_base_url(radarr_url)
            row.radarr_api_key = encrypt_secret_for_storage((radarr_api_key or "").strip())
        elif step == 3:
            row.emby_enabled = emby_enabled
            row.emby_url = _normalize_base_url(emby_url)
            row.emby_api_key = encrypt_secret_for_storage((emby_api_key or "").strip())
            row.emby_user_id = (emby_user_id or "").strip()
        elif step == 4:
            # Per-app intervals (same as Fetcher Settings / Trimmer Settings).
            def _clamp_interval(raw: object) -> int:
                try:
                    v = int(raw)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    v = 60
                return max(5, min(7 * 24 * 60, v))

            row.sonarr_interval_minutes = _clamp_interval(sonarr_interval_minutes)
            row.radarr_interval_minutes = _clamp_interval(radarr_interval_minutes)
            row.emby_interval_minutes = _clamp_interval(emby_interval_minutes)
            row.timezone = _resolve_timezone_name(timezone)
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session):
            return respond_err(path=f"/setup/{step}", save_fail_reason="db_busy")

    nxt = step + 1
    if nxt > WIZARD_LAST_STEP_INDEX:
        return respond_redirect("/?setup=complete")
    return respond_redirect(f"/setup/{nxt}")
