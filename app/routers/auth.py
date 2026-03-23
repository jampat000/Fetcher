from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import (
    INVALID_LOGIN_MESSAGE,
    TOO_MANY_ATTEMPTS_MESSAGE,
    attach_session_cookie,
    clear_login_failures,
    clear_session_cookie,
    get_client_ip,
    hash_password,
    login_rate_limited,
    record_login_failure,
    request_prefers_json,
    require_auth,
    sanitize_next_param,
    verify_password,
)
from app.branding import APP_NAME, APP_TAGLINE
from app.db import _get_or_create_settings, get_session
from app.ui_templates import templates

router = APIRouter()


@router.get("/login", response_class=HTMLResponse, response_model=None)
async def login_get(
    request: Request,
    error: str = "",
    next_q: str = Query("", alias="next"),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse | RedirectResponse:
    settings = await _get_or_create_settings(session)
    if not (settings.auth_password_hash or "").strip():
        return RedirectResponse("/setup/0", status_code=302)
    login_next = sanitize_next_param(next_q)
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Sign in",
            "subtitle": "Sign in to continue",
            "error": (error or "").strip(),
            "login_next": login_next,
        },
    )


@router.post("/login", response_model=None)
async def login_post(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    next_q: str = Form("", alias="next"),
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse | RedirectResponse | JSONResponse:
    settings = await _get_or_create_settings(session)
    next_dest = sanitize_next_param(next_q)
    if not (settings.auth_password_hash or "").strip():
        if request_prefers_json(request):
            return JSONResponse(
                status_code=401,
                content={"message": "Set a password in the setup wizard first.", "setup_path": "/setup/0"},
            )
        return RedirectResponse("/setup/0", status_code=303)

    ip = get_client_ip(request)
    if login_rate_limited(ip):
        if request_prefers_json(request):
            return JSONResponse(status_code=429, content={"message": TOO_MANY_ATTEMPTS_MESSAGE})
        return HTMLResponse(TOO_MANY_ATTEMPTS_MESSAGE, status_code=429)
    expected_user = (settings.auth_username or "admin").strip() or "admin"
    u = (username or "").strip()
    p = password or ""
    ok = u == expected_user and verify_password(password=p, stored_hash=(settings.auth_password_hash or ""))
    if ok:
        clear_login_failures(ip)
        secret = (settings.auth_session_secret or "").strip()
        if not secret:
            if request_prefers_json(request):
                return JSONResponse(status_code=500, content={"message": "Server misconfiguration"})
            return HTMLResponse("Server misconfiguration", status_code=500)
        resp = RedirectResponse(next_dest, status_code=303)
        attach_session_cookie(resp, secret=secret, username=expected_user)
        return resp

    record_login_failure(ip)
    if request_prefers_json(request):
        return JSONResponse(status_code=401, content={"message": INVALID_LOGIN_MESSAGE})
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Sign in",
            "subtitle": "Sign in to continue",
            "error": INVALID_LOGIN_MESSAGE,
            "login_next": next_dest,
        },
    )


@router.get("/logout", response_class=RedirectResponse)
async def logout_get(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    dest = (
        "/setup/0"
        if not (settings.auth_password_hash or "").strip()
        else "/login"
    )
    resp = RedirectResponse(dest, status_code=303)
    clear_session_cookie(resp)
    return resp
