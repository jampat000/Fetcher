"""Auth HTTP routes (security-sensitive).

Routes should stay thin and delegate orchestration to ``AuthService``; behavior changes require regression tests.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import (
    attach_session_cookie,
    clear_session_cookie,
    request_prefers_json,
    sanitize_next_param,
)
from app.auth_service import AuthService
from app.branding import APP_NAME, APP_TAGLINE
from app.db import get_session
from app.rate_limit import limiter
from app.ui_templates import templates
from app.web_common import is_setup_complete, sidebar_health_dots

router = APIRouter()


class RefreshIn(BaseModel):
    refresh_token: str


def get_auth_service() -> AuthService:
    return AuthService()


@router.get("/login", response_class=HTMLResponse, response_model=None)
async def login_get(
    request: Request,
    error: str = "",
    next_q: str = Query("", alias="next"),
    session: AsyncSession = Depends(get_session),
    auth_service: AuthService = Depends(get_auth_service),
) -> HTMLResponse | RedirectResponse:
    settings = await auth_service.get_settings(session)
    if not (settings.auth_password_hash or "").strip():
        return RedirectResponse("/setup/0", status_code=302)
    show_setup_wizard = not is_setup_complete(settings)
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
            "show_setup_wizard": show_setup_wizard,
            "sidebar_health": sidebar_health_dots({}),
        },
    )


@router.post("/login", response_model=None)
@limiter.limit("10/minute")
async def login_post(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    next_q: str = Form("", alias="next"),
    session: AsyncSession = Depends(get_session),
    auth_service: AuthService = Depends(get_auth_service),
) -> HTMLResponse | RedirectResponse | JSONResponse:
    settings = await auth_service.get_settings(session)
    show_setup_wizard = not is_setup_complete(settings)
    next_dest = sanitize_next_param(next_q)
    if not (settings.auth_password_hash or "").strip():
        if request_prefers_json(request):
            return JSONResponse(
                status_code=401,
                content={"message": "Set a password in the setup wizard first.", "setup_path": "/setup/0"},
            )
        return RedirectResponse("/setup/0", status_code=303)

    result = await auth_service.login(
        session=session,
        request=request,
        username=username,
        password=password,
    )
    if result.ok:
        secret = result.cookie_secret
        if not secret:
            if request_prefers_json(request):
                return JSONResponse(
                    status_code=500,
                    content={
                        "message": "Session could not be created (server configuration). Restart Fetcher or try again.",
                    },
                )
            return HTMLResponse(
                "Session could not be created. Restart Fetcher or contact support.", status_code=500
            )
        resp = RedirectResponse(next_dest, status_code=303)
        attach_session_cookie(resp, secret=secret, username=result.cookie_username, request=request)
        return resp

    if request_prefers_json(request):
        return JSONResponse(status_code=result.status_code, content={"message": result.message})
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Sign in",
            "subtitle": "Sign in to continue",
            "error": result.message,
            "login_next": next_dest,
            "show_setup_wizard": show_setup_wizard,
            "sidebar_health": sidebar_health_dots({}),
        },
    )


@router.get("/logout", response_class=RedirectResponse)
async def logout_get(
    session: AsyncSession = Depends(get_session),
    auth_service: AuthService = Depends(get_auth_service),
) -> RedirectResponse:
    settings = await auth_service.get_settings(session)
    dest = (
        "/setup/0"
        if not (settings.auth_password_hash or "").strip()
        else "/login"
    )
    resp = RedirectResponse(dest, status_code=303)
    clear_session_cookie(resp)
    return resp


@router.post("/api/auth/token")
@limiter.limit("10/minute")
async def api_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session),
    auth_service: AuthService = Depends(get_auth_service),
) -> JSONResponse:
    jwt_secret = (request.app.state.jwt_secret or "").strip()
    result, payload = await auth_service.issue_api_token(
        session=session,
        request=request,
        username=form_data.username,
        password=form_data.password,
        jwt_secret=jwt_secret,
    )
    if not result.ok:
        return JSONResponse(status_code=result.status_code, content={"message": result.message})
    return JSONResponse(payload or {})


@router.post("/api/auth/refresh")
@limiter.limit("20/minute")
async def api_refresh_token(
    request: Request,
    body: RefreshIn,
    session: AsyncSession = Depends(get_session),
    auth_service: AuthService = Depends(get_auth_service),
) -> JSONResponse:
    refresh_token = (body.refresh_token or "").strip()
    if not refresh_token:
        return JSONResponse(status_code=400, content={"message": "refresh_token is required"})
    jwt_secret = (request.app.state.jwt_secret or "").strip()
    result, payload = await auth_service.refresh_token(
        session=session,
        refresh_token=refresh_token,
        jwt_secret=jwt_secret,
    )
    if not result.ok:
        return JSONResponse(status_code=result.status_code, content={"message": result.message})
    return JSONResponse(payload or {})
