from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.branding import APP_NAME
from app.db import _get_or_create_settings, get_session
from app.resolvers.api_keys import resolve_setup_api_key
from app.schemas import ArrSearchNowIn, SetupConnTestIn, SetupEmbyTestIn
from app.service_logic import run_once
from app.setup_helpers import test_emby_connection, test_radarr_connection, test_sonarr_connection
from app.version_info import get_app_version
from app.web_common import build_dashboard_status

from app.routers.deps import AUTH_DEPS

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness for monitors (incl. packaged build smoke tests)."""
    return {
        "status": "ok",
        "app": APP_NAME,
        "version": get_app_version(),
    }


@router.get("/api/version")
async def api_version() -> dict[str, str]:
    """Lightweight version endpoint for automation / dashboards."""
    return {"app": APP_NAME, "version": get_app_version()}


@router.post("/api/setup/test-sonarr", dependencies=AUTH_DEPS)
async def api_setup_test_sonarr(body: SetupConnTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "sonarr")
    ok, msg = await test_sonarr_connection(body.url, key)
    return JSONResponse({"ok": ok, "message": msg})


@router.post("/api/setup/test-radarr", dependencies=AUTH_DEPS)
async def api_setup_test_radarr(body: SetupConnTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "radarr")
    ok, msg = await test_radarr_connection(body.url, key)
    return JSONResponse({"ok": ok, "message": msg})


@router.post("/api/setup/test-emby", dependencies=AUTH_DEPS)
async def api_setup_test_emby(body: SetupEmbyTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "emby")
    ok, msg = await test_emby_connection(body.url, key, body.user_id)
    return JSONResponse({"ok": ok, "message": msg})


@router.post("/api/arr/search-now", dependencies=AUTH_DEPS)
async def api_arr_search_now(body: ArrSearchNowIn, session: AsyncSession = Depends(get_session)) -> JSONResponse:
    """One-shot missing or upgrade search for Sonarr (TV) or Radarr (movies); bypasses schedule + run-interval gates."""
    result = await run_once(session, arr_manual_scope=body.scope)
    return JSONResponse({"ok": result.ok, "message": result.message})


@router.get("/api/dashboard/status", dependencies=AUTH_DEPS)
async def api_dashboard_status(
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    settings = await _get_or_create_settings(session)
    tz = settings.timezone or "UTC"
    return JSONResponse(await build_dashboard_status(session, tz))
