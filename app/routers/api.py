from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.branding import APP_NAME
from app.db import SessionLocal, _get_or_create_settings, get_session
from app.resolvers.api_keys import resolve_setup_api_key
from app.schemas import ArrSearchNowIn, SetupConnTestIn, SetupEmbyTestIn
from app.service_logic import run_once
from app.setup_helpers import test_emby_connection, test_radarr_connection, test_sonarr_connection
from app.version_info import get_app_version
from app.web_common import build_dashboard_status

from app.routers.deps import AUTH_DEPS

router = APIRouter()
logger = logging.getLogger(__name__)


async def _run_manual_search_task(scope: str) -> None:
    async with SessionLocal() as session:
        try:
            await run_once(session, arr_manual_scope=scope)
        except Exception:  # noqa: BLE001 - background task boundary
            logger.exception("Manual Arr search task failed for scope=%s", scope)


def enqueue_manual_arr_search(scope: str) -> None:
    """Queue manual Arr search in background and return immediately to caller."""
    asyncio.create_task(_run_manual_search_task(scope))


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
async def api_arr_search_now(body: ArrSearchNowIn) -> JSONResponse:
    """Queue one-shot missing/upgrade Arr search and return immediately."""
    enqueue_manual_arr_search(body.scope)
    return JSONResponse({"ok": True, "queued": True, "message": "Manual search queued."})


@router.get("/api/dashboard/status", dependencies=AUTH_DEPS)
async def api_dashboard_status(
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    settings = await _get_or_create_settings(session)
    tz = settings.timezone or "UTC"
    return JSONResponse(await build_dashboard_status(session, tz))
