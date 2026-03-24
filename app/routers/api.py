from __future__ import annotations

import logging

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import (
    ArrClient,
    ArrConfig,
    trigger_radarr_cutoff_search,
    trigger_radarr_missing_search,
    trigger_sonarr_cutoff_search,
    trigger_sonarr_missing_search,
)
from app.branding import APP_NAME
from app.db import _get_or_create_settings, get_session
from app.models import ActivityLog
from app.resolvers.api_keys import resolve_setup_api_key
from app.resolvers.api_keys import resolve_radarr_api_key, resolve_sonarr_api_key
from app.schemas import ArrSearchNowIn, SetupConnTestIn, SetupEmbyTestIn
from app.setup_helpers import test_emby_connection, test_radarr_connection, test_sonarr_connection
from app.version_info import get_app_version
from app.web_common import build_dashboard_status

from app.routers.deps import AUTH_DEPS

router = APIRouter()
logger = logging.getLogger(__name__)


async def trigger_manual_arr_search_now(scope: str, session: AsyncSession) -> None:
    """Dispatch the Arr search command immediately for manual button clicks."""
    settings = await _get_or_create_settings(session)
    kind = "missing" if scope.endswith("_missing") else "upgrade"
    if scope.startswith("sonarr_"):
        son_key = resolve_sonarr_api_key(settings)
        if not (settings.sonarr_enabled and settings.sonarr_url and son_key):
            raise RuntimeError("Sonarr is not enabled or missing URL/API key.")
        client = ArrClient(ArrConfig(settings.sonarr_url, son_key))
        try:
            await client.health()
            if scope == "sonarr_missing":
                await trigger_sonarr_missing_search(client)
            else:
                await trigger_sonarr_cutoff_search(client)
        finally:
            await client.aclose()
        session.add(
            ActivityLog(
                app="sonarr",
                kind=kind,
                count=0,
                detail=f"Manual {kind} search: command triggered immediately.",
            )
        )
        await session.commit()
        return

    rad_key = resolve_radarr_api_key(settings)
    if not (settings.radarr_enabled and settings.radarr_url and rad_key):
        raise RuntimeError("Radarr is not enabled or missing URL/API key.")
    client = ArrClient(ArrConfig(settings.radarr_url, rad_key))
    try:
        await client.health()
        if scope == "radarr_missing":
            await trigger_radarr_missing_search(client)
        else:
            await trigger_radarr_cutoff_search(client)
    finally:
        await client.aclose()
    session.add(
        ActivityLog(
            app="radarr",
            kind=kind,
            count=0,
            detail=f"Manual {kind} search: command triggered immediately.",
        )
    )
    await session.commit()


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
    """Trigger Arr search command immediately for manual action."""
    try:
        await trigger_manual_arr_search_now(body.scope, session)
        return JSONResponse({"ok": True, "queued": False, "message": "Manual search triggered."})
    except Exception as e:  # noqa: BLE001 - API boundary
        logger.warning("Manual Arr search failed for scope=%s: %s", body.scope, e)
        session.add(
            ActivityLog(
                app="sonarr" if body.scope.startswith("sonarr_") else "radarr",
                kind="error",
                status="failed",
                count=0,
                detail=f"Manual search failed: {type(e).__name__}: {e}",
            )
        )
        await session.commit()
        return JSONResponse({"ok": False, "queued": False, "message": f"Manual search failed: {e}"})


@router.get("/api/dashboard/status", dependencies=AUTH_DEPS)
async def api_dashboard_status(
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    settings = await _get_or_create_settings(session)
    tz = settings.timezone or "UTC"
    return JSONResponse(await build_dashboard_status(session, tz))
