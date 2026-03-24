from __future__ import annotations

import asyncio
import logging

import httpx
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
from app.db import SessionLocal, _get_or_create_settings, get_session
from app.models import ActivityLog
from app.resolvers.api_keys import resolve_setup_api_key
from app.resolvers.api_keys import resolve_radarr_api_key, resolve_sonarr_api_key
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
            logger.exception("Queued manual Arr search task failed for scope=%s", scope)


def enqueue_manual_arr_search(scope: str) -> None:
    asyncio.create_task(_run_manual_search_task(scope))


def _manual_detail_text(scope: str, records: list[dict]) -> str:
    lines: list[str] = []
    if scope.startswith("sonarr_"):
        for r in records:
            series = str(r.get("seriesTitle") or "").strip()
            season = r.get("seasonNumber")
            episode = r.get("episodeNumber")
            title = str(r.get("title") or "").strip()
            se = ""
            if season is not None and episode is not None:
                try:
                    se = f"S{int(season):02d}E{int(episode):02d}"
                except (TypeError, ValueError):
                    se = ""
            if series and se and title:
                lines.append(f"{series} {se} - {title}")
            elif series and se:
                lines.append(f"{series} {se}")
            elif series and title:
                lines.append(f"{series} - {title}")
            elif title:
                lines.append(title)
    else:
        for r in records:
            title = str(r.get("title") or "").strip()
            year = r.get("year")
            if title and year not in (None, ""):
                lines.append(f"{title} ({year})")
            elif title:
                lines.append(title)
    return "\n".join(line for line in lines if line)


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
            page = (
                await client.wanted_missing(page=1, page_size=50)
                if scope == "sonarr_missing"
                else await client.wanted_cutoff_unmet(page=1, page_size=50)
            )
            records = page.get("records") if isinstance(page, dict) else []
            records = records if isinstance(records, list) else []
            total = page.get("totalRecords") if isinstance(page, dict) else 0
            try:
                total_count = int(total or 0)
            except (TypeError, ValueError):
                total_count = len(records)
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
                count=max(0, total_count),
                detail=_manual_detail_text(scope, records)
                or f"Manual {kind} search: command triggered immediately.",
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
        page = (
            await client.wanted_missing(page=1, page_size=50)
            if scope == "radarr_missing"
            else await client.wanted_cutoff_unmet(page=1, page_size=50)
        )
        records = page.get("records") if isinstance(page, dict) else []
        records = records if isinstance(records, list) else []
        total = page.get("totalRecords") if isinstance(page, dict) else 0
        try:
            total_count = int(total or 0)
        except (TypeError, ValueError):
            total_count = len(records)
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
            count=max(0, total_count),
            detail=_manual_detail_text(scope, records)
            or f"Manual {kind} search: command triggered immediately.",
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
    except httpx.HTTPStatusError as e:
        # Arr can occasionally return 5xx for immediate command submission. Fall back to queued
        # orchestration so user action still succeeds without another click.
        logger.warning("Immediate manual Arr command failed for scope=%s: %s", body.scope, e)
        enqueue_manual_arr_search(body.scope)
        session.add(
            ActivityLog(
                app="sonarr" if body.scope.startswith("sonarr_") else "radarr",
                kind="error",
                status="failed",
                count=0,
                detail=f"Immediate manual command failed and was queued for retry: {type(e).__name__}: {e}",
            )
        )
        await session.commit()
        return JSONResponse(
            {
                "ok": True,
                "queued": True,
                "message": "Manual search queued (immediate Arr command failed).",
            }
        )
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
