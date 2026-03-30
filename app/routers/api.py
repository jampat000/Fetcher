from __future__ import annotations

import asyncio
import logging
from typing import cast

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
from app.service_logic import (
    ArrManualScope,
    _arr_search_no_dispatch_messages,
    _arr_search_partial_dispatch_extra,
    _build_run_context,
    _paginate_wanted_for_search,
    _radarr_select_monitored_missing_with_cooldown,
    _sonarr_select_monitored_missing_with_cooldown,
    run_once,
)
from app.setup_helpers import test_emby_connection, test_radarr_connection, test_sonarr_connection
from app.version_info import get_app_version
from app.dashboard_service import build_dashboard_status


def _manual_search_scope_phrase(scope: str) -> tuple[str, str]:
    """Return (app display name, search flavor) for API responses."""
    mapping = {
        "sonarr_missing": ("Sonarr", "missing"),
        "sonarr_upgrade": ("Sonarr", "upgrade"),
        "radarr_missing": ("Radarr", "missing"),
        "radarr_upgrade": ("Radarr", "upgrade"),
    }
    return mapping.get(scope, ("Arr", "search"))

from app.routers.deps import AUTH_API_DEPS

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
    manual_scope: ArrManualScope | None = (
        cast(ArrManualScope, scope)
        if scope in ("sonarr_missing", "sonarr_upgrade", "radarr_missing", "radarr_upgrade")
        else None
    )
    if scope.startswith("sonarr_"):
        son_key = resolve_sonarr_api_key(settings)
        if not (settings.sonarr_enabled and settings.sonarr_url and son_key):
            raise RuntimeError("Sonarr is not enabled or missing URL/API key.")
        client = ArrClient(ArrConfig(settings.sonarr_url, son_key))
        ids: list[int] = []
        selected_records: list[dict] = []
        pool_total = 0
        try:
            await client.health()
            ctx = _build_run_context(settings, arr_manual_scope=manual_scope)
            sonarr_limit = max(1, int((settings.sonarr_max_items_per_run or 0) or 50))
            if scope == "sonarr_missing":
                ids, selected_records, pool_total = await _sonarr_select_monitored_missing_with_cooldown(
                    client,
                    session,
                    limit=sonarr_limit,
                    cooldown_minutes=ctx.sonarr_retry_delay_minutes,
                    now=ctx.now,
                )
            else:
                ids, selected_records, pool_total = await _paginate_wanted_for_search(
                    client,
                    session,
                    kind="cutoff",
                    id_keys=("id", "episodeId"),
                    item_type="episode",
                    app="sonarr",
                    action="upgrade",
                    limit=sonarr_limit,
                    cooldown_minutes=ctx.sonarr_retry_delay_minutes,
                    now=ctx.now,
                )
            if ids:
                if scope == "sonarr_missing":
                    await trigger_sonarr_missing_search(client, episode_ids=ids)
                else:
                    await trigger_sonarr_cutoff_search(client, episode_ids=ids)
        finally:
            await client.aclose()
        if ids:
            base_detail = _manual_detail_text(scope, selected_records) or (
                f"Manual {kind} search: command triggered immediately."
            )
            mode_partial = "missing" if scope == "sonarr_missing" else "upgrade"
            partial = _arr_search_partial_dispatch_extra(
                app_label="Sonarr",
                mode=mode_partial,
                started=len(ids),
                pool_total=pool_total,
                per_run_limit=sonarr_limit,
                item_label="episode(s)",
                manual_context=True,
            )
            detail = f"{partial[0]}\n\n{base_detail}" if partial else base_detail
        else:
            mode = "missing" if scope == "sonarr_missing" else "upgrade"
            reason = "retry_delay_all" if pool_total > 0 else "empty_pool"
            detail, _ = _arr_search_no_dispatch_messages(
                app_label="Sonarr",
                mode=mode,
                item_singular="episode",
                item_plural="episodes",
                pool_total=int(pool_total or 0),
                per_run_limit=sonarr_limit,
                reason=reason,
                manual_context=True,
            )
        session.add(
            ActivityLog(
                app="sonarr",
                kind=kind,
                count=len(ids),
                detail=detail,
            )
        )
        await session.commit()
        return

    rad_key = resolve_radarr_api_key(settings)
    if not (settings.radarr_enabled and settings.radarr_url and rad_key):
        raise RuntimeError("Radarr is not enabled or missing URL/API key.")
    client = ArrClient(ArrConfig(settings.radarr_url, rad_key))
    ids_r: list[int] = []
    selected_records_r: list[dict] = []
    pool_total_r = 0
    try:
        await client.health()
        ctx = _build_run_context(settings, arr_manual_scope=manual_scope)
        radarr_limit = max(1, int((settings.radarr_max_items_per_run or 0) or 50))
        if scope == "radarr_missing":
            ids_r, selected_records_r, pool_total_r = await _radarr_select_monitored_missing_with_cooldown(
                client,
                session,
                limit=radarr_limit,
                cooldown_minutes=ctx.radarr_retry_delay_minutes,
                now=ctx.now,
            )
        else:
            ids_r, selected_records_r, pool_total_r = await _paginate_wanted_for_search(
                client,
                session,
                kind="cutoff",
                id_keys=("id", "movieId"),
                item_type="movie",
                app="radarr",
                action="upgrade",
                limit=radarr_limit,
                cooldown_minutes=ctx.radarr_retry_delay_minutes,
                now=ctx.now,
            )
        if ids_r:
            if scope == "radarr_missing":
                await trigger_radarr_missing_search(client, movie_ids=ids_r)
            else:
                await trigger_radarr_cutoff_search(client, movie_ids=ids_r)
    finally:
        await client.aclose()
    if ids_r:
        base_detail_r = _manual_detail_text(scope, selected_records_r) or (
            f"Manual {kind} search: command triggered immediately."
        )
        mode_partial_r = "missing" if scope == "radarr_missing" else "upgrade"
        partial_r = _arr_search_partial_dispatch_extra(
            app_label="Radarr",
            mode=mode_partial_r,
            started=len(ids_r),
            pool_total=pool_total_r,
            per_run_limit=radarr_limit,
            item_label="movie(s)",
            manual_context=True,
        )
        detail_r = f"{partial_r[0]}\n\n{base_detail_r}" if partial_r else base_detail_r
    else:
        mode_r = "missing" if scope == "radarr_missing" else "upgrade"
        reason_r = "retry_delay_all" if pool_total_r > 0 else "empty_pool"
        detail_r, _ = _arr_search_no_dispatch_messages(
            app_label="Radarr",
            mode=mode_r,
            item_singular="movie",
            item_plural="movies",
            pool_total=int(pool_total_r or 0),
            per_run_limit=radarr_limit,
            reason=reason_r,
            manual_context=True,
        )
    session.add(
        ActivityLog(
            app="radarr",
            kind=kind,
            count=len(ids_r),
            detail=detail_r,
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


@router.post("/api/setup/test-sonarr", dependencies=AUTH_API_DEPS)
async def api_setup_test_sonarr(body: SetupConnTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "sonarr")
    ok, msg = await test_sonarr_connection(body.url, key)
    return JSONResponse({"ok": ok, "message": msg})


@router.post("/api/setup/test-radarr", dependencies=AUTH_API_DEPS)
async def api_setup_test_radarr(body: SetupConnTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "radarr")
    ok, msg = await test_radarr_connection(body.url, key)
    return JSONResponse({"ok": ok, "message": msg})


@router.post("/api/setup/test-emby", dependencies=AUTH_API_DEPS)
async def api_setup_test_emby(body: SetupEmbyTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "emby")
    ok, msg = await test_emby_connection(body.url, key, body.user_id)
    return JSONResponse({"ok": ok, "message": msg})


@router.post("/api/arr/search-now", dependencies=AUTH_API_DEPS)
async def api_arr_search_now(body: ArrSearchNowIn, session: AsyncSession = Depends(get_session)) -> JSONResponse:
    """Trigger Arr search command immediately for manual action."""
    try:
        await trigger_manual_arr_search_now(body.scope, session)
        scope_app, flavor = _manual_search_scope_phrase(body.scope)
        return JSONResponse(
            {
                "ok": True,
                "queued": False,
                "message": f"Manual {flavor} search sent to {scope_app} successfully.",
            }
        )
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
        scope_app, flavor = _manual_search_scope_phrase(body.scope)
        return JSONResponse(
            {
                "ok": True,
                "queued": True,
                "message": (
                    f"{scope_app} rejected the immediate manual {flavor} search; Fetcher queued a full automation "
                    "pass instead. Check Activity in a moment."
                ),
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
        scope_app, flavor = _manual_search_scope_phrase(body.scope)
        return JSONResponse(
            {
                "ok": False,
                "queued": False,
                "message": (
                    f"Could not run manual {flavor} search for {scope_app} ({type(e).__name__}). "
                    f"Confirm URL and API key in Fetcher settings, then try again. Details: {e}"
                ),
            }
        )


@router.get("/api/dashboard/status", dependencies=AUTH_API_DEPS)
async def api_dashboard_status(
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    settings = await _get_or_create_settings(session)
    tz = settings.timezone or "UTC"
    return JSONResponse(await build_dashboard_status(session, tz))
