from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler
from sqlalchemy.exc import SQLAlchemyError

from app.auth import FetcherAuthRequired, bootstrap_auth_on_startup
from app.branding import APP_NAME
from app.database_resolution import get_last_database_resolution, log_database_resolution_startup
from app.database_startup import (
    run_schema_upgrade_phase,
    verify_sqlite_engine_matches_canonical_path,
)
from app.db import db_path, engine
from app.httpx_shared import aclose_shared_httpx_client, init_shared_httpx_client
from app.log_sanitize import configure_fetcher_logging
from app.models import Base
from app.schema_validation import (
    validate_app_settings_schema_version,
    validate_refiner_app_settings_schema,
)
from app.paths import STATIC_DIR, resolved_logs_dir
from app.rate_limit import limiter
from app.refiner_service import reconcile_refiner_processing_rows_on_worker_boot
from app.scheduler import scheduler
from app.security_utils import resolve_fetcher_jwt_secret_at_startup, warn_if_data_encryption_key_missing
from app.web_common import refiner_settings_redirect_url, trimmer_settings_redirect_url
from app import updates as app_updates
from app.routers import api as api_router
from app.routers import auth as auth_router
from app.routers import dashboard as dashboard_router
from app.routers import settings as settings_router
from app.routers import setup as setup_router
from app.routers import refiner as refiner_router
from app.routers import trimmer as trimmer_router

configure_fetcher_logging()

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    configure_fetcher_logging()
    jwt_secret = resolve_fetcher_jwt_secret_at_startup(logger=logger)
    _app.state.jwt_secret = jwt_secret
    warn_if_data_encryption_key_missing(logger)
    _ = db_path()
    res = get_last_database_resolution()
    if res is not None:
        log_database_resolution_startup(res)
    logger.info(
        "Application log file: %s (override with FETCHER_LOG_DIR)",
        resolved_logs_dir() / "fetcher.log",
    )
    # When the Windows service holds fetcher.db, startup can block until SQLite times out — retry a few times.
    delays_sec = (0, 2, 5, 10, 15)
    last_err: BaseException | None = None
    for attempt, delay in enumerate(delays_sec):
        if delay:
            await asyncio.sleep(delay)
        try:
            verify_sqlite_engine_matches_canonical_path(
                engine, canonical_db_file=db_path(), log=logger
            )
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await run_schema_upgrade_phase(engine, log=logger)
            logger.info("Startup: Refiner worker reconcile (processing rows)")
            await reconcile_refiner_processing_rows_on_worker_boot()
            await validate_refiner_app_settings_schema(engine)
            last_err = None
            break
        except SQLAlchemyError as e:
            last_err = e
            logger.warning(
                "Database setup blocked (attempt %s/%s): %s",
                attempt + 1,
                len(delays_sec),
                e,
            )
    if last_err is not None:
        logger.error(
            "Fetcher could not finish database setup. If the Windows service is running, run "
            "scripts/dev-start.ps1 (uses FETCHER_DEV_DB_PATH / %%TEMP%%\\fetcher-dev.sqlite3 by default) "
            "or stop the service. DB path: %s",
            db_path(),
        )
        raise last_err

    await bootstrap_auth_on_startup()
    await validate_app_settings_schema_version(engine)
    logger.info("Startup: database schema ready — continuing (auth, HTTP client, scheduler)")
    await init_shared_httpx_client()
    # Packaged-build CI smoke test: skip background scheduler so /healthz is reachable quickly
    # (first scheduler tick can otherwise block startup on Arr/Emby HTTP before the server listens).
    _ci_smoke = (os.environ.get("FETCHER_CI_SMOKE") or "").strip().lower() in ("1", "true", "yes")
    if _ci_smoke:
        logger.warning("FETCHER_CI_SMOKE set — background scheduler not started (CI / smoke test only)")
    else:
        await scheduler.start()
    yield
    try:
        scheduler.shutdown(wait=False)
        logger.info("Background scheduler shut down successfully.")
    except Exception:
        logger.exception("Background scheduler shutdown failed.")
    await aclose_shared_httpx_client()


app = FastAPI(title=APP_NAME, lifespan=_lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)


@app.middleware("http")
async def _security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


@app.exception_handler(FetcherAuthRequired)
async def _fetcher_auth_redirect_handler(_request: Request, exc: FetcherAuthRequired) -> Response:
    """Depends(require_auth) cannot return RedirectResponse — FastAPI would ignore it."""
    return exc.response


@app.exception_handler(RequestValidationError)
async def _form_validation_redirect(request: Request, exc: RequestValidationError) -> Response:
    """Browser form posts expect a redirect/HTML — avoid a raw 422 JSON body ('page isn't working')."""
    if request.method == "POST" and request.url.path == "/settings":
        tab_q = "global"
        try:
            form = await request.form()
            raw = form.get("save_scope")
            if isinstance(raw, list):
                raw = raw[0] if raw else ""
            s = (str(raw) if raw is not None else "").strip().lower()
            if s in ("global", "sonarr", "radarr"):
                tab_q = s
        except Exception:
            pass
        return RedirectResponse(f"/settings?save=fail&reason=invalid&tab={tab_q}", status_code=303)
    if request.method == "POST" and request.url.path == "/trimmer/settings/cleaner":
        sec = (request.query_params.get("trimmer_section") or "").strip().lower()
        if sec not in ("connection", "schedule", "rules", "people"):
            sec = None
        ui_sec = sec or "schedule"
        raw_ss = (request.query_params.get("trimmer_save_scope") or "").strip().lower()
        if not raw_ss:
            try:
                form = await request.form()
                raw = form.get("save_scope")
                if isinstance(raw, list):
                    raw = raw[0] if raw else ""
                raw_ss = (str(raw) if raw is not None else "").strip().lower()
            except Exception:
                raw_ss = ""
        if (request.headers.get("x-fetcher-trimmer-settings-async") or "").strip() == "1":
            return JSONResponse(
                {"ok": False, "reason": "invalid", "section": ui_sec, "save_scope": raw_ss}
            )
        return RedirectResponse(
            trimmer_settings_redirect_url(
                saved=False, reason="invalid", section=sec, save_scope=raw_ss or None
            ),
            status_code=303,
        )
    if request.method == "POST" and request.url.path == "/refiner/settings/save":
        rs = (request.query_params.get("refiner_section") or "").strip().lower()
        rsec = rs if rs in ("processing", "folders", "audio", "subtitles", "schedule") else None
        ui_sec = rsec or "processing"
        if (request.headers.get("x-fetcher-refiner-settings-async") or "").strip() == "1":
            return JSONResponse({"ok": False, "reason": "invalid", "section": ui_sec})
        return RedirectResponse(
            refiner_settings_redirect_url(saved=False, reason="invalid", section=rsec),
            status_code=303,
        )
    if request.method == "POST" and request.url.path.startswith("/settings/auth"):
        return RedirectResponse("/settings?save=fail&reason=invalid&tab=security", status_code=303)
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(app_updates.router)
app.include_router(api_router.router)
app.include_router(auth_router.router)
app.include_router(setup_router.router)
app.include_router(dashboard_router.router)
app.include_router(settings_router.router)
app.include_router(refiner_router.router)
app.include_router(trimmer_router.router)
