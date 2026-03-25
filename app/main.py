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
from app.db import db_path, engine
from app.httpx_shared import aclose_shared_httpx_client, init_shared_httpx_client
from app.log_sanitize import configure_fetcher_logging
from app.migrations import migrate
from app.models import Base
from app.paths import STATIC_DIR
from app.rate_limit import limiter
from app.scheduler import scheduler
from app.security_utils import get_jwt_secret_from_env
from app.web_common import trimmer_settings_redirect_url
from app import updates as app_updates
from app.routers import api as api_router
from app.routers import auth as auth_router
from app.routers import dashboard as dashboard_router
from app.routers import settings as settings_router
from app.routers import setup as setup_router
from app.routers import trimmer as trimmer_router

configure_fetcher_logging()

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    configure_fetcher_logging()
    jwt_secret = (get_jwt_secret_from_env() or "").strip()
    if not jwt_secret:
        raise RuntimeError(
            "Missing required JWT configuration: set FETCHER_JWT_SECRET to a stable, high-entropy value."
        )
    _app.state.jwt_secret = jwt_secret
    logger.info("SQLite database path: %s", db_path())
    # When the Windows service holds fetcher.db, startup can block until SQLite times out — retry a few times.
    delays_sec = (0, 2, 5, 10, 15)
    last_err: BaseException | None = None
    for attempt, delay in enumerate(delays_sec):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await migrate(engine)
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
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="invalid", section=sec),
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
app.include_router(trimmer_router.router)
