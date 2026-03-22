from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from fastapi import Depends, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import _get_or_create_settings, db_path, engine, fetch_latest_app_snapshots, get_session
from app.migrations import migrate
import httpx

from app.backup import export_json_bytes, import_settings_replace
from app.arr_client import ArrClient, ArrConfig
from app.constants import _MOVIE_GENRE_OPTIONS, _PEOPLE_CREDIT_OPTIONS, _TIMEZONE_CHOICES
from app.display_helpers import (
    _fmt_local,
    _now_local,
    _normalize_hhmm,
    _schedule_days_display,
    _schedule_time_range_friendly,
    _time_select_orphan,
    _to_12h,
    _truncate_display,
)
from app.form_helpers import (
    _looks_like_url,
    _normalize_base_url,
    _people_credit_types_csv_from_form,
    _resolve_timezone_name,
)
from app.emby_client import EmbyClient, EmbyConfig
from app.emby_rules import (
    evaluate_candidate,
    movie_matches_people,
    movie_matches_selected_genres,
    parse_genres_csv,
    parse_movie_people_credit_types_csv,
    parse_movie_people_phrases,
    tv_matches_selected_genres,
)
from app.models import ActivityLog, AppSettings, AppSnapshot, Base, JobRunLog
from app.schemas import ArrSearchNowIn, SetupConnTestIn, SetupEmbyTestIn, SettingsIn
from app.setup_helpers import test_emby_connection, test_radarr_connection, test_sonarr_connection
from app.scheduler import ServiceScheduler
from app.schedule import DAY_NAMES, normalize_schedule_days_csv, schedule_time_dropdown_choices
from app.service_logic import apply_emby_trimmer_live_deletes, run_once
from app.time_util import utc_now_naive
from app import updates as app_updates
from app.version_info import get_app_version
from app.log_sanitize import configure_fetcher_logging
from app.resolvers.api_keys import (
    resolve_emby_api_key,
    resolve_radarr_api_key,
    resolve_setup_api_key,
    resolve_sonarr_api_key,
)
from app.auth import (
    FetcherAuthRequired,
    INVALID_LOGIN_MESSAGE,
    TOO_MANY_ATTEMPTS_MESSAGE,
    attach_session_cookie,
    bootstrap_auth_on_startup,
    clear_login_failures,
    clear_session_cookie,
    get_client_ip,
    get_csrf_token_for_template,
    hash_password,
    login_rate_limited,
    normalize_auth_ip_allowlist_input,
    record_login_failure,
    request_prefers_json,
    require_auth,
    require_csrf,
    sanitize_next_param,
    verify_password,
)

_AUTH_DEPS = [Depends(require_auth)]
_AUTH_FORM_DEPS = [Depends(require_auth), Depends(require_csrf)]

configure_fetcher_logging()

APP_NAME = "Fetcher"
APP_TAGLINE = "Never miss a release."

logger = logging.getLogger(__name__)

scheduler = ServiceScheduler()

# Activity list shows 5 title lines + “+N more”; full list is stored in ``ActivityLog.detail``.
ACTIVITY_DETAIL_PREVIEW_LINES = 5
_ACTIVITY_LOG_LEGACY_MORE = re.compile(r"^\+\d+ more$")


def _activity_log_title_lines(detail: str) -> list[str]:
    """Split stored detail into display lines; drop legacy synthetic ``+N more`` rows."""
    lines: list[str] = []
    for raw in (detail or "").splitlines():
        s = raw.strip()
        if not s or _ACTIVITY_LOG_LEGACY_MORE.match(s):
            continue
        lines.append(s)
    return lines


def _activity_display_row(e: ActivityLog, tz: str) -> dict[str, Any]:
    raw_detail = (getattr(e, "detail", "") or "").strip()
    return {
        "id": e.id,
        "time_local": _fmt_local(e.created_at, tz),
        "app": e.app,
        "kind": e.kind,
        "status": (getattr(e, "status", "") or "ok").strip().lower(),
        "count": e.count,
        "detail_lines": _activity_log_title_lines(raw_detail),
    }


def _settings_looks_like_existing_fetcher_install(settings: AppSettings) -> bool:
    """True when Sonarr/Radarr/Emby were already configured — tailors setup step 0 for upgrades."""
    return bool(
        (settings.sonarr_url or "").strip()
        or (settings.radarr_url or "").strip()
        or (settings.emby_url or "").strip()
        or settings.sonarr_enabled
        or settings.radarr_enabled
        or settings.emby_enabled
    )


async def _try_commit_and_reschedule(session: AsyncSession) -> bool:
    """Persist settings and refresh scheduler tick. False if SQLite could not commit (e.g. DB locked)."""
    try:
        await session.commit()
    except SQLAlchemyError:
        try:
            await session.rollback()
        except Exception:
            logger.exception("rollback after failed settings commit")
        return False
    try:
        await scheduler.reschedule()
    except Exception:
        logger.warning("scheduler.reschedule failed after settings commit", exc_info=True)
    return True


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    configure_fetcher_logging()
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
    # Packaged-build CI smoke test: skip background scheduler so /healthz is reachable quickly
    # (first scheduler tick can otherwise block startup on Arr/Emby HTTP before the server listens).
    _ci_smoke = (os.environ.get("FETCHER_CI_SMOKE") or "").strip().lower() in ("1", "true", "yes")
    if _ci_smoke:
        logger.warning("FETCHER_CI_SMOKE set — background scheduler not started (CI / smoke test only)")
    else:
        await scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title=APP_NAME, lifespan=_lifespan)


@app.exception_handler(FetcherAuthRequired)
async def _fetcher_auth_redirect_handler(_request: Request, exc: FetcherAuthRequired) -> Response:
    """Depends(require_auth) cannot return RedirectResponse — FastAPI would ignore it."""
    return exc.response


@app.exception_handler(RequestValidationError)
async def _form_validation_redirect(request: Request, exc: RequestValidationError) -> Response:
    """Browser form posts expect a redirect/HTML — avoid a raw 422 JSON body ('page isn't working')."""
    if request.method == "POST" and request.url.path == "/settings":
        return RedirectResponse("/settings?save=fail&reason=invalid", status_code=303)
    if request.method == "POST" and request.url.path == "/trimmer/settings/cleaner":
        return RedirectResponse("/trimmer/settings?save=fail&reason=invalid", status_code=303)
    if request.method == "POST" and request.url.path.startswith("/settings/auth"):
        return RedirectResponse("/settings?save=fail&reason=invalid", status_code=303)
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.globals["app_version"] = get_app_version()

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.include_router(app_updates.router)


@app.get("/login", response_class=HTMLResponse, response_model=None)
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


@app.post("/login", response_model=None)
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


@app.get("/logout", response_class=RedirectResponse)
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


def _movie_credit_types_summary(types: frozenset[str]) -> str:
    short = {
        "actor": "Cast",
        "director": "Director",
        "writer": "Writer",
        "producer": "Producer",
        "gueststar": "Guest",
    }
    order = ("actor", "director", "writer", "producer", "gueststar")
    parts = [short[k] for k in order if k in types]
    return "+".join(parts) if parts else "Cast"


def _schedule_days_csv_from_named_day_checks(
    mon: int,
    tue: int,
    wed: int,
    thu: int,
    fri: int,
    sat: int,
    sun: int,
) -> str:
    """One checkbox per day (`name=prefix_Mon` value=1). Uncheck all → store "" (not full week)."""
    flags = (mon, tue, wed, thu, fri, sat, sun)
    parts = [DAY_NAMES[i] for i, v in enumerate(flags) if int(v or 0) != 0]
    if not parts:
        return ""
    return normalize_schedule_days_csv(",".join(parts))


def _schedule_weekdays_selected_dict(days_csv: str) -> dict[str, bool]:
    """Per-day flags from DB column (raw). Empty stored value → all False."""
    n = normalize_schedule_days_csv((days_csv or "").strip())
    if not n.strip():
        return {d: False for d in DAY_NAMES}
    allowed = {p.strip() for p in n.split(",") if p.strip() in DAY_NAMES}
    return {d: (d in allowed) for d in DAY_NAMES}


def _effective_emby_rules(settings: AppSettings) -> dict[str, int | bool]:
    global_rating = max(0, int(settings.emby_rule_watched_rating_below or 0))
    global_unwatched = max(0, int(settings.emby_rule_unwatched_days or 0))

    movie_rating = max(0, int(settings.emby_rule_movie_watched_rating_below or 0)) or global_rating
    movie_unwatched = max(0, int(settings.emby_rule_movie_unwatched_days or 0)) or global_unwatched
    tv_delete_watched = bool(settings.emby_rule_tv_delete_watched)
    tv_unwatched = max(0, int(settings.emby_rule_tv_unwatched_days or 0)) or global_unwatched

    return {
        "movie_rating_below": movie_rating,
        "movie_unwatched_days": movie_unwatched,
        "tv_delete_watched": tv_delete_watched,
        "tv_unwatched_days": tv_unwatched,
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness for monitors (incl. packaged build smoke tests)."""
    return {
        "status": "ok",
        "app": APP_NAME,
        "version": get_app_version(),
    }


@app.get("/api/version")
async def api_version() -> dict[str, str]:
    """Lightweight version endpoint for automation / dashboards."""
    return {"app": APP_NAME, "version": get_app_version()}


@app.post("/api/setup/test-sonarr", dependencies=_AUTH_DEPS)
async def api_setup_test_sonarr(body: SetupConnTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "sonarr")
    ok, msg = await test_sonarr_connection(body.url, key)
    return JSONResponse({"ok": ok, "message": msg})


@app.post("/api/setup/test-radarr", dependencies=_AUTH_DEPS)
async def api_setup_test_radarr(body: SetupConnTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "radarr")
    ok, msg = await test_radarr_connection(body.url, key)
    return JSONResponse({"ok": ok, "message": msg})


@app.post("/api/setup/test-emby", dependencies=_AUTH_DEPS)
async def api_setup_test_emby(body: SetupEmbyTestIn) -> JSONResponse:
    key = resolve_setup_api_key(body.api_key, "emby")
    ok, msg = await test_emby_connection(body.url, key, body.user_id)
    return JSONResponse({"ok": ok, "message": msg})


@app.post("/api/arr/search-now", dependencies=_AUTH_DEPS)
async def api_arr_search_now(body: ArrSearchNowIn, session: AsyncSession = Depends(get_session)) -> JSONResponse:
    """One-shot missing or upgrade search for Sonarr (TV) or Radarr (movies); bypasses schedule + run-interval gates."""
    result = await run_once(session, arr_manual_scope=body.scope)
    return JSONResponse({"ok": result.ok, "message": result.message})


@app.get("/setup", response_class=RedirectResponse)
async def setup_wizard_entry(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    if not (settings.auth_password_hash or "").strip():
        return RedirectResponse("/setup/0", status_code=302)
    return RedirectResponse("/setup/1", status_code=302)


# Total wizard screens (0 .. _WIZARD_LAST_STEP_INDEX inclusive). Last index is the "done" page.
_SETUP_WIZARD_STEPS = 6
_WIZARD_LAST_STEP_INDEX = _SETUP_WIZARD_STEPS - 1


def _setup_wizard_step_title(step: int) -> str:
    return {
        0: "Account",
        1: "Sonarr",
        2: "Radarr",
        3: "Emby",
        4: "Schedule & timezone",
        5: "What's next",
    }.get(step, "Setup")


@app.get("/setup/{step}", response_class=HTMLResponse, response_model=None)
async def setup_wizard_page(
    step: int, request: Request, session: AsyncSession = Depends(get_session)
) -> HTMLResponse | RedirectResponse:
    settings = await _get_or_create_settings(session)
    if not (settings.auth_password_hash or "").strip():
        if step != 0:
            return RedirectResponse("/setup/0", status_code=302)
    elif step == 0:
        return RedirectResponse("/setup/1", status_code=302)

    if step < 0 or step > _WIZARD_LAST_STEP_INDEX:
        if not (settings.auth_password_hash or "").strip():
            return RedirectResponse("/setup/0", status_code=302)
        return RedirectResponse("/setup/1", status_code=302)

    tz = settings.timezone or "UTC"
    setup_error = (request.query_params.get("error") or "").strip()
    setup_save_fail = (request.query_params.get("save") or "").strip().lower() == "fail"
    if step == 0:
        setup_account_intro = (
            "upgrade" if _settings_looks_like_existing_fetcher_install(settings) else "new"
        )
    else:
        setup_account_intro = ""
    return templates.TemplateResponse(
        request,
        "setup_wizard.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Setup (step {step + 1} of {_SETUP_WIZARD_STEPS})",
            "subtitle": "Connect your apps",
            "settings": settings,
            "step": step,
            "setup_steps_total": _SETUP_WIZARD_STEPS,
            "step_title": _setup_wizard_step_title(step),
            "setup_step_labels": ["Account", "Sonarr", "Radarr", "Emby", "Schedule", "Next steps"],
            "timezone_choices": _TIMEZONE_CHOICES,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "setup_error": setup_error,
            "setup_save_fail": setup_save_fail,
            "setup_account_intro": setup_account_intro,
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )


@app.post("/setup/{step}", dependencies=[Depends(require_csrf)])
async def setup_wizard_save(
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
) -> RedirectResponse:
    row0 = await _get_or_create_settings(session)
    if not (row0.auth_password_hash or "").strip():
        if step != 0:
            return RedirectResponse("/setup/0", status_code=303)
    else:
        if step == 0:
            return RedirectResponse("/setup/1", status_code=303)

    if step < 0 or step > _WIZARD_LAST_STEP_INDEX:
        if not (row0.auth_password_hash or "").strip():
            return RedirectResponse("/setup/0", status_code=303)
        return RedirectResponse("/setup/1", status_code=303)
    if step == _WIZARD_LAST_STEP_INDEX:
        return RedirectResponse("/?setup=complete", status_code=303)

    skip = (wizard_action or "").strip().lower() == "skip"
    if skip and step == 0:
        return RedirectResponse("/setup/0?error=account_required", status_code=303)

    if not skip:
        row = await _get_or_create_settings(session)
        if step == 0:
            u = (setup_auth_username or "admin").strip() or "admin"
            pw = (setup_auth_password or "").strip()
            if len(pw) < 8:
                return RedirectResponse("/setup/0?error=short_password", status_code=303)
            row.auth_username = u
            row.auth_password_hash = hash_password(pw)
            row.updated_at = utc_now_naive()
            if not await _try_commit_and_reschedule(session):
                return RedirectResponse("/setup/0?save=fail&reason=db_busy", status_code=303)
            secret = (row.auth_session_secret or "").strip()
            expected_user = (row.auth_username or "admin").strip() or "admin"
            resp = RedirectResponse("/setup/1", status_code=303)
            attach_session_cookie(resp, secret=secret, username=expected_user)
            return resp
        if step == 1:
            row.sonarr_enabled = sonarr_enabled
            row.sonarr_url = _normalize_base_url(sonarr_url)
            row.sonarr_api_key = (sonarr_api_key or "").strip()
        elif step == 2:
            row.radarr_enabled = radarr_enabled
            row.radarr_url = _normalize_base_url(radarr_url)
            row.radarr_api_key = (radarr_api_key or "").strip()
        elif step == 3:
            row.emby_enabled = emby_enabled
            row.emby_url = _normalize_base_url(emby_url)
            row.emby_api_key = (emby_api_key or "").strip()
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
        if not await _try_commit_and_reschedule(session):
            return RedirectResponse(f"/setup/{step}?save=fail&reason=db_busy", status_code=303)

    nxt = step + 1
    if nxt > _WIZARD_LAST_STEP_INDEX:
        return RedirectResponse("/?setup=complete", status_code=303)
    return RedirectResponse(f"/setup/{nxt}", status_code=303)


async def _build_dashboard_status(
    session: AsyncSession,
    tz: str,
    *,
    snapshots: dict[str, AppSnapshot | None] | None = None,
) -> dict[str, Any]:
    """Shared JSON payload for dashboard live polling and server-rendered page."""
    last_run = (
        (await session.execute(select(JobRunLog).order_by(desc(JobRunLog.id)).limit(1))).scalars().first()
    )
    last_run_display: dict[str, Any] | None = None
    if last_run:
        last_run_display = {
            "started_local": _fmt_local(last_run.started_at, tz),
            "finished_local": _fmt_local(last_run.finished_at, tz) if last_run.finished_at else "",
            "has_finished": last_run.finished_at is not None,
            "ok": bool(last_run.ok),
            "message": _truncate_display(last_run.message or ""),
        }
    next_tick = scheduler.next_fetcher_run_at()
    next_tick_local = _fmt_local(next_tick, tz) if next_tick else ""
    snaps = snapshots if snapshots is not None else await fetch_latest_app_snapshots(session)
    sonarr_snap = snaps.get("sonarr")
    radarr_snap = snaps.get("radarr")
    emby_snap = snaps.get("emby")
    return {
        "last_run": last_run_display,
        "next_scheduler_tick_local": next_tick_local,
        "sonarr_missing": int(sonarr_snap.missing_total) if sonarr_snap else 0,
        "sonarr_upgrades": int(sonarr_snap.cutoff_unmet_total) if sonarr_snap else 0,
        "radarr_missing": int(radarr_snap.missing_total) if radarr_snap else 0,
        "radarr_upgrades": int(radarr_snap.cutoff_unmet_total) if radarr_snap else 0,
        "emby_matched": int(emby_snap.missing_total) if emby_snap else 0,
    }


@app.get("/api/dashboard/status", dependencies=_AUTH_DEPS)
async def api_dashboard_status(
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    settings = await _get_or_create_settings(session)
    tz = settings.timezone or "UTC"
    return JSONResponse(await _build_dashboard_status(session, tz))


@app.get("/", response_class=HTMLResponse, dependencies=_AUTH_DEPS)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    activity = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(30)))
        .scalars().all()
    )
    tz = settings.timezone or "UTC"
    activity_display = [_activity_display_row(e, tz) for e in activity]
    suggest_setup_wizard = not (
        (settings.sonarr_url or "").strip()
        or (settings.radarr_url or "").strip()
        or (settings.emby_url or "").strip()
    )
    snapshots = await fetch_latest_app_snapshots(session)
    dash_status = await _build_dashboard_status(session, tz, snapshots=snapshots)
    last_run_display = dash_status["last_run"]
    next_tick_local = dash_status["next_scheduler_tick_local"]
    sonarr_snap = snapshots.get("sonarr")
    radarr_snap = snapshots.get("radarr")
    emby_snap = snapshots.get("emby")
    emby_schedule_start_display = _to_12h(settings.emby_schedule_start or "00:00", "12:00 AM")
    emby_schedule_end_display = _to_12h(settings.emby_schedule_end or "23:59", "11:59 PM")
    sonarr_schedule_start_display = _to_12h(settings.sonarr_schedule_start or "00:00", "12:00 AM")
    sonarr_schedule_end_display = _to_12h(settings.sonarr_schedule_end or "23:59", "11:59 PM")
    radarr_schedule_start_display = _to_12h(settings.radarr_schedule_start or "00:00", "12:00 AM")
    radarr_schedule_end_display = _to_12h(settings.radarr_schedule_end or "23:59", "11:59 PM")
    sonarr_schedule_days_display = _schedule_days_display(settings.sonarr_schedule_days or "")
    sonarr_schedule_time_friendly = _schedule_time_range_friendly(
        settings.sonarr_schedule_start or "00:00",
        settings.sonarr_schedule_end or "23:59",
    )
    radarr_schedule_days_display = _schedule_days_display(settings.radarr_schedule_days or "")
    radarr_schedule_time_friendly = _schedule_time_range_friendly(
        settings.radarr_schedule_start or "00:00",
        settings.radarr_schedule_end or "23:59",
    )
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Dashboard",
            "subtitle": "Status overview and counts",
            "settings": settings,
            "suggest_setup_wizard": suggest_setup_wizard,
            "last_run": last_run_display,
            "next_scheduler_tick_local": next_tick_local,
            "emby_schedule_start_display": emby_schedule_start_display,
            "emby_schedule_end_display": emby_schedule_end_display,
            "sonarr_schedule_start_display": sonarr_schedule_start_display,
            "sonarr_schedule_end_display": sonarr_schedule_end_display,
            "radarr_schedule_start_display": radarr_schedule_start_display,
            "radarr_schedule_end_display": radarr_schedule_end_display,
            "sonarr_schedule_days_display": sonarr_schedule_days_display,
            "sonarr_schedule_time_friendly": sonarr_schedule_time_friendly,
            "radarr_schedule_days_display": radarr_schedule_days_display,
            "radarr_schedule_time_friendly": radarr_schedule_time_friendly,
            "activity": activity_display,
            "activity_detail_preview": ACTIVITY_DETAIL_PREVIEW_LINES,
            "sonarr": sonarr_snap,
            "radarr": radarr_snap,
            "emby": emby_snap,
            "selected_movie_genres": sorted(parse_genres_csv(settings.emby_rule_movie_genres_csv)),
            "selected_tv_genres": sorted(parse_genres_csv(settings.emby_rule_tv_genres_csv)),
            "movie_people_phrases": parse_movie_people_phrases(settings.emby_rule_movie_people_csv),
            "movie_people_credit_types": parse_movie_people_credit_types_csv(
                settings.emby_rule_movie_people_credit_types_csv
            ),
            "movie_people_credit_summary": _movie_credit_types_summary(
                parse_movie_people_credit_types_csv(
                    settings.emby_rule_movie_people_credit_types_csv
                )
            ),
            "tv_people_phrases": parse_movie_people_phrases(settings.emby_rule_tv_people_csv),
            "tv_people_credit_summary": _movie_credit_types_summary(
                parse_movie_people_credit_types_csv(
                    settings.emby_rule_tv_people_credit_types_csv
                )
            ),
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )


@app.get("/logs", response_class=HTMLResponse, dependencies=_AUTH_DEPS)
async def logs_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    logs = (await session.execute(select(JobRunLog).order_by(desc(JobRunLog.id)).limit(200))).scalars().all()
    tz = settings.timezone or "UTC"
    logs_display = [
        {"started_local": _fmt_local(r.started_at, tz), "ok": r.ok, "message": r.message}
        for r in logs
    ]
    return templates.TemplateResponse(
        request,
        "logs.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Logs",
            "subtitle": "Service run history",
            "logs": logs_display,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )


@app.get("/activity", response_class=HTMLResponse, dependencies=_AUTH_DEPS)
async def activity_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    activity = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(200)))
        .scalars().all()
    )
    tz = settings.timezone or "UTC"
    activity_display = [_activity_display_row(e, tz) for e in activity]
    return templates.TemplateResponse(
        request,
        "activity.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Activity",
            "subtitle": "What Fetcher grabbed",
            "activity": activity_display,
            "activity_detail_preview": ACTIVITY_DETAIL_PREVIEW_LINES,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )


@app.get("/settings", response_class=HTMLResponse, dependencies=_AUTH_DEPS)
async def settings_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    snaps = await fetch_latest_app_snapshots(session)
    sonarr_snap = snaps.get("sonarr")
    radarr_snap = snaps.get("radarr")
    tz = settings.timezone or "UTC"
    time_choices = schedule_time_dropdown_choices(step_minutes=30)
    time_choice_keys = {v for v, _ in time_choices}
    sn_days = normalize_schedule_days_csv(settings.sonarr_schedule_days or "")
    rd_days = normalize_schedule_days_csv(settings.radarr_schedule_days or "")
    ss = _normalize_hhmm(settings.sonarr_schedule_start, "00:00")
    se = _normalize_hhmm(settings.sonarr_schedule_end, "23:59")
    rs = _normalize_hhmm(settings.radarr_schedule_start, "00:00")
    re = _normalize_hhmm(settings.radarr_schedule_end, "23:59")
    sec_notice = (request.query_params.get("sec") or "").strip()
    response = templates.TemplateResponse(
        request,
        "settings.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Fetcher settings",
            "subtitle": "Configure connections, schedules, and limits",
            "settings": settings,
            "sec_notice": sec_notice,
            "sonarr": sonarr_snap,
            "radarr": radarr_snap,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "timezones": _TIMEZONE_CHOICES,
            "schedule_time_choices": time_choices,
            "sonarr_schedule_days_normalized": sn_days,
            "radarr_schedule_days_normalized": rd_days,
            "sonarr_schedule_days_selected": _schedule_weekdays_selected_dict(
                settings.sonarr_schedule_days or ""
            ),
            "radarr_schedule_days_selected": _schedule_weekdays_selected_dict(
                settings.radarr_schedule_days or ""
            ),
            "sonarr_schedule_start_hhmm": ss,
            "sonarr_schedule_end_hhmm": se,
            "radarr_schedule_start_hhmm": rs,
            "radarr_schedule_end_hhmm": re,
            "sonarr_start_orphan": _time_select_orphan(ss, time_choice_keys, fallback_display="12:00 AM"),
            "sonarr_end_orphan": _time_select_orphan(se, time_choice_keys, fallback_display="11:59 PM"),
            "radarr_start_orphan": _time_select_orphan(rs, time_choice_keys, fallback_display="12:00 AM"),
            "radarr_end_orphan": _time_select_orphan(re, time_choice_keys, fallback_display="11:59 PM"),
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )
    # Simple Browser / embedded WebViews often cache HTML; force reload of Settings.
    response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response


@app.get("/settings/backup/export", dependencies=_AUTH_DEPS)
async def settings_backup_export(session: AsyncSession = Depends(get_session)) -> Response:
    row = await _get_or_create_settings(session)
    body = export_json_bytes(row)
    d = datetime.now(timezone.utc).strftime("%d-%m-%Y")
    fname = f"fetcher-settings-backup-{d}.json"
    return Response(
        content=body,
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.post("/settings/auth/credentials", dependencies=_AUTH_FORM_DEPS)
async def settings_auth_credentials(
    auth_form: str = Form(""),
    current_password: str = Form(""),
    new_username: str = Form(""),
    new_password: str = Form(""),
    confirm_new_password: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await _get_or_create_settings(session)
    cp = current_password or ""
    if not verify_password(password=cp, stored_hash=(row.auth_password_hash or "")):
        return RedirectResponse("/settings?sec=bad_current", status_code=303)

    form = (auth_form or "").strip().lower()
    if form == "username":
        nu = (new_username or "").strip()
        if not nu:
            return RedirectResponse("/settings?sec=user_empty", status_code=303)
        row.auth_username = nu
        row.updated_at = utc_now_naive()
        if not await _try_commit_and_reschedule(session):
            return RedirectResponse("/settings?sec=save_fail", status_code=303)
        return RedirectResponse("/settings?sec=user_ok", status_code=303)

    if form == "password":
        np = (new_password or "").strip()
        cf = (confirm_new_password or "").strip()
        if not np or len(np) < 8:
            return RedirectResponse("/settings?sec=pass_short", status_code=303)
        if np != cf:
            return RedirectResponse("/settings?sec=pass_mismatch", status_code=303)
        row.auth_password_hash = hash_password(np)
        row.updated_at = utc_now_naive()
        if not await _try_commit_and_reschedule(session):
            return RedirectResponse("/settings?sec=save_fail", status_code=303)
        return RedirectResponse("/settings?sec=pass_ok", status_code=303)

    return RedirectResponse("/settings?sec=invalid", status_code=303)


@app.post("/settings/auth/access_control", dependencies=_AUTH_FORM_DEPS)
async def settings_auth_access_control(
    auth_ip_allowlist: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await _get_or_create_settings(session)
    try:
        normalized = normalize_auth_ip_allowlist_input(auth_ip_allowlist)
    except ValueError:
        return RedirectResponse("/settings?save=fail&reason=invalid_ip", status_code=303)
    row.auth_ip_allowlist = normalized
    row.updated_at = utc_now_naive()
    if not await _try_commit_and_reschedule(session):
        return RedirectResponse("/settings?sec=save_fail", status_code=303)
    return RedirectResponse("/settings?saved=1", status_code=303)


@app.post("/settings/backup/import", dependencies=_AUTH_FORM_DEPS)
async def settings_backup_import(
    session: AsyncSession = Depends(get_session),
    file: UploadFile = File(...),
    confirm: str = Form(""),
) -> RedirectResponse:
    if (confirm or "").strip() != "yes":
        return RedirectResponse("/settings?import=need_confirm", status_code=303)
    raw = await file.read()
    if not raw.strip():
        return RedirectResponse("/settings?import=empty", status_code=303)
    try:
        await import_settings_replace(session, raw)
    except ValueError as e:
        r = str(e)
        if len(r) > 180:
            r = r[:177] + "..."
        return RedirectResponse(f"/settings?import=fail&reason={quote(r, safe='')}", status_code=303)
    return RedirectResponse("/settings?import=ok", status_code=303)


@app.get("/trimmer/settings", response_class=HTMLResponse, dependencies=_AUTH_DEPS)
async def trimmer_settings_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    emby_snap = (await fetch_latest_app_snapshots(session)).get("emby")
    tz = settings.timezone or "UTC"
    time_choices = schedule_time_dropdown_choices(step_minutes=30)
    time_choice_keys = {v for v, _ in time_choices}
    em_days = normalize_schedule_days_csv(settings.emby_schedule_days or "")
    es = _normalize_hhmm(settings.emby_schedule_start, "00:00")
    ee = _normalize_hhmm(settings.emby_schedule_end, "23:59")
    return templates.TemplateResponse(
        request,
        "trimmer_settings.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Trimmer settings",
            "subtitle": "Configure Emby Trimmer and schedule",
            "settings": settings,
            "emby": emby_snap,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "schedule_time_choices": time_choices,
            "emby_schedule_days_normalized": em_days,
            "emby_schedule_days_selected": _schedule_weekdays_selected_dict(
                settings.emby_schedule_days or ""
            ),
            "emby_schedule_start_hhmm": es,
            "emby_schedule_end_hhmm": ee,
            "emby_start_orphan": _time_select_orphan(es, time_choice_keys, fallback_display="12:00 AM"),
            "emby_end_orphan": _time_select_orphan(ee, time_choice_keys, fallback_display="11:59 PM"),
            "movie_genre_options": _MOVIE_GENRE_OPTIONS,
            "selected_movie_genres": parse_genres_csv(settings.emby_rule_movie_genres_csv),
            "selected_tv_genres": parse_genres_csv(settings.emby_rule_tv_genres_csv),
            "people_credit_options": _PEOPLE_CREDIT_OPTIONS,
            "selected_movie_people_credit_types": parse_movie_people_credit_types_csv(
                settings.emby_rule_movie_people_credit_types_csv
            ),
            "selected_tv_people_credit_types": parse_movie_people_credit_types_csv(
                settings.emby_rule_tv_people_credit_types_csv
            ),
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )


@app.get("/trimmer", response_class=HTMLResponse, dependencies=_AUTH_DEPS)
async def trimmer_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    tz = settings.timezone or "UTC"
    rows: list[dict] = []
    error = ""
    used_user_id = (settings.emby_user_id or "").strip()
    used_user_name = ""

    rules = _effective_emby_rules(settings)
    movie_rating_below = rules["movie_rating_below"]
    movie_unwatched_days = rules["movie_unwatched_days"]
    tv_delete_watched = bool(rules["tv_delete_watched"])
    tv_unwatched_days = rules["tv_unwatched_days"]
    _v_scan = settings.emby_max_items_scan
    _raw_scan = int(_v_scan) if _v_scan is not None else 2000
    scan_limit = 0 if _raw_scan <= 0 else max(1, min(100_000, _raw_scan))
    max_deletes = max(1, int(settings.emby_max_deletes_per_run or 25))
    selected_movie_genres = parse_genres_csv(settings.emby_rule_movie_genres_csv)
    selected_tv_genres = parse_genres_csv(settings.emby_rule_tv_genres_csv)
    selected_movie_people = parse_movie_people_phrases(settings.emby_rule_movie_people_csv)
    selected_movie_credit_types = parse_movie_people_credit_types_csv(
        settings.emby_rule_movie_people_credit_types_csv
    )
    selected_tv_people = parse_movie_people_phrases(settings.emby_rule_tv_people_csv)
    selected_tv_credit_types = parse_movie_people_credit_types_csv(
        settings.emby_rule_tv_people_credit_types_csv
    )

    _truthy = ("1", "true", "yes")
    qp = request.query_params
    run_emby_scan = qp.get("scan", "").strip().lower() in _truthy
    scan_prompt = False
    scan_loaded = False

    _emby_key = resolve_emby_api_key(settings)
    if not settings.emby_url or not _emby_key:
        error = "Emby URL and API key are required."
    elif movie_rating_below <= 0 and movie_unwatched_days <= 0 and (not tv_delete_watched) and tv_unwatched_days <= 0:
        error = "No rules are enabled. Set at least one Emby Trimmer rule in Trimmer settings."
    elif not run_emby_scan:
        # Fast path: sidebar / default navigation should not scan the whole library.
        scan_prompt = True
    else:
        client = EmbyClient(EmbyConfig(settings.emby_url, _emby_key))
        try:
            await client.health()
            users = await client.users()
            users_by_id = {str(u.get("Id", "")).strip(): str(u.get("Name", "")).strip() for u in users}
            if not used_user_id and users:
                used_user_id = str(users[0].get("Id", "")).strip()
            used_user_name = users_by_id.get(used_user_id, "")
            if not used_user_id:
                error = "No Emby user available."
            elif not used_user_name:
                error = "Configured Emby user ID was not found."
            else:
                scan_loaded = True
                items = await client.items_for_user(user_id=used_user_id, limit=scan_limit)
                candidates: list[tuple[str, str, str, dict]] = []
                for item in items:
                    item_id = str(item.get("Id", "")).strip()
                    if not item_id:
                        continue
                    is_candidate, reasons, age_days, rating, played = evaluate_candidate(
                        item,
                        movie_watched_rating_below=movie_rating_below,
                        movie_unwatched_days=movie_unwatched_days,
                        tv_delete_watched=tv_delete_watched,
                        tv_unwatched_days=tv_unwatched_days,
                    )
                    item_type = str(item.get("Type", "")).strip()
                    if item_type == "Movie" and not movie_matches_selected_genres(item, selected_movie_genres):
                        is_candidate = False
                    if item_type == "Movie" and not movie_matches_people(
                        item, selected_movie_people, credit_types=selected_movie_credit_types
                    ):
                        is_candidate = False
                    if item_type in {"Series", "Season", "Episode"} and not tv_matches_selected_genres(item, selected_tv_genres):
                        is_candidate = False
                    if item_type in {"Series", "Season", "Episode"} and not movie_matches_people(
                        item, selected_tv_people, credit_types=selected_tv_credit_types
                    ):
                        is_candidate = False
                    if not is_candidate:
                        continue
                    name = str(item.get("Name", "") or item_id)
                    item_type = str(item.get("Type", "") or "").strip()
                    candidates.append((item_id, name, item_type, item))
                    rows.append(
                        {
                            "id": item_id,
                            "name": name,
                            "type": item_type or "-",
                            "played": played,
                            "rating": rating,
                            "age_days": age_days,
                            "reasons": reasons,
                        }
                    )
                    if len(candidates) >= max_deletes:
                        break
                if candidates and not settings.emby_dry_run:
                    sk = resolve_sonarr_api_key(settings)
                    rk = resolve_radarr_api_key(settings)
                    await apply_emby_trimmer_live_deletes(
                        settings, client, candidates, son_key=sk, rad_key=rk
                    )
                    settings.emby_last_run_at = utc_now_naive()
                    await session.commit()
        except Exception as e:  # noqa: BLE001 - user-facing review path
            error = f"Review failed: {type(e).__name__}: {e}"
            scan_loaded = False
        finally:
            await client.aclose()

    return templates.TemplateResponse(
        request,
        "trimmer.html",
        {
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Trimmer",
            "subtitle": "Review exact titles matching Emby Trimmer rules",
            "settings": settings,
            "rows": rows,
            "error": error,
            "used_user_id": used_user_id,
            "used_user_name": used_user_name,
            "movie_rating_below": movie_rating_below,
            "movie_unwatched_days": movie_unwatched_days,
            "tv_delete_watched": tv_delete_watched,
            "tv_unwatched_days": tv_unwatched_days,
            "scan_limit": scan_limit,
            "max_deletes": max_deletes,
            "selected_movie_genres_display": sorted(selected_movie_genres),
            "selected_tv_genres_display": sorted(selected_tv_genres),
            "selected_movie_people_display": selected_movie_people,
            "movie_people_credit_summary": _movie_credit_types_summary(selected_movie_credit_types),
            "selected_tv_people_display": selected_tv_people,
            "tv_people_credit_summary": _movie_credit_types_summary(selected_tv_credit_types),
            "dry_run": bool(settings.emby_dry_run),
            "matched_count": len(rows),
            "scan_prompt": scan_prompt,
            "scan_loaded": scan_loaded,
            "now": utc_now_naive(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "csrf_token": await get_csrf_token_for_template(request, session),
        },
    )


@app.post("/settings", dependencies=_AUTH_FORM_DEPS)
async def save_settings(
    sonarr_enabled: bool = Form(False),
    sonarr_url: str = Form(""),
    sonarr_api_key: str = Form(""),
    sonarr_search_missing: bool = Form(False),
    sonarr_search_upgrades: bool = Form(False),
    sonarr_max_items_per_run: int = Form(50),
    sonarr_interval_minutes: int = Form(60),
    sonarr_schedule_enabled: bool = Form(False),
    sonarr_schedule_Mon: int = Form(0),
    sonarr_schedule_Tue: int = Form(0),
    sonarr_schedule_Wed: int = Form(0),
    sonarr_schedule_Thu: int = Form(0),
    sonarr_schedule_Fri: int = Form(0),
    sonarr_schedule_Sat: int = Form(0),
    sonarr_schedule_Sun: int = Form(0),
    sonarr_schedule_start: str = Form("00:00"),
    sonarr_schedule_end: str = Form("23:59"),
    radarr_enabled: bool = Form(False),
    radarr_url: str = Form(""),
    radarr_api_key: str = Form(""),
    radarr_search_missing: bool = Form(False),
    radarr_search_upgrades: bool = Form(False),
    radarr_max_items_per_run: int = Form(50),
    radarr_interval_minutes: int = Form(60),
    radarr_schedule_enabled: bool = Form(False),
    radarr_schedule_Mon: int = Form(0),
    radarr_schedule_Tue: int = Form(0),
    radarr_schedule_Wed: int = Form(0),
    radarr_schedule_Thu: int = Form(0),
    radarr_schedule_Fri: int = Form(0),
    radarr_schedule_Sat: int = Form(0),
    radarr_schedule_Sun: int = Form(0),
    radarr_schedule_start: str = Form("00:00"),
    radarr_schedule_end: str = Form("23:59"),
    arr_search_cooldown_minutes: int = Form(1440),
    log_retention_days: int = Form(90),
    timezone: str = Form("UTC"),
    save_scope: str = Form("all"),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    try:
        row = await _get_or_create_settings(session)
        data = SettingsIn(
            sonarr_enabled=sonarr_enabled,
            sonarr_url=_normalize_base_url(sonarr_url),
            sonarr_api_key=sonarr_api_key.strip(),
            sonarr_search_missing=sonarr_search_missing,
            sonarr_search_upgrades=sonarr_search_upgrades,
            sonarr_max_items_per_run=sonarr_max_items_per_run,
            sonarr_interval_minutes=sonarr_interval_minutes,
            # schedule fields are not in SettingsIn; set on ORM row below
            radarr_enabled=radarr_enabled,
            radarr_url=_normalize_base_url(radarr_url),
            radarr_api_key=radarr_api_key.strip(),
            radarr_search_missing=radarr_search_missing,
            radarr_search_upgrades=radarr_search_upgrades,
            radarr_max_items_per_run=radarr_max_items_per_run,
            radarr_interval_minutes=radarr_interval_minutes,
            arr_search_cooldown_minutes=arr_search_cooldown_minutes,
        )
        scope = (save_scope or "all").strip().lower()
        # Sonarr/Radarr: persist on app-specific save OR "Save Global" (same form posts all fields).
        if scope in ("all", "sonarr", "global"):
            row.sonarr_enabled = data.sonarr_enabled
            row.sonarr_url = data.sonarr_url
            row.sonarr_api_key = data.sonarr_api_key
            row.sonarr_search_missing = data.sonarr_search_missing
            row.sonarr_search_upgrades = data.sonarr_search_upgrades
            row.sonarr_max_items_per_run = data.sonarr_max_items_per_run
            row.sonarr_interval_minutes = data.sonarr_interval_minutes
            row.sonarr_schedule_enabled = sonarr_schedule_enabled
            row.sonarr_schedule_days = _schedule_days_csv_from_named_day_checks(
                sonarr_schedule_Mon,
                sonarr_schedule_Tue,
                sonarr_schedule_Wed,
                sonarr_schedule_Thu,
                sonarr_schedule_Fri,
                sonarr_schedule_Sat,
                sonarr_schedule_Sun,
            )
            row.sonarr_schedule_start = _normalize_hhmm(sonarr_schedule_start, "00:00")
            row.sonarr_schedule_end = _normalize_hhmm(sonarr_schedule_end, "23:59")

        if scope in ("all", "radarr", "global"):
            row.radarr_enabled = data.radarr_enabled
            row.radarr_url = data.radarr_url
            row.radarr_api_key = data.radarr_api_key
            row.radarr_search_missing = data.radarr_search_missing
            row.radarr_search_upgrades = data.radarr_search_upgrades
            row.radarr_max_items_per_run = data.radarr_max_items_per_run
            row.radarr_interval_minutes = data.radarr_interval_minutes
            row.radarr_schedule_enabled = radarr_schedule_enabled
            row.radarr_schedule_days = _schedule_days_csv_from_named_day_checks(
                radarr_schedule_Mon,
                radarr_schedule_Tue,
                radarr_schedule_Wed,
                radarr_schedule_Thu,
                radarr_schedule_Fri,
                radarr_schedule_Sat,
                radarr_schedule_Sun,
            )
            row.radarr_schedule_start = _normalize_hhmm(radarr_schedule_start, "00:00")
            row.radarr_schedule_end = _normalize_hhmm(radarr_schedule_end, "23:59")

        if scope in ("all", "global"):
            row.arr_search_cooldown_minutes = data.arr_search_cooldown_minutes
            row.log_retention_days = max(7, min(3650, int(log_retention_days or 90)))
            row.timezone = _resolve_timezone_name(timezone)

        row.updated_at = utc_now_naive()
        if not await _try_commit_and_reschedule(session):
            return RedirectResponse("/settings?save=fail&reason=db_busy", status_code=303)
        return RedirectResponse("/settings?saved=1", status_code=303)
    except SQLAlchemyError:
        logger.exception("POST /settings SQLAlchemyError")
        return RedirectResponse("/settings?save=fail&reason=db_error", status_code=303)
    except ValueError:
        logger.exception("POST /settings ValueError")
        return RedirectResponse("/settings?save=fail&reason=invalid", status_code=303)
    except Exception:
        logger.exception("POST /settings failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return RedirectResponse("/settings?save=fail&reason=error", status_code=303)


@app.post("/trimmer/settings", dependencies=_AUTH_FORM_DEPS)
async def save_emby_settings(
    emby_enabled: bool = Form(False),
    emby_url: str = Form(""),
    emby_api_key: str = Form(""),
    emby_user_id: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    # Backward-compatible endpoint: save both sections if old form posts here.
    try:
        row = await _get_or_create_settings(session)
        row.emby_enabled = emby_enabled
        row.emby_url = _normalize_base_url(emby_url)
        row.emby_api_key = emby_api_key.strip()
        row.emby_user_id = emby_user_id.strip()
        row.updated_at = utc_now_naive()
        if not await _try_commit_and_reschedule(session):
            return RedirectResponse("/trimmer/settings?save=fail&reason=db_busy", status_code=303)
        return RedirectResponse("/trimmer/settings?saved=1", status_code=303)
    except SQLAlchemyError:
        logger.exception("POST /trimmer/settings SQLAlchemyError")
        return RedirectResponse("/trimmer/settings?save=fail&reason=db_error", status_code=303)
    except ValueError:
        logger.exception("POST /trimmer/settings ValueError")
        return RedirectResponse("/trimmer/settings?save=fail&reason=invalid", status_code=303)
    except Exception:
        logger.exception("POST /trimmer/settings failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return RedirectResponse("/trimmer/settings?save=fail&reason=error", status_code=303)


@app.post("/trimmer/settings/connection", dependencies=_AUTH_FORM_DEPS)
async def save_emby_connection_settings(
    emby_enabled: bool = Form(False),
    emby_url: str = Form(""),
    emby_api_key: str = Form(""),
    emby_user_id: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    try:
        row = await _get_or_create_settings(session)
        row.emby_enabled = emby_enabled
        row.emby_url = _normalize_base_url(emby_url)
        row.emby_api_key = emby_api_key.strip()
        row.emby_user_id = emby_user_id.strip()
        row.updated_at = utc_now_naive()
        if not await _try_commit_and_reschedule(session):
            return RedirectResponse("/trimmer/settings?save=fail&reason=db_busy", status_code=303)
        return RedirectResponse("/trimmer/settings?saved=1", status_code=303)
    except SQLAlchemyError:
        logger.exception("POST /trimmer/settings/connection SQLAlchemyError")
        return RedirectResponse("/trimmer/settings?save=fail&reason=db_error", status_code=303)
    except ValueError:
        logger.exception("POST /trimmer/settings/connection ValueError")
        return RedirectResponse("/trimmer/settings?save=fail&reason=invalid", status_code=303)
    except Exception:
        logger.exception("POST /trimmer/settings/connection failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return RedirectResponse("/trimmer/settings?save=fail&reason=error", status_code=303)


@app.post("/trimmer/settings/cleaner", dependencies=_AUTH_FORM_DEPS)
async def save_trimmer_settings(
    emby_interval_minutes: int = Form(60),
    emby_dry_run: bool = Form(False),
    emby_schedule_enabled: bool = Form(False),
    emby_schedule_Mon: int = Form(0),
    emby_schedule_Tue: int = Form(0),
    emby_schedule_Wed: int = Form(0),
    emby_schedule_Thu: int = Form(0),
    emby_schedule_Fri: int = Form(0),
    emby_schedule_Sat: int = Form(0),
    emby_schedule_Sun: int = Form(0),
    emby_schedule_start: str = Form("00:00"),
    emby_schedule_end: str = Form("23:59"),
    emby_max_items_scan: int = Form(2000),
    emby_max_deletes_per_run: int = Form(25),
    emby_rule_movie_watched_rating_below: int = Form(0),
    emby_rule_movie_unwatched_days: int = Form(0),
    emby_rule_movie_genres: list[str] = Form([]),
    emby_rule_movie_people: str = Form(""),
    emby_rule_movie_people_credit_types: list[str] = Form([]),
    emby_rule_tv_delete_watched: bool = Form(False),
    emby_rule_tv_genres: list[str] = Form([]),
    emby_rule_tv_people: str = Form(""),
    emby_rule_tv_people_credit_types: list[str] = Form([]),
    emby_rule_tv_unwatched_days: int = Form(0),
    save_scope: str = Form("all"),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    try:
        row = await _get_or_create_settings(session)
        scope = (save_scope or "all").strip().lower()
        # One shared form: persist Emby Trimmer cadence on any save (independent of Fetcher / Arr scheduler base).
        eim = max(5, min(7 * 24 * 60, int(emby_interval_minutes or 60)))
        row.emby_interval_minutes = eim
        # One shared HTML form: schedule / dry run / scan limits are always posted; persist on any save button.
        row.emby_dry_run = emby_dry_run
        row.emby_schedule_enabled = emby_schedule_enabled
        row.emby_schedule_days = _schedule_days_csv_from_named_day_checks(
            emby_schedule_Mon,
            emby_schedule_Tue,
            emby_schedule_Wed,
            emby_schedule_Thu,
            emby_schedule_Fri,
            emby_schedule_Sat,
            emby_schedule_Sun,
        )
        row.emby_schedule_start = _normalize_hhmm(emby_schedule_start, "00:00")
        row.emby_schedule_end = _normalize_hhmm(emby_schedule_end, "23:59")
        _scan = int(emby_max_items_scan)
        row.emby_max_items_scan = 0 if _scan <= 0 else max(1, min(100_000, _scan))
        row.emby_max_deletes_per_run = max(1, min(500, int(emby_max_deletes_per_run or 25)))

        if scope in ("all", "movies"):
            row.emby_rule_movie_watched_rating_below = max(0, min(10, int(emby_rule_movie_watched_rating_below or 0)))
            row.emby_rule_movie_unwatched_days = max(0, min(36500, int(emby_rule_movie_unwatched_days or 0)))
            selected_genres = sorted({str(v).strip() for v in (emby_rule_movie_genres or []) if str(v).strip()})
            row.emby_rule_movie_genres_csv = ",".join(selected_genres)
            row.emby_rule_movie_people_csv = (emby_rule_movie_people or "").strip()[:8000]
            row.emby_rule_movie_people_credit_types_csv = _people_credit_types_csv_from_form(emby_rule_movie_people_credit_types)

        if scope in ("all", "tv"):
            row.emby_rule_tv_delete_watched = emby_rule_tv_delete_watched
            selected_tv_genres = sorted({str(v).strip() for v in (emby_rule_tv_genres or []) if str(v).strip()})
            row.emby_rule_tv_genres_csv = ",".join(selected_tv_genres)
            row.emby_rule_tv_people_csv = (emby_rule_tv_people or "").strip()[:8000]
            row.emby_rule_tv_people_credit_types_csv = _people_credit_types_csv_from_form(emby_rule_tv_people_credit_types)
            row.emby_rule_tv_watched_rating_below = 0
            row.emby_rule_tv_unwatched_days = max(0, min(36500, int(emby_rule_tv_unwatched_days or 0)))

        # Keep aggregate Emby rule fields aligned with movie/TV columns (used as fallbacks in rule evaluation).
        row.emby_rule_watched_rating_below = max(
            row.emby_rule_movie_watched_rating_below,
            0,
        )
        row.emby_rule_unwatched_days = max(
            row.emby_rule_movie_unwatched_days,
            row.emby_rule_tv_unwatched_days,
        )
        row.updated_at = utc_now_naive()
        if not await _try_commit_and_reschedule(session):
            return RedirectResponse("/trimmer/settings?save=fail&reason=db_busy", status_code=303)
        return RedirectResponse("/trimmer/settings?saved=1", status_code=303)
    except SQLAlchemyError:
        logger.exception("POST /trimmer/settings/cleaner SQLAlchemyError")
        return RedirectResponse("/trimmer/settings?save=fail&reason=db_error", status_code=303)
    except ValueError:
        logger.exception("POST /trimmer/settings/cleaner ValueError")
        return RedirectResponse("/trimmer/settings?save=fail&reason=invalid", status_code=303)
    except Exception:
        logger.exception("POST /trimmer/settings/cleaner failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return RedirectResponse("/trimmer/settings?save=fail&reason=error", status_code=303)


@app.post("/test/sonarr", dependencies=_AUTH_FORM_DEPS)
async def test_sonarr(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    try:
        c = ArrClient(ArrConfig(settings.sonarr_url, resolve_sonarr_api_key(settings)))
        try:
            await c.health()
        finally:
            await c.aclose()
        session.add(AppSnapshot(app="sonarr", ok=True, status_message="Connection test succeeded.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=sonarr_ok", status_code=303)
    except httpx.HTTPError as e:
        session.add(AppSnapshot(app="sonarr", ok=False, status_message=f"Connection test failed: {type(e).__name__}: {e}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=sonarr_fail", status_code=303)


@app.post("/test/radarr", dependencies=_AUTH_FORM_DEPS)
async def test_radarr(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    try:
        c = ArrClient(ArrConfig(settings.radarr_url, resolve_radarr_api_key(settings)))
        try:
            await c.health()
        finally:
            await c.aclose()
        session.add(AppSnapshot(app="radarr", ok=True, status_message="Connection test succeeded.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=radarr_ok", status_code=303)
    except httpx.HTTPError as e:
        session.add(AppSnapshot(app="radarr", ok=False, status_message=f"Connection test failed: {type(e).__name__}: {e}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=radarr_fail", status_code=303)


@app.post("/test/emby", dependencies=_AUTH_FORM_DEPS)
async def test_emby(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    emby_url = _normalize_base_url(settings.emby_url)
    emby_token = resolve_emby_api_key(settings)
    if not emby_url:
        session.add(AppSnapshot(app="emby", ok=False, status_message="Connection test failed: Emby URL is required.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)
    if not emby_token:
        session.add(AppSnapshot(app="emby", ok=False, status_message="Connection test failed: Emby API key is required.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)
    if _looks_like_url(emby_token):
        session.add(
            AppSnapshot(
                app="emby",
                ok=False,
                status_message="Connection test failed: Emby API key looks like a URL. Paste the key from Emby Dashboard → Advanced → API keys.",
                missing_total=0,
                cutoff_unmet_total=0,
            )
        )
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)
    try:
        c = EmbyClient(EmbyConfig(emby_url, emby_token))
        try:
            await c.health()
            if settings.emby_user_id:
                users = await c.users()
                ok = any(str(u.get("Id", "")) == settings.emby_user_id for u in users)
                if not ok:
                    raise ValueError("Configured Emby user ID was not found.")
        finally:
            await c.aclose()
        session.add(AppSnapshot(app="emby", ok=True, status_message="Connection test succeeded.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_ok", status_code=303)
    except httpx.HTTPStatusError as e:
        detail = f"HTTP {e.response.status_code}: {e}"
        if e.response.status_code in (401, 403):
            detail += " | Check Emby API key permissions and base URL."
        session.add(AppSnapshot(app="emby", ok=False, status_message=f"Connection test failed: {detail}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)
    except (httpx.HTTPError, ValueError) as e:
        session.add(AppSnapshot(app="emby", ok=False, status_message=f"Connection test failed: {type(e).__name__}: {e}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)


@app.post("/test/emby-form", dependencies=_AUTH_FORM_DEPS)
async def test_emby_from_form(
    emby_enabled: bool = Form(False),
    emby_url: str = Form(""),
    emby_api_key: str = Form(""),
    emby_user_id: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    # Test using current form values so users don't need to save first.
    emby_url_n = _normalize_base_url(emby_url)
    emby_api_key_n = (emby_api_key or "").strip()
    emby_user_id_n = (emby_user_id or "").strip()
    if not emby_url_n:
        session.add(AppSnapshot(app="emby", ok=False, status_message="Connection test failed: Emby URL is required.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)
    row = await _get_or_create_settings(session)
    emby_token_n = resolve_emby_api_key(row, form=emby_api_key)
    if not emby_token_n:
        session.add(AppSnapshot(app="emby", ok=False, status_message="Connection test failed: Emby API key is required.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)
    if _looks_like_url(emby_token_n):
        session.add(
            AppSnapshot(
                app="emby",
                ok=False,
                status_message="Connection test failed: Emby API key looks like a URL. Paste the key from Emby Dashboard → Advanced → API keys.",
                missing_total=0,
                cutoff_unmet_total=0,
            )
        )
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)
    # Persist entered connection values so users don't lose them after testing.
    row.emby_enabled = emby_enabled
    row.emby_url = emby_url_n
    row.emby_api_key = emby_api_key_n
    row.emby_user_id = emby_user_id_n
    row.updated_at = utc_now_naive()
    await session.commit()
    try:
        c = EmbyClient(EmbyConfig(emby_url_n, emby_token_n))
        try:
            await c.health()
            if emby_user_id_n:
                users = await c.users()
                ok = any(str(u.get("Id", "")) == emby_user_id_n for u in users)
                if not ok:
                    raise ValueError("Configured Emby user ID was not found.")
        finally:
            await c.aclose()
        session.add(AppSnapshot(app="emby", ok=True, status_message="Connection test succeeded.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_ok", status_code=303)
    except httpx.HTTPStatusError as e:
        detail = f"HTTP {e.response.status_code}: {e}"
        if e.response.status_code in (401, 403):
            detail += " | Check Emby API key permissions and base URL."
        session.add(AppSnapshot(app="emby", ok=False, status_message=f"Connection test failed: {detail}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)
    except (httpx.HTTPError, ValueError) as e:
        session.add(AppSnapshot(app="emby", ok=False, status_message=f"Connection test failed: {type(e).__name__}: {e}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/trimmer/settings?test=emby_fail", status_code=303)

