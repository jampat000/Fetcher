from __future__ import annotations

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import engine, get_session
from app.migrations import migrate
import httpx

from app.arr_client import ArrClient, ArrConfig
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
from app.schemas import SettingsIn
from app.scheduler import ServiceScheduler


APP_NAME = "Grabby"
APP_TAGLINE = "Never miss a release."

app = FastAPI(title=APP_NAME)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

scheduler = ServiceScheduler()


@app.on_event("startup")
async def on_startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await migrate(engine)
    await scheduler.start()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    scheduler.shutdown()


async def _get_or_create_settings(session: AsyncSession) -> AppSettings:
    row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
    if row:
        return row
    row = AppSettings()
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


_TIMEZONE_CHOICES = [
    ("UTC", "UTC"),
    ("America/New_York", "America/New_York"),
    ("America/Chicago", "America/Chicago"),
    ("America/Denver", "America/Denver"),
    ("America/Los_Angeles", "America/Los_Angeles"),
    ("America/Phoenix", "America/Phoenix"),
    ("America/Anchorage", "America/Anchorage"),
    ("America/Toronto", "America/Toronto"),
    ("America/Vancouver", "America/Vancouver"),
    ("Europe/London", "Europe/London"),
    ("Europe/Paris", "Europe/Paris"),
    ("Europe/Berlin", "Europe/Berlin"),
    ("Europe/Amsterdam", "Europe/Amsterdam"),
    ("Europe/Rome", "Europe/Rome"),
    ("Australia/Sydney", "AEDT/AEST (Australia/Sydney)"),
    ("Australia/Brisbane", "AEST (Australia/Brisbane)"),
    ("Australia/Melbourne", "Australia/Melbourne"),
    ("Australia/Perth", "Australia/Perth"),
    ("Australia/Adelaide", "Australia/Adelaide"),
    ("Asia/Tokyo", "Asia/Tokyo"),
    ("Asia/Shanghai", "Asia/Shanghai"),
    ("Asia/Singapore", "Asia/Singapore"),
    ("Pacific/Auckland", "Pacific/Auckland"),
]

_TZ_ALIASES = {
    "AEDT": "Australia/Sydney",
    "AEST": "Australia/Brisbane",
}

_MOVIE_GENRE_OPTIONS = [
    "Action",
    "Adventure",
    "Animation",
    "Comedy",
    "Crime",
    "Documentary",
    "Drama",
    "Family",
    "Fantasy",
    "History",
    "Horror",
    "Music",
    "Mystery",
    "Romance",
    "Science Fiction",
    "Thriller",
    "TV Movie",
    "War",
    "Western",
]

# Values must match Emby People[].Type (storage is canonical casing).
_PEOPLE_CREDIT_OPTIONS: list[tuple[str, str]] = [
    ("Actor", "Cast (actors)"),
    ("Director", "Directors"),
    ("Writer", "Writers"),
    ("Producer", "Producers"),
    ("GuestStar", "Guest stars"),
]

_PEOPLE_CREDIT_TYPE_FORM_MAP = {
    "actor": "Actor",
    "director": "Director",
    "writer": "Writer",
    "producer": "Producer",
    "gueststar": "GuestStar",
}


def _people_credit_types_csv_from_form(form_values: list[str] | None) -> str:
    credit_vals: list[str] = []
    for v in form_values or []:
        key = str(v).strip().lower().replace(" ", "")
        canon = _PEOPLE_CREDIT_TYPE_FORM_MAP.get(key)
        if canon:
            credit_vals.append(canon)
    credit_vals = sorted(set(credit_vals))
    return ",".join(credit_vals) if credit_vals else "Actor"


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


def _resolve_timezone_name(raw: str) -> str:
    v = (raw or "UTC").strip() or "UTC"
    return _TZ_ALIASES.get(v.upper(), v)


def _normalize_hhmm(raw: str, default: str) -> str:
    v = (raw or "").strip()
    if not v:
        return default
    # Already 24h HH:MM
    try:
        dt = datetime.strptime(v, "%H:%M")
        return dt.strftime("%H:%M")
    except Exception:
        pass
    # 12h forms like 9:30 PM / 09:30pm
    for fmt in ("%I:%M %p", "%I:%M%p"):
        try:
            dt = datetime.strptime(v.upper(), fmt)
            return dt.strftime("%H:%M")
        except Exception:
            continue
    return default


def _to_12h(hhmm: str, default: str) -> str:
    try:
        dt = datetime.strptime((hhmm or "").strip(), "%H:%M")
        return dt.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return default


def _effective_emby_rules(settings: AppSettings) -> dict[str, int | bool]:
    global_rating = max(0, int(getattr(settings, "emby_rule_watched_rating_below", 0) or 0))
    global_unwatched = max(0, int(getattr(settings, "emby_rule_unwatched_days", 0) or 0))

    movie_rating = max(0, int(getattr(settings, "emby_rule_movie_watched_rating_below", 0) or 0)) or global_rating
    movie_unwatched = max(0, int(getattr(settings, "emby_rule_movie_unwatched_days", 0) or 0)) or global_unwatched
    tv_delete_watched = bool(getattr(settings, "emby_rule_tv_delete_watched", False))
    tv_unwatched = max(0, int(getattr(settings, "emby_rule_tv_unwatched_days", 0) or 0)) or global_unwatched

    return {
        "movie_rating_below": movie_rating,
        "movie_unwatched_days": movie_unwatched,
        "tv_delete_watched": tv_delete_watched,
        "tv_unwatched_days": tv_unwatched,
    }


def _now_local(timezone: str) -> str:
    try:
        tz = ZoneInfo(_resolve_timezone_name(timezone))
    except Exception:
        tz = ZoneInfo("UTC")
    # Keep a stable-width display across tabs to avoid subtle layout jitter.
    return datetime.now(tz).strftime("%Y-%m-%d %I:%M %p")


def _fmt_local(dt: datetime, tz_name: str) -> str:
    try:
        tz = ZoneInfo(_resolve_timezone_name(tz_name))
    except Exception:
        tz = ZoneInfo("UTC")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(tz).strftime("%m-%d %I:%M %p")


def _normalize_base_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    # If user enters "10.0.0.5:8989", assume http://
    if "://" not in raw:
        raw = "http://" + raw
    p = urlparse(raw)
    if not p.scheme or not p.netloc:
        return raw
    # Common pitfall: Sonarr/Radarr default ports are HTTP, not HTTPS.
    # If user enters https://host:8989 (or :7878) it will fail with SSL WRONG_VERSION_NUMBER.
    if p.scheme == "https" and (p.port in (8989, 7878)) and (p.path in ("", "/")):
        base = f"http://{p.netloc}".rstrip("/")
        return base
    # Strip trailing slash, keep path if they run behind a reverse proxy subpath.
    base = f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")
    return base


def _looks_like_url(raw: str) -> bool:
    v = (raw or "").strip().lower()
    return v.startswith("http://") or v.startswith("https://")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "app": APP_NAME}


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    activity = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(30)))
        .scalars().all()
    )
    tz = getattr(settings, "timezone", None) or "UTC"
    activity_display = [
        {"time_local": _fmt_local(e.created_at, tz), "app": e.app, "kind": e.kind, "count": e.count}
        for e in activity
    ]
    sonarr_snap = (
        (await session.execute(select(AppSnapshot).where(AppSnapshot.app == "sonarr").order_by(desc(AppSnapshot.id)).limit(1)))
        .scalars()
        .first()
    )
    radarr_snap = (
        (await session.execute(select(AppSnapshot).where(AppSnapshot.app == "radarr").order_by(desc(AppSnapshot.id)).limit(1)))
        .scalars()
        .first()
    )
    emby_snap = (
        (await session.execute(select(AppSnapshot).where(AppSnapshot.app == "emby").order_by(desc(AppSnapshot.id)).limit(1)))
        .scalars()
        .first()
    )
    tz = getattr(settings, "timezone", None) or "UTC"
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Dashboard",
            "subtitle": "Status overview and counts",
            "settings": settings,
            "activity": activity_display,
            "sonarr": sonarr_snap,
            "radarr": radarr_snap,
            "emby": emby_snap,
            "selected_movie_genres": sorted(parse_genres_csv(getattr(settings, "emby_rule_movie_genres_csv", ""))),
            "selected_tv_genres": sorted(parse_genres_csv(getattr(settings, "emby_rule_tv_genres_csv", ""))),
            "movie_people_phrases": parse_movie_people_phrases(getattr(settings, "emby_rule_movie_people_csv", "")),
            "movie_people_credit_types": parse_movie_people_credit_types_csv(
                getattr(settings, "emby_rule_movie_people_credit_types_csv", "Actor")
            ),
            "movie_people_credit_summary": _movie_credit_types_summary(
                parse_movie_people_credit_types_csv(
                    getattr(settings, "emby_rule_movie_people_credit_types_csv", "Actor")
                )
            ),
            "tv_people_phrases": parse_movie_people_phrases(getattr(settings, "emby_rule_tv_people_csv", "")),
            "tv_people_credit_summary": _movie_credit_types_summary(
                parse_movie_people_credit_types_csv(
                    getattr(settings, "emby_rule_tv_people_credit_types_csv", "Actor")
                )
            ),
            "now": datetime.utcnow(),
            "now_local": _now_local(tz),
            "timezone": tz,
        },
    )


@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    logs = (await session.execute(select(JobRunLog).order_by(desc(JobRunLog.id)).limit(200))).scalars().all()
    tz = getattr(settings, "timezone", None) or "UTC"
    logs_display = [
        {"started_local": _fmt_local(r.started_at, tz), "ok": r.ok, "message": r.message}
        for r in logs
    ]
    return templates.TemplateResponse(
        "logs.html",
        {
            "request": request,
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Logs",
            "subtitle": "Service run history",
            "logs": logs_display,
            "now": datetime.utcnow(),
            "now_local": _now_local(tz),
            "timezone": tz,
        },
    )


@app.get("/activity", response_class=HTMLResponse)
async def activity_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    activity = (
        (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(200)))
        .scalars().all()
    )
    tz = getattr(settings, "timezone", None) or "UTC"
    activity_display = [
        {"time_local": _fmt_local(e.created_at, tz), "app": e.app, "kind": e.kind, "count": e.count}
        for e in activity
    ]
    return templates.TemplateResponse(
        "activity.html",
        {
            "request": request,
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Grabbed",
            "subtitle": "What was grabbed",
            "activity": activity_display,
            "now": datetime.utcnow(),
            "now_local": _now_local(tz),
            "timezone": tz,
        },
    )


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    sonarr_snap = (
        (await session.execute(select(AppSnapshot).where(AppSnapshot.app == "sonarr").order_by(desc(AppSnapshot.id)).limit(1)))
        .scalars()
        .first()
    )
    radarr_snap = (
        (await session.execute(select(AppSnapshot).where(AppSnapshot.app == "radarr").order_by(desc(AppSnapshot.id)).limit(1)))
        .scalars()
        .first()
    )
    tz = getattr(settings, "timezone", None) or "UTC"
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Grabby Settings",
            "subtitle": "Configure connections, schedules, and limits",
            "settings": settings,
            "sonarr": sonarr_snap,
            "radarr": radarr_snap,
            "now": datetime.utcnow(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "timezones": _TIMEZONE_CHOICES,
            "sonarr_schedule_start_display": _to_12h(settings.sonarr_schedule_start, "12:00 AM"),
            "sonarr_schedule_end_display": _to_12h(settings.sonarr_schedule_end, "11:59 PM"),
            "radarr_schedule_start_display": _to_12h(settings.radarr_schedule_start, "12:00 AM"),
            "radarr_schedule_end_display": _to_12h(settings.radarr_schedule_end, "11:59 PM"),
        },
    )


@app.get("/emby/settings", response_class=HTMLResponse)
async def emby_settings_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    emby_snap = (
        (await session.execute(select(AppSnapshot).where(AppSnapshot.app == "emby").order_by(desc(AppSnapshot.id)).limit(1)))
        .scalars()
        .first()
    )
    tz = getattr(settings, "timezone", None) or "UTC"
    return templates.TemplateResponse(
        "emby_settings.html",
        {
            "request": request,
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Cleaner Settings",
            "subtitle": "Configure Emby cleanup and schedule",
            "settings": settings,
            "emby": emby_snap,
            "now": datetime.utcnow(),
            "now_local": _now_local(tz),
            "timezone": tz,
            "emby_schedule_start_display": _to_12h(settings.emby_schedule_start, "12:00 AM"),
            "emby_schedule_end_display": _to_12h(settings.emby_schedule_end, "11:59 PM"),
            "movie_genre_options": _MOVIE_GENRE_OPTIONS,
            "selected_movie_genres": parse_genres_csv(getattr(settings, "emby_rule_movie_genres_csv", "")),
            "selected_tv_genres": parse_genres_csv(getattr(settings, "emby_rule_tv_genres_csv", "")),
            "people_credit_options": _PEOPLE_CREDIT_OPTIONS,
            "selected_movie_people_credit_types": parse_movie_people_credit_types_csv(
                getattr(settings, "emby_rule_movie_people_credit_types_csv", "Actor")
            ),
            "selected_tv_people_credit_types": parse_movie_people_credit_types_csv(
                getattr(settings, "emby_rule_tv_people_credit_types_csv", "Actor")
            ),
        },
    )


@app.get("/cleaner", response_class=HTMLResponse)
@app.get("/emby/preview", response_class=HTMLResponse)
async def emby_preview_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    tz = getattr(settings, "timezone", None) or "UTC"
    rows: list[dict] = []
    error = ""
    used_user_id = (settings.emby_user_id or "").strip()
    used_user_name = ""

    rules = _effective_emby_rules(settings)
    movie_rating_below = rules["movie_rating_below"]
    movie_unwatched_days = rules["movie_unwatched_days"]
    tv_delete_watched = bool(rules["tv_delete_watched"])
    tv_unwatched_days = rules["tv_unwatched_days"]
    _v_scan = getattr(settings, "emby_max_items_scan", 2000)
    _raw_scan = int(_v_scan) if _v_scan is not None else 2000
    scan_limit = 0 if _raw_scan <= 0 else max(1, min(100_000, _raw_scan))
    max_deletes = max(1, int(getattr(settings, "emby_max_deletes_per_run", 25) or 25))
    selected_movie_genres = parse_genres_csv(getattr(settings, "emby_rule_movie_genres_csv", ""))
    selected_tv_genres = parse_genres_csv(getattr(settings, "emby_rule_tv_genres_csv", ""))
    selected_movie_people = parse_movie_people_phrases(getattr(settings, "emby_rule_movie_people_csv", ""))
    selected_movie_credit_types = parse_movie_people_credit_types_csv(
        getattr(settings, "emby_rule_movie_people_credit_types_csv", "Actor")
    )
    selected_tv_people = parse_movie_people_phrases(getattr(settings, "emby_rule_tv_people_csv", ""))
    selected_tv_credit_types = parse_movie_people_credit_types_csv(
        getattr(settings, "emby_rule_tv_people_credit_types_csv", "Actor")
    )

    if not settings.emby_url or not settings.emby_api_key:
        error = "Emby URL and API key are required."
    elif movie_rating_below <= 0 and movie_unwatched_days <= 0 and (not tv_delete_watched) and tv_unwatched_days <= 0:
        error = "No rules are enabled. Set at least one Emby cleanup rule in Settings."
    else:
        client = EmbyClient(EmbyConfig(settings.emby_url, settings.emby_api_key))
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
                items = await client.items_for_user(user_id=used_user_id, limit=scan_limit)
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
                    rows.append(
                        {
                            "id": item_id,
                            "name": str(item.get("Name", "") or item_id),
                            "type": str(item.get("Type", "") or "-"),
                            "played": played,
                            "rating": rating,
                            "age_days": age_days,
                            "reasons": reasons,
                        }
                    )
                    if len(rows) >= max_deletes:
                        break
        except Exception as e:  # noqa: BLE001 - user-facing review path
            error = f"Review failed: {type(e).__name__}: {e}"
        finally:
            await client.aclose()

    return templates.TemplateResponse(
        "cleaner.html",
        {
            "request": request,
            "app_name": APP_NAME,
            "app_tagline": APP_TAGLINE,
            "title": f"{APP_NAME} — Cleaner",
            "subtitle": "Review exact titles matching cleanup rules",
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
            "dry_run": bool(getattr(settings, "emby_dry_run", True)),
            "matched_count": len(rows),
            "now": datetime.utcnow(),
            "now_local": _now_local(tz),
            "timezone": tz,
        },
    )


@app.post("/settings")
async def save_settings(
    sonarr_enabled: bool = Form(False),
    sonarr_url: str = Form(""),
    sonarr_api_key: str = Form(""),
    sonarr_search_missing: bool = Form(False),
    sonarr_search_upgrades: bool = Form(False),
    sonarr_max_items_per_run: int = Form(50),
    sonarr_schedule_enabled: bool = Form(False),
    sonarr_schedule_days: str = Form("Mon,Tue,Wed,Thu,Fri,Sat,Sun"),
    sonarr_schedule_start: str = Form("00:00"),
    sonarr_schedule_end: str = Form("23:59"),
    radarr_enabled: bool = Form(False),
    radarr_url: str = Form(""),
    radarr_api_key: str = Form(""),
    radarr_search_missing: bool = Form(False),
    radarr_search_upgrades: bool = Form(False),
    radarr_max_items_per_run: int = Form(50),
    radarr_schedule_enabled: bool = Form(False),
    radarr_schedule_days: str = Form("Mon,Tue,Wed,Thu,Fri,Sat,Sun"),
    radarr_schedule_start: str = Form("00:00"),
    radarr_schedule_end: str = Form("23:59"),
    interval_minutes: int = Form(60),
    timezone: str = Form("UTC"),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    # Validate via Pydantic (keeps server-side constraints consistent)
    data = SettingsIn(
        sonarr_enabled=sonarr_enabled,
        sonarr_url=_normalize_base_url(sonarr_url),
        sonarr_api_key=sonarr_api_key.strip(),
        sonarr_search_missing=sonarr_search_missing,
        sonarr_search_upgrades=sonarr_search_upgrades,
        sonarr_max_items_per_run=sonarr_max_items_per_run,
        # schedule fields are not in SettingsIn; set on ORM row below
        radarr_enabled=radarr_enabled,
        radarr_url=_normalize_base_url(radarr_url),
        radarr_api_key=radarr_api_key.strip(),
        radarr_search_missing=radarr_search_missing,
        radarr_search_upgrades=radarr_search_upgrades,
        radarr_max_items_per_run=radarr_max_items_per_run,
        interval_minutes=interval_minutes,
    )

    row = await _get_or_create_settings(session)
    # Keep Arr + global settings isolated from Emby settings.
    row.sonarr_enabled = data.sonarr_enabled
    row.sonarr_url = data.sonarr_url
    row.sonarr_api_key = data.sonarr_api_key
    row.sonarr_search_missing = data.sonarr_search_missing
    row.sonarr_search_upgrades = data.sonarr_search_upgrades
    row.sonarr_max_items_per_run = data.sonarr_max_items_per_run

    row.radarr_enabled = data.radarr_enabled
    row.radarr_url = data.radarr_url
    row.radarr_api_key = data.radarr_api_key
    row.radarr_search_missing = data.radarr_search_missing
    row.radarr_search_upgrades = data.radarr_search_upgrades
    row.radarr_max_items_per_run = data.radarr_max_items_per_run

    row.interval_minutes = data.interval_minutes

    # Per-app schedules
    row.sonarr_schedule_enabled = sonarr_schedule_enabled
    row.sonarr_schedule_days = (sonarr_schedule_days or "Mon,Tue,Wed,Thu,Fri,Sat,Sun").strip()
    row.sonarr_schedule_start = _normalize_hhmm(sonarr_schedule_start, "00:00")
    row.sonarr_schedule_end = _normalize_hhmm(sonarr_schedule_end, "23:59")

    row.radarr_schedule_enabled = radarr_schedule_enabled
    row.radarr_schedule_days = (radarr_schedule_days or "Mon,Tue,Wed,Thu,Fri,Sat,Sun").strip()
    row.radarr_schedule_start = _normalize_hhmm(radarr_schedule_start, "00:00")
    row.radarr_schedule_end = _normalize_hhmm(radarr_schedule_end, "23:59")

    row.timezone = _resolve_timezone_name(timezone)
    row.updated_at = datetime.utcnow()
    await session.commit()

    await scheduler.reschedule()
    return RedirectResponse("/settings?saved=1", status_code=303)


@app.post("/emby/settings")
async def save_emby_settings(
    emby_enabled: bool = Form(False),
    emby_url: str = Form(""),
    emby_api_key: str = Form(""),
    emby_user_id: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    # Backward-compatible endpoint: save both sections if old form posts here.
    row = await _get_or_create_settings(session)
    row.emby_enabled = emby_enabled
    row.emby_url = _normalize_base_url(emby_url)
    row.emby_api_key = emby_api_key.strip()
    row.emby_user_id = emby_user_id.strip()
    row.updated_at = datetime.utcnow()
    await session.commit()
    await scheduler.reschedule()
    return RedirectResponse("/emby/settings?saved=1", status_code=303)


@app.post("/emby/settings/connection")
async def save_emby_connection_settings(
    emby_enabled: bool = Form(False),
    emby_url: str = Form(""),
    emby_api_key: str = Form(""),
    emby_user_id: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await _get_or_create_settings(session)
    row.emby_enabled = emby_enabled
    row.emby_url = _normalize_base_url(emby_url)
    row.emby_api_key = emby_api_key.strip()
    row.emby_user_id = emby_user_id.strip()
    row.updated_at = datetime.utcnow()
    await session.commit()
    await scheduler.reschedule()
    return RedirectResponse("/emby/settings?saved=1", status_code=303)


@app.post("/emby/settings/cleaner")
async def save_cleaner_settings(
    emby_dry_run: bool = Form(False),
    emby_schedule_enabled: bool = Form(False),
    emby_schedule_days: str = Form("Mon,Tue,Wed,Thu,Fri,Sat,Sun"),
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
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await _get_or_create_settings(session)
    row.emby_dry_run = emby_dry_run
    row.emby_schedule_enabled = emby_schedule_enabled
    row.emby_schedule_days = (emby_schedule_days or "Mon,Tue,Wed,Thu,Fri,Sat,Sun").strip()
    row.emby_schedule_start = _normalize_hhmm(emby_schedule_start, "00:00")
    row.emby_schedule_end = _normalize_hhmm(emby_schedule_end, "23:59")
    _scan = int(emby_max_items_scan)
    row.emby_max_items_scan = 0 if _scan <= 0 else max(1, min(100_000, _scan))
    row.emby_max_deletes_per_run = max(1, min(500, int(emby_max_deletes_per_run or 25)))
    row.emby_rule_movie_watched_rating_below = max(0, min(10, int(emby_rule_movie_watched_rating_below or 0)))
    row.emby_rule_movie_unwatched_days = max(0, min(36500, int(emby_rule_movie_unwatched_days or 0)))
    selected_genres = sorted({str(v).strip() for v in (emby_rule_movie_genres or []) if str(v).strip()})
    row.emby_rule_movie_genres_csv = ",".join(selected_genres)
    row.emby_rule_movie_people_csv = (emby_rule_movie_people or "").strip()[:8000]
    row.emby_rule_movie_people_credit_types_csv = _people_credit_types_csv_from_form(emby_rule_movie_people_credit_types)
    row.emby_rule_tv_delete_watched = emby_rule_tv_delete_watched
    selected_tv_genres = sorted({str(v).strip() for v in (emby_rule_tv_genres or []) if str(v).strip()})
    row.emby_rule_tv_genres_csv = ",".join(selected_tv_genres)
    row.emby_rule_tv_people_csv = (emby_rule_tv_people or "").strip()[:8000]
    row.emby_rule_tv_people_credit_types_csv = _people_credit_types_csv_from_form(emby_rule_tv_people_credit_types)
    row.emby_rule_tv_watched_rating_below = 0
    row.emby_rule_tv_unwatched_days = max(0, min(36500, int(emby_rule_tv_unwatched_days or 0)))
    # Keep legacy global fields in sync for backward compatibility.
    row.emby_rule_watched_rating_below = max(
        row.emby_rule_movie_watched_rating_below,
        0,
    )
    row.emby_rule_unwatched_days = max(
        row.emby_rule_movie_unwatched_days,
        row.emby_rule_tv_unwatched_days,
    )
    row.updated_at = datetime.utcnow()
    await session.commit()
    await scheduler.reschedule()
    return RedirectResponse("/emby/settings?saved=1", status_code=303)


@app.post("/test/sonarr")
async def test_sonarr(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    try:
        c = ArrClient(ArrConfig(settings.sonarr_url, settings.sonarr_api_key))
        try:
            await c.health()
        finally:
            await c.aclose()
        session.add(AppSnapshot(app="sonarr", ok=True, status_message="Test OK", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=sonarr_ok", status_code=303)
    except httpx.HTTPError as e:
        session.add(AppSnapshot(app="sonarr", ok=False, status_message=f"Test failed: {type(e).__name__}: {e}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=sonarr_fail", status_code=303)


@app.post("/test/radarr")
async def test_radarr(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    try:
        c = ArrClient(ArrConfig(settings.radarr_url, settings.radarr_api_key))
        try:
            await c.health()
        finally:
            await c.aclose()
        session.add(AppSnapshot(app="radarr", ok=True, status_message="Test OK", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=radarr_ok", status_code=303)
    except httpx.HTTPError as e:
        session.add(AppSnapshot(app="radarr", ok=False, status_message=f"Test failed: {type(e).__name__}: {e}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/settings?test=radarr_fail", status_code=303)


@app.post("/test/emby")
async def test_emby(session: AsyncSession = Depends(get_session)) -> RedirectResponse:
    settings = await _get_or_create_settings(session)
    emby_url = _normalize_base_url(settings.emby_url)
    emby_api_key = (settings.emby_api_key or "").strip()
    if not emby_url:
        session.add(AppSnapshot(app="emby", ok=False, status_message="Test failed: Emby URL is required.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)
    if not emby_api_key:
        session.add(AppSnapshot(app="emby", ok=False, status_message="Test failed: Emby API key is required.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)
    if _looks_like_url(emby_api_key):
        session.add(
            AppSnapshot(
                app="emby",
                ok=False,
                status_message="Test failed: Emby API key looks like a URL. Paste the key from Emby Dashboard -> Advanced -> API Keys.",
                missing_total=0,
                cutoff_unmet_total=0,
            )
        )
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)
    try:
        c = EmbyClient(EmbyConfig(emby_url, emby_api_key))
        try:
            await c.health()
            if settings.emby_user_id:
                users = await c.users()
                ok = any(str(u.get("Id", "")) == settings.emby_user_id for u in users)
                if not ok:
                    raise ValueError("Configured Emby User ID was not found.")
        finally:
            await c.aclose()
        session.add(AppSnapshot(app="emby", ok=True, status_message="Test OK", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_ok", status_code=303)
    except httpx.HTTPStatusError as e:
        detail = f"HTTP {e.response.status_code}: {e}"
        if e.response.status_code in (401, 403):
            detail += " | Check Emby API key permissions and base URL."
        session.add(AppSnapshot(app="emby", ok=False, status_message=f"Test failed: {detail}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)
    except (httpx.HTTPError, ValueError) as e:
        session.add(AppSnapshot(app="emby", ok=False, status_message=f"Test failed: {type(e).__name__}: {e}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)


@app.post("/test/emby-form")
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
        session.add(AppSnapshot(app="emby", ok=False, status_message="Test failed: Emby URL is required.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)
    if not emby_api_key_n:
        session.add(AppSnapshot(app="emby", ok=False, status_message="Test failed: Emby API key is required.", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)
    if _looks_like_url(emby_api_key_n):
        session.add(
            AppSnapshot(
                app="emby",
                ok=False,
                status_message="Test failed: Emby API key looks like a URL. Paste the key from Emby Dashboard -> Advanced -> API Keys.",
                missing_total=0,
                cutoff_unmet_total=0,
            )
        )
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)
    # Persist entered connection values so users don't lose them after testing.
    row = await _get_or_create_settings(session)
    row.emby_enabled = emby_enabled
    row.emby_url = emby_url_n
    row.emby_api_key = emby_api_key_n
    row.emby_user_id = emby_user_id_n
    row.updated_at = datetime.utcnow()
    await session.commit()
    try:
        c = EmbyClient(EmbyConfig(emby_url_n, emby_api_key_n))
        try:
            await c.health()
            if emby_user_id_n:
                users = await c.users()
                ok = any(str(u.get("Id", "")) == emby_user_id_n for u in users)
                if not ok:
                    raise ValueError("Configured Emby User ID was not found.")
        finally:
            await c.aclose()
        session.add(AppSnapshot(app="emby", ok=True, status_message="Test OK", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_ok", status_code=303)
    except httpx.HTTPStatusError as e:
        detail = f"HTTP {e.response.status_code}: {e}"
        if e.response.status_code in (401, 403):
            detail += " | Check Emby API key permissions and base URL."
        session.add(AppSnapshot(app="emby", ok=False, status_message=f"Test failed: {detail}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)
    except (httpx.HTTPError, ValueError) as e:
        session.add(AppSnapshot(app="emby", ok=False, status_message=f"Test failed: {type(e).__name__}: {e}", missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RedirectResponse("/emby/settings?test=emby_fail", status_code=303)

