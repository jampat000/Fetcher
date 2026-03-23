from __future__ import annotations

import logging
import os
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_csrf_token_for_template, require_csrf
from app.branding import APP_NAME, APP_TAGLINE
from app.constants import _MOVIE_GENRE_OPTIONS, _PEOPLE_CREDIT_OPTIONS
from app.db import _get_or_create_settings, fetch_latest_app_snapshots, get_session
from app.display_helpers import _normalize_hhmm, _now_local, _time_select_orphan
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
from app.form_helpers import _looks_like_url, _normalize_base_url, _people_credit_types_csv_from_form
from app.models import AppSnapshot
from app.resolvers.api_keys import resolve_emby_api_key, resolve_radarr_api_key, resolve_sonarr_api_key
from app.schedule import normalize_schedule_days_csv, schedule_time_dropdown_choices
from app.service_logic import apply_emby_trimmer_live_deletes
from app.time_util import utc_now_naive
from app.ui_templates import templates
from app.web_common import (
    effective_emby_rules,
    movie_credit_types_summary,
    schedule_days_csv_from_named_day_checks,
    schedule_weekdays_selected_dict,
    trimmer_settings_redirect_url,
    try_commit_and_reschedule,
)

from app.routers.deps import AUTH_DEPS, AUTH_FORM_DEPS

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=AUTH_DEPS)


@router.get("/trimmer/settings", response_class=HTMLResponse)
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
            "emby_schedule_days_selected": schedule_weekdays_selected_dict(
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


@router.get("/trimmer", response_class=HTMLResponse)
async def trimmer_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    settings = await _get_or_create_settings(session)
    tz = settings.timezone or "UTC"
    rows: list[dict] = []
    error = ""
    used_user_id = (settings.emby_user_id or "").strip()
    used_user_name = ""

    rules = effective_emby_rules(settings)
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
            "movie_people_credit_summary": movie_credit_types_summary(selected_movie_credit_types),
            "selected_tv_people_display": selected_tv_people,
            "tv_people_credit_summary": movie_credit_types_summary(selected_tv_credit_types),
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


@router.post("/trimmer/settings", dependencies=AUTH_FORM_DEPS)
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
        if not await try_commit_and_reschedule(session):
            return RedirectResponse(
                trimmer_settings_redirect_url(saved=False, reason="db_busy", section="connection"),
                status_code=303,
            )
        return RedirectResponse(trimmer_settings_redirect_url(saved=True, section="connection"), status_code=303)
    except SQLAlchemyError:
        logger.exception("POST /trimmer/settings SQLAlchemyError")
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="db_error", section="connection"),
            status_code=303,
        )
    except ValueError:
        logger.exception("POST /trimmer/settings ValueError")
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="invalid", section="connection"),
            status_code=303,
        )
    except Exception:
        logger.exception("POST /trimmer/settings failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="error", section="connection"),
            status_code=303,
        )


@router.post("/trimmer/settings/connection", dependencies=AUTH_FORM_DEPS)
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
        if not await try_commit_and_reschedule(session):
            return RedirectResponse(
                trimmer_settings_redirect_url(saved=False, reason="db_busy", section="connection"),
                status_code=303,
            )
        return RedirectResponse(trimmer_settings_redirect_url(saved=True, section="connection"), status_code=303)
    except SQLAlchemyError:
        logger.exception("POST /trimmer/settings/connection SQLAlchemyError")
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="db_error", section="connection"),
            status_code=303,
        )
    except ValueError:
        logger.exception("POST /trimmer/settings/connection ValueError")
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="invalid", section="connection"),
            status_code=303,
        )
    except Exception:
        logger.exception("POST /trimmer/settings/connection failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="error", section="connection"),
            status_code=303,
        )


@router.post("/trimmer/settings/cleaner", dependencies=AUTH_FORM_DEPS)
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
    trimmer_section: Annotated[str | None, Query()] = None,
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
        row.emby_schedule_days = schedule_days_csv_from_named_day_checks(
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
        if not await try_commit_and_reschedule(session):
            return RedirectResponse(
                trimmer_settings_redirect_url(saved=False, reason="db_busy", section=trimmer_section),
                status_code=303,
            )
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=True, section=trimmer_section),
            status_code=303,
        )
    except SQLAlchemyError:
        logger.exception("POST /trimmer/settings/cleaner SQLAlchemyError")
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="db_error", section=trimmer_section),
            status_code=303,
        )
    except ValueError:
        logger.exception("POST /trimmer/settings/cleaner ValueError")
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="invalid", section=trimmer_section),
            status_code=303,
        )
    except Exception:
        logger.exception("POST /trimmer/settings/cleaner failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").strip().upper() == "DEBUG":
            raise
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="error", section=trimmer_section),
            status_code=303,
        )


@router.post("/test/emby", dependencies=AUTH_FORM_DEPS)
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


@router.post("/test/emby-form", dependencies=AUTH_FORM_DEPS)
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
