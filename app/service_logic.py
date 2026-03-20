from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.arr_client import (
    ArrClient,
    ArrConfig,
    trigger_radarr_cutoff_search,
    trigger_radarr_missing_search,
    trigger_sonarr_cutoff_search,
    trigger_sonarr_missing_search,
)
from app.http_status_hints import format_http_error_detail, hint_for_http_status
from app.log_sanitize import redact_url_for_logging
from app.models import ActivityLog, AppSettings, AppSnapshot, JobRunLog
from app.schedule import in_window
from app.time_util import utc_now_naive
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


@dataclass(frozen=True)
class RunResult:
    ok: bool
    message: str


async def _get_or_create_settings(session: AsyncSession) -> AppSettings:
    row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
    if row:
        return row
    row = AppSettings()
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


def _take_int_ids(records: list[dict], *keys: str, limit: int) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for r in records:
        for k in keys:
            v = r.get(k)
            if isinstance(v, int) and v > 0 and v not in seen:
                seen.add(v)
                out.append(v)
                break
        if len(out) >= limit:
            return out
    return out


def _sonarr_series_ids_for_episode_batch(records: list[dict], *episode_keys: str, limit: int) -> list[int]:
    """Unique seriesIds for the first `limit` episodes (same walk order as _take_int_ids)."""
    series_out: list[int] = []
    seen_series: set[int] = set()
    taken = 0
    for r in records:
        if taken >= limit:
            break
        ep_id: int | None = None
        for k in episode_keys:
            v = r.get(k)
            if isinstance(v, int) and v > 0:
                ep_id = v
                break
        if ep_id is None:
            continue
        taken += 1
        sid = r.get("seriesId")
        if isinstance(sid, int) and sid > 0 and sid not in seen_series:
            seen_series.add(sid)
            series_out.append(sid)
    return series_out


async def run_once(session: AsyncSession) -> RunResult:
    log = JobRunLog(started_at=utc_now_naive(), ok=False, message="")
    session.add(log)
    await session.commit()
    await session.refresh(log)

    try:
        settings = await _get_or_create_settings(session)

        actions: list[str] = []
        default_limit = max(1, int(settings.max_items_per_run or 50))

        tz = (getattr(settings, "timezone", None) or "UTC").strip() or "UTC"

        # Sonarr
        if settings.sonarr_enabled and settings.sonarr_url and settings.sonarr_api_key:
            if not in_window(
                schedule_enabled=getattr(settings, "sonarr_schedule_enabled", False),
                schedule_days=getattr(settings, "sonarr_schedule_days", "Mon,Tue,Wed,Thu,Fri,Sat,Sun"),
                schedule_start=getattr(settings, "sonarr_schedule_start", "00:00"),
                schedule_end=getattr(settings, "sonarr_schedule_end", "23:59"),
                timezone=tz,
            ):
                actions.append("Sonarr: skipped (outside schedule window)")
            else:
                sonarr = ArrClient(ArrConfig(settings.sonarr_url, settings.sonarr_api_key))
                try:
                    await sonarr.health()

                    sonarr_limit = max(1, int((settings.sonarr_max_items_per_run or 0) or default_limit))
                    sonarr_missing_enabled = bool(getattr(settings, "sonarr_search_missing", settings.search_missing))
                    sonarr_upgrades_enabled = bool(getattr(settings, "sonarr_search_upgrades", settings.search_upgrades))

                    missing_total = 0
                    cutoff_total = 0

                    if sonarr_missing_enabled:
                        missing = await sonarr.wanted_missing(page=1, page_size=min(100, sonarr_limit))
                        missing_total = int(missing.get("totalRecords") or 0)
                        ids = _take_int_ids(missing.get("records", []) or [], "episodeId", "id", limit=sonarr_limit)
                        if ids:
                            try:
                                tag_id = await sonarr.ensure_tag("grabby-missing")
                                series_ids = _sonarr_series_ids_for_episode_batch(
                                    missing.get("records", []) or [],
                                    "episodeId",
                                    "id",
                                    limit=sonarr_limit,
                                )
                                await sonarr.add_tags_to_series(series_ids=series_ids, tag_ids=[tag_id])
                            except Exception as e:  # noqa: BLE001 - tag failure should not block searches
                                actions.append(
                                    f"Sonarr: tag apply warning (grabby-missing): {format_http_error_detail(e)}"
                                )
                            await trigger_sonarr_missing_search(sonarr, episode_ids=ids)
                            actions.append(f"Sonarr: missing search for {len(ids)} episode(s)")
                            session.add(ActivityLog(job_run_id=log.id, app="sonarr", kind="missing", count=len(ids)))
                        else:
                            actions.append("Sonarr: no missing episodes found")

                    if sonarr_upgrades_enabled:
                        cutoff = await sonarr.wanted_cutoff_unmet(page=1, page_size=min(100, sonarr_limit))
                        cutoff_total = int(cutoff.get("totalRecords") or 0)
                        ids = _take_int_ids(cutoff.get("records", []) or [], "episodeId", "id", limit=sonarr_limit)
                        if ids:
                            try:
                                tag_id = await sonarr.ensure_tag("grabby-upgrade")
                                series_ids = _sonarr_series_ids_for_episode_batch(
                                    cutoff.get("records", []) or [],
                                    "episodeId",
                                    "id",
                                    limit=sonarr_limit,
                                )
                                await sonarr.add_tags_to_series(series_ids=series_ids, tag_ids=[tag_id])
                            except Exception as e:  # noqa: BLE001 - tag failure should not block searches
                                actions.append(
                                    f"Sonarr: tag apply warning (grabby-upgrade): {format_http_error_detail(e)}"
                                )
                            await trigger_sonarr_cutoff_search(sonarr, episode_ids=ids)
                            actions.append(f"Sonarr: cutoff-unmet search for {len(ids)} episode(s)")
                            session.add(ActivityLog(job_run_id=log.id, app="sonarr", kind="upgrade", count=len(ids)))
                        else:
                            actions.append("Sonarr: no cutoff-unmet episodes found")

                    session.add(
                        AppSnapshot(
                            app="sonarr",
                            ok=True,
                            status_message="OK",
                            missing_total=missing_total,
                            cutoff_unmet_total=cutoff_total,
                        )
                    )
                finally:
                    await sonarr.aclose()

        # Radarr
        if settings.radarr_enabled and settings.radarr_url and settings.radarr_api_key:
            if not in_window(
                schedule_enabled=getattr(settings, "radarr_schedule_enabled", False),
                schedule_days=getattr(settings, "radarr_schedule_days", "Mon,Tue,Wed,Thu,Fri,Sat,Sun"),
                schedule_start=getattr(settings, "radarr_schedule_start", "00:00"),
                schedule_end=getattr(settings, "radarr_schedule_end", "23:59"),
                timezone=tz,
            ):
                actions.append("Radarr: skipped (outside schedule window)")
            else:
                radarr = ArrClient(ArrConfig(settings.radarr_url, settings.radarr_api_key))
                try:
                    await radarr.health()

                    radarr_limit = max(1, int((settings.radarr_max_items_per_run or 0) or default_limit))
                    radarr_missing_enabled = bool(getattr(settings, "radarr_search_missing", settings.search_missing))
                    radarr_upgrades_enabled = bool(getattr(settings, "radarr_search_upgrades", settings.search_upgrades))

                    missing_total = 0
                    cutoff_total = 0

                    if radarr_missing_enabled:
                        missing = await radarr.wanted_missing(page=1, page_size=min(100, radarr_limit))
                        missing_total = int(missing.get("totalRecords") or 0)
                        ids = _take_int_ids(missing.get("records", []) or [], "movieId", "id", limit=radarr_limit)
                        if ids:
                            try:
                                tag_id = await radarr.ensure_tag("grabby-missing")
                                await radarr.add_tags_to_movies(movie_ids=ids, tag_ids=[tag_id])
                            except Exception as e:  # noqa: BLE001 - tag failure should not block searches
                                actions.append(
                                    f"Radarr: tag apply warning (grabby-missing): {format_http_error_detail(e)}"
                                )
                            await trigger_radarr_missing_search(radarr, movie_ids=ids)
                            actions.append(f"Radarr: missing search for {len(ids)} movie(s)")
                            session.add(ActivityLog(job_run_id=log.id, app="radarr", kind="missing", count=len(ids)))
                        else:
                            actions.append("Radarr: no missing movies found")

                    if radarr_upgrades_enabled:
                        cutoff = await radarr.wanted_cutoff_unmet(page=1, page_size=min(100, radarr_limit))
                        cutoff_total = int(cutoff.get("totalRecords") or 0)
                        ids = _take_int_ids(cutoff.get("records", []) or [], "movieId", "id", limit=radarr_limit)
                        if ids:
                            try:
                                tag_id = await radarr.ensure_tag("grabby-upgrade")
                                await radarr.add_tags_to_movies(movie_ids=ids, tag_ids=[tag_id])
                            except Exception as e:  # noqa: BLE001 - tag failure should not block searches
                                actions.append(
                                    f"Radarr: tag apply warning (grabby-upgrade): {format_http_error_detail(e)}"
                                )
                            await trigger_radarr_cutoff_search(radarr, movie_ids=ids)
                            actions.append(f"Radarr: cutoff-unmet search for {len(ids)} movie(s)")
                            session.add(ActivityLog(job_run_id=log.id, app="radarr", kind="upgrade", count=len(ids)))
                        else:
                            actions.append("Radarr: no cutoff-unmet movies found")

                    session.add(
                        AppSnapshot(
                            app="radarr",
                            ok=True,
                            status_message="OK",
                            missing_total=missing_total,
                            cutoff_unmet_total=cutoff_total,
                        )
                    )
                finally:
                    await radarr.aclose()

        # Emby Cleaner
        if settings.emby_enabled and settings.emby_url and settings.emby_api_key:
            if not in_window(
                schedule_enabled=getattr(settings, "emby_schedule_enabled", False),
                schedule_days=getattr(settings, "emby_schedule_days", "Mon,Tue,Wed,Thu,Fri,Sat,Sun"),
                schedule_start=getattr(settings, "emby_schedule_start", "00:00"),
                schedule_end=getattr(settings, "emby_schedule_end", "23:59"),
                timezone=tz,
            ):
                actions.append("Emby: skipped (outside schedule window)")
            else:
                emby = EmbyClient(EmbyConfig(settings.emby_url, settings.emby_api_key))
                try:
                    await emby.health()
                    users = await emby.users()
                    configured_user_id = (settings.emby_user_id or "").strip()
                    effective_user_id = configured_user_id
                    if not effective_user_id and users:
                        first_user = users[0]
                        effective_user_id = str(first_user.get("Id", "")).strip()
                        actions.append("Emby: no user configured, using first Emby user")
                    if not effective_user_id:
                        raise ValueError("Emby has no users available to query.")

                    _v_scan = getattr(settings, "emby_max_items_scan", 2000)
                    _raw_scan = int(_v_scan) if _v_scan is not None else 2000
                    scan_limit = 0 if _raw_scan <= 0 else max(1, min(100_000, _raw_scan))
                    max_deletes = max(1, int(getattr(settings, "emby_max_deletes_per_run", 25) or 25))
                    global_rating = max(0, int(getattr(settings, "emby_rule_watched_rating_below", 0) or 0))
                    global_unwatched = max(0, int(getattr(settings, "emby_rule_unwatched_days", 0) or 0))
                    movie_rating_below = max(0, int(getattr(settings, "emby_rule_movie_watched_rating_below", 0) or 0)) or global_rating
                    movie_unwatched_days = max(0, int(getattr(settings, "emby_rule_movie_unwatched_days", 0) or 0)) or global_unwatched
                    selected_movie_genres = parse_genres_csv(getattr(settings, "emby_rule_movie_genres_csv", ""))
                    selected_movie_people = parse_movie_people_phrases(getattr(settings, "emby_rule_movie_people_csv", ""))
                    selected_movie_credit_types = parse_movie_people_credit_types_csv(
                        getattr(settings, "emby_rule_movie_people_credit_types_csv", "Actor")
                    )
                    selected_tv_people = parse_movie_people_phrases(getattr(settings, "emby_rule_tv_people_csv", ""))
                    selected_tv_credit_types = parse_movie_people_credit_types_csv(
                        getattr(settings, "emby_rule_tv_people_credit_types_csv", "Actor")
                    )
                    tv_delete_watched = bool(getattr(settings, "emby_rule_tv_delete_watched", False))
                    selected_tv_genres = parse_genres_csv(getattr(settings, "emby_rule_tv_genres_csv", ""))
                    tv_unwatched_days = max(0, int(getattr(settings, "emby_rule_tv_unwatched_days", 0) or 0)) or global_unwatched
                    dry_run = bool(getattr(settings, "emby_dry_run", True))

                    if movie_rating_below <= 0 and movie_unwatched_days <= 0 and (not tv_delete_watched) and tv_unwatched_days <= 0:
                        actions.append("Emby: skipped (no Emby Cleaner rules enabled)")
                    else:
                        items = await emby.items_for_user(user_id=effective_user_id, limit=scan_limit)
                        candidates: list[tuple[str, str]] = []
                        for item in items:
                            item_id = str(item.get("Id", "")).strip()
                            if not item_id:
                                continue
                            is_candidate, _, _, _, _ = evaluate_candidate(
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
                            if is_candidate:
                                name = str(item.get("Name", "") or item_id)
                                candidates.append((item_id, name))
                                if len(candidates) >= max_deletes:
                                    break

                        if dry_run:
                            actions.append(f"Emby: dry-run matched {len(candidates)} item(s)")
                        else:
                            for item_id, _ in candidates:
                                await emby.delete_item(item_id)
                            actions.append(f"Emby: deleted {len(candidates)} item(s)")

                        session.add(
                            AppSnapshot(
                                app="emby",
                                ok=True,
                                status_message="OK",
                                missing_total=len(candidates),
                                cutoff_unmet_total=0 if dry_run else len(candidates),
                            )
                        )
                        session.add(
                            ActivityLog(
                                job_run_id=log.id,
                                app="emby",
                                kind="cleanup",
                                count=len(candidates),
                            )
                        )
                finally:
                    await emby.aclose()
        elif settings.emby_enabled:
            actions.append("Emby: skipped (missing URL/API key)")

        msg = " | ".join(actions) if actions else "No actions (check enabled flags + URLs + API keys)."
        log.ok = True
        log.message = msg
        log.finished_at = utc_now_naive()
        await session.commit()
        return RunResult(ok=True, message=msg)
    except httpx.HTTPStatusError as e:
        # Include response payload in logs to make Arr-side errors debuggable.
        try:
            body = e.response.text
            if len(body) > 500:
                body = body[:500] + "...(truncated)"
        except Exception:
            body = "<unavailable>"
        log.ok = False
        safe_url = redact_url_for_logging(e.request.url)
        code = e.response.status_code
        hint = hint_for_http_status(code)
        hint_suffix = f" {hint}" if hint else ""
        log.message = f"Run failed: HTTP {code} for {e.request.method} {safe_url}{hint_suffix} | {body}"
        log.finished_at = utc_now_naive()
        # Snapshot failure if it’s clearly Sonarr/Radarr/Emby
        url = safe_url
        app = "sonarr" if ":8989" in url else ("radarr" if ":7878" in url else ("emby" if (":8096" in url or ":8920" in url) else ""))
        if app:
            session.add(AppSnapshot(app=app, ok=False, status_message=log.message, missing_total=0, cutoff_unmet_total=0))
        await session.commit()
        return RunResult(ok=False, message=log.message)
    except Exception as e:  # noqa: BLE001 - service boundary logging
        log.ok = False
        log.message = f"Run failed: {type(e).__name__}: {e}"
        log.finished_at = utc_now_naive()
        await session.commit()
        return RunResult(ok=False, message=log.message)

