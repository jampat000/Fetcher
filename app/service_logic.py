from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re

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


def _norm_title(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").strip().lower())


def _safe_int(v: object) -> int | None:
    try:
        n = int(str(v).strip())
        return n if n > 0 else None
    except Exception:
        return None


def _emby_provider_id(item: dict, key: str) -> str:
    providers = item.get("ProviderIds") if isinstance(item.get("ProviderIds"), dict) else {}
    return str(providers.get(key) or "").strip()


def _emby_year(item: dict) -> int | None:
    y = _safe_int(item.get("ProductionYear"))
    if y:
        return y
    pd = str(item.get("PremiereDate") or "").strip()
    if len(pd) >= 4 and pd[:4].isdigit():
        return int(pd[:4])
    return None


def _match_radarr_movie_id(emby_item: dict, radarr_movies: list[dict]) -> int | None:
    emby_tmdb = _safe_int(_emby_provider_id(emby_item, "Tmdb"))
    emby_imdb = _emby_provider_id(emby_item, "Imdb").lower()
    emby_title = _norm_title(str(emby_item.get("Name") or ""))
    emby_year = _emby_year(emby_item)

    if emby_tmdb:
        for m in radarr_movies:
            if _safe_int(m.get("tmdbId")) == emby_tmdb:
                return _safe_int(m.get("id"))
    if emby_imdb:
        for m in radarr_movies:
            if str(m.get("imdbId") or "").strip().lower() == emby_imdb:
                return _safe_int(m.get("id"))
    for m in radarr_movies:
        if _norm_title(str(m.get("title") or "")) != emby_title:
            continue
        my = _safe_int(m.get("year"))
        if emby_year is None or my is None or emby_year == my:
            return _safe_int(m.get("id"))
    return None


def _match_sonarr_series_id(emby_item: dict, sonarr_series: list[dict]) -> int | None:
    emby_tvdb = _safe_int(_emby_provider_id(emby_item, "Tvdb"))
    emby_title = _norm_title(str(emby_item.get("Name") or ""))
    emby_year = _emby_year(emby_item)

    if emby_tvdb:
        for s in sonarr_series:
            if _safe_int(s.get("tvdbId")) == emby_tvdb:
                return _safe_int(s.get("id"))
    for s in sonarr_series:
        if _norm_title(str(s.get("title") or "")) != emby_title:
            continue
        sy = _safe_int(s.get("year"))
        if emby_year is None or sy is None or emby_year == sy:
            return _safe_int(s.get("id"))
    return None


def _episode_ids_for_emby_tv_item(emby_item: dict, sonarr_episodes: list[dict]) -> list[int]:
    """Map an Emby TV candidate to Sonarr episode ids (episode/season/series scopes)."""
    item_type = str(emby_item.get("Type") or "").strip()
    if item_type == "Series":
        return [int(e.get("id")) for e in sonarr_episodes if _safe_int(e.get("id"))]

    season_no = _safe_int(emby_item.get("ParentIndexNumber"))
    episode_no = _safe_int(emby_item.get("IndexNumber"))
    episode_end = _safe_int(emby_item.get("IndexNumberEnd")) or episode_no

    # Some Emby payloads use ParentIndexNumber as season for Season items.
    if item_type == "Season":
        if season_no is None:
            season_no = _safe_int(emby_item.get("IndexNumber"))
        if season_no is None:
            return []
        return [
            int(e.get("id"))
            for e in sonarr_episodes
            if _safe_int(e.get("id")) and _safe_int(e.get("seasonNumber")) == season_no
        ]

    if item_type == "Episode":
        if season_no is None or episode_no is None:
            return []
        out: list[int] = []
        lo = min(episode_no, episode_end or episode_no)
        hi = max(episode_no, episode_end or episode_no)
        for e in sonarr_episodes:
            eid = _safe_int(e.get("id"))
            if not eid:
                continue
            if _safe_int(e.get("seasonNumber")) != season_no:
                continue
            e_no = _safe_int(e.get("episodeNumber"))
            if e_no is None:
                continue
            if lo <= e_no <= hi:
                out.append(eid)
        return out

    return []


def _sonarr_episode_label(rec: dict) -> str:
    series_obj = rec.get("series") if isinstance(rec.get("series"), dict) else {}
    title = str(
        rec.get("seriesTitle")
        or series_obj.get("title")
        or rec.get("seriesName")
        or rec.get("title")
        or ""
    ).strip()
    season = _safe_int(rec.get("seasonNumber")) or 0
    ep_no = _safe_int(rec.get("episodeNumber")) or _safe_int(rec.get("episodeNumberStart")) or 0
    ep_end = _safe_int(rec.get("episodeNumberEnd")) or ep_no
    code = f"S{season:02d}E{ep_no:02d}" if season > 0 and ep_no > 0 else "Episode"
    if ep_end and ep_end != ep_no:
        code = f"{code}-E{ep_end:02d}"
    episode_title = str(rec.get("title") or "").strip()
    if title and episode_title:
        return f"{title} {code} - {episode_title}"
    if title:
        return f"{title} {code}"
    return episode_title or code


def _radarr_movie_label(rec: dict) -> str:
    title = str(rec.get("title") or "").strip() or "Movie"
    year = _safe_int(rec.get("year"))
    if year:
        return f"{title} ({year})"
    return title


def _detail_from_labels(labels: list[str], *, total: int) -> str:
    uniq = [x for x in labels if x]
    if not uniq:
        return ""
    max_items = 8
    shown = uniq[:max_items]
    remain = max(0, total - len(shown))
    if remain > 0:
        return ", ".join(shown) + f", +{remain} more"
    return ", ".join(shown)


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
                        missing_records = missing.get("records", []) or []
                        missing_total = int(missing.get("totalRecords") or 0)
                        ids = _take_int_ids(missing_records, "episodeId", "id", limit=sonarr_limit)
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
                            labels = [_sonarr_episode_label(r) for r in missing_records[: len(ids)]]
                            session.add(
                                ActivityLog(
                                    job_run_id=log.id,
                                    app="sonarr",
                                    kind="missing",
                                    status="ok",
                                    count=len(ids),
                                    detail=_detail_from_labels(labels, total=len(ids)),
                                )
                            )
                        else:
                            actions.append("Sonarr: no missing episodes found")

                    if sonarr_upgrades_enabled:
                        cutoff = await sonarr.wanted_cutoff_unmet(page=1, page_size=min(100, sonarr_limit))
                        cutoff_records = cutoff.get("records", []) or []
                        cutoff_total = int(cutoff.get("totalRecords") or 0)
                        ids = _take_int_ids(cutoff_records, "episodeId", "id", limit=sonarr_limit)
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
                            labels = [_sonarr_episode_label(r) for r in cutoff_records[: len(ids)]]
                            session.add(
                                ActivityLog(
                                    job_run_id=log.id,
                                    app="sonarr",
                                    kind="upgrade",
                                    status="ok",
                                    count=len(ids),
                                    detail=_detail_from_labels(labels, total=len(ids)),
                                )
                            )
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
                        missing_records = missing.get("records", []) or []
                        missing_total = int(missing.get("totalRecords") or 0)
                        ids = _take_int_ids(missing_records, "movieId", "id", limit=radarr_limit)
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
                            labels = [_radarr_movie_label(r) for r in missing_records[: len(ids)]]
                            session.add(
                                ActivityLog(
                                    job_run_id=log.id,
                                    app="radarr",
                                    kind="missing",
                                    status="ok",
                                    count=len(ids),
                                    detail=_detail_from_labels(labels, total=len(ids)),
                                )
                            )
                        else:
                            actions.append("Radarr: no missing movies found")

                    if radarr_upgrades_enabled:
                        cutoff = await radarr.wanted_cutoff_unmet(page=1, page_size=min(100, radarr_limit))
                        cutoff_records = cutoff.get("records", []) or []
                        cutoff_total = int(cutoff.get("totalRecords") or 0)
                        ids = _take_int_ids(cutoff_records, "movieId", "id", limit=radarr_limit)
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
                            labels = [_radarr_movie_label(r) for r in cutoff_records[: len(ids)]]
                            session.add(
                                ActivityLog(
                                    job_run_id=log.id,
                                    app="radarr",
                                    kind="upgrade",
                                    status="ok",
                                    count=len(ids),
                                    detail=_detail_from_labels(labels, total=len(ids)),
                                )
                            )
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
                        candidates: list[tuple[str, str, str, dict]] = []
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
                                item_type = str(item.get("Type", "") or "").strip()
                                candidates.append((item_id, name, item_type, item))
                                if len(candidates) >= max_deletes:
                                    break

                        if dry_run:
                            actions.append(f"Emby: dry-run matched {len(candidates)} item(s)")
                        else:
                            movie_candidates = [raw for _, _, t, raw in candidates if t == "Movie"]
                            tv_candidates = [raw for _, _, t, raw in candidates if t in {"Series", "Season", "Episode"}]

                            if movie_candidates and settings.radarr_url and settings.radarr_api_key:
                                radarr2 = ArrClient(ArrConfig(settings.radarr_url, settings.radarr_api_key))
                                try:
                                    catalog = await radarr2.movies()
                                    movie_ids: list[int] = []
                                    for item in movie_candidates:
                                        mid = _match_radarr_movie_id(item, catalog)
                                        if mid and mid not in movie_ids:
                                            movie_ids.append(mid)
                                    if movie_ids:
                                        await radarr2.unmonitor_movies(movie_ids=movie_ids)
                                    actions.append(
                                        f"Radarr: unmonitored {len(movie_ids)}/{len(movie_candidates)} movie(s) after Emby delete match"
                                    )
                                except Exception as e:
                                    actions.append(f"Radarr: unmonitor warning after Emby deletes: {format_http_error_detail(e)}")
                                finally:
                                    await radarr2.aclose()

                            if tv_candidates and settings.sonarr_url and settings.sonarr_api_key:
                                sonarr2 = ArrClient(ArrConfig(settings.sonarr_url, settings.sonarr_api_key))
                                try:
                                    catalog = await sonarr2.series()
                                    episode_ids: list[int] = []
                                    seen_episode_ids: set[int] = set()
                                    episodes_cache: dict[int, list[dict]] = {}
                                    for item in tv_candidates:
                                        sid = _match_sonarr_series_id(item, catalog)
                                        if not sid:
                                            continue
                                        if sid not in episodes_cache:
                                            episodes_cache[sid] = await sonarr2.episodes_for_series(series_id=sid)
                                        for eid in _episode_ids_for_emby_tv_item(item, episodes_cache[sid]):
                                            if eid not in seen_episode_ids:
                                                seen_episode_ids.add(eid)
                                                episode_ids.append(eid)
                                    if episode_ids:
                                        await sonarr2.unmonitor_episodes(episode_ids=episode_ids)
                                    actions.append(
                                        f"Sonarr: unmonitored {len(episode_ids)} episode(s) for {len(tv_candidates)} TV delete candidate(s)"
                                    )
                                except Exception as e:
                                    actions.append(f"Sonarr: unmonitor warning after Emby deletes: {format_http_error_detail(e)}")
                                finally:
                                    await sonarr2.aclose()

                            for item_id, _, _, _ in candidates:
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
                                status="ok",
                                count=len(candidates),
                                detail=_detail_from_labels([name for _, name, _, _ in candidates], total=len(candidates)),
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
            session.add(
                ActivityLog(
                    job_run_id=log.id,
                    app=app,
                    kind="error",
                    status="failed",
                    count=0,
                    detail=(log.message or "")[:500],
                )
            )
        await session.commit()
        return RunResult(ok=False, message=log.message)
    except Exception as e:  # noqa: BLE001 - service boundary logging
        log.ok = False
        log.message = f"Run failed: {type(e).__name__}: {e}"
        log.finished_at = utc_now_naive()
        session.add(
            ActivityLog(
                job_run_id=log.id,
                app="service",
                kind="error",
                status="failed",
                count=0,
                detail=(log.message or "")[:500],
            )
        )
        await session.commit()
        return RunResult(ok=False, message=log.message)

