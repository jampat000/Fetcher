from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re

import httpx
from sqlalchemy import delete, select
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
from app.models import ActivityLog, ArrActionLog, AppSettings, AppSnapshot, JobRunLog
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


def _extract_first_int(rec: dict, *keys: str) -> int | None:
    for k in keys:
        v = rec.get(k)
        if isinstance(v, int) and v > 0:
            return v
        if isinstance(v, str) and v.isdigit():
            n = int(v)
            if n > 0:
                return n
    return None


def _take_records_and_ids(records: list[dict], *keys: str, limit: int) -> tuple[list[int], list[dict]]:
    """Deduplicate by id while preserving Sonarr/Radarr record order."""
    ids_out: list[int] = []
    recs_out: list[dict] = []
    seen: set[int] = set()
    for r in records:
        rid = _extract_first_int(r, *keys)
        if rid is None:
            continue
        if rid in seen:
            continue
        seen.add(rid)
        ids_out.append(rid)
        recs_out.append(r)
        if len(ids_out) >= limit:
            break
    return ids_out, recs_out


async def _filter_ids_by_cooldown(
    session: AsyncSession,
    *,
    app: str,
    action: str,
    item_type: str,
    ids: list[int],
    cooldown_minutes: int,
    now,
    max_apply: int | None = None,
) -> list[int]:
    """Return ids that are not triggered again inside the cooldown window; record those triggers.

    Cooldown is keyed by (app, item_type, item_id) only — not by ``action``. That way a movie
    cannot be hit twice in one Grabby run (e.g. both missing + cutoff-unmet), and Sonarr
    episodes are not double-triggered the same way. The ``action`` field is still stored for logs.

    If ``max_apply`` is set, only the first N passing ids are logged/returned (for paginated
    queues where we must not mark cooldown on items we are not searching this run).
    """
    if not ids:
        return []
    cooldown_minutes = max(1, int(cooldown_minutes or 60))
    window_start = now - timedelta(minutes=cooldown_minutes)

    recent_q = await session.execute(
        select(ArrActionLog.item_id).where(
            ArrActionLog.app == app,
            ArrActionLog.item_type == item_type,
            ArrActionLog.item_id.in_(ids),
            ArrActionLog.created_at >= window_start,
        )
    )
    recent_ids = {int(x) for (x,) in recent_q.all()}
    allowed = [i for i in ids if i not in recent_ids]
    if max_apply is not None and max_apply >= 0:
        allowed = allowed[: int(max_apply)]

    if allowed:
        session.add_all(
            [
                ArrActionLog(
                    created_at=now,
                    app=app,
                    action=action,
                    item_type=item_type,
                    item_id=int(i),
                )
                for i in allowed
            ]
        )
    return allowed


async def _paginate_wanted_for_search(
    client: ArrClient,
    session: AsyncSession,
    *,
    kind: str,
    id_keys: tuple[str, ...],
    item_type: str,
    app: str,
    action: str,
    limit: int,
    cooldown_minutes: int,
    now,
) -> tuple[list[int], list[dict], int]:
    """Walk Sonarr/Radarr wanted pages until we collect up to ``limit`` items that pass cooldown.

    Without this, only *page 1* is considered — the same titles stay at the top of the queue,
    so everything deeper never gets a turn (unlike Huntarr-style “batch through the backlog”).
    """
    limit = max(1, int(limit))
    page_size = min(100, max(50, limit))
    allowed_ids: list[int] = []
    allowed_recs: list[dict] = []
    seen: set[int] = set()
    total_records = 0
    page = 1
    max_pages = 250  # safety: ~25k rows at page_size 100

    fetch = client.wanted_missing if kind == "missing" else client.wanted_cutoff_unmet

    while len(allowed_ids) < limit and page <= max_pages:
        data = await fetch(page=page, page_size=page_size)
        records = data.get("records") or []
        if page == 1:
            total_records = int(data.get("totalRecords") or 0)
        if not records:
            break

        ids_page, recs_page = _take_records_and_ids(records, *id_keys, limit=len(records))
        candidates = [(i, r) for i, r in zip(ids_page, recs_page) if i not in seen]
        if not candidates:
            page += 1
            continue

        batch_ids = [i for i, _ in candidates]
        need = limit - len(allowed_ids)
        newly = await _filter_ids_by_cooldown(
            session,
            app=app,
            action=action,
            item_type=item_type,
            ids=batch_ids,
            cooldown_minutes=cooldown_minutes,
            now=now,
            max_apply=need,
        )
        new_set = set(newly)
        for i, r in candidates:
            if i in new_set:
                seen.add(i)
                allowed_ids.append(i)
                allowed_recs.append(r)
                if len(allowed_ids) >= limit:
                    break
        page += 1

    return allowed_ids, allowed_recs, total_records


async def _prune_action_log(session: AsyncSession, *, older_than_days: int = 7) -> None:
    # Keep DB small; safe since cooldown is time-windowed.
    cutoff = utc_now_naive() - timedelta(days=max(1, int(older_than_days)))
    await session.execute(delete(ArrActionLog).where(ArrActionLog.created_at < cutoff))


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


def _sonarr_series_is_ended(series: dict | None) -> bool:
    """Sonarr series.status is typically 'continuing', 'ended', or 'upcoming'."""
    if not series or not isinstance(series, dict):
        return False
    return str(series.get("status") or "").strip().lower() == "ended"


def _sonarr_episode_file_id(episode: dict) -> int | None:
    fid = _safe_int(episode.get("episodeFileId"))
    if fid:
        return fid
    ef = episode.get("episodeFile")
    if isinstance(ef, dict):
        return _safe_int(ef.get("id"))
    return None


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


def _sonarr_episode_label_with_fallback(rec: dict, series_title_map: dict[int, str]) -> str:
    """Prefer explicit show title from record; fallback to seriesId lookup if needed."""
    base = _sonarr_episode_label(rec)
    # If base has no show context, try series map.
    if base and (" S" in base or base.startswith("Episode")):
        sid = _safe_int(rec.get("seriesId"))
        show = series_title_map.get(sid or 0, "").strip()
        if show:
            return f"{show} {base}"
    return base


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
    max_items = 4
    shown = uniq[:max_items]
    remain = max(0, total - len(shown))
    if remain > 0:
        return "\n".join(shown + [f"+{remain} more"])
    return "\n".join(shown)


def _effective_app_interval_minutes(specific: object, *, global_minutes: int) -> int:
    """Per-app Arr tick length; invalid or less than 1 uses Grabby ``interval_minutes`` base (min 5)."""
    try:
        v = int(specific) if specific is not None else 0
    except (TypeError, ValueError):
        v = 0
    base = max(5, int(global_minutes or 60))
    return max(5, v) if v > 0 else base


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
        interval_m = max(5, int(getattr(settings, "interval_minutes", 60) or 60))
        sonarr_tick_m = _effective_app_interval_minutes(
            getattr(settings, "sonarr_interval_minutes", None), global_minutes=interval_m
        )
        radarr_tick_m = _effective_app_interval_minutes(
            getattr(settings, "radarr_interval_minutes", None), global_minutes=interval_m
        )
        _cd = getattr(settings, "arr_search_cooldown_minutes", None)
        try:
            cd_raw = int(_cd) if _cd is not None else 0
        except (TypeError, ValueError):
            cd_raw = 0
        # 0 = tie Arr cooldown fallback to that app's scheduler interval.
        sonarr_cooldown_minutes = max(
            1, sonarr_tick_m if cd_raw <= 0 else min(cd_raw, 365 * 24 * 60)
        )
        radarr_cooldown_minutes = max(
            1, radarr_tick_m if cd_raw <= 0 else min(cd_raw, 365 * 24 * 60)
        )
        now = utc_now_naive()
        await _prune_action_log(session, older_than_days=14)

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
                last_sonarr = getattr(settings, "sonarr_last_run_at", None)
                if last_sonarr is not None and (now - last_sonarr).total_seconds() < sonarr_tick_m * 60:
                    actions.append("Sonarr: skipped (run interval not elapsed)")
                else:
                    sonarr = ArrClient(ArrConfig(settings.sonarr_url, settings.sonarr_api_key))
                    try:
                        await sonarr.health()
                        sonarr_series_title_map: dict[int, str] = {}
                        try:
                            for s in await sonarr.series():
                                sid = _safe_int(s.get("id"))
                                title = str(s.get("title") or "").strip()
                                if sid and title:
                                    sonarr_series_title_map[sid] = title
                        except Exception:
                            # Keep run resilient if catalog fetch fails; labels still use record data.
                            sonarr_series_title_map = {}

                        sonarr_limit = max(1, int((settings.sonarr_max_items_per_run or 0) or default_limit))
                        sonarr_missing_enabled = bool(getattr(settings, "sonarr_search_missing", settings.search_missing))
                        sonarr_upgrades_enabled = bool(getattr(settings, "sonarr_search_upgrades", settings.search_upgrades))

                        missing_total = 0
                        cutoff_total = 0

                        if sonarr_missing_enabled:
                            allowed_ids, allowed_records, missing_total = await _paginate_wanted_for_search(
                                sonarr,
                                session,
                                kind="missing",
                                id_keys=("episodeId", "id"),
                                item_type="episode",
                                app="sonarr",
                                action="missing",
                                limit=sonarr_limit,
                                cooldown_minutes=sonarr_cooldown_minutes,
                                now=now,
                            )
                            if allowed_ids:
                                # Tagging is best-effort; it should not block the search trigger.
                                try:
                                    tag_id = await sonarr.ensure_tag("grabby-missing")
                                    series_ids = _sonarr_series_ids_for_episode_batch(
                                        allowed_records,
                                        "episodeId",
                                        "id",
                                        limit=len(allowed_records),
                                    )
                                    await sonarr.add_tags_to_series(series_ids=series_ids, tag_ids=[tag_id])
                                except Exception as e:  # noqa: BLE001
                                    actions.append(f"Sonarr: tag apply warning (grabby-missing): {format_http_error_detail(e)}")

                                await trigger_sonarr_missing_search(sonarr, episode_ids=allowed_ids)
                                actions.append(f"Sonarr: missing search for {len(allowed_ids)} episode(s)")
                                labels = [
                                    _sonarr_episode_label_with_fallback(r, sonarr_series_title_map)
                                    for r in allowed_records
                                ]
                                session.add(
                                    ActivityLog(
                                        job_run_id=log.id,
                                        app="sonarr",
                                        kind="missing",
                                        count=len(allowed_ids),
                                        detail=_detail_from_labels(labels, total=len(allowed_ids)),
                                    )
                                )
                            elif missing_total > 0:
                                actions.append("Sonarr: missing search suppressed (cooldown)")
                            else:
                                actions.append("Sonarr: no missing episodes found")

                        if sonarr_upgrades_enabled:
                            allowed_ids, allowed_records, cutoff_total = await _paginate_wanted_for_search(
                                sonarr,
                                session,
                                kind="cutoff",
                                id_keys=("episodeId", "id"),
                                item_type="episode",
                                app="sonarr",
                                action="upgrade",
                                limit=sonarr_limit,
                                cooldown_minutes=sonarr_cooldown_minutes,
                                now=now,
                            )
                            if allowed_ids:
                                try:
                                    tag_id = await sonarr.ensure_tag("grabby-upgrade")
                                    series_ids = _sonarr_series_ids_for_episode_batch(
                                        allowed_records,
                                        "episodeId",
                                        "id",
                                        limit=len(allowed_records),
                                    )
                                    await sonarr.add_tags_to_series(series_ids=series_ids, tag_ids=[tag_id])
                                except Exception as e:  # noqa: BLE001
                                    actions.append(f"Sonarr: tag apply warning (grabby-upgrade): {format_http_error_detail(e)}")

                                await trigger_sonarr_cutoff_search(sonarr, episode_ids=allowed_ids)
                                actions.append(f"Sonarr: cutoff-unmet search for {len(allowed_ids)} episode(s)")
                                labels = [
                                    _sonarr_episode_label_with_fallback(r, sonarr_series_title_map)
                                    for r in allowed_records
                                ]
                                session.add(
                                    ActivityLog(
                                        job_run_id=log.id,
                                        app="sonarr",
                                        kind="upgrade",
                                        count=len(allowed_ids),
                                        detail=_detail_from_labels(labels, total=len(allowed_ids)),
                                    )
                                )
                            elif cutoff_total > 0:
                                actions.append("Sonarr: cutoff-unmet search suppressed (cooldown)")
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
                        settings.sonarr_last_run_at = now
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
                last_radarr = getattr(settings, "radarr_last_run_at", None)
                if last_radarr is not None and (now - last_radarr).total_seconds() < radarr_tick_m * 60:
                    actions.append("Radarr: skipped (run interval not elapsed)")
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
                            allowed_ids, allowed_records, missing_total = await _paginate_wanted_for_search(
                                radarr,
                                session,
                                kind="missing",
                                id_keys=("movieId", "id"),
                                item_type="movie",
                                app="radarr",
                                action="missing",
                                limit=radarr_limit,
                                cooldown_minutes=radarr_cooldown_minutes,
                                now=now,
                            )
                            if allowed_ids:
                                try:
                                    tag_id = await radarr.ensure_tag("grabby-missing")
                                    await radarr.add_tags_to_movies(movie_ids=allowed_ids, tag_ids=[tag_id])
                                except Exception as e:  # noqa: BLE001
                                    actions.append(f"Radarr: tag apply warning (grabby-missing): {format_http_error_detail(e)}")

                                await trigger_radarr_missing_search(radarr, movie_ids=allowed_ids)
                                actions.append(f"Radarr: missing search for {len(allowed_ids)} movie(s)")
                                labels = [_radarr_movie_label(r) for r in allowed_records]
                                session.add(
                                    ActivityLog(
                                        job_run_id=log.id,
                                        app="radarr",
                                        kind="missing",
                                        count=len(allowed_ids),
                                        detail=_detail_from_labels(labels, total=len(allowed_ids)),
                                    )
                                )
                            elif missing_total > 0:
                                actions.append("Radarr: missing search suppressed (cooldown)")
                            else:
                                actions.append("Radarr: no missing movies found")

                        if radarr_upgrades_enabled:
                            allowed_ids, allowed_records, cutoff_total = await _paginate_wanted_for_search(
                                radarr,
                                session,
                                kind="cutoff",
                                id_keys=("movieId", "id"),
                                item_type="movie",
                                app="radarr",
                                action="upgrade",
                                limit=radarr_limit,
                                cooldown_minutes=radarr_cooldown_minutes,
                                now=now,
                            )
                            if allowed_ids:
                                try:
                                    tag_id = await radarr.ensure_tag("grabby-upgrade")
                                    await radarr.add_tags_to_movies(movie_ids=allowed_ids, tag_ids=[tag_id])
                                except Exception as e:  # noqa: BLE001
                                    actions.append(f"Radarr: tag apply warning (grabby-upgrade): {format_http_error_detail(e)}")

                                await trigger_radarr_cutoff_search(radarr, movie_ids=allowed_ids)
                                actions.append(f"Radarr: cutoff-unmet search for {len(allowed_ids)} movie(s)")
                                labels = [_radarr_movie_label(r) for r in allowed_records]
                                session.add(
                                    ActivityLog(
                                        job_run_id=log.id,
                                        app="radarr",
                                        kind="upgrade",
                                        count=len(allowed_ids),
                                        detail=_detail_from_labels(labels, total=len(allowed_ids)),
                                    )
                                )
                            elif cutoff_total > 0:
                                actions.append("Radarr: cutoff-unmet search suppressed (cooldown)")
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
                        settings.radarr_last_run_at = now
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
                last_emby = getattr(settings, "emby_last_run_at", None)
                if last_emby is not None and (now - last_emby).total_seconds() < emby_interval_m * 60:
                    actions.append("Emby: skipped (run interval not elapsed)")
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
                                        series_by_id: dict[int, dict] = {}
                                        for s in catalog:
                                            sid = _safe_int(s.get("id"))
                                            if sid:
                                                series_by_id[sid] = s

                                        to_unmonitor: list[int] = []
                                        to_keep_monitored: list[int] = []
                                        seen_episode_ids: set[int] = set()
                                        episodes_cache: dict[int, list[dict]] = {}
                                        for item in tv_candidates:
                                            sid = _match_sonarr_series_id(item, catalog)
                                            if not sid:
                                                continue
                                            if sid not in episodes_cache:
                                                episodes_cache[sid] = await sonarr2.episodes_for_series(series_id=sid)
                                            series_rec = series_by_id.get(sid)
                                            ended = _sonarr_series_is_ended(series_rec)
                                            for eid in _episode_ids_for_emby_tv_item(item, episodes_cache[sid]):
                                                if eid in seen_episode_ids:
                                                    continue
                                                seen_episode_ids.add(eid)
                                                if ended:
                                                    to_unmonitor.append(eid)
                                                else:
                                                    to_keep_monitored.append(eid)

                                        to_unmonitor = list(dict.fromkeys(to_unmonitor))
                                        to_keep_monitored = list(dict.fromkeys(to_keep_monitored))

                                        episode_by_id: dict[int, dict] = {}
                                        for eps in episodes_cache.values():
                                            for ep in eps:
                                                eid = _safe_int(ep.get("id"))
                                                if eid:
                                                    episode_by_id[eid] = ep

                                        # Always remove on-disk files in Sonarr when present; monitoring differs by series status.
                                        all_tv_episode_ids = list(dict.fromkeys(to_keep_monitored + to_unmonitor))
                                        deleted_files = 0
                                        if all_tv_episode_ids:
                                            for eid in all_tv_episode_ids:
                                                ep = episode_by_id.get(eid) or {}
                                                efid = _sonarr_episode_file_id(ep)
                                                if efid:
                                                    await sonarr2.delete_episode_file(episode_file_id=efid)
                                                    deleted_files += 1
                                            actions.append(
                                                f"Sonarr: deleted {deleted_files} on-disk episode file(s) "
                                                f"for {len(all_tv_episode_ids)} episode(s)"
                                            )

                                        if to_keep_monitored:
                                            # Keep season/show grabbing new eps (Sonarr may unmonitor on file delete).
                                            await sonarr2.set_episodes_monitored(episode_ids=to_keep_monitored, monitored=True)
                                            actions.append(
                                                f"Sonarr: left {len(to_keep_monitored)} episode(s) monitored "
                                                f"(series still airing)"
                                            )

                                        if to_unmonitor:
                                            await sonarr2.unmonitor_episodes(episode_ids=to_unmonitor)
                                            actions.append(
                                                f"Sonarr: unmonitored {len(to_unmonitor)} episode(s) "
                                                f"(ended series) after delete criteria met"
                                            )

                                        if not all_tv_episode_ids:
                                            actions.append("Sonarr: no episodes linked for TV delete candidate(s)")
                                    except Exception as e:
                                        actions.append(f"Sonarr: sync warning after Emby deletes: {format_http_error_detail(e)}")
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
                                    count=len(candidates),
                                    detail=_detail_from_labels([name for _, name, _, _ in candidates], total=len(candidates)),
                                )
                            )
                        settings.emby_last_run_at = now
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

