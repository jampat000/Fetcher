from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import re
from typing import Any, Literal

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import _get_or_create_settings
from app.arr_intervals import effective_arr_interval_minutes
from app.arr_client import (
    ArrClient,
    ArrConfig,
    trigger_radarr_cutoff_search,
    trigger_radarr_missing_search,
    trigger_sonarr_cutoff_search,
    trigger_sonarr_missing_search,
)
from app.http_status_hints import format_http_error_detail, hint_for_http_status
from app.log_sanitize import redact_sensitive_text, redact_url_for_logging
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
from app.resolvers.api_keys import resolve_emby_api_key, resolve_radarr_api_key, resolve_sonarr_api_key

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    ok: bool
    message: str


ArrManualScope = Literal["sonarr_missing", "sonarr_upgrade", "radarr_missing", "radarr_upgrade"]


@dataclass(frozen=True)
class RunContext:
    """Read-only, derived orchestration input computed once per run."""

    settings: AppSettings
    arr_manual_scope: ArrManualScope | None
    son_key: str
    rad_key: str
    em_key: str
    tz: str
    sonarr_tick_m: int
    radarr_tick_m: int
    sonarr_cooldown_minutes: int
    radarr_cooldown_minutes: int
    emby_interval_m: int
    now: datetime
    do_sonarr_block: bool
    do_radarr_block: bool


def _build_run_context(settings: AppSettings, *, arr_manual_scope: ArrManualScope | None) -> RunContext:
    # Keep this builder side-effect free; persistence and app execution remain in executors/coordinator.
    son_key = resolve_sonarr_api_key(settings)
    rad_key = resolve_radarr_api_key(settings)
    em_key = resolve_emby_api_key(settings)
    tz = (settings.timezone or "UTC").strip() or "UTC"
    sonarr_tick_m = effective_arr_interval_minutes(settings.sonarr_interval_minutes)
    radarr_tick_m = effective_arr_interval_minutes(settings.radarr_interval_minutes)
    _cd = settings.arr_search_cooldown_minutes
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
    emby_interval_m = max(5, int(settings.emby_interval_minutes or 60))
    do_sonarr_block = bool(
        settings.sonarr_enabled
        and (settings.sonarr_url or "").strip()
        and son_key
        and (arr_manual_scope is None or arr_manual_scope in ("sonarr_missing", "sonarr_upgrade"))
    )
    do_radarr_block = bool(
        settings.radarr_enabled
        and (settings.radarr_url or "").strip()
        and rad_key
        and (arr_manual_scope is None or arr_manual_scope in ("radarr_missing", "radarr_upgrade"))
    )
    return RunContext(
        settings=settings,
        arr_manual_scope=arr_manual_scope,
        son_key=son_key,
        rad_key=rad_key,
        em_key=em_key,
        tz=tz,
        sonarr_tick_m=sonarr_tick_m,
        radarr_tick_m=radarr_tick_m,
        sonarr_cooldown_minutes=sonarr_cooldown_minutes,
        radarr_cooldown_minutes=radarr_cooldown_minutes,
        emby_interval_m=emby_interval_m,
        now=now,
        do_sonarr_block=do_sonarr_block,
        do_radarr_block=do_radarr_block,
    )

# One automation pass at a time (scheduler tick vs manual “search now”).
_run_once_lock = asyncio.Lock()


async def run_once(session: AsyncSession, *, arr_manual_scope: ArrManualScope | None = None) -> RunResult:
    async with _run_once_lock:
        return await _run_once_inner(session, arr_manual_scope=arr_manual_scope)


def _take_int_ids(records: list[dict[str, Any]], *keys: str, limit: int) -> list[int]:
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


def _extract_first_int(rec: dict[str, Any], *keys: str) -> int | None:
    for k in keys:
        v = rec.get(k)
        if isinstance(v, int) and v > 0:
            return v
        if isinstance(v, str) and v.isdigit():
            n = int(v)
            if n > 0:
                return n
    return None


def _take_records_and_ids(
    records: list[dict[str, Any]], *keys: str, limit: int
) -> tuple[list[int], list[dict[str, Any]]]:
    """Deduplicate by id while preserving Sonarr/Radarr record order."""
    ids_out: list[int] = []
    recs_out: list[dict[str, Any]] = []
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
    now: datetime,
    max_apply: int | None = None,
) -> list[int]:
    """Return ids that are not triggered again inside the cooldown window; record those triggers.

    Cooldown is keyed by (app, item_type, item_id) only — not by ``action``. That way a movie
    cannot be hit twice in one Fetcher run (e.g. both missing + cutoff-unmet), and Sonarr
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


# Safety cap for wanted-queue pagination (tests may monkeypatch for deterministic coverage).
_PAGINATE_WANTED_FOR_SEARCH_MAX_PAGES = 250


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
    now: datetime,
) -> tuple[list[int], list[dict[str, Any]], int]:
    """Walk Sonarr/Radarr wanted pages until we collect up to ``limit`` items that pass cooldown.

    Without this, only *page 1* is considered — the same titles stay at the top of the queue,
    so everything deeper never gets a turn (unlike Huntarr-style “batch through the backlog”).
    """
    limit = max(1, int(limit))
    page_size = min(100, max(50, limit))
    allowed_ids: list[int] = []
    allowed_recs: list[dict[str, Any]] = []
    seen: set[int] = set()
    total_records = 0
    page = 1
    max_pages = _PAGINATE_WANTED_FOR_SEARCH_MAX_PAGES

    fetch = client.wanted_missing if kind == "missing" else client.wanted_cutoff_unmet

    # At most one HTTP fetch runs ahead of the current page: while cooldown+merge runs for
    # page N, page N+1 may be in flight (bounded overlap; strict page order preserved).
    pending_prefetch: asyncio.Task | None = None

    while len(allowed_ids) < limit and page <= max_pages:
        if pending_prefetch is not None:
            data = await pending_prefetch
            pending_prefetch = None
        else:
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
        # Prefetch only when this page cannot supply enough distinct ids to satisfy ``need``
        # even if all passed cooldown — avoids an extra HTTP when the current batch alone
        # could fill the remainder (matches single-page fetch patterns in tests).
        if page + 1 <= max_pages and len(batch_ids) < need:
            pending_prefetch = asyncio.create_task(fetch(page=page + 1, page_size=page_size))

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
        if len(allowed_ids) >= limit and pending_prefetch is not None:
            pending_prefetch.cancel()
            try:
                await pending_prefetch
            except asyncio.CancelledError:
                pass
            pending_prefetch = None
        page += 1

    return allowed_ids, allowed_recs, total_records


async def _wanted_queue_total(client: ArrClient, *, kind: str) -> int:
    """API totalRecords for missing or cutoff-unmet queue (page 1 only)."""
    fetch = client.wanted_missing if kind == "missing" else client.wanted_cutoff_unmet
    data = await fetch(page=1, page_size=50)
    return int(data.get("totalRecords") or 0)


async def prune_old_records(session: AsyncSession, settings: AppSettings | None = None) -> None:
    """Delete old log/snapshot rows in one transaction batch (committed with the next run commit).

    - ``arr_action_log``: older than ``arr_search_cooldown_minutes * 2`` (minutes); if cooldown is 0, use 2880 min.
    - ``activity_log``, ``job_run_log``, ``app_snapshot``: older than ``log_retention_days`` (clamped 7–3650).

    Pass ``settings`` when already loaded to avoid a duplicate ``AppSettings`` query (e.g. from ``run_once``).

    Never raises — failures are logged at WARNING only.
    """
    try:
        now = utc_now_naive()
        row = settings
        if row is None:
            row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
        if row is None:
            return

        try:
            cd_raw = int(row.arr_search_cooldown_minutes)
        except (TypeError, ValueError):
            cd_raw = 0
        arr_window_min = 2880 if cd_raw <= 0 else cd_raw * 2
        arr_cutoff = now - timedelta(minutes=arr_window_min)

        try:
            stored_ret = int(row.log_retention_days)
        except (TypeError, ValueError):
            stored_ret = 90
        ret_days = max(7, min(3650, stored_ret))
        ret_cutoff = now - timedelta(days=ret_days)

        # Batched deletes (SQLite/aiosqlite: one statement per execute; same transaction until commit).
        await session.execute(delete(ArrActionLog).where(ArrActionLog.created_at < arr_cutoff))
        await session.execute(delete(ActivityLog).where(ActivityLog.created_at < ret_cutoff))
        await session.execute(delete(JobRunLog).where(JobRunLog.started_at < ret_cutoff))
        await session.execute(delete(AppSnapshot).where(AppSnapshot.created_at < ret_cutoff))
    except Exception:
        logger.warning("prune_old_records failed; continuing run", exc_info=True)


def _sonarr_series_ids_for_episode_batch(
    records: list[dict[str, Any]], *episode_keys: str, limit: int
) -> list[int]:
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


def _emby_provider_id(item: dict[str, Any], key: str) -> str:
    providers = item.get("ProviderIds") if isinstance(item.get("ProviderIds"), dict) else {}
    return str(providers.get(key) or "").strip()


def _emby_year(item: dict[str, Any]) -> int | None:
    y = _safe_int(item.get("ProductionYear"))
    if y:
        return y
    pd = str(item.get("PremiereDate") or "").strip()
    if len(pd) >= 4 and pd[:4].isdigit():
        return int(pd[:4])
    return None


def _match_radarr_movie_id(emby_item: dict[str, Any], radarr_movies: list[dict[str, Any]]) -> int | None:
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


def _match_sonarr_series_id(emby_item: dict[str, Any], sonarr_series: list[dict[str, Any]]) -> int | None:
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


def _episode_ids_for_emby_tv_item(
    emby_item: dict[str, Any], sonarr_episodes: list[dict[str, Any]]
) -> list[int]:
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


def _sonarr_series_is_ended(series: dict[str, Any] | None) -> bool:
    """Sonarr series.status is typically 'continuing', 'ended', or 'upcoming'."""
    if not series or not isinstance(series, dict):
        return False
    return str(series.get("status") or "").strip().lower() == "ended"


def _sonarr_episode_file_id(episode: dict[str, Any]) -> int | None:
    fid = _safe_int(episode.get("episodeFileId"))
    if fid:
        return fid
    ef = episode.get("episodeFile")
    if isinstance(ef, dict):
        return _safe_int(ef.get("id"))
    return None


# Bounded concurrent Sonarr episode-file DELETEs (Emby trimmer live apply). Intentionally
# parallelizes HTTP up to this limit; see ``_delete_sonarr_episode_files_bounded``.
_SONARR_TRIMMER_EPISODE_FILE_DELETE_CONCURRENCY = 5

# Bounded concurrent Emby ``DELETE /Items/{id}`` (Emby trimmer live apply). Intentionally
# caps parallel HTTP deletes for latency vs load; see ``_delete_emby_items_bounded``.
_EMBY_TRIMMER_ITEM_DELETE_CONCURRENCY = 5

_SONARR_EPISODE_DELETE_FAILURE_DETAIL_MAX_CHARS = 240
_EMBY_ITEM_DELETE_FAILURE_DETAIL_MAX_CHARS = 240


@dataclass(frozen=True)
class SonarrEpisodeFileDeleteResult:
    """Aggregated outcome after bounded concurrent ``DELETE /api/v3/episodeFile/{id}`` calls.

    Fields align with :func:`_delete_sonarr_episode_files_bounded`: per-id results are
    combined (not fail-fast). ``failure_summaries`` parallels ``failed_episode_file_ids``.
    """

    success_count: int
    failed_episode_file_ids: list[int]
    failure_summaries: list[str]


def _sonarr_episode_delete_action_line(
    *,
    success_count: int,
    episode_count: int,
    failed_episode_file_ids: list[int],
    failure_summaries: list[str],
) -> str:
    """Build the JobRunLog / UI action string for Sonarr on-disk file deletes.

    **Regression-sensitive:** wording and structure are asserted in tests (full success vs
    partial failure, including the ``; N failed —`` suffix and failure detail shape).
    """
    if not failed_episode_file_ids:
        return (
            f"Sonarr: deleted {success_count} on-disk episode file(s) "
            f"for {episode_count} episode(s)"
        )
    n_fail = len(failed_episode_file_ids)
    detail = _truncate_sonarr_episode_delete_failure_detail(
        failed_episode_file_ids, failure_summaries
    )
    return (
        f"Sonarr: deleted {success_count} on-disk episode file(s) "
        f"for {episode_count} episode(s); {n_fail} failed — {detail}"
    )


def _truncate_sonarr_episode_delete_failure_detail(
    failed_episode_file_ids: list[int],
    failure_summaries: list[str],
) -> str:
    """Keep log lines readable; dedupe repeated error text."""
    id_part = ",".join(str(i) for i in failed_episode_file_ids[:16])
    if len(failed_episode_file_ids) > 16:
        id_part += ",…"
    uniq: list[str] = []
    seen: set[str] = set()
    for raw in failure_summaries:
        t = (raw or "").strip()
        if not t:
            t = "(no detail)"
        if t not in seen:
            seen.add(t)
            uniq.append(t[:120])
    err_part = " | ".join(uniq)
    out = f"episode file id(s) [{id_part}]: {err_part}"
    if len(out) > _SONARR_EPISODE_DELETE_FAILURE_DETAIL_MAX_CHARS:
        return out[: _SONARR_EPISODE_DELETE_FAILURE_DETAIL_MAX_CHARS - 1] + "…"
    return out


async def _delete_sonarr_episode_files_bounded(
    sonarr: ArrClient,
    episode_file_ids: list[int],
) -> SonarrEpisodeFileDeleteResult:
    """Delete on-disk episode files via Sonarr with bounded concurrency.

    **Concurrency:** Uses ``asyncio.Semaphore`` + ``gather`` so up to
    ``_SONARR_TRIMMER_EPISODE_FILE_DELETE_CONCURRENCY`` deletes run in flight (performance
    intent; not a single sequential await chain).

    **Deduping:** ``episode_file_ids`` is reduced with ``dict.fromkeys`` before scheduling
    so each distinct id is attempted once; order is first-seen.

    **Failures:** Exceptions are caught per id and recorded; all scheduled ids are
    attempted—no fail-fast abort from this helper.

    **Regression-sensitive:** Pair with :func:`_sonarr_episode_delete_action_line` and
    ``tests/test_apply_emby_trimmer_sonarr_tv.py`` when changing semantics.
    """
    unique = list(dict.fromkeys(episode_file_ids))
    if not unique:
        return SonarrEpisodeFileDeleteResult(0, [], [])

    sem = asyncio.Semaphore(_SONARR_TRIMMER_EPISODE_FILE_DELETE_CONCURRENCY)

    async def _attempt(efid: int) -> tuple[int, Exception | None]:
        async with sem:
            try:
                await sonarr.delete_episode_file(episode_file_id=efid)
                return (efid, None)
            except Exception as e:  # noqa: BLE001
                return (efid, e)

    pairs = await asyncio.gather(*(_attempt(e) for e in unique))
    success_count = 0
    failed_ids: list[int] = []
    summaries: list[str] = []
    for efid, err in pairs:
        if err is None:
            success_count += 1
        else:
            failed_ids.append(efid)
            summaries.append(format_http_error_detail(err))
    return SonarrEpisodeFileDeleteResult(
        success_count=success_count,
        failed_episode_file_ids=failed_ids,
        failure_summaries=summaries,
    )


@dataclass(frozen=True)
class EmbyItemDeleteResult:
    """Aggregated outcome after bounded concurrent Emby ``delete_item`` calls.

    **Concurrency:** Results from :func:`_delete_emby_items_bounded` (semaphore + gather).

    **No dedupe:** One row per scheduled attempt; duplicate candidate ids appear multiple times.

    **Failures:** Aggregated per attempt (not fail-fast); ``failure_summaries`` pairs with
    ``failed_item_ids`` by position.
    """

    success_count: int
    failed_item_ids: list[str]
    failure_summaries: list[str]


def _emby_item_delete_action_line(
    *,
    success_count: int,
    failed_item_ids: list[str],
    failure_summaries: list[str],
) -> str:
    """Build the JobRunLog / UI action string for Emby library item deletes.

    Reflects bounded concurrent deletes: ``success_count`` is successful attempts only;
    partial runs append ``; N failed —`` plus truncated detail (see
    :func:`_truncate_emby_item_delete_failure_detail`).

    **Regression-sensitive:** Wording and structure are asserted in
    ``tests/test_apply_emby_trimmer_emby_delete_phase.py`` (full success vs partial failure).
    """
    if not failed_item_ids:
        return f"Emby: deleted {success_count} item(s)"
    n_fail = len(failed_item_ids)
    detail = _truncate_emby_item_delete_failure_detail(failed_item_ids, failure_summaries)
    return f"Emby: deleted {success_count} item(s); {n_fail} failed — {detail}"


def _truncate_emby_item_delete_failure_detail(
    failed_item_ids: list[str],
    failure_summaries: list[str],
) -> str:
    """Keep log lines readable; dedupe repeated error text."""
    id_part = ",".join(failed_item_ids[:16])
    if len(failed_item_ids) > 16:
        id_part += ",…"
    uniq: list[str] = []
    seen: set[str] = set()
    for raw in failure_summaries:
        t = (raw or "").strip()
        if not t:
            t = "(no detail)"
        if t not in seen:
            seen.add(t)
            uniq.append(t[:120])
    err_part = " | ".join(uniq)
    out = f"item id(s) [{id_part}]: {err_part}"
    if len(out) > _EMBY_ITEM_DELETE_FAILURE_DETAIL_MAX_CHARS:
        return out[: _EMBY_ITEM_DELETE_FAILURE_DETAIL_MAX_CHARS - 1] + "…"
    return out


async def _delete_emby_items_bounded(emby: EmbyClient, item_ids: list[str]) -> EmbyItemDeleteResult:
    """Delete Emby library items with bounded concurrency (not fail-fast).

    **Concurrency:** ``asyncio.Semaphore`` + ``asyncio.gather`` — up to
    ``_EMBY_TRIMMER_ITEM_DELETE_CONCURRENCY`` deletes in flight (intentional cap).

    **No dedupe:** ``item_ids`` is scheduled as-is; duplicate ids mean duplicate HTTP attempts.

    **Failures:** Per-attempt exceptions are caught and rolled into :class:`EmbyItemDeleteResult`;
    every scheduled id is attempted (aggregated failures, not fail-fast).

    **Regression-sensitive:** Pair with :func:`_emby_item_delete_action_line` and
    ``tests/test_apply_emby_trimmer_emby_delete_phase.py`` when changing semantics.
    """
    if not item_ids:
        return EmbyItemDeleteResult(0, [], [])

    sem = asyncio.Semaphore(_EMBY_TRIMMER_ITEM_DELETE_CONCURRENCY)

    async def _attempt(iid: str) -> tuple[str, Exception | None]:
        async with sem:
            try:
                await emby.delete_item(iid)
                return (iid, None)
            except Exception as e:  # noqa: BLE001
                return (iid, e)

    pairs = await asyncio.gather(*(_attempt(iid) for iid in item_ids))
    success_count = 0
    failed_ids: list[str] = []
    summaries: list[str] = []
    for iid, err in pairs:
        if err is None:
            success_count += 1
        else:
            failed_ids.append(iid)
            summaries.append(format_http_error_detail(err))
    return EmbyItemDeleteResult(
        success_count=success_count,
        failed_item_ids=failed_ids,
        failure_summaries=summaries,
    )


def _sonarr_episode_label(rec: dict[str, Any]) -> str:
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


def _sonarr_episode_label_with_fallback(rec: dict[str, Any], series_title_map: dict[int, str]) -> str:
    """Prefer explicit show title from record; fallback to seriesId lookup if needed."""
    base = _sonarr_episode_label(rec)
    # If base has no show context, try series map.
    if base and (" S" in base or base.startswith("Episode")):
        sid = _safe_int(rec.get("seriesId"))
        show = series_title_map.get(sid or 0, "").strip()
        if show:
            return f"{show} {base}"
    return base


def _radarr_movie_label(rec: dict[str, Any]) -> str:
    title = str(rec.get("title") or "").strip() or "Movie"
    year = _safe_int(rec.get("year"))
    if year:
        return f"{title} ({year})"
    return title


def _interval_skip_detail(last_at: datetime, now: datetime, tick_minutes: int) -> str:
    """Explain run-interval gate (Sonarr and Radarr each use their own last_run_at and interval)."""
    age_m = max(0, int((now - last_at).total_seconds() // 60))
    left = max(0, tick_minutes - age_m)
    return f"{age_m} min since last run for this app; interval {tick_minutes} min (~{left} min until eligible)"


# Activity log ``detail`` stores every title line (UI shows 5 + “+N more”, expandable).
# Extreme safety cap only — normal runs stay well below this.
ACTIVITY_DETAIL_MAX_CHARS = 400_000


def _detail_from_labels(labels: list[str]) -> str:
    """Join non-empty labels with newlines. Full list is stored; list UI previews first 5 lines."""
    parts: list[str] = []
    for raw in labels:
        label = _sanitize_log_text(raw).strip()
        if label:
            parts.append(label)
    body = "\n".join(parts)
    if len(body) > ACTIVITY_DETAIL_MAX_CHARS:
        body = body[:ACTIVITY_DETAIL_MAX_CHARS] + "\n… (detail truncated)"
    return body


def _sanitize_log_text(text: str | None) -> str:
    """Apply log-text redaction before persisting ActivityLog / JobRunLog fields."""
    return redact_sensitive_text(text)


def _sanitize_pending_log_rows(session: AsyncSession) -> None:
    """Sanitize log rows before commit so DB never stores raw secrets."""
    for obj in session.new:
        if isinstance(obj, JobRunLog):
            obj.message = _sanitize_log_text(obj.message)
        elif isinstance(obj, ActivityLog):
            obj.detail = _sanitize_log_text(obj.detail)


async def apply_emby_trimmer_live_deletes(
    settings: AppSettings,
    emby: EmbyClient,
    candidates: list[tuple[str, str, str, dict[str, Any]]],
    *,
    son_key: str | None,
    rad_key: str | None,
) -> list[str]:
    """Radarr/Sonarr sync, then delete matched Emby items (same operations as scheduled live trimmer).

    Emby ``DELETE`` calls run with bounded concurrency (see ``_delete_emby_items_bounded``); per-item
    failures are collected and reported without failing the whole batch.
    """
    actions: list[str] = []
    movie_candidates = [raw for _, _, t, raw in candidates if t == "Movie"]
    tv_candidates = [raw for _, _, t, raw in candidates if t in {"Series", "Season", "Episode"}]

    if movie_candidates and settings.radarr_url and rad_key:
        radarr2 = ArrClient(ArrConfig(settings.radarr_url, rad_key))
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
        except Exception as e:  # noqa: BLE001
            actions.append(f"Radarr: unmonitor warning after Emby deletes: {format_http_error_detail(e)}")
        finally:
            await radarr2.aclose()

    if tv_candidates and settings.sonarr_url and son_key:
        sonarr2 = ArrClient(ArrConfig(settings.sonarr_url, son_key))
        try:
            catalog = await sonarr2.series()
            series_by_id: dict[int, dict[str, Any]] = {}
            for s in catalog:
                sid = _safe_int(s.get("id"))
                if sid:
                    series_by_id[sid] = s

            to_unmonitor: list[int] = []
            to_keep_monitored: list[int] = []
            seen_episode_ids: set[int] = set()
            episodes_cache: dict[int, list[dict[str, Any]]] = {}
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

            episode_by_id: dict[int, dict[str, Any]] = {}
            for eps in episodes_cache.values():
                for ep in eps:
                    eid = _safe_int(ep.get("id"))
                    if eid:
                        episode_by_id[eid] = ep

            all_tv_episode_ids = list(dict.fromkeys(to_keep_monitored + to_unmonitor))
            if all_tv_episode_ids:
                episode_file_ids_to_delete: list[int] = []
                for eid in all_tv_episode_ids:
                    ep = episode_by_id.get(eid) or {}
                    efid = _sonarr_episode_file_id(ep)
                    if efid:
                        episode_file_ids_to_delete.append(efid)
                delete_result = await _delete_sonarr_episode_files_bounded(
                    sonarr2, episode_file_ids_to_delete
                )
                actions.append(
                    _sonarr_episode_delete_action_line(
                        success_count=delete_result.success_count,
                        episode_count=len(all_tv_episode_ids),
                        failed_episode_file_ids=delete_result.failed_episode_file_ids,
                        failure_summaries=delete_result.failure_summaries,
                    )
                )

            if to_keep_monitored:
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
        except Exception as e:  # noqa: BLE001
            actions.append(f"Sonarr: sync warning after Emby deletes: {format_http_error_detail(e)}")
        finally:
            await sonarr2.aclose()

    item_ids = [item_id for item_id, _, _, _ in candidates]
    emby_result = await _delete_emby_items_bounded(emby, item_ids)
    actions.append(
        _emby_item_delete_action_line(
            success_count=emby_result.success_count,
            failed_item_ids=emby_result.failed_item_ids,
            failure_summaries=emby_result.failure_summaries,
        )
    )
    return actions


async def _execute_sonarr_block(
    session: AsyncSession,
    *,
    log: JobRunLog,
    ctx: RunContext,
    actions: list[str],
    default_limit: int,
) -> None:
    """Owns Sonarr-specific orchestration and Sonarr side effects for one run."""

    settings = ctx.settings
    arr_manual_scope = ctx.arr_manual_scope
    skip_sonarr = False
    if arr_manual_scope is None:
        if not in_window(
            schedule_enabled=settings.sonarr_schedule_enabled,
            schedule_days=settings.sonarr_schedule_days or "",
            schedule_start=settings.sonarr_schedule_start,
            schedule_end=settings.sonarr_schedule_end,
            timezone=ctx.tz,
        ):
            actions.append("Sonarr: skipped (outside schedule window)")
            skip_sonarr = True
        else:
            last_sonarr = settings.sonarr_last_run_at
            if last_sonarr is not None and (ctx.now - last_sonarr).total_seconds() < ctx.sonarr_tick_m * 60:
                actions.append(
                    "Sonarr: skipped (run interval not elapsed — "
                    + _interval_skip_detail(last_sonarr, ctx.now, ctx.sonarr_tick_m)
                    + ")"
                )
                skip_sonarr = True
    if skip_sonarr:
        return

    sonarr = ArrClient(ArrConfig(settings.sonarr_url, ctx.son_key))
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
        sonarr_missing_enabled = bool(settings.sonarr_search_missing)
        sonarr_upgrades_enabled = bool(settings.sonarr_search_upgrades)
        if arr_manual_scope == "sonarr_missing":
            want_missing, want_upgrade = True, False
        elif arr_manual_scope == "sonarr_upgrade":
            want_missing, want_upgrade = False, True
        else:
            want_missing = sonarr_missing_enabled
            want_upgrade = sonarr_upgrades_enabled

        missing_total = 0
        cutoff_total = 0

        if want_missing:
            allowed_ids, allowed_records, missing_total = await _paginate_wanted_for_search(
                sonarr,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=sonarr_limit,
                cooldown_minutes=ctx.sonarr_cooldown_minutes,
                now=ctx.now,
            )
            if allowed_ids:
                # Tagging is best-effort; it should not block the search trigger.
                try:
                    tag_id = await sonarr.ensure_tag("fetcher-missing")
                    series_ids = _sonarr_series_ids_for_episode_batch(
                        allowed_records,
                        "episodeId",
                        "id",
                        limit=len(allowed_records),
                    )
                    await sonarr.add_tags_to_series(series_ids=series_ids, tag_ids=[tag_id])
                except Exception as e:  # noqa: BLE001
                    actions.append(f"Sonarr: tag apply warning (fetcher-missing): {format_http_error_detail(e)}")

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
                        detail=_detail_from_labels(labels),
                    )
                )
            elif missing_total > 0:
                actions.append("Sonarr: missing search suppressed (cooldown)")
            else:
                actions.append("Sonarr: no missing episodes found")

        if want_upgrade:
            allowed_ids, allowed_records, cutoff_total = await _paginate_wanted_for_search(
                sonarr,
                session,
                kind="cutoff",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="upgrade",
                limit=sonarr_limit,
                cooldown_minutes=ctx.sonarr_cooldown_minutes,
                now=ctx.now,
            )
            if allowed_ids:
                try:
                    tag_id = await sonarr.ensure_tag("fetcher-upgrade")
                    series_ids = _sonarr_series_ids_for_episode_batch(
                        allowed_records,
                        "episodeId",
                        "id",
                        limit=len(allowed_records),
                    )
                    await sonarr.add_tags_to_series(series_ids=series_ids, tag_ids=[tag_id])
                except Exception as e:  # noqa: BLE001
                    actions.append(f"Sonarr: tag apply warning (fetcher-upgrade): {format_http_error_detail(e)}")

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
                        detail=_detail_from_labels(labels),
                    )
                )
            elif cutoff_total > 0:
                actions.append("Sonarr: cutoff-unmet search suppressed (cooldown)")
            else:
                actions.append("Sonarr: no cutoff-unmet episodes found")

        if want_missing and not want_upgrade:
            cutoff_total = await _wanted_queue_total(sonarr, kind="cutoff")
        elif want_upgrade and not want_missing:
            missing_total = await _wanted_queue_total(sonarr, kind="missing")

        session.add(
            AppSnapshot(
                app="sonarr",
                ok=True,
                status_message="OK",
                missing_total=missing_total,
                cutoff_unmet_total=cutoff_total,
            )
        )
        settings.sonarr_last_run_at = ctx.now
    finally:
        await sonarr.aclose()


async def _execute_radarr_block(
    session: AsyncSession,
    *,
    log: JobRunLog,
    ctx: RunContext,
    actions: list[str],
    default_limit: int,
) -> None:
    """Owns Radarr-specific orchestration and Radarr side effects for one run."""

    settings = ctx.settings
    arr_manual_scope = ctx.arr_manual_scope
    skip_radarr = False
    if arr_manual_scope is None:
        if not in_window(
            schedule_enabled=settings.radarr_schedule_enabled,
            schedule_days=settings.radarr_schedule_days or "",
            schedule_start=settings.radarr_schedule_start,
            schedule_end=settings.radarr_schedule_end,
            timezone=ctx.tz,
        ):
            actions.append("Radarr: skipped (outside schedule window)")
            skip_radarr = True
        else:
            last_radarr = settings.radarr_last_run_at
            if last_radarr is not None and (ctx.now - last_radarr).total_seconds() < ctx.radarr_tick_m * 60:
                actions.append(
                    "Radarr: skipped (run interval not elapsed — "
                    + _interval_skip_detail(last_radarr, ctx.now, ctx.radarr_tick_m)
                    + ")"
                )
                skip_radarr = True
    if skip_radarr:
        return

    radarr = ArrClient(ArrConfig(settings.radarr_url, ctx.rad_key))
    try:
        await radarr.health()

        radarr_limit = max(1, int((settings.radarr_max_items_per_run or 0) or default_limit))
        radarr_missing_enabled = bool(settings.radarr_search_missing)
        radarr_upgrades_enabled = bool(settings.radarr_search_upgrades)
        if arr_manual_scope == "radarr_missing":
            want_missing, want_upgrade = True, False
        elif arr_manual_scope == "radarr_upgrade":
            want_missing, want_upgrade = False, True
        else:
            want_missing = radarr_missing_enabled
            want_upgrade = radarr_upgrades_enabled

        missing_total = 0
        cutoff_total = 0

        if want_missing:
            allowed_ids, allowed_records, missing_total = await _paginate_wanted_for_search(
                radarr,
                session,
                kind="missing",
                id_keys=("movieId", "id"),
                item_type="movie",
                app="radarr",
                action="missing",
                limit=radarr_limit,
                cooldown_minutes=ctx.radarr_cooldown_minutes,
                now=ctx.now,
            )
            if allowed_ids:
                try:
                    tag_id = await radarr.ensure_tag("fetcher-missing")
                    await radarr.add_tags_to_movies(movie_ids=allowed_ids, tag_ids=[tag_id])
                except Exception as e:  # noqa: BLE001
                    actions.append(f"Radarr: tag apply warning (fetcher-missing): {format_http_error_detail(e)}")

                await trigger_radarr_missing_search(radarr, movie_ids=allowed_ids)
                actions.append(f"Radarr: missing search for {len(allowed_ids)} movie(s)")
                labels = [_radarr_movie_label(r) for r in allowed_records]
                session.add(
                    ActivityLog(
                        job_run_id=log.id,
                        app="radarr",
                        kind="missing",
                        count=len(allowed_ids),
                        detail=_detail_from_labels(labels),
                    )
                )
            elif missing_total > 0:
                actions.append("Radarr: missing search suppressed (cooldown)")
            else:
                actions.append("Radarr: no missing movies found")

        if want_upgrade:
            allowed_ids, allowed_records, cutoff_total = await _paginate_wanted_for_search(
                radarr,
                session,
                kind="cutoff",
                id_keys=("movieId", "id"),
                item_type="movie",
                app="radarr",
                action="upgrade",
                limit=radarr_limit,
                cooldown_minutes=ctx.radarr_cooldown_minutes,
                now=ctx.now,
            )
            if allowed_ids:
                try:
                    tag_id = await radarr.ensure_tag("fetcher-upgrade")
                    await radarr.add_tags_to_movies(movie_ids=allowed_ids, tag_ids=[tag_id])
                except Exception as e:  # noqa: BLE001
                    actions.append(f"Radarr: tag apply warning (fetcher-upgrade): {format_http_error_detail(e)}")

                await trigger_radarr_cutoff_search(radarr, movie_ids=allowed_ids)
                actions.append(f"Radarr: cutoff-unmet search for {len(allowed_ids)} movie(s)")
                labels = [_radarr_movie_label(r) for r in allowed_records]
                session.add(
                    ActivityLog(
                        job_run_id=log.id,
                        app="radarr",
                        kind="upgrade",
                        count=len(allowed_ids),
                        detail=_detail_from_labels(labels),
                    )
                )
            elif cutoff_total > 0:
                actions.append("Radarr: cutoff-unmet search suppressed (cooldown)")
            else:
                actions.append("Radarr: no cutoff-unmet movies found")

        if want_missing and not want_upgrade:
            cutoff_total = await _wanted_queue_total(radarr, kind="cutoff")
        elif want_upgrade and not want_missing:
            missing_total = await _wanted_queue_total(radarr, kind="missing")

        session.add(
            AppSnapshot(
                app="radarr",
                ok=True,
                status_message="OK",
                missing_total=missing_total,
                cutoff_unmet_total=cutoff_total,
            )
        )
        settings.radarr_last_run_at = ctx.now
    finally:
        await radarr.aclose()


async def _execute_emby_block(
    session: AsyncSession,
    *,
    log: JobRunLog,
    ctx: RunContext,
    actions: list[str],
) -> None:
    """Owns Emby-specific orchestration and Emby side effects for one run."""

    settings = ctx.settings
    last_emby = settings.emby_last_run_at
    if last_emby is not None and (ctx.now - last_emby).total_seconds() < ctx.emby_interval_m * 60:
        actions.append(
            "Emby: skipped (run interval not elapsed — "
            + _interval_skip_detail(last_emby, ctx.now, ctx.emby_interval_m)
            + ")"
        )
        return

    emby = EmbyClient(EmbyConfig(settings.emby_url, ctx.em_key))
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

        _v_scan = settings.emby_max_items_scan
        _raw_scan = int(_v_scan) if _v_scan is not None else 2000
        scan_limit = 0 if _raw_scan <= 0 else max(1, min(100_000, _raw_scan))
        max_deletes = max(1, int(settings.emby_max_deletes_per_run or 25))
        global_rating = max(0, int(settings.emby_rule_watched_rating_below or 0))
        global_unwatched = max(0, int(settings.emby_rule_unwatched_days or 0))
        movie_rating_below = max(0, int(settings.emby_rule_movie_watched_rating_below or 0)) or global_rating
        movie_unwatched_days = max(0, int(settings.emby_rule_movie_unwatched_days or 0)) or global_unwatched
        selected_movie_genres = parse_genres_csv(settings.emby_rule_movie_genres_csv)
        selected_movie_people = parse_movie_people_phrases(settings.emby_rule_movie_people_csv)
        selected_movie_credit_types = parse_movie_people_credit_types_csv(
            settings.emby_rule_movie_people_credit_types_csv
        )
        selected_tv_people = parse_movie_people_phrases(settings.emby_rule_tv_people_csv)
        selected_tv_credit_types = parse_movie_people_credit_types_csv(
            settings.emby_rule_tv_people_credit_types_csv
        )
        tv_delete_watched = bool(settings.emby_rule_tv_delete_watched)
        selected_tv_genres = parse_genres_csv(settings.emby_rule_tv_genres_csv)
        tv_unwatched_days = max(0, int(settings.emby_rule_tv_unwatched_days or 0)) or global_unwatched
        dry_run = bool(settings.emby_dry_run)

        if movie_rating_below <= 0 and movie_unwatched_days <= 0 and (not tv_delete_watched) and tv_unwatched_days <= 0:
            actions.append("Emby: skipped (no Emby Trimmer rules enabled)")
        else:
            items = await emby.items_for_user(user_id=effective_user_id, limit=scan_limit)
            candidates: list[tuple[str, str, str, dict[str, Any]]] = []
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
                actions.extend(
                    await apply_emby_trimmer_live_deletes(
                        settings, emby, candidates, son_key=ctx.son_key, rad_key=ctx.rad_key
                    )
                )

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
                    kind="trimmed",
                    count=len(candidates),
                    detail=_detail_from_labels([name for _, name, _, _ in candidates]),
                )
            )
        settings.emby_last_run_at = ctx.now
    finally:
        await emby.aclose()


async def _run_once_inner(
    session: AsyncSession,
    *,
    arr_manual_scope: ArrManualScope | None,
) -> RunResult:
    # Outer coordinator: builds shared context once, dispatches app executors, and finalizes run outcome.
    # Behavior/message/side-effect timing here is regression-sensitive; update tests before changing.
    settings = await _get_or_create_settings(session)
    await prune_old_records(session, settings)

    log = JobRunLog(started_at=utc_now_naive(), ok=False, message="")
    session.add(log)
    _sanitize_pending_log_rows(session)
    await session.commit()
    await session.refresh(log)

    try:
        ctx = _build_run_context(settings, arr_manual_scope=arr_manual_scope)
        son_key = ctx.son_key
        rad_key = ctx.rad_key
        em_key = ctx.em_key

        actions: list[str] = []
        default_limit = 50

        tz = ctx.tz
        sonarr_tick_m = ctx.sonarr_tick_m
        radarr_tick_m = ctx.radarr_tick_m
        sonarr_cooldown_minutes = ctx.sonarr_cooldown_minutes
        radarr_cooldown_minutes = ctx.radarr_cooldown_minutes
        now = ctx.now
        emby_interval_m = ctx.emby_interval_m

        if arr_manual_scope is not None:
            actions.append(
                "Manual search: bypassing schedule windows and Sonarr/Radarr run-interval gates for this action only."
            )

        do_sonarr_block = ctx.do_sonarr_block
        do_radarr_block = ctx.do_radarr_block

        if arr_manual_scope in ("sonarr_missing", "sonarr_upgrade") and not do_sonarr_block:
            actions.append("Sonarr: skipped (enable Sonarr and set URL + API key in Settings)")
        if arr_manual_scope in ("radarr_missing", "radarr_upgrade") and not do_radarr_block:
            actions.append("Radarr: skipped (enable Radarr and set URL + API key in Settings)")

        # Sonarr (TV)
        if do_sonarr_block:
            await _execute_sonarr_block(
                session,
                log=log,
                ctx=ctx,
                actions=actions,
                default_limit=default_limit,
            )

        # Radarr (movies)
        if do_radarr_block:
            await _execute_radarr_block(
                session,
                log=log,
                ctx=ctx,
                actions=actions,
                default_limit=default_limit,
            )

        # Emby Trimmer (not part of manual Arr “search now”)
        if arr_manual_scope is None and settings.emby_enabled and settings.emby_url and em_key:
            if not in_window(
                schedule_enabled=settings.emby_schedule_enabled,
                schedule_days=settings.emby_schedule_days or "",
                schedule_start=settings.emby_schedule_start,
                schedule_end=settings.emby_schedule_end,
                timezone=tz,
            ):
                actions.append("Emby: skipped (outside schedule window)")
            else:
                await _execute_emby_block(
                    session,
                    log=log,
                    ctx=ctx,
                    actions=actions,
                )
        elif settings.emby_enabled:
            actions.append("Emby: skipped (missing URL/API key)")

        msg = " | ".join(actions) if actions else "No actions (check enabled flags + URLs + API keys)."
        log.ok = True
        log.message = _sanitize_log_text(msg)
        log.finished_at = utc_now_naive()
        _sanitize_pending_log_rows(session)
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
        log.message = _sanitize_log_text(
            f"Run failed: HTTP {code} for {e.request.method} {safe_url}{hint_suffix} | "
            f"{redact_sensitive_text(body)}"
        )
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
                    detail=_sanitize_log_text((log.message or "")[:500]),
                )
            )
        _sanitize_pending_log_rows(session)
        await session.commit()
        return RunResult(ok=False, message=log.message)
    except Exception as e:  # noqa: BLE001 - service boundary logging
        log.ok = False
        log.message = _sanitize_log_text(f"Run failed: {type(e).__name__}: {e}")
        log.finished_at = utc_now_naive()
        session.add(
            ActivityLog(
                job_run_id=log.id,
                app="service",
                kind="error",
                status="failed",
                count=0,
                detail=_sanitize_log_text((log.message or "")[:500]),
            )
        )
        _sanitize_pending_log_rows(session)
        await session.commit()
        return RunResult(ok=False, message=log.message)

