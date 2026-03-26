from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import SchedulerNotRunningError
from sqlalchemy import select

from app.arr_intervals import effective_arr_interval_minutes
from app.db import SessionLocal
from app.models import AppSettings
from app.service_logic import run_once
from app.stream_manager_service import run_scheduled_stream_manager_pass
from app.resolvers.api_keys import resolve_emby_api_key, resolve_radarr_api_key, resolve_sonarr_api_key
from app.time_util import utc_now_naive


def _sonarr_configured(settings: AppSettings) -> bool:
    return bool(
        settings.sonarr_enabled
        and (settings.sonarr_url or "").strip()
        and resolve_sonarr_api_key(settings)
    )


def _radarr_configured(settings: AppSettings) -> bool:
    return bool(
        settings.radarr_enabled
        and (settings.radarr_url or "").strip()
        and resolve_radarr_api_key(settings)
    )


def _emby_configured(settings: AppSettings) -> bool:
    return bool(
        settings.emby_enabled
        and (settings.emby_url or "").strip()
        and resolve_emby_api_key(settings)
    )


def _stream_manager_configured(settings: AppSettings) -> bool:
    return bool(
        getattr(settings, "stream_manager_enabled", False)
        and (getattr(settings, "stream_manager_watched_folder", "") or "").strip()
        and (getattr(settings, "stream_manager_output_folder", "") or "").strip()
    )


def compute_job_intervals_minutes(settings: AppSettings) -> dict[str, int]:
    """Independent scheduler intervals per configured app job."""
    out: dict[str, int] = {}
    if _sonarr_configured(settings):
        out["sonarr"] = effective_arr_interval_minutes(getattr(settings, "sonarr_interval_minutes", None))
    if _radarr_configured(settings):
        out["radarr"] = effective_arr_interval_minutes(getattr(settings, "radarr_interval_minutes", None))
    if _emby_configured(settings):
        out["trimmer"] = max(5, int(settings.emby_interval_minutes or 60))
    if _stream_manager_configured(settings):
        out["stream_manager"] = max(
            5, int(getattr(settings, "stream_manager_interval_minutes", 60) or 60)
        )
    return out


class ServiceScheduler:
    def __init__(self) -> None:
        self._sched = AsyncIOScheduler()
        self._run_lock = asyncio.Lock()
        self._job_ids = {
            "sonarr": "fetcher_sonarr",
            "radarr": "fetcher_radarr",
            "trimmer": "fetcher_trimmer",
            "stream_manager": "fetcher_stream_manager",
        }

    async def _current_job_intervals_minutes(self) -> dict[str, int]:
        async with SessionLocal() as session:
            settings = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
            if not settings:
                return {}
            return compute_job_intervals_minutes(settings)

    async def _run_scope(self, scope: str) -> None:
        if self._run_lock.locked():
            return
        async with self._run_lock:
            async with SessionLocal() as session:
                await run_once(session, scheduled_scope=scope)

    async def _job_sonarr(self) -> None:
        await self._run_scope("sonarr")

    async def _job_radarr(self) -> None:
        await self._run_scope("radarr")

    async def _job_trimmer(self) -> None:
        await self._run_scope("trimmer")

    async def _job_stream_manager(self) -> None:
        async with SessionLocal() as session:
            await run_scheduled_stream_manager_pass(session)

    def _job_fn_for_scope(self, scope: str):
        if scope == "sonarr":
            return self._job_sonarr
        if scope == "radarr":
            return self._job_radarr
        if scope == "stream_manager":
            return self._job_stream_manager
        return self._job_trimmer

    async def start(self) -> None:
        intervals = await self._current_job_intervals_minutes()
        for scope, minutes in intervals.items():
            self._sched.add_job(
                self._job_fn_for_scope(scope),
                "interval",
                minutes=minutes,
                id=self._job_ids[scope],
                replace_existing=True,
                next_run_time=utc_now_naive(),
            )
        self._sched.start()

    async def reschedule(self, *, targets: set[str] | None = None) -> None:
        if not self._sched.running:
            return
        intervals = await self._current_job_intervals_minutes()
        active_targets = {"sonarr", "radarr", "trimmer"} if targets is None else set(targets)
        for scope in active_targets:
            if scope not in self._job_ids:
                continue
            minutes = intervals.get(scope)
            job_id = self._job_ids[scope]
            if minutes is None:
                job = self._sched.get_job(job_id)
                if job:
                    self._sched.remove_job(job_id)
                continue
            self._sched.add_job(
                self._job_fn_for_scope(scope),
                "interval",
                minutes=minutes,
                id=job_id,
                replace_existing=True,
            )

    def _job_next_run_at(self, scope: str) -> datetime | None:
        if not self._sched.running:
            return None
        job = self._sched.get_job(self._job_ids[scope])
        if not job:
            return None
        nrt = job.next_run_time
        if nrt is None:
            return None
        if nrt.tzinfo is not None:
            return nrt.astimezone(timezone.utc).replace(tzinfo=None)
        return nrt

    def next_runs_by_job(self) -> dict[str, datetime | None]:
        return {
            "sonarr": self._job_next_run_at("sonarr"),
            "radarr": self._job_next_run_at("radarr"),
            "trimmer": self._job_next_run_at("trimmer"),
            "stream_manager": self._job_next_run_at("stream_manager"),
        }

    def is_run_in_progress(self) -> bool:
        """True while a scheduled Sonarr/Radarr/Trimmer pass holds the run lock (read-only UI hint)."""
        return self._run_lock.locked()

    def next_fetcher_run_at(self) -> datetime | None:
        """Compatibility: earliest next scheduled job run across independent jobs."""
        runs = [d for d in self.next_runs_by_job().values() if d is not None]
        return min(runs) if runs else None

    def shutdown(self, *, wait: bool = True) -> None:
        """Stop APScheduler. Use ``wait=False`` on process exit so the event loop is not blocked."""
        if not self._sched.running:
            return
        try:
            self._sched.shutdown(wait=wait)
        except (RuntimeError, SchedulerNotRunningError):
            pass


# Process-wide scheduler (``app.main`` re-exports as ``scheduler`` for route registration + tests).
scheduler = ServiceScheduler()
