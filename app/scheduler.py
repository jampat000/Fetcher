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
from app.time_util import utc_now_naive


def _sonarr_configured(settings: AppSettings) -> bool:
    return bool(
        settings.sonarr_enabled
        and (settings.sonarr_url or "").strip()
        and (settings.sonarr_api_key or "").strip()
    )


def _radarr_configured(settings: AppSettings) -> bool:
    return bool(
        settings.radarr_enabled
        and (settings.radarr_url or "").strip()
        and (settings.radarr_api_key or "").strip()
    )


def _emby_configured(settings: AppSettings) -> bool:
    return bool(
        settings.emby_enabled
        and (settings.emby_url or "").strip()
        and (settings.emby_api_key or "").strip()
    )


def compute_grabby_tick_minutes(settings: AppSettings) -> int:
    """How often the Grabby scheduler wakes: minimum effective Sonarr/Radarr run intervals in play."""
    tick: int | None = None
    if _sonarr_configured(settings):
        s_int = effective_arr_interval_minutes(getattr(settings, "sonarr_interval_minutes", None))
        tick = s_int if tick is None else min(tick, s_int)
    if _radarr_configured(settings):
        r_int = effective_arr_interval_minutes(getattr(settings, "radarr_interval_minutes", None))
        tick = r_int if tick is None else min(tick, r_int)
    if tick is None:
        return effective_arr_interval_minutes(0)
    return max(5, tick)


class ServiceScheduler:
    def __init__(self) -> None:
        self._sched = AsyncIOScheduler()
        self._lock = asyncio.Lock()
        self._job_id = "grabby"

    async def _current_tick_minutes(self) -> int:
        async with SessionLocal() as session:
            settings = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
            if not settings:
                return 60
            return compute_grabby_tick_minutes(settings)

    async def _job(self) -> None:
        if self._lock.locked():
            return
        async with self._lock:
            async with SessionLocal() as session:
                await run_once(session)

    async def start(self) -> None:
        interval = await self._current_tick_minutes()
        self._sched.add_job(
            self._job,
            "interval",
            minutes=interval,
            id=self._job_id,
            replace_existing=True,
            next_run_time=utc_now_naive(),
        )
        self._sched.start()

    async def reschedule(self) -> None:
        if not self._sched.running:
            return
        interval = await self._current_tick_minutes()
        self._sched.add_job(
            self._job,
            "interval",
            minutes=interval,
            id=self._job_id,
            replace_existing=True,
        )

    def next_grabby_run_at(self) -> datetime | None:
        """Next scheduled tick for the main automation job (naive UTC if job uses naive)."""
        if not self._sched.running:
            return None
        job = self._sched.get_job(self._job_id)
        if not job:
            return None
        nrt = job.next_run_time
        if nrt is None:
            return None
        if nrt.tzinfo is not None:
            return nrt.astimezone(timezone.utc).replace(tzinfo=None)
        return nrt

    def shutdown(self) -> None:
        if not self._sched.running:
            return
        try:
            self._sched.shutdown(wait=True)
        except (RuntimeError, SchedulerNotRunningError):
            pass
