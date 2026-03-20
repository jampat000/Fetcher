from __future__ import annotations

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import SchedulerNotRunningError
from sqlalchemy import select

from app.db import SessionLocal
from app.models import AppSettings
from app.service_logic import run_once
from app.time_util import utc_now_naive


class ServiceScheduler:
    def __init__(self) -> None:
        self._sched = AsyncIOScheduler()
        self._lock = asyncio.Lock()
        self._job_id = "arr-manager"

    async def _current_interval_minutes(self) -> int:
        async with SessionLocal() as session:
            settings = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
            if not settings:
                return 60
            return max(5, int(settings.interval_minutes or 60))

    async def _job(self) -> None:
        # Prevent overlapping runs
        if self._lock.locked():
            return
        async with self._lock:
            async with SessionLocal() as session:
                await run_once(session)

    async def start(self) -> None:
        interval = await self._current_interval_minutes()
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
        interval = await self._current_interval_minutes()
        self._sched.add_job(
            self._job,
            "interval",
            minutes=interval,
            id=self._job_id,
            replace_existing=True,
        )

    def shutdown(self) -> None:
        if not self._sched.running:
            return
        try:
            # Prefer waiting for active jobs to finish to avoid noisy cancellation traces.
            self._sched.shutdown(wait=True)
        except (RuntimeError, SchedulerNotRunningError):
            # Can happen during process teardown when the event loop is gone.
            pass

