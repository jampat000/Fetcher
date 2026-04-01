from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import SchedulerNotRunningError
from sqlalchemy import select

from app.arr_intervals import effective_arr_interval_minutes
from app.db import SessionLocal
from app.models import AppSettings
from app.refiner_readiness import refiner_scheduler_should_run
from app.refiner_watch_config import clamp_refiner_interval_seconds
from app.refiner_service import run_scheduled_refiner_pass
from app.service_logic import run_once
from app.resolvers.api_keys import resolve_emby_api_key, resolve_radarr_api_key, resolve_sonarr_api_key
from app.time_util import utc_now_naive

logger = logging.getLogger(__name__)

_dashboard_changed: asyncio.Event = asyncio.Event()


def notify_dashboard_changed() -> None:
    global _dashboard_changed
    _dashboard_changed.set()
    _dashboard_changed = asyncio.Event()


async def wait_dashboard_changed(timeout: float = 29.0) -> None:
    try:
        await asyncio.wait_for(_dashboard_changed.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        pass


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


def _refiner_configured(settings: AppSettings) -> bool:
    return refiner_scheduler_should_run(settings)


def compute_job_intervals_minutes(settings: AppSettings) -> dict[str, int]:
    """Sonarr / Radarr / Trimmer — interval jobs use minutes. (Refiner uses seconds; see ``effective_refiner_interval_seconds``.)"""
    out: dict[str, int] = {}
    if _sonarr_configured(settings):
        out["sonarr"] = effective_arr_interval_minutes(getattr(settings, "sonarr_interval_minutes", None))
    if _radarr_configured(settings):
        out["radarr"] = effective_arr_interval_minutes(getattr(settings, "radarr_interval_minutes", None))
    if _emby_configured(settings):
        out["trimmer"] = max(5, int(settings.emby_interval_minutes or 60))
    return out


def effective_refiner_interval_seconds(settings: AppSettings) -> int | None:
    if not _refiner_configured(settings):
        return None
    return clamp_refiner_interval_seconds(
        getattr(settings, "refiner_interval_seconds", None),
    )


class ServiceScheduler:
    def __init__(self) -> None:
        self._sched = AsyncIOScheduler()
        self._run_lock = asyncio.Lock()
        self._job_ids = {
            "sonarr": "fetcher_sonarr",
            "radarr": "fetcher_radarr",
            "trimmer": "fetcher_trimmer",
            "refiner": "fetcher_refiner",
        }

    async def _current_job_intervals_minutes(self) -> dict[str, int]:
        async with SessionLocal() as session:
            settings = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
            if not settings:
                return {}
            return compute_job_intervals_minutes(settings)

    async def _current_scheduler_intervals(self) -> tuple[dict[str, int], int | None]:
        async with SessionLocal() as session:
            settings = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
            if not settings:
                return {}, None
            return compute_job_intervals_minutes(settings), effective_refiner_interval_seconds(settings)

    async def _run_scope(self, scope: str) -> None:
        if self._run_lock.locked():
            logger.info("Scheduler: skipping %s tick — previous run still in progress", scope)
            return
        async with self._run_lock:
            async with SessionLocal() as session:
                await run_once(session, scheduled_scope=scope)
            try:
                notify_dashboard_changed()
            except Exception:
                pass

    async def _job_sonarr(self) -> None:
        await self._run_scope("sonarr")

    async def _job_radarr(self) -> None:
        await self._run_scope("radarr")

    async def _job_trimmer(self) -> None:
        await self._run_scope("trimmer")

    async def _job_refiner(self) -> None:
        async with SessionLocal() as session:
            await run_scheduled_refiner_pass(session)

    def _job_fn_for_scope(self, scope: str):
        if scope == "sonarr":
            return self._job_sonarr
        if scope == "radarr":
            return self._job_radarr
        if scope == "refiner":
            return self._job_refiner
        return self._job_trimmer

    async def start(self) -> None:
        intervals, refiner_interval_seconds = await self._current_scheduler_intervals()
        for scope, minutes in intervals.items():
            self._sched.add_job(
                self._job_fn_for_scope(scope),
                "interval",
                minutes=minutes,
                id=self._job_ids[scope],
                replace_existing=True,
                next_run_time=utc_now_naive(),
            )
        if refiner_interval_seconds is not None:
            self._sched.add_job(
                self._job_refiner,
                "interval",
                seconds=refiner_interval_seconds,
                id=self._job_ids["refiner"],
                replace_existing=True,
                next_run_time=utc_now_naive(),
            )
        self._sched.start()

    async def reschedule(self, *, targets: set[str] | None = None) -> None:
        if not self._sched.running:
            return
        intervals, refiner_interval_seconds = await self._current_scheduler_intervals()
        active_targets = (
            {"sonarr", "radarr", "trimmer", "refiner"} if targets is None else set(targets)
        )
        for scope in active_targets:
            if scope not in self._job_ids:
                continue
            job_id = self._job_ids[scope]
            if scope == "refiner":
                if refiner_interval_seconds is None:
                    job = self._sched.get_job(job_id)
                    if job:
                        self._sched.remove_job(job_id)
                else:
                    self._sched.add_job(
                        self._job_refiner,
                        "interval",
                        seconds=refiner_interval_seconds,
                        id=job_id,
                        replace_existing=True,
                    )
                continue
            minutes = intervals.get(scope)
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
            "refiner": self._job_next_run_at("refiner"),
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
