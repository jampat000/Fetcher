from __future__ import annotations

import asyncio
from datetime import datetime

from sqlalchemy import delete

from app.db import SessionLocal, _get_or_create_settings
from app.models import AppSnapshot
from app.web_common import build_dashboard_status


async def _seed_snapshot_state() -> None:
    async with SessionLocal() as s:
        await s.execute(delete(AppSnapshot))
        row = await _get_or_create_settings(s)
        row.timezone = "UTC"
        row.sonarr_enabled = True
        row.sonarr_url = "http://localhost:8989"
        row.sonarr_interval_minutes = 30
        row.radarr_enabled = True
        row.radarr_url = "http://localhost:7878"
        row.radarr_interval_minutes = 45
        row.emby_enabled = True
        row.emby_url = "http://localhost:8096"
        row.emby_interval_minutes = 60
        row.sonarr_last_run_at = datetime(2026, 3, 24, 10, 0, 0)
        row.radarr_last_run_at = datetime(2026, 3, 24, 10, 15, 0)
        row.emby_last_run_at = datetime(2026, 3, 24, 10, 30, 0)
        s.add(AppSnapshot(app="sonarr", ok=True, status_message="OK", missing_total=1, cutoff_unmet_total=2))
        s.add(AppSnapshot(app="radarr", ok=False, status_message="err", missing_total=3, cutoff_unmet_total=4))
        s.add(AppSnapshot(app="emby", ok=True, status_message="OK", missing_total=5, cutoff_unmet_total=0))
        await s.commit()


def test_build_dashboard_status_has_per_app_last_run_status(monkeypatch) -> None:
    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {
                "sonarr": datetime(2026, 3, 24, 11, 0, 0),
                "radarr": datetime(2026, 3, 24, 11, 30, 0),
                "trimmer": datetime(2026, 3, 24, 12, 0, 0),
            }

    monkeypatch.setattr("app.web_common.scheduler", _FakeScheduler())
    asyncio.run(_seed_snapshot_state())

    async def _go():
        async with SessionLocal() as s:
            data = await build_dashboard_status(s, "UTC")
            assert data["last_sonarr_run"]["time_local"] != ""
            assert data["last_sonarr_run"]["ok"] is True
            assert data["last_radarr_run"]["time_local"] != ""
            assert data["last_radarr_run"]["ok"] is False
            assert data["last_trimmer_run"]["time_local"] != ""
            assert data["last_trimmer_run"]["ok"] is True
            assert data["next_sonarr_tick_local"] != ""
            assert data["next_radarr_tick_local"] != ""
            assert data["next_trimmer_tick_local"] != ""

    asyncio.run(_go())
