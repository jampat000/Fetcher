from __future__ import annotations

import asyncio
from datetime import datetime

from sqlalchemy import delete

from app.db import SessionLocal, get_or_create_settings
from app.models import AppSnapshot, JobRunLog, RefinerActivity
from app.dashboard_service import build_dashboard_status, fetch_live_dashboard_queue_totals


async def _seed_snapshot_state() -> None:
    async with SessionLocal() as s:
        await s.execute(delete(AppSnapshot))
        row = await get_or_create_settings(s)
        row.timezone = "UTC"
        row.sonarr_enabled = True
        row.sonarr_url = "http://localhost:8989"
        row.sonarr_search_interval_minutes = 30
        row.radarr_enabled = True
        row.radarr_url = "http://localhost:7878"
        row.radarr_search_interval_minutes = 45
        row.emby_enabled = True
        row.emby_url = "http://localhost:8096"
        row.emby_api_key = "test-emby-key"
        row.trimmer_interval_minutes = 60
        row.movie_refiner_interval_seconds = 60
        row.tv_refiner_interval_seconds = 60
        row.sonarr_last_run_at = datetime(2026, 3, 24, 10, 0, 0)
        row.radarr_last_run_at = datetime(2026, 3, 24, 10, 15, 0)
        row.emby_last_run_at = datetime(2026, 3, 24, 10, 30, 0)
        s.add(AppSnapshot(app="sonarr", ok=True, status_message="OK", missing_total=1, cutoff_unmet_total=2))
        s.add(AppSnapshot(app="radarr", ok=False, status_message="err", missing_total=3, cutoff_unmet_total=4))
        s.add(AppSnapshot(app="emby", ok=True, status_message="OK", missing_total=5, cutoff_unmet_total=0))
        await s.commit()


async def _seed_refiner_last_run_and_rows(*rows: RefinerActivity) -> None:
    async with SessionLocal() as s:
        await s.execute(delete(RefinerActivity))
        st = await get_or_create_settings(s)
        st.refiner_last_run_at = datetime(2026, 3, 24, 10, 45, 0)
        for r in rows:
            s.add(r)
        await s.commit()


async def _seed_refiner_job_log(*, ok: bool) -> None:
    async with SessionLocal() as s:
        await s.execute(delete(JobRunLog).where(JobRunLog.app == "refiner"))
        t = datetime(2026, 3, 24, 11, 0, 0)
        s.add(
            JobRunLog(
                started_at=t,
                finished_at=t,
                ok=ok,
                message="Refiner: batch",
                app="refiner",
            )
        )
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

        @staticmethod
        def is_run_in_progress():
            return False

    async def _no_live(_settings):
        return {}

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _no_live)
    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())
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
            assert data["next_sonarr_display"]["state"] == "scheduled"
            assert data["next_sonarr_display"]["primary"] == data["next_sonarr_relative"]
            assert data["next_sonarr_display"]["secondary"] == ""
            assert data["next_radarr_display"]["state"] == "scheduled"
            assert data["next_trimmer_display"]["state"] == "scheduled"
            assert data["last_sonarr_run"]["relative"] != ""
            assert data["next_sonarr_relative"] != ""
            assert data["fetcher_phase"] in ("processing", "idle", "active")
            assert "No line for this app" not in (data["sonarr_automation_sub"] or "")
            assert data["trimmer_connection_type"] == "Trimmer"
            assert data["trimmer_connection_status"] == "Connected"
            assert "last_sonarr_cleanup_run" in data
            assert "next_sonarr_cleanup_display" in data
            assert "sonarr_cleanup_ui_active" in data

    asyncio.run(_go())


def test_build_dashboard_status_unscheduled_and_disabled_next_run_display(monkeypatch) -> None:
    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {}

        @staticmethod
        def is_run_in_progress():
            return False

    async def _no_live(_settings):
        return {}

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _no_live)
    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())
    asyncio.run(_seed_snapshot_state())

    async def _go():
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            row.sonarr_enabled = True
            row.sonarr_schedule_enabled = False
            row.radarr_enabled = True
            row.radarr_schedule_enabled = False
            row.refiner_enabled = True
            row.refiner_schedule_enabled = False
            row.emby_enabled = False
            await s.commit()
            data = await build_dashboard_status(s, "UTC")
            assert data["next_sonarr_display"] == {
                "state": "enabled_unscheduled",
                "primary": "Always on",
                "secondary": "No schedule configured",
            }
            assert data["next_radarr_display"] == {
                "state": "enabled_unscheduled",
                "primary": "Always on",
                "secondary": "No schedule configured",
            }
            assert data["next_refiner_display"] == {
                "state": "watching",
                "primary": "Watching for files",
                "secondary": "Scans every 1m",
            }
            assert data["next_trimmer_display"] == {
                "state": "disabled",
                "primary": "Off",
                "secondary": "Disabled in settings",
            }

    asyncio.run(_go())


def test_fetch_live_dashboard_missing_uses_including_unreleased_semantics(monkeypatch) -> None:
    """Live missing counts must match service_logic (monitored, no file, incl. unreleased), not /wanted/missing only."""

    async def _sonarr_missing_fixed(_client) -> int:
        return 501

    monkeypatch.setattr(
        "app.dashboard_service._sonarr_missing_total_including_unreleased",
        _sonarr_missing_fixed,
    )

    class _FakeArrClient:
        """Radarr path uses movies(); Sonarr path only hits wanted_cutoff_unmet after patched missing helper."""

        def __init__(self, cfg, *, timeout_s: float = 30.0, http_client=None) -> None:
            self._base = cfg.base_url

        async def wanted_cutoff_unmet(self, *, page: int = 1, page_size: int = 50) -> dict:
            return (
                {"totalRecords": 33}
                if "7878" in self._base
                else {"totalRecords": 44}
            )

        async def movies(self) -> list:
            return [
                {"monitored": True, "hasFile": False},
                {"monitored": True, "hasFile": False},
                {"monitored": True, "hasFile": True},
                {"monitored": False, "hasFile": False},
            ]

    monkeypatch.setattr("app.dashboard_service.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.dashboard_service.resolve_sonarr_api_key", lambda _row: "k1")
    monkeypatch.setattr("app.dashboard_service.resolve_radarr_api_key", lambda _row: "k2")
    asyncio.run(_seed_snapshot_state())

    async def _go():
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            live = await fetch_live_dashboard_queue_totals(row)
        assert live["sonarr_missing"] == 501
        assert live["radarr_missing"] == 2
        assert live["sonarr_upgrades"] == 44
        assert live["radarr_upgrades"] == 33

    asyncio.run(_go())


def test_build_dashboard_status_live_queue_totals_override_snapshot(monkeypatch) -> None:
    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {}

        @staticmethod
        def is_run_in_progress():
            return False

    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())

    async def _fake_live(_settings):
        return {
            "sonarr_missing": 42,
            "sonarr_upgrades": 7,
            "radarr_missing": 80,
            "radarr_upgrades": 9,
        }

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _fake_live)
    asyncio.run(_seed_snapshot_state())

    async def _go():
        async with SessionLocal() as s:
            data = await build_dashboard_status(s, "UTC")
            assert data["sonarr_missing"] == 42
            assert data["sonarr_upgrades"] == 7
            assert data["radarr_missing"] == 80
            assert data["radarr_upgrades"] == 9
            assert data["emby_matched"] == 5

    asyncio.run(_go())


def test_dashboard_refiner_ok_from_job_log_not_stale_per_file_activity(monkeypatch) -> None:
    """Latest Refiner JobRunLog wins; old per-file RefinerActivity does not pin the pill."""
    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {}

        @staticmethod
        def is_run_in_progress():
            return False

    async def _no_live(_settings):
        return {}

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _no_live)
    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())
    asyncio.run(_seed_snapshot_state())
    asyncio.run(
        _seed_refiner_last_run_and_rows(
            RefinerActivity(
                file_name="x.mkv",
                media_title="X",
                status="failed",
                size_before_bytes=0,
                size_after_bytes=0,
                audio_tracks_before=0,
                audio_tracks_after=0,
                subtitle_tracks_before=0,
                subtitle_tracks_after=0,
                activity_context='{"reason_code":"radarr_queue_active_download","failure_reason":"waiting"}',
            )
        )
    )
    asyncio.run(_seed_refiner_job_log(ok=True))

    async def _go():
        async with SessionLocal() as s:
            data = await build_dashboard_status(s, "UTC")
            assert data["last_refiner_run"]["outcome"] == "none"
            assert data["last_refiner_run"]["ok"] is True

    asyncio.run(_go())


def test_dashboard_refiner_failed_from_latest_job_log(monkeypatch) -> None:
    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {}

        @staticmethod
        def is_run_in_progress():
            return False

    async def _no_live(_settings):
        return {}

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _no_live)
    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())
    asyncio.run(_seed_snapshot_state())
    asyncio.run(_seed_refiner_last_run_and_rows())
    asyncio.run(_seed_refiner_job_log(ok=False))

    async def _go():
        async with SessionLocal() as s:
            data = await build_dashboard_status(s, "UTC")
            assert data["last_refiner_run"]["outcome"] == "none"
            assert data["last_refiner_run"]["ok"] is False

    asyncio.run(_go())


def test_dashboard_refiner_success_from_latest_job_log(monkeypatch) -> None:
    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {}

        @staticmethod
        def is_run_in_progress():
            return False

    async def _no_live(_settings):
        return {}

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _no_live)
    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())
    asyncio.run(_seed_snapshot_state())
    asyncio.run(_seed_refiner_last_run_and_rows())
    asyncio.run(_seed_refiner_job_log(ok=True))

    async def _go():
        async with SessionLocal() as s:
            data = await build_dashboard_status(s, "UTC")
            assert data["last_refiner_run"]["outcome"] == "none"
            assert data["last_refiner_run"]["ok"] is True

    asyncio.run(_go())


def test_dashboard_refiner_no_rows_preserves_fallback(monkeypatch) -> None:
    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {}

        @staticmethod
        def is_run_in_progress():
            return False

    async def _no_live(_settings):
        return {}

    monkeypatch.setattr("app.dashboard_service.fetch_live_dashboard_queue_totals", _no_live)
    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())
    asyncio.run(_seed_snapshot_state())

    async def _go():
        async with SessionLocal() as s:
            await s.execute(delete(RefinerActivity))
            await s.execute(delete(JobRunLog).where(JobRunLog.app == "refiner"))
            st = await get_or_create_settings(s)
            st.refiner_last_run_at = datetime(2026, 3, 24, 10, 45, 0)
            await s.commit()
            data = await build_dashboard_status(s, "UTC")
            assert data["last_refiner_run"]["ok"] is None
            assert data["last_refiner_run"]["outcome"] == "none"
            assert data["last_refiner_run"]["time_local"] != ""

    asyncio.run(_go())


def test_build_dashboard_status_includes_sonarr_refiner_keys(monkeypatch) -> None:
    """Sonarr Refiner pipeline keys are present in dashboard status even when disabled."""

    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {
                "sonarr": None,
                "radarr": None,
                "trimmer": None,
                "refiner": None,
                "sonarr_refiner": None,
            }

        @staticmethod
        def is_run_in_progress():
            return False

    async def _no_live(_settings):
        return {}

    monkeypatch.setattr(
        "app.dashboard_service.fetch_live_dashboard_queue_totals",
        _no_live,
    )
    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())

    async def _go() -> None:
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            row.sonarr_refiner_enabled = False
            await s.commit()
            data = await build_dashboard_status(s, "UTC")
            assert "last_sonarr_refiner_run" in data
            assert "next_sonarr_refiner_display" in data
            assert "sonarr_refiner_sparkline" in data
            assert "sonarr_refiner_live_total" in data
            assert "sonarr_refiner_live_done" in data
            assert "sonarr_refiner_enabled" in data
            assert data["next_sonarr_refiner_display"] == {
                "state": "disabled",
                "primary": "Off",
                "secondary": "Disabled in settings",
            }
            assert data["sonarr_refiner_enabled"] is False

    asyncio.run(_go())


def test_build_dashboard_status_sonarr_refiner_watching_when_enabled(monkeypatch) -> None:
    """When Sonarr Refiner is enabled, next run shows Watching for files."""

    class _FakeScheduler:
        @staticmethod
        def next_runs_by_job():
            return {
                "sonarr": None,
                "radarr": None,
                "trimmer": None,
                "refiner": None,
                "sonarr_refiner": None,
            }

        @staticmethod
        def is_run_in_progress():
            return False

    async def _no_live(_settings):
        return {}

    monkeypatch.setattr(
        "app.dashboard_service.fetch_live_dashboard_queue_totals",
        _no_live,
    )
    monkeypatch.setattr("app.dashboard_service.scheduler", _FakeScheduler())

    async def _go() -> None:
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            row.sonarr_refiner_enabled = True
            row.sonarr_refiner_primary_audio_lang = "eng"
            row.tv_refiner_interval_seconds = 30
            await s.commit()
            data = await build_dashboard_status(s, "UTC")
            assert data["next_sonarr_refiner_display"] == {
                "state": "watching",
                "primary": "Watching for files",
                "secondary": "Scans every 30s",
            }
            assert data["sonarr_refiner_enabled"] is True

    asyncio.run(_go())
