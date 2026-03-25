from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.db import SessionLocal, _get_or_create_settings
from app.main import app
from app.migrations import migrate
from app.service_logic import _build_run_context


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


def test_migration_copies_old_arr_search_cooldown_to_retry_delays() -> None:
    async def run() -> tuple[int, int]:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    CREATE TABLE app_settings (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      interval_minutes INTEGER NOT NULL DEFAULT 60,
                      arr_search_cooldown_minutes INTEGER NOT NULL DEFAULT 1440
                    )
                    """
                )
            )
            await conn.execute(
                text("INSERT INTO app_settings(interval_minutes, arr_search_cooldown_minutes) VALUES (60, 321)")
            )
        await migrate(engine)
        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT sonarr_retry_delay_minutes, radarr_retry_delay_minutes "
                        "FROM app_settings ORDER BY id ASC LIMIT 1"
                    )
                )
            ).first()
        await engine.dispose()
        assert row is not None
        return int(row[0]), int(row[1])

    son, rad = asyncio.run(run())
    assert son == 321
    assert rad == 321


def test_sonarr_retry_delay_validation_rejects_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "save_scope": "sonarr",
                "sonarr_enabled": "false",
                "sonarr_url": "",
                "sonarr_api_key": "",
                "sonarr_search_missing": "true",
                "sonarr_search_upgrades": "true",
                "sonarr_max_items_per_run": "50",
                "sonarr_interval_minutes": "60",
                "sonarr_retry_delay_minutes": "0",
                "sonarr_schedule_start": "00:00",
                "sonarr_schedule_end": "23:59",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "reason=sonarr_retry_delay_min" in (resp.headers.get("location") or "")


def test_radarr_retry_delay_validation_rejects_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "save_scope": "radarr",
                "radarr_enabled": "false",
                "radarr_url": "",
                "radarr_api_key": "",
                "radarr_search_missing": "true",
                "radarr_search_upgrades": "true",
                "radarr_remove_failed_imports": "false",
                "radarr_max_items_per_run": "50",
                "radarr_interval_minutes": "60",
                "radarr_retry_delay_minutes": "0",
                "radarr_schedule_start": "00:00",
                "radarr_schedule_end": "23:59",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "reason=radarr_retry_delay_min" in (resp.headers.get("location") or "")


def test_retry_delay_settings_are_isolated_and_persistent(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        son = client.post(
            "/settings",
            data={
                "save_scope": "sonarr",
                "sonarr_enabled": "false",
                "sonarr_url": "",
                "sonarr_api_key": "",
                "sonarr_search_missing": "true",
                "sonarr_search_upgrades": "true",
                "sonarr_max_items_per_run": "50",
                "sonarr_interval_minutes": "60",
                "sonarr_retry_delay_minutes": "17",
                "sonarr_schedule_start": "00:00",
                "sonarr_schedule_end": "23:59",
            },
            follow_redirects=False,
        )
        rad = client.post(
            "/settings",
            data={
                "save_scope": "radarr",
                "radarr_enabled": "false",
                "radarr_url": "",
                "radarr_api_key": "",
                "radarr_search_missing": "true",
                "radarr_search_upgrades": "true",
                "radarr_remove_failed_imports": "false",
                "radarr_max_items_per_run": "50",
                "radarr_interval_minutes": "60",
                "radarr_retry_delay_minutes": "89",
                "radarr_schedule_start": "00:00",
                "radarr_schedule_end": "23:59",
            },
            follow_redirects=False,
        )
        page = client.get("/settings")
    assert son.status_code == 303
    assert rad.status_code == 303
    assert page.status_code == 200
    assert 'name="sonarr_retry_delay_minutes" value="17"' in page.text
    assert 'name="radarr_retry_delay_minutes" value="89"' in page.text

    async def verify_db() -> tuple[int, int]:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            return int(row.sonarr_retry_delay_minutes), int(row.radarr_retry_delay_minutes)

    assert asyncio.run(verify_db()) == (17, 89)


def test_run_context_uses_per_app_retry_delays_only() -> None:
    async def seed_and_build():
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.sonarr_retry_delay_minutes = 11
            row.radarr_retry_delay_minutes = 73
            row.sonarr_interval_minutes = 5
            row.radarr_interval_minutes = 300
            row.timezone = "UTC"
            await session.commit()
            return _build_run_context(row, arr_manual_scope=None)

    ctx = asyncio.run(seed_and_build())
    assert ctx.sonarr_retry_delay_minutes == 11
    assert ctx.radarr_retry_delay_minutes == 73


def test_missing_progression_path_does_not_use_wanted_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.service_logic import run_once

    async def prep() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.sonarr_enabled = True
            row.sonarr_url = "http://localhost:8989"
            row.sonarr_search_missing = True
            row.sonarr_search_upgrades = False
            row.sonarr_retry_delay_minutes = 15
            row.radarr_enabled = False
            row.emby_enabled = False
            row.sonarr_last_run_at = None
            await session.commit()

    asyncio.run(prep())

    class _FakeArrClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            return None

        async def series(self):
            return [{"id": 1, "title": "A"}]

        async def episodes_for_series(self, *, series_id: int):
            assert series_id == 1
            return [{"id": 99, "monitored": True, "hasFile": False, "seriesId": 1}]

        async def ensure_tag(self, _label: str):
            return 1

        async def add_tags_to_series(self, **_kwargs):
            return None

        async def aclose(self):
            return None

    async def _wanted_total(*args, **kwargs):
        return 0

    async def _trigger(*args, **kwargs):
        return None

    async def _panic_paginate(*_args, **kwargs):
        assert kwargs.get("kind") != "missing", "wanted/missing should not be used for missing progression path"
        return [], [], 0

    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "k")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.in_window", lambda **_kw: True)
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.service_logic._paginate_wanted_for_search", _panic_paginate)
    monkeypatch.setattr("app.service_logic._wanted_queue_total", _wanted_total)
    monkeypatch.setattr("app.service_logic.trigger_sonarr_missing_search", _trigger)
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: datetime(2026, 3, 25, 12, 0, 0))

    async def run() -> str:
        async with SessionLocal() as session:
            result = await run_once(session)
            return result.message

    msg = asyncio.run(run())
    assert "Sonarr: missing search for 1 episode(s)" in msg
