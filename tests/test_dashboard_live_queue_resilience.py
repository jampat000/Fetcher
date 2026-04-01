"""fetch_live_dashboard_queue_totals: failure paths omit keys; snapshot fallback remains valid."""

from __future__ import annotations

import asyncio

import pytest

from app.db import SessionLocal, get_or_create_settings
from app.dashboard_service import fetch_live_dashboard_queue_totals


@pytest.fixture
def _enabled_sonarr_radarr_settings():
    async def _seed():
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            row.sonarr_enabled = True
            row.sonarr_url = "http://127.0.0.1:8989"
            row.sonarr_api_key = "sk"
            row.radarr_enabled = True
            row.radarr_url = "http://127.0.0.1:7878"
            row.radarr_api_key = "rk"
            await s.commit()

    asyncio.run(_seed())


def test_fetch_live_omits_sonarr_missing_when_helper_raises(
    monkeypatch: pytest.MonkeyPatch, _enabled_sonarr_radarr_settings
) -> None:
    async def _boom(_client):
        raise OSError("simulated unreachable")

    class _Arr:
        def __init__(self, *_a, **_kw) -> None:
            pass

        async def wanted_cutoff_unmet(self, *, page: int = 1, page_size: int = 50) -> dict:
            return {"totalRecords": 12}

        async def movies(self) -> list:
            return []

    monkeypatch.setattr(
        "app.dashboard_service._sonarr_missing_total_including_unreleased",
        _boom,
    )
    monkeypatch.setattr("app.dashboard_service.ArrClient", _Arr)
    monkeypatch.setattr("app.dashboard_service.resolve_sonarr_api_key", lambda _r: "k")
    monkeypatch.setattr("app.dashboard_service.resolve_radarr_api_key", lambda _r: "k")

    async def _go():
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            return await fetch_live_dashboard_queue_totals(row)

    live = asyncio.run(_go())
    assert "sonarr_missing" not in live
    assert live.get("sonarr_upgrades") == 12


def test_fetch_live_omits_radarr_missing_when_movies_raises(
    monkeypatch: pytest.MonkeyPatch, _enabled_sonarr_radarr_settings
) -> None:
    class _Arr:
        def __init__(self, *_a, **_kw) -> None:
            self._base = ""

        async def wanted_cutoff_unmet(self, *, page: int = 1, page_size: int = 50) -> dict:
            return {"totalRecords": 7}

        async def movies(self) -> list:
            raise ConnectionError("simulated connection failure")

    monkeypatch.setattr("app.dashboard_service.ArrClient", _Arr)
    monkeypatch.setattr("app.dashboard_service.resolve_sonarr_api_key", lambda _r: "k")
    monkeypatch.setattr("app.dashboard_service.resolve_radarr_api_key", lambda _r: "k")
    async def _son_zero(_client):
        return 0

    monkeypatch.setattr(
        "app.dashboard_service._sonarr_missing_total_including_unreleased",
        _son_zero,
    )

    async def _go():
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            return await fetch_live_dashboard_queue_totals(row)

    live = asyncio.run(_go())
    assert "radarr_missing" not in live
    assert live.get("radarr_upgrades") == 7


def test_fetch_live_sonarr_cutoff_omitted_on_invalid_response_shape(
    monkeypatch: pytest.MonkeyPatch, _enabled_sonarr_radarr_settings
) -> None:
    class _Arr:
        def __init__(self, *_a, **_kw) -> None:
            pass

        async def wanted_cutoff_unmet(self, *, page: int = 1, page_size: int = 50) -> object:
            return "not-a-dict"

        async def movies(self) -> list:
            return []

    async def _one(_client):
        return 1

    monkeypatch.setattr(
        "app.dashboard_service._sonarr_missing_total_including_unreleased",
        _one,
    )
    monkeypatch.setattr("app.dashboard_service.ArrClient", _Arr)
    monkeypatch.setattr("app.dashboard_service.resolve_sonarr_api_key", lambda _r: "k")
    monkeypatch.setattr("app.dashboard_service.resolve_radarr_api_key", lambda _r: "k")

    async def _go():
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            return await fetch_live_dashboard_queue_totals(row)

    live = asyncio.run(_go())
    assert live.get("sonarr_missing") == 1
    assert "sonarr_upgrades" not in live


def test_fetch_live_applies_wait_for_timeout_to_sonarr_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    real_wait = asyncio.wait_for

    async def _wait_wrapper(coro, timeout=None):
        if timeout == 25.0:
            coro.close()
            raise TimeoutError
        return await real_wait(coro, timeout=timeout)

    class _Arr:
        def __init__(self, *_a, **_kw) -> None:
            pass

        async def wanted_cutoff_unmet(self, *, page: int = 1, page_size: int = 50) -> dict:
            return {"totalRecords": 0}

        async def movies(self) -> list:
            return []

    monkeypatch.setattr("app.dashboard_service.asyncio.wait_for", _wait_wrapper)
    async def _ninety_nine(_client):
        return 99

    monkeypatch.setattr(
        "app.dashboard_service._sonarr_missing_total_including_unreleased",
        _ninety_nine,
    )
    monkeypatch.setattr("app.dashboard_service.ArrClient", _Arr)
    monkeypatch.setattr("app.dashboard_service.resolve_sonarr_api_key", lambda _r: "k")
    monkeypatch.setattr("app.dashboard_service.resolve_radarr_api_key", lambda _r: "k")

    async def _seed():
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            row.sonarr_enabled = True
            row.sonarr_url = "http://127.0.0.1:8989"
            row.sonarr_api_key = "sk"
            row.radarr_enabled = False
            await s.commit()

    asyncio.run(_seed())

    async def _go():
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            return await fetch_live_dashboard_queue_totals(row)

    live = asyncio.run(_go())
    assert "sonarr_missing" not in live
    assert live.get("sonarr_upgrades") == 0
