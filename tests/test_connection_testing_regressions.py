from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, desc, select

from app.db import SessionLocal, _get_or_create_settings
from app.main import app
from app.models import AppSnapshot
from app.setup_helpers import (
    test_radarr_connection as run_radarr_connection_test,
    test_sonarr_connection as run_sonarr_connection_test,
)


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


async def _set_settings(**updates: Any) -> None:
    async with SessionLocal() as s:
        row = await _get_or_create_settings(s)
        for k, v in updates.items():
            setattr(row, k, v)
        await s.commit()


async def _clear_snapshots() -> None:
    async with SessionLocal() as s:
        await s.execute(delete(AppSnapshot))
        await s.commit()


async def _latest_snapshot() -> AppSnapshot:
    async with SessionLocal() as s:
        res = await s.execute(select(AppSnapshot).order_by(desc(AppSnapshot.id)).limit(1))
        row = res.scalars().first()
        assert row is not None
        return row


def test_settings_test_sonarr_redirect_snapshot_and_key_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    asyncio.run(_clear_snapshots())
    asyncio.run(_set_settings(sonarr_url="http://sonarr.local:8989", sonarr_api_key="db-key"))
    seen: dict[str, Any] = {"resolver_called": 0, "service_called": 0, "api_key": None}

    def _resolve(row) -> str:
        seen["resolver_called"] += 1
        assert (row.sonarr_url or "").strip() == "http://sonarr.local:8989"
        return "resolved-sonarr-key"

    async def _check_arr_health(self, *, url: str, api_key: str):
        seen["service_called"] += 1
        assert url == "http://sonarr.local:8989"
        seen["api_key"] = api_key
        from app.connection_test_service import ArrHealthCheckResult

        return ArrHealthCheckResult(ok=True, error_kind="none")

    monkeypatch.setattr("app.routers.settings.resolve_sonarr_api_key", _resolve)
    monkeypatch.setattr("app.routers.settings.ConnectionTestService.check_arr_health", _check_arr_health)
    with _client(monkeypatch) as client:
        resp = client.post("/test/sonarr", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers.get("location") == "/settings?test=sonarr_ok"
    assert seen["resolver_called"] == 1
    assert seen["service_called"] == 1
    assert seen["api_key"] == "resolved-sonarr-key"
    snap = asyncio.run(_latest_snapshot())
    assert snap.app == "sonarr"
    assert snap.ok is True
    assert snap.status_message == "Connection test succeeded."
    assert snap.missing_total == 0
    assert snap.cutoff_unmet_total == 0


def test_settings_test_radarr_httpstatuserror_message_and_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    asyncio.run(_clear_snapshots())
    asyncio.run(_set_settings(radarr_url="http://radarr.local:7878", radarr_api_key="db-key"))
    seen = {"service_called": 0}

    async def _check_arr_health(self, *, url: str, api_key: str):
        seen["service_called"] += 1
        assert url == "http://radarr.local:7878"
        assert api_key == "resolved-radarr-key"
        from app.connection_test_service import ArrHealthCheckResult

        return ArrHealthCheckResult(
            ok=False,
            error_kind="http_status",
            status_code=401,
            error_message="401 Unauthorized",
            error_name="HTTPStatusError",
        )

    monkeypatch.setattr("app.routers.settings.resolve_radarr_api_key", lambda _row: "resolved-radarr-key")
    monkeypatch.setattr("app.routers.settings.ConnectionTestService.check_arr_health", _check_arr_health)
    with _client(monkeypatch) as client:
        resp = client.post("/test/radarr", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers.get("location") == "/settings?test=radarr_fail"
    assert seen["service_called"] == 1
    snap = asyncio.run(_latest_snapshot())
    assert snap.app == "radarr"
    assert snap.ok is False
    assert snap.status_message == "Connection test failed: HTTPStatusError: 401 Unauthorized"
    assert snap.missing_total == 0
    assert snap.cutoff_unmet_total == 0


def test_setup_helper_httpstatuserror_differs_from_settings_style(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _check_arr_health(self, *, url: str, api_key: str):
        assert url == "http://sonarr.local:8989"
        assert api_key == "abc"
        from app.connection_test_service import ArrHealthCheckResult

        return ArrHealthCheckResult(
            ok=False,
            error_kind="http_status",
            status_code=401,
            error_message="401 Unauthorized",
            error_name="HTTPStatusError",
        )

    monkeypatch.setattr("app.setup_helpers.ConnectionTestService.check_arr_health", _check_arr_health)
    ok, msg = asyncio.run(run_sonarr_connection_test("http://sonarr.local:8989", "abc"))
    assert ok is False
    assert msg == "HTTP 401 — check the API key in Sonarr (Settings → General)."


def test_setup_helper_http_error_reports_exception_class(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _check_arr_health(self, *, url: str, api_key: str):
        assert url == "http://radarr.local:7878"
        assert api_key == "abc"
        from app.connection_test_service import ArrHealthCheckResult

        return ArrHealthCheckResult(
            ok=False,
            error_kind="http_error",
            error_message="boom",
            error_name="ConnectError",
        )

    monkeypatch.setattr("app.setup_helpers.ConnectionTestService.check_arr_health", _check_arr_health)
    ok, msg = asyncio.run(run_radarr_connection_test("http://radarr.local:7878", "abc"))
    assert ok is False
    assert msg == "ConnectError: boom"


def test_api_setup_test_sonarr_uses_resolver_and_returns_helper_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    seen: dict[str, Any] = {"scope": None, "api_key": None, "helper_key": None}

    def _resolve(api_key: str, scope: str) -> str:
        seen["api_key"] = api_key
        seen["scope"] = scope
        return "resolved-api-key"

    async def _helper(url: str, key: str) -> tuple[bool, str]:
        seen["helper_key"] = key
        assert url == "http://sonarr.local:8989"
        return True, "Sonarr responded OK."

    monkeypatch.setattr("app.routers.api.resolve_setup_api_key", _resolve)
    monkeypatch.setattr("app.routers.api.test_sonarr_connection", _helper)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/api/setup/test-sonarr",
            json={"url": "http://sonarr.local:8989", "api_key": "from-body"},
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "message": "Sonarr responded OK."}
    assert seen["scope"] == "sonarr"
    assert seen["api_key"] == "from-body"
    assert seen["helper_key"] == "resolved-api-key"


def test_api_setup_test_radarr_propagates_helper_httpstatus_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")

    async def _helper(_url: str, _key: str) -> tuple[bool, str]:
        return False, "HTTP 401 — check the API key in Radarr (Settings → General)."

    monkeypatch.setattr("app.routers.api.resolve_setup_api_key", lambda api_key, scope: api_key)
    monkeypatch.setattr("app.routers.api.test_radarr_connection", _helper)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/api/setup/test-radarr",
            json={"url": "http://radarr.local:7878", "api_key": "abc"},
        )
    assert resp.status_code == 200
    assert resp.json() == {
        "ok": False,
        "message": "HTTP 401 — check the API key in Radarr (Settings → General).",
    }
