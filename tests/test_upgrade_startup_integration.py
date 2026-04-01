"""Integration-style checks for upgrade/diagnostic logging on real app startup (pytest DB)."""

from __future__ import annotations

import asyncio
import logging

import pytest
from fastapi.testclient import TestClient

from app.auth import bootstrap_auth_on_startup, hash_password
from app.db import SessionLocal, get_or_create_settings
from app.main import app
from app.time_util import utc_now_naive

pytestmark = pytest.mark.no_auth_override


def _client_no_scheduler(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


def test_lifespan_logs_auth_startup_diagnostic(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    # Root logger defaults to WARNING in configure_fetcher_logging(); diagnostics are INFO.
    with caplog.at_level(logging.INFO, logger="app.auth"):
        with _client_no_scheduler(monkeypatch) as client:
            r = client.get("/healthz")
            assert r.status_code == 200
    messages = " ".join(rec.getMessage() for rec in caplog.records)
    assert "Auth startup diagnostic:" in messages
    assert "password_hash_configured=" in messages


def test_fetcher_reset_auth_logs_error_level(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    async def seed_password() -> None:
        async with SessionLocal() as s:
            r = await get_or_create_settings(s)
            r.auth_password_hash = hash_password("x" * 12)
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(seed_password())
    monkeypatch.setenv("FETCHER_RESET_AUTH", "1")
    with caplog.at_level(logging.ERROR, logger="app.auth"):
        asyncio.run(bootstrap_auth_on_startup())
    assert any("FETCHER_RESET_AUTH" in rec.getMessage() for rec in caplog.records)
