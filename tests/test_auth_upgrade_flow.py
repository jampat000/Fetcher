"""Auth behaviour without the global require_auth override (upgrade / no-password path)."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from app.auth import hash_password

pytestmark = pytest.mark.no_auth_override
from app.db import SessionLocal, _get_or_create_settings
from app.main import app
from app.time_util import utc_now_naive


def _client_no_scheduler(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown() -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


async def _restore_seeded_auth_state() -> None:
    async with SessionLocal() as s:
        r = await _get_or_create_settings(s)
        r.auth_password_hash = hash_password("testpass12")
        r.auth_username = "admin"
        r.auth_bypass_lan = False
        r.auth_ip_allowlist = ""
        r.sonarr_url = ""
        r.updated_at = utc_now_naive()
        await s.commit()


@pytest.fixture
def client_real_auth(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """DB has no password until restored (``no_auth_override`` marker = real ``require_auth``)."""
    asyncio.run(_clear_password_and_bypass())
    with _client_no_scheduler(monkeypatch) as client:
        try:
            yield client
        finally:
            asyncio.run(_restore_seeded_auth_state())


async def _clear_password_and_bypass() -> None:
    async with SessionLocal() as s:
        r = await _get_or_create_settings(s)
        r.auth_password_hash = ""
        r.auth_bypass_lan = False
        r.auth_ip_allowlist = ""
        r.updated_at = utc_now_naive()
        await s.commit()


def test_no_password_home_redirects_to_setup(client_real_auth: TestClient) -> None:
    r = client_real_auth.get("/", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers.get("location", "").endswith("/setup/0")


def test_no_password_login_redirects_to_setup(client_real_auth: TestClient) -> None:
    r = client_real_auth.get("/login", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers.get("location", "").endswith("/setup/0")


def test_no_password_login_post_redirects(client_real_auth: TestClient) -> None:
    r = client_real_auth.post(
        "/login",
        data={"username": "admin", "password": "whatever"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers.get("location", "").endswith("/setup/0")


def test_no_password_json_home_returns_401_with_setup_path(client_real_auth: TestClient) -> None:
    r = client_real_auth.get("/", headers={"Accept": "application/json"}, follow_redirects=False)
    assert r.status_code == 401
    body = r.json()
    assert "detail" in body
    detail = body["detail"]
    assert detail.get("setup_path") == "/setup/0"
    assert "password" in (detail.get("message") or "").lower()


def test_ip_allowlist_does_not_skip_setup_when_no_password(client_real_auth: TestClient) -> None:
    async def set_allowlist() -> None:
        async with SessionLocal() as s:
            r = await _get_or_create_settings(s)
            r.auth_ip_allowlist = "127.0.0.1"
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(set_allowlist())
    r = client_real_auth.get("/", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers.get("location", "").endswith("/setup/0")


def test_ip_allowlist_bypasses_when_password_set_and_ip_matches(
    client_real_auth: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def password_plus_allowlist() -> None:
        async with SessionLocal() as s:
            r = await _get_or_create_settings(s)
            r.auth_password_hash = hash_password("testpass12")
            r.auth_ip_allowlist = "127.0.0.1"
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(password_plus_allowlist())
    monkeypatch.setattr("app.auth.get_client_ip", lambda _request: "127.0.0.1")
    r = client_real_auth.get("/", follow_redirects=False)
    assert r.status_code == 200


def test_setup_zero_upgrade_banner_when_sonarr_configured(client_real_auth: TestClient) -> None:
    async def prime() -> None:
        async with SessionLocal() as s:
            r = await _get_or_create_settings(s)
            r.sonarr_url = "http://127.0.0.1:8989"
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(prime())
    r = client_real_auth.get("/setup/0")
    assert r.status_code == 200
    assert b"Welcome back" in r.content
