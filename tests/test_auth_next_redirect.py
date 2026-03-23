"""Post-login ``next`` redirect and ``sanitize_next_param`` (open-redirect safe)."""

from __future__ import annotations

import asyncio
from urllib.parse import parse_qs, unquote, urlparse

import pytest
from fastapi.testclient import TestClient

from app.auth import hash_password, sanitize_next_param

pytestmark = pytest.mark.no_auth_override
from app.db import SessionLocal, _get_or_create_settings
from app.main import app
from app.time_util import utc_now_naive


async def _restore_seeded_auth_state() -> None:
    async with SessionLocal() as s:
        r = await _get_or_create_settings(s)
        r.auth_password_hash = hash_password("testpass12")
        r.auth_username = "admin"
        r.auth_bypass_lan = False
        r.auth_ip_allowlist = ""
        r.updated_at = utc_now_naive()
        await s.commit()


def _client_no_scheduler(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


@pytest.fixture
def client_real_auth(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _clear_password() -> None:
        async with SessionLocal() as s:
            r = await _get_or_create_settings(s)
            r.auth_password_hash = ""
            r.auth_bypass_lan = False
            r.auth_ip_allowlist = ""
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(_clear_password())
    with _client_no_scheduler(monkeypatch) as client:
        try:
            yield client
        finally:
            asyncio.run(_restore_seeded_auth_state())


def test_sanitize_next_param() -> None:
    assert sanitize_next_param("/settings") == "/settings"
    assert sanitize_next_param("/settings?saved=1") == "/settings?saved=1"
    assert sanitize_next_param("") == "/"
    assert sanitize_next_param("//evil.com") == "/"
    assert sanitize_next_param("https://evil.com") == "/"
    assert sanitize_next_param("/open?x=http://evil.com") == "/open?x=http://evil.com"


def test_unauthed_settings_redirect_includes_next(client_real_auth: TestClient) -> None:
    asyncio.run(_restore_seeded_auth_state())
    r = client_real_auth.get("/settings", follow_redirects=False)
    assert r.status_code == 303
    loc = r.headers.get("location", "")
    assert loc.startswith("/login")
    parsed = urlparse(loc)
    qs = parse_qs(parsed.query)
    raw_next = (qs.get("next") or [""])[0]
    assert unquote(raw_next) == "/settings"


def test_login_post_respects_next(client_real_auth: TestClient) -> None:
    asyncio.run(_restore_seeded_auth_state())
    r = client_real_auth.post(
        "/login",
        data={"username": "admin", "password": "testpass12", "next": "/settings"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers.get("location") == "/settings"


def test_login_post_ignores_malicious_next(client_real_auth: TestClient) -> None:
    asyncio.run(_restore_seeded_auth_state())
    r = client_real_auth.post(
        "/login",
        data={"username": "admin", "password": "testpass12", "next": "https://evil.com/phish"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers.get("location") == "/"
