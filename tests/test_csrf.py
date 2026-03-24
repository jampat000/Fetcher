"""CSRF protection for form POSTs (real ``require_csrf``; other tests override it)."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient
from itsdangerous import SignatureExpired
from unittest.mock import patch

from app.auth import build_session_cookie_value, generate_csrf_token
from app.db import SessionLocal, _get_or_create_settings
from app.main import app

# Must match ``tests/conftest.py`` seed after ``_init_fetcher_test_database``.
_TEST_SESSION_SECRET = "0123456789abcdef" * 4

pytestmark = pytest.mark.real_csrf


def _scheduler_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)


def _set_fetcher_session_cookie(client: TestClient) -> None:
    """Attach session cookie on the client (avoids per-request ``cookies=`` deprecation)."""
    client.cookies.set(
        "fetcher_session",
        build_session_cookie_value(secret=_TEST_SESSION_SECRET, username="admin"),
    )


def _csrf_value() -> str:
    return generate_csrf_token(_TEST_SESSION_SECRET, "admin")


def test_post_protected_form_without_csrf_returns_403(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    with TestClient(app) as client:
        _set_fetcher_session_cookie(client)
        r = client.post(
            "/settings/auth/access_control",
            data={"auth_ip_allowlist": "127.0.0.1"},
            follow_redirects=False,
        )
    assert r.status_code == 403
    assert "Invalid or expired CSRF token" in (r.json().get("detail") or "")


def test_post_protected_form_with_valid_csrf_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    try:
        with TestClient(app) as client:
            _set_fetcher_session_cookie(client)
            r = client.post(
                "/settings/auth/access_control",
                data={
                    "auth_ip_allowlist": "127.0.0.1",
                    "csrf_token": _csrf_value(),
                },
                follow_redirects=False,
            )
        assert r.status_code == 303
        assert "saved=1" in (r.headers.get("location") or "")
    finally:

        async def _clear_allowlist() -> None:
            async with SessionLocal() as s:
                row = await _get_or_create_settings(s)
                row.auth_ip_allowlist = ""
                await s.commit()

        asyncio.run(_clear_allowlist())


def test_post_login_without_csrf_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    with TestClient(app) as client:
        r = client.post(
            "/login",
            data={"username": "admin", "password": "testpass12", "next": "/"},
            follow_redirects=False,
        )
    assert r.status_code == 303


def test_post_api_arr_search_now_without_csrf_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)

    def _fake_enqueue(_scope: str):
        return None

    monkeypatch.setattr("app.routers.api.enqueue_manual_arr_search", _fake_enqueue)
    with TestClient(app) as client:
        r = client.post("/api/arr/search-now", json={"scope": "sonarr_missing"})
    assert r.status_code == 200
    assert r.json() == {"ok": True, "queued": True, "message": "Manual search queued."}


def test_expired_csrf_token_returns_403(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    # Build cookie before patching TimestampSigner (session + CSRF use the same class).
    session_cookie_value = build_session_cookie_value(
        secret=_TEST_SESSION_SECRET,
        username="admin",
    )
    with patch("app.auth.TimestampSigner") as MockSigner:
        MockSigner.return_value.unsign.side_effect = SignatureExpired("expired", payload=b"x")
        with TestClient(app) as client:
            client.cookies.set("fetcher_session", session_cookie_value)
            r = client.post(
                "/settings/auth/access_control",
                data={
                    "auth_ip_allowlist": "127.0.0.1",
                    "csrf_token": "any-token",
                },
                follow_redirects=False,
            )
    assert r.status_code == 403
    assert "Invalid or expired CSRF token" in (r.json().get("detail") or "")
