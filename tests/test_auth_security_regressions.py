from __future__ import annotations

import asyncio
import logging

import pytest
from fastapi.testclient import TestClient

from app.auth import hash_password
from app.db import SessionLocal, _get_or_create_settings
from app.main import app
from app.time_util import utc_now_naive

pytestmark = pytest.mark.no_auth_override


def _scheduler_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)


async def _seed_auth_state() -> None:
    async with SessionLocal() as s:
        row = await _get_or_create_settings(s)
        row.auth_username = "admin"
        row.auth_password_hash = hash_password("testpass12")
        row.auth_refresh_token_hash = ""
        row.auth_refresh_expires_at = None
        row.auth_bypass_lan = False
        row.auth_ip_allowlist = ""
        row.updated_at = utc_now_naive()
        await s.commit()


def _token_pair(client: TestClient) -> dict[str, str | int]:
    resp = client.post(
        "/api/auth/token",
        data={"username": "admin", "password": "testpass12"},
        follow_redirects=False,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "access_token" in body
    assert "refresh_token" in body
    return body


def test_startup_fails_without_fetcher_jwt_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    monkeypatch.delenv("FETCHER_JWT_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="FETCHER_JWT_SECRET"):
        with TestClient(app):
            pass


def test_startup_warns_when_data_encryption_key_missing(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    _scheduler_noop(monkeypatch)
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    monkeypatch.delenv("FETCHER_DATA_ENCRYPTION_KEY", raising=False)
    caplog.set_level(logging.WARNING)
    with TestClient(app):
        pass
    assert any("FETCHER_DATA_ENCRYPTION_KEY" in r.message for r in caplog.records)


def test_refresh_token_cannot_be_reused_after_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    asyncio.run(_seed_auth_state())
    with TestClient(app) as client:
        first = _token_pair(client)
        r1 = client.post("/api/auth/refresh", json={"refresh_token": first["refresh_token"]})
        assert r1.status_code == 200
        rotated = r1.json()
        assert rotated["refresh_token"] != first["refresh_token"]

        # Old refresh token must be rejected after rotation.
        r2 = client.post("/api/auth/refresh", json={"refresh_token": first["refresh_token"]})
        assert r2.status_code == 401
        assert r2.json() == {"message": "Invalid refresh token"}


def _http_exception_message(resp) -> str:
    body = resp.json()
    detail = body.get("detail")
    if isinstance(detail, dict):
        return str(detail.get("message") or "")
    if isinstance(detail, str):
        return detail
    return str(body.get("message") or "")


def test_api_invalid_bearer_token_returns_actionable_json(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    asyncio.run(_seed_auth_state())
    with TestClient(app) as client:
        r = client.get(
            "/api/dashboard/status",
            headers={
                "Authorization": "Bearer not-a-valid-jwt",
                "Accept": "application/json",
            },
        )
    assert r.status_code == 401
    msg = _http_exception_message(r)
    assert "POST /api/auth/token" in msg
    assert "Bearer" in msg


def test_api_refresh_token_as_bearer_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    asyncio.run(_seed_auth_state())
    with TestClient(app) as client:
        tokens = _token_pair(client)
        r = client.get(
            "/api/dashboard/status",
            headers={
                "Authorization": f"Bearer {tokens['refresh_token']}",
                "Accept": "application/json",
            },
        )
    assert r.status_code == 401
    assert "access token" in _http_exception_message(r).lower()


def test_refresh_validation_fails_with_wrong_jwt_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    monkeypatch.setenv("FETCHER_JWT_SECRET", "jwt-secret-a")
    asyncio.run(_seed_auth_state())
    with TestClient(app) as client:
        first = _token_pair(client)
        app.state.jwt_secret = "jwt-secret-b"
        r = client.post("/api/auth/refresh", json={"refresh_token": first["refresh_token"]})
        assert r.status_code == 401
        assert r.json() == {"message": "Invalid refresh token"}


def test_invalid_login_does_not_reveal_username_existence(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    asyncio.run(_seed_auth_state())
    with TestClient(app) as client:
        wrong_user = client.post(
            "/api/auth/token",
            data={"username": "not-admin", "password": "testpass12"},
            follow_redirects=False,
        )
        wrong_pass = client.post(
            "/api/auth/token",
            data={"username": "admin", "password": "badpass"},
            follow_redirects=False,
        )

    assert wrong_user.status_code == 401
    assert wrong_pass.status_code == 401
    expected = {
        "message": (
            "That username or password does not match. Check spelling and caps lock, "
            "and use the same username you set during setup (default is admin)."
        )
    }
    assert wrong_user.json() == expected
    assert wrong_pass.json() == expected


def test_password_hash_upgrade_only_after_successful_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    _scheduler_noop(monkeypatch)
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    asyncio.run(_seed_auth_state())

    async def _get_hash() -> str:
        async with SessionLocal() as s:
            row = await _get_or_create_settings(s)
            return row.auth_password_hash

    before = asyncio.run(_get_hash())
    with TestClient(app) as client:
        # Failed verification path: hash must not change.
        r_fail = client.post("/api/auth/token", data={"username": "admin", "password": "wrong-pass"})
        assert r_fail.status_code == 401
    after_fail = asyncio.run(_get_hash())
    assert after_fail == before

    with (
        pytest.MonkeyPatch.context() as m,
        TestClient(app) as client,
    ):
        # Force "needs rehash" to ensure upgrade branch runs only on success.
        m.setattr("app.auth_service.needs_password_rehash", lambda _h: True)
        m.setattr("app.auth_service.hash_password", lambda _p: "upgraded-hash-value")
        r_ok = client.post("/api/auth/token", data={"username": "admin", "password": "testpass12"})
        assert r_ok.status_code == 200

    after_success = asyncio.run(_get_hash())
    assert after_success == "upgraded-hash-value"
