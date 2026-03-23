"""Unit tests for ``is_ip_allowed`` and auth access-control save validation."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from app.auth import is_ip_allowed, normalize_auth_ip_allowlist_input
from app.db import SessionLocal, _get_or_create_settings
from app.main import app


def test_single_ip_matches_exact() -> None:
    assert is_ip_allowed("192.168.1.50", "192.168.1.50") is True


def test_single_ip_no_match_other() -> None:
    assert is_ip_allowed("192.168.1.51", "192.168.1.50") is False


def test_cidr_matches_inside() -> None:
    assert is_ip_allowed("192.168.1.50", "192.168.1.0/24") is True


def test_cidr_no_match_outside() -> None:
    assert is_ip_allowed("10.0.0.1", "192.168.1.0/24") is False


def test_empty_allowlist_always_false() -> None:
    assert is_ip_allowed("127.0.0.1", "") is False
    assert is_ip_allowed("127.0.0.1", "   \n  \t  ") is False


def test_invalid_line_skipped_no_raise() -> None:
    assert is_ip_allowed("192.168.1.1", "not-a-valid-line\n192.168.1.1") is True


def test_mixed_valid_invalid_still_matches() -> None:
    text = "bogus!!!\n10.0.0.0/8\nnope"
    assert is_ip_allowed("10.5.5.5", text) is True
    assert is_ip_allowed("192.168.0.1", text) is False


def test_comment_line_skipped() -> None:
    assert is_ip_allowed("192.168.1.2", "# 192.168.1.2\n192.168.1.2") is True
    assert is_ip_allowed("192.168.1.3", "# home\n192.168.1.3") is True


def test_loopback_ipv4_allowlist_matches_ipv6_client() -> None:
    assert is_ip_allowed("::1", "127.0.0.1") is True


def test_loopback_ipv6_allowlist_matches_ipv4_client() -> None:
    assert is_ip_allowed("127.0.0.1", "::1") is True


def test_normalize_strips_and_rejoins() -> None:
    assert normalize_auth_ip_allowlist_input("  10.0.0.1  \n  10.0.0.2  ") == "10.0.0.1\n10.0.0.2"


def test_normalize_rejects_invalid_line() -> None:
    with pytest.raises(ValueError):
        normalize_auth_ip_allowlist_input("192.168.1.0/24\nnot-valid")


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


def test_access_control_post_invalid_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.post(
            "/settings/auth/access_control",
            data={"auth_ip_allowlist": "hello-world"},
            follow_redirects=False,
        )
    assert r.status_code == 303
    loc = r.headers.get("location", "")
    assert "save=fail" in loc and "invalid_ip" in loc


def test_access_control_post_valid_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    try:
        with _client(monkeypatch) as client:
            r = client.post(
                "/settings/auth/access_control",
                data={"auth_ip_allowlist": "  192.168.1.1  \n  10.0.0.0/8  "},
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
