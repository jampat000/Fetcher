"""Regression: login and setup forms must not invite cross-form credential autofill."""

from __future__ import annotations

import asyncio
import re

import pytest
from fastapi.testclient import TestClient

from app.auth import hash_password
from app.db import SessionLocal, _get_or_create_settings
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


@pytest.fixture
def client_login_page(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Password set so /login renders (not redirect to setup)."""
    async def seed() -> None:
        async with SessionLocal() as s:
            r = await _get_or_create_settings(s)
            r.auth_password_hash = hash_password("testpass12")
            r.auth_username = "admin"
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(seed())
    with _client_no_scheduler(monkeypatch) as c:
        yield c


def test_login_username_input_has_no_value_attribute_server_side(client_login_page: TestClient) -> None:
    r = client_login_page.get("/login")
    assert r.status_code == 200
    html = r.text
    m = re.search(r"<input[^>]*\bid=\"username\"[^>]*>", html, re.I)
    assert m, "expected #username input"
    tag = m.group(0)
    assert "value=" not in tag.lower(), "login username must not use value= (autofill is client-side only)"
    assert "section-fetcher-login username" in tag
    assert "section-fetcher-login current-password" in html
    assert "login-autofill-hint" in html


def test_setup_step0_form_autocomplete_off_and_scoped_auth_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def clear_pw() -> None:
        async with SessionLocal() as s:
            r = await _get_or_create_settings(s)
            r.auth_password_hash = ""
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(clear_pw())
    with _client_no_scheduler(monkeypatch) as c:
        r = c.get("/setup/0")
    assert r.status_code == 200
    html = r.text
    m = re.search(r"<form[^>]*\bclass=\"[^\"]*setup-wizard-form[^\"]*\"[^>]*>", html)
    assert m, "expected setup wizard form"
    assert 'autocomplete="off"' in m.group(0)
    assert "section-fetcher-setup username" in html
    assert "section-fetcher-setup new-password" in html


def test_setup_step1_sonarr_fields_have_integration_autofill_hardening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def clear_pw() -> None:
        async with SessionLocal() as s:
            r = await _get_or_create_settings(s)
            r.auth_password_hash = ""
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(clear_pw())
    with _client_no_scheduler(monkeypatch) as c:
        p = c.post(
            "/setup/0",
            data={
                "setup_auth_username": "admin",
                "setup_auth_password": "newpass123",
                "wizard_action": "continue",
            },
            follow_redirects=False,
        )
        assert p.status_code == 303
        r = c.get("/setup/1")
    assert r.status_code == 200
    html = r.text
    for fid, data_attr in (
        ("sonarr_url", "data-fetcher-integration-url"),
        ("sonarr_api_key", "data-fetcher-integration-secret"),
    ):
        m = re.search(rf"<input[^>]*\bid=\"{re.escape(fid)}\"[^>]*>", html, re.I)
        assert m, f"missing #{fid}"
        tag = m.group(0)
        assert "readonly" in tag.lower(), f"{fid} should be readonly until focus"
        assert data_attr in tag, f"{fid} missing {data_attr}"


def test_setup_step3_emby_fields_have_integration_autofill_hardening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def clear_pw() -> None:
        async with SessionLocal() as s:
            r = await _get_or_create_settings(s)
            r.auth_password_hash = ""
            r.updated_at = utc_now_naive()
            await s.commit()

    asyncio.run(clear_pw())
    with _client_no_scheduler(monkeypatch) as c:
        c.post(
            "/setup/0",
            data={
                "setup_auth_username": "admin",
                "setup_auth_password": "newpass123",
                "wizard_action": "continue",
            },
            follow_redirects=False,
        )
        for step, action in ((1, "continue"), (2, "continue")):
            p = c.post(
                f"/setup/{step}",
                data={"wizard_action": action},
                follow_redirects=False,
            )
            assert p.status_code == 303, (step, p.status_code, p.text[:200])
        r = c.get("/setup/3")
    assert r.status_code == 200
    html = r.text
    for fid, data_attr in (
        ("emby_url", "data-fetcher-integration-url"),
        ("emby_api_key", "data-fetcher-integration-secret"),
        ("emby_user_id", "data-fetcher-integration-text"),
    ):
        m = re.search(rf"<input[^>]*\bid=\"{re.escape(fid)}\"[^>]*>", html, re.I)
        assert m, f"missing #{fid}"
        tag = m.group(0)
        assert "readonly" in tag.lower()
        assert data_attr in tag
