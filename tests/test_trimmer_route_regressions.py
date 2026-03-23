from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient

from app.db import SessionLocal, _get_or_create_settings
from app.main import app


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


async def _set_trimmer_state(**updates: Any) -> None:
    async with SessionLocal() as s:
        row = await _get_or_create_settings(s)
        for k, v in updates.items():
            setattr(row, k, v)
        await s.commit()


def _capture_template_context(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def _fake_template_response(request, template_name: str, context: dict[str, Any]):
        captured["template"] = template_name
        captured["context"] = context
        return HTMLResponse("ok")

    monkeypatch.setattr("app.routers.trimmer.templates.TemplateResponse", _fake_template_response)
    return captured


def _install_fake_scan_dependencies(monkeypatch: pytest.MonkeyPatch, *, item_name: str = "Candidate Movie") -> None:
    class _FakeEmbyClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            return None

        async def users(self):
            return [{"Id": "u1", "Name": "Primary"}]

        async def items_for_user(self, user_id: str, limit: int):
            assert user_id == "u1"
            assert limit >= 1
            return [{"Id": "m1", "Name": item_name, "Type": "Movie"}]

        async def aclose(self):
            return None

    monkeypatch.setattr("app.trimmer_service.EmbyClient", _FakeEmbyClient)
    monkeypatch.setattr(
        "app.trimmer_service.evaluate_candidate",
        lambda *a, **kw: (True, ["movie: matched"], 42, 7.5, False),
    )
    monkeypatch.setattr("app.trimmer_service.movie_matches_selected_genres", lambda *a, **kw: True)
    monkeypatch.setattr("app.trimmer_service.movie_matches_people", lambda *a, **kw: True)
    monkeypatch.setattr("app.trimmer_service.tv_matches_selected_genres", lambda *a, **kw: True)


def test_trimmer_page_no_scan_fast_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    captured = _capture_template_context(monkeypatch)
    asyncio.run(
        _set_trimmer_state(
            emby_url="http://emby.local:8096",
            emby_api_key="plain-key",
            emby_rule_movie_unwatched_days=30,
            emby_dry_run=True,
        )
    )
    with _client(monkeypatch) as client:
        resp = client.get("/trimmer")
    assert resp.status_code == 200
    ctx = captured["context"]
    assert captured["template"] == "trimmer.html"
    assert ctx["scan_prompt"] is True
    assert ctx["scan_loaded"] is False
    assert ctx["error"] == ""
    assert ctx["matched_count"] == 0


def test_trimmer_page_missing_emby_url_or_key_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    captured = _capture_template_context(monkeypatch)
    asyncio.run(
        _set_trimmer_state(
            emby_url="",
            emby_api_key="",
            emby_rule_movie_unwatched_days=30,
        )
    )
    with _client(monkeypatch) as client:
        resp = client.get("/trimmer")
    assert resp.status_code == 200
    ctx = captured["context"]
    assert ctx["error"] == "Emby URL and API key are required."
    assert ctx["scan_loaded"] is False
    assert ctx["scan_prompt"] is False


def test_trimmer_scan_success_context_shaping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    captured = _capture_template_context(monkeypatch)
    _install_fake_scan_dependencies(monkeypatch, item_name="My Candidate")
    asyncio.run(
        _set_trimmer_state(
            emby_url="http://emby.local:8096",
            emby_api_key="plain-key",
            emby_user_id="",
            emby_rule_movie_unwatched_days=30,
            emby_dry_run=True,
        )
    )
    with _client(monkeypatch) as client:
        resp = client.get("/trimmer?scan=1")
    assert resp.status_code == 200
    ctx = captured["context"]
    assert ctx["scan_loaded"] is True
    assert ctx["scan_prompt"] is False
    assert ctx["error"] == ""
    assert ctx["used_user_id"] == "u1"
    assert ctx["used_user_name"] == "Primary"
    assert ctx["matched_count"] == 1
    assert len(ctx["rows"]) == 1
    row = ctx["rows"][0]
    assert row["id"] == "m1"
    assert row["name"] == "My Candidate"
    assert row["type"] == "Movie"
    assert row["reasons"] == ["movie: matched"]


def test_trimmer_scan_dry_run_skips_live_delete_and_last_run_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    _capture_template_context(monkeypatch)
    _install_fake_scan_dependencies(monkeypatch)
    called = {"live_delete": 0}

    async def _fake_apply(*args, **kwargs):
        called["live_delete"] += 1

    monkeypatch.setattr("app.trimmer_service.apply_emby_trimmer_live_deletes", _fake_apply)
    asyncio.run(
        _set_trimmer_state(
            emby_url="http://emby.local:8096",
            emby_api_key="plain-key",
            emby_user_id="",
            emby_rule_movie_unwatched_days=30,
            emby_dry_run=True,
            emby_last_run_at=None,
        )
    )
    with _client(monkeypatch) as client:
        resp = client.get("/trimmer?scan=1")
    assert resp.status_code == 200
    assert called["live_delete"] == 0

    async def _get_last_run():
        async with SessionLocal() as s:
            row = await _get_or_create_settings(s)
            return row.emby_last_run_at

    assert asyncio.run(_get_last_run()) is None


def test_trimmer_scan_live_mode_calls_delete_and_persists_last_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")
    # Keep this regression test stable even when CI injects ARR API keys in env.
    monkeypatch.delenv("FETCHER_SONARR_API_KEY", raising=False)
    monkeypatch.delenv("FETCHER_RADARR_API_KEY", raising=False)
    _capture_template_context(monkeypatch)
    _install_fake_scan_dependencies(monkeypatch)
    called = {"live_delete": 0, "candidates": 0}

    async def _fake_apply(_settings, _client, candidates, *, son_key, rad_key):
        called["live_delete"] += 1
        called["candidates"] = len(candidates)
        # CI fixtures may populate DB-backed ARR keys; either empty or DB value is valid here.
        assert son_key in ("", None, "db-key")
        assert rad_key in ("", None, "db-key")

    monkeypatch.setattr("app.trimmer_service.apply_emby_trimmer_live_deletes", _fake_apply)
    asyncio.run(
        _set_trimmer_state(
            emby_url="http://emby.local:8096",
            emby_api_key="plain-key",
            emby_user_id="",
            emby_rule_movie_unwatched_days=30,
            emby_dry_run=False,
            emby_last_run_at=None,
        )
    )
    with _client(monkeypatch) as client:
        resp = client.get("/trimmer?scan=1")
    assert resp.status_code == 200
    assert called["live_delete"] == 1
    assert called["candidates"] == 1

    async def _get_last_run():
        async with SessionLocal() as s:
            row = await _get_or_create_settings(s)
            return row.emby_last_run_at

    assert asyncio.run(_get_last_run()) is not None
