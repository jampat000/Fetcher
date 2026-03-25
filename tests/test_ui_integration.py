"""Smoke + form tests for every main page and save endpoint (no live Emby/Sonarr)."""

from __future__ import annotations

import asyncio
import re
from urllib.parse import urlencode

import pytest
import httpx
from fastapi.testclient import TestClient

from app.db import SessionLocal, _get_or_create_settings
from app.main import app

_WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _schedule_flag_pairs(field_prefix: str) -> list[tuple[str, str]]:
    """Unique names: sonarr_schedule_Mon=1 … (browser checkbox when checked)."""
    return [(f"{field_prefix}_{d}", "1") for d in _WEEKDAYS]


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/logs",
        "/activity",
        "/settings",
        "/settings?saved=1",
        "/settings?save=fail&reason=db_busy",
        "/settings?save=fail&reason=db_busy&tab=sonarr",
        "/settings?save=fail&reason=invalid_scope&tab=global",
        "/settings?test=sonarr_ok&tab=sonarr",
        "/trimmer/settings",
        "/trimmer/settings?saved=1",
        "/trimmer/settings?save=fail&reason=db_busy",
        "/trimmer",
        "/healthz",
        "/settings?import=ok",
        "/settings/backup/export",
        "/setup/1",
        "/setup/2",
        "/setup/3",
        "/setup/4",
        "/setup/5",
    ],
)
def test_get_pages_200(monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    with _client(monkeypatch) as client:
        resp = client.get(path)
    assert resp.status_code == 200, f"{path}: {resp.status_code}"


def test_healthz_json(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.get("/healthz")
    data = resp.json()
    assert data["status"] == "ok"
    assert data["app"] == "Fetcher"


def test_dashboard_renders_main_sections(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert b"dashboard-overview" in r.content
    assert b"Dashboard" in r.content


def test_settings_page_has_forms(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/settings")
    assert r.status_code == 200
    assert b"sonarr_url" in r.content
    assert b"radarr_url" in r.content
    assert b"section-trimmer" not in r.content
    assert b"Trimmer settings" in r.content


def test_settings_page_hidden_save_scope_per_section_form(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: fetch/FormData must always send save_scope (submit button name is not in FormData)."""
    with _client(monkeypatch) as client:
        r = client.get("/settings")
    html = r.text
    assert html.count('name="save_scope"') == 3
    assert 'value="global"' in html
    assert 'value="sonarr"' in html
    assert 'value="radarr"' in html
    assert 'type="hidden"' in html
    assert 'type="submit" name="save_scope"' not in html
    assert 'data-fetcher-async-test="1"' in html


def test_trimmer_settings_has_content_criteria(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/trimmer/settings")
    assert r.status_code == 200
    html = r.text
    assert "trimmer-connection" in html
    assert "trimmer-schedule" in html
    assert "trimmer-rules" in html
    assert "trimmer-people" in html
    assert "trimmer-area-tabs" in html
    assert "People rules" in html
    assert "emby_rule_movie_people" in html
    assert "emby_rule_tv_people" in html


@pytest.mark.parametrize(
    "scope",
    ["sonarr_missing", "sonarr_upgrade", "radarr_missing", "radarr_upgrade"],
)
def test_post_api_arr_search_now(monkeypatch: pytest.MonkeyPatch, scope: str) -> None:
    seen: dict[str, str | None] = {"scope": None}

    async def _fake_trigger(s: str, _session):
        seen["scope"] = s

    monkeypatch.setattr("app.routers.api.trigger_manual_arr_search_now", _fake_trigger)
    with _client(monkeypatch) as client:
        resp = client.post("/api/arr/search-now", json={"scope": scope})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "queued": False, "message": "Manual search triggered."}
    assert seen["scope"] == scope


def test_post_api_arr_search_now_falls_back_to_queue_on_http_status_error(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str | None] = {"queued_scope": None}

    async def _boom(_scope: str, _session):
        req = httpx.Request("POST", "http://localhost:8310/api/v3/command")
        resp = httpx.Response(500, request=req, text="internal")
        raise httpx.HTTPStatusError("boom", request=req, response=resp)

    def _fake_enqueue(scope: str):
        seen["queued_scope"] = scope

    monkeypatch.setattr("app.routers.api.trigger_manual_arr_search_now", _boom)
    monkeypatch.setattr("app.routers.api.enqueue_manual_arr_search", _fake_enqueue)
    with _client(monkeypatch) as client:
        resp = client.post("/api/arr/search-now", json={"scope": "radarr_upgrade"})
    assert resp.status_code == 200
    assert resp.json() == {
        "ok": True,
        "queued": True,
        "message": "Manual search queued (immediate Arr command failed).",
    }
    assert seen["queued_scope"] == "radarr_upgrade"


def test_post_api_arr_search_now_invalid_scope_422(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post("/api/arr/search-now", json={"scope": "nope"})
    assert resp.status_code == 422


def test_post_setup_wizard_continue_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wizard step 1 saves Sonarr fields and advances to step 2."""
    with _client(monkeypatch) as client:
        resp = client.post(
            "/setup/1",
            data={
                "wizard_action": "continue",
                "sonarr_enabled": "true",
                "sonarr_url": "http://127.0.0.1:8989",
                "sonarr_api_key": "k1",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert resp.headers.get("location", "").endswith("/setup/2")


def test_post_setup_wizard_continue_async_json_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async header returns same outcomes as redirect (JSON body + client navigates)."""
    with _client(monkeypatch) as client:
        resp = client.post(
            "/setup/1",
            data={
                "wizard_action": "continue",
                "sonarr_enabled": "true",
                "sonarr_url": "http://127.0.0.1:8989",
                "sonarr_api_key": "k1",
            },
            headers={"X-Fetcher-Setup-Async": "1", "Accept": "application/json"},
            follow_redirects=False,
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("ok") is True
    assert str(body.get("redirect") or "").endswith("/setup/2")


def test_post_setup_wizard_step4_redirects_to_step5(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/setup/4",
            data={
                "wizard_action": "continue",
                "sonarr_interval_minutes": "45",
                "radarr_interval_minutes": "90",
                "emby_interval_minutes": "120",
                "timezone": "UTC",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert resp.headers.get("location", "").endswith("/setup/5")


def test_post_setup_wizard_step5_redirects_home(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/setup/5",
            data={"wizard_action": "continue"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "setup=complete" in (resp.headers.get("location") or "")


def test_post_settings_validation_error_redirects_not_422(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invalid numeric field should redirect to settings with save=fail (browser-friendly), not raw 422 JSON."""
    payload = {
        "sonarr_enabled": "false",
        "sonarr_url": "",
        "sonarr_api_key": "",
        "sonarr_search_missing": "true",
        "sonarr_search_upgrades": "true",
        "sonarr_max_items_per_run": "not-a-number",
        "sonarr_schedule_enabled": "false",
        "sonarr_schedule_start": "00:00",
        "sonarr_schedule_end": "23:59",
        "radarr_enabled": "false",
        "radarr_url": "",
        "radarr_api_key": "",
        "radarr_search_missing": "true",
        "radarr_search_upgrades": "true",
        "radarr_max_items_per_run": "50",
        "radarr_schedule_enabled": "false",
        "radarr_schedule_start": "00:00",
        "radarr_schedule_end": "23:59",
        "sonarr_interval_minutes": "60",
        "radarr_interval_minutes": "60",
        "arr_search_cooldown_minutes": "1440",
        "log_retention_days": "90",
        "timezone": "UTC",
        "save_scope": "sonarr",
    }
    form: list[tuple[str, str]] = [(k, str(v)) for k, v in payload.items()]
    form.extend(_schedule_flag_pairs("sonarr_schedule"))
    form.extend(_schedule_flag_pairs("radarr_schedule"))
    encoded = urlencode(form)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    loc = resp.headers.get("location", "")
    assert "/settings" in loc
    assert "save=fail" in loc
    assert "reason=invalid" in loc
    assert "tab=sonarr" in loc


def test_post_settings_save_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal global Fetcher settings form post (strict save_scope)."""
    payload = {
        "arr_search_cooldown_minutes": "1440",
        "log_retention_days": "90",
        "timezone": "UTC",
        "save_scope": "global",
    }
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data=payload,
            follow_redirects=False,
        )
    assert resp.status_code == 303
    loc = resp.headers.get("location", "")
    assert loc.startswith("/settings")
    assert "saved=1" in loc
    assert "tab=global" in loc


def test_post_emby_connection_save(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/connection",
            data={
                "emby_enabled": "false",
                "emby_url": "http://localhost:8096",
                "emby_api_key": "test-key-not-real",
                "emby_user_id": "",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "/trimmer/settings" in resp.headers.get("location", "")


def test_post_emby_form_async_returns_json_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    """Trimmer Emby test-from-form: JSON contract for in-place UI (no 303)."""

    class _FakeEmbyClient:
        def __init__(self, *_a: object, **_kw: object) -> None:
            pass

        async def health(self) -> bool:
            return True

        async def users(self) -> list[dict[str, str]]:
            return []

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr("app.routers.trimmer.EmbyClient", _FakeEmbyClient)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/test/emby-form",
            data={
                "emby_enabled": "false",
                "emby_url": "http://localhost:8096",
                "emby_api_key": "fake-key",
                "emby_user_id": "",
            },
            headers={"X-Fetcher-Trimmer-Settings-Async": "1"},
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "section": "connection", "test": "emby_ok"}


def test_post_trimmer_connection_async_header_returns_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Trimmer connection XHR path: JSON instead of 303 (same persistence as normal POST)."""
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/connection",
            data={
                "emby_enabled": "false",
                "emby_url": "http://localhost:8096",
                "emby_api_key": "async-test-key",
                "emby_user_id": "",
            },
            headers={"X-Fetcher-Trimmer-Settings-Async": "1"},
        )
    assert resp.status_code == 200
    assert "application/json" in (resp.headers.get("content-type") or "").lower()
    assert resp.json() == {"ok": True, "section": "connection"}


def test_post_trimmer_cleaner_async_header_returns_json(monkeypatch: pytest.MonkeyPatch) -> None:
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "10:00"),
        ("emby_schedule_end", "18:00"),
        ("emby_max_items_scan", "2000"),
        ("emby_max_deletes_per_run", "25"),
        ("emby_interval_minutes", "95"),
        ("save_scope", "schedule"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=encoded,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Fetcher-Trimmer-Settings-Async": "1",
            },
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "section": "schedule"}

    async def verify_interval() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.emby_interval_minutes == 95

    asyncio.run(verify_interval())


def test_post_trimmer_cleaner_rejects_missing_save_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "00:00"),
        ("emby_schedule_end", "23:59"),
        ("emby_max_items_scan", "2000"),
        ("emby_max_deletes_per_run", "25"),
        ("emby_interval_minutes", "60"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    loc = resp.headers.get("location") or ""
    assert "save=fail" in loc
    assert "invalid_scope" in loc


def test_post_trimmer_cleaner_async_rejects_invalid_save_scope_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetcher-only scopes must not be accepted on Trimmer cleaner POST."""
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "00:00"),
        ("emby_schedule_end", "23:59"),
        ("emby_max_items_scan", "2000"),
        ("emby_max_deletes_per_run", "25"),
        ("emby_interval_minutes", "60"),
        ("save_scope", "sonarr"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=encoded,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Fetcher-Trimmer-Settings-Async": "1",
            },
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": False, "section": "schedule", "reason": "invalid_scope"}


def test_post_trimmer_cleaner_rejects_legacy_global_save_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    """``save_scope=global`` is Fetcher-only; Trimmer schedule saves must use ``schedule``."""
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "false"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "00:00"),
        ("emby_schedule_end", "23:59"),
        ("emby_max_items_scan", "2000"),
        ("emby_max_deletes_per_run", "25"),
        ("emby_interval_minutes", "30"),
        ("save_scope", "global"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    loc = resp.headers.get("location") or ""
    assert "save=fail" in loc
    assert "invalid_scope" in loc


def test_post_trimmer_cleaner_legacy_global_async_json(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            data={
                "emby_dry_run": "true",
                "emby_schedule_enabled": "false",
                **{f"emby_schedule_{d}": "0" for d in _WEEKDAYS},
                "emby_schedule_start": "00:00",
                "emby_schedule_end": "23:59",
                "emby_max_items_scan": "2000",
                "emby_max_deletes_per_run": "25",
                "emby_interval_minutes": "60",
                "save_scope": "global",
            },
            headers={"X-Fetcher-Trimmer-Settings-Async": "1"},
        )
    assert resp.status_code == 200
    assert resp.json()["ok"] is False
    assert resp.json()["reason"] == "invalid_scope"


def test_post_trimmer_cleaner_legacy_global_does_not_mutate_db(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.emby_interval_minutes = 88
            row.emby_dry_run = True
            row.emby_max_items_scan = 2000
            await session.commit()

    asyncio.run(seed())
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "false"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "00:00"),
        ("emby_schedule_end", "23:59"),
        ("emby_max_items_scan", "1"),
        ("emby_max_deletes_per_run", "99"),
        ("emby_interval_minutes", "5"),
        ("save_scope", "global"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "invalid_scope" in (resp.headers.get("location") or "")

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.emby_interval_minutes == 88
            assert row.emby_dry_run is True
            assert row.emby_max_items_scan == 2000

    asyncio.run(verify())


def test_post_trimmer_cleaner_rejects_save_scope_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catch-all ``save_scope=all`` is not part of the Trimmer cleaner contract."""
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "00:00"),
        ("emby_schedule_end", "23:59"),
        ("emby_max_items_scan", "2000"),
        ("emby_max_deletes_per_run", "25"),
        ("emby_interval_minutes", "60"),
        ("save_scope", "all"),
    ]
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=urlencode(pairs),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "invalid_scope" in (resp.headers.get("location") or "")


def test_post_trimmer_cleaner_save_scope_all_async_json(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            data={"save_scope": "all", "emby_interval_minutes": "99"},
            headers={"X-Fetcher-Trimmer-Settings-Async": "1"},
        )
    assert resp.status_code == 200
    assert resp.json()["ok"] is False
    assert resp.json()["reason"] == "invalid_scope"


def test_post_trimmer_cleaner_save_scope_all_does_not_mutate_db(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.emby_interval_minutes = 33
            row.emby_rule_movie_watched_rating_below = 5
            row.emby_rule_tv_unwatched_days = 20
            await session.commit()

    asyncio.run(seed())
    pairs: list[tuple[str, str]] = [
        ("save_scope", "all"),
        ("emby_interval_minutes", "1"),
        ("emby_rule_movie_watched_rating_below", "9"),
        ("emby_rule_tv_unwatched_days", "99"),
    ]
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=urlencode(pairs),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "invalid_scope" in (resp.headers.get("location") or "")

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.emby_interval_minutes == 33
            assert row.emby_rule_movie_watched_rating_below == 5
            assert row.emby_rule_tv_unwatched_days == 20

    asyncio.run(verify())


def test_trimmer_schedule_save_does_not_update_rule_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.emby_rule_movie_watched_rating_below = 7
            row.emby_rule_tv_unwatched_days = 42
            row.emby_rule_movie_genres_csv = "SciFi"
            row.emby_rule_tv_genres_csv = "News"
            await session.commit()

    asyncio.run(seed())
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "10:00"),
        ("emby_schedule_end", "11:00"),
        ("emby_max_items_scan", "1234"),
        ("emby_max_deletes_per_run", "15"),
        ("emby_interval_minutes", "45"),
        ("save_scope", "schedule"),
        ("emby_rule_movie_watched_rating_below", "1"),
        ("emby_rule_tv_unwatched_days", "2"),
        ("emby_rule_movie_genres", "Horror"),
        ("emby_rule_tv_genres", "Sport"),
    ]
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=urlencode(pairs),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.emby_interval_minutes == 45
            assert row.emby_max_items_scan == 1234
            assert row.emby_rule_movie_watched_rating_below == 7
            assert row.emby_rule_tv_unwatched_days == 42
            assert row.emby_rule_movie_genres_csv == "SciFi"
            assert row.emby_rule_tv_genres_csv == "News"

    asyncio.run(verify())


def test_trimmer_tv_save_does_not_update_movie_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.emby_rule_movie_watched_rating_below = 8
            row.emby_rule_movie_unwatched_days = 15
            row.emby_rule_tv_unwatched_days = 10
            await session.commit()

    asyncio.run(seed())
    pairs: list[tuple[str, str]] = [
        ("save_scope", "tv"),
        ("emby_rule_tv_delete_watched", "true"),
        ("emby_rule_tv_genres", "Comedy"),
        ("emby_rule_tv_unwatched_days", "25"),
        ("emby_rule_movie_watched_rating_below", "1"),
        ("emby_rule_movie_unwatched_days", "99"),
    ]
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=urlencode(pairs),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.emby_rule_tv_unwatched_days == 25
            assert row.emby_rule_movie_watched_rating_below == 8
            assert row.emby_rule_movie_unwatched_days == 15

    asyncio.run(verify())


def test_trimmer_movies_save_does_not_update_tv_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.emby_rule_tv_unwatched_days = 40
            row.emby_rule_tv_delete_watched = False
            row.emby_rule_movie_watched_rating_below = 2
            await session.commit()

    asyncio.run(seed())
    pairs: list[tuple[str, str]] = [
        ("save_scope", "movies"),
        ("emby_rule_movie_watched_rating_below", "6"),
        ("emby_rule_movie_unwatched_days", "11"),
        ("emby_rule_tv_unwatched_days", "1"),
        ("emby_rule_tv_delete_watched", "true"),
    ]
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=urlencode(pairs),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.emby_rule_movie_watched_rating_below == 6
            assert row.emby_rule_movie_unwatched_days == 11
            assert row.emby_rule_tv_unwatched_days == 40
            assert row.emby_rule_tv_delete_watched is False

    asyncio.run(verify())


def test_trimmer_cleaner_validation_redirect_preserves_section_in_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """422 form validation on cleaner should redirect back with trimmer_section fragment (non-JS)."""
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner?trimmer_section=people",
            data={
                "save_scope": "tv",
                "emby_interval_minutes": "not-an-int",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    loc = resp.headers.get("location") or ""
    assert "save=fail" in loc
    assert "reason=invalid" in loc
    assert "#trimmer-people" in loc


def test_global_fetcher_save_does_not_mutate_trimmer_emby_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.emby_interval_minutes = 77
            row.emby_dry_run = True
            await session.commit()

    asyncio.run(seed())
    payload = {
        "arr_search_cooldown_minutes": "720",
        "log_retention_days": "120",
        "timezone": "Europe/Berlin",
        "save_scope": "global",
    }
    encoded = urlencode(list(payload.items()))
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.emby_interval_minutes == 77
            assert row.emby_dry_run is True

    asyncio.run(verify())


def test_trimmer_schedule_save_does_not_mutate_sonarr_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.sonarr_url = "http://sonarr-preserved.example:8989"
            row.sonarr_interval_minutes = 31
            await session.commit()

    asyncio.run(seed())
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "false"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "08:00"),
        ("emby_schedule_end", "20:00"),
        ("emby_max_items_scan", "100"),
        ("emby_max_deletes_per_run", "5"),
        ("emby_interval_minutes", "55"),
        ("save_scope", "schedule"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.sonarr_url == "http://sonarr-preserved.example:8989"
            assert row.sonarr_interval_minutes == 31
            assert row.emby_interval_minutes == 55

    asyncio.run(verify())


def test_post_trimmer_settings_full(monkeypatch: pytest.MonkeyPatch) -> None:
    """Trimmer cleaner: strict scopes — schedule, TV, and movies require separate saves (like the UI)."""
    schedule_pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "09:00"),
        ("emby_schedule_end", "17:00"),
        ("emby_max_items_scan", "1500"),
        ("emby_max_deletes_per_run", "10"),
        ("emby_interval_minutes", "60"),
        ("save_scope", "schedule"),
    ]
    tv_pairs: list[tuple[str, str]] = [
        ("save_scope", "tv"),
        ("emby_rule_tv_delete_watched", "true"),
        ("emby_rule_tv_genres", "Comedy"),
        ("emby_rule_tv_people", "Show Runner"),
        ("emby_rule_tv_people_credit_types", "Writer"),
        ("emby_rule_tv_unwatched_days", "14"),
    ]
    movie_pairs: list[tuple[str, str]] = [
        ("save_scope", "movies"),
        ("emby_rule_movie_watched_rating_below", "3"),
        ("emby_rule_movie_unwatched_days", "30"),
        ("emby_rule_movie_genres", "Action"),
        ("emby_rule_movie_genres", "Drama"),
        ("emby_rule_movie_people", "Test Actor"),
        ("emby_rule_movie_people_credit_types", "Actor"),
        ("emby_rule_movie_people_credit_types", "Director"),
    ]
    with _client(monkeypatch) as client:
        for pairs in (schedule_pairs, tv_pairs, movie_pairs):
            resp = client.post(
                "/trimmer/settings/cleaner",
                content=urlencode(pairs),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert "saved=1" in (resp.headers.get("location") or "")
        page = client.get("/trimmer/settings")
    body = page.text
    assert "Test Actor" in body
    assert "Show Runner" in body


def test_post_settings_async_header_returns_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """XHR/fetch path: JSON body instead of 303 (same scoping as normal POST)."""
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "arr_search_cooldown_minutes": "2000",
                "log_retention_days": "90",
                "timezone": "UTC",
                "save_scope": "global",
            },
            headers={"X-Fetcher-Settings-Async": "1"},
        )
    assert resp.status_code == 200
    assert "application/json" in (resp.headers.get("content-type") or "")
    assert resp.json() == {"ok": True, "tab": "global"}

    async def verify_cooldown() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.arr_search_cooldown_minutes == 2000

    asyncio.run(verify_cooldown())


def test_post_settings_rejects_missing_save_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "arr_search_cooldown_minutes": "100",
                "log_retention_days": "90",
                "timezone": "UTC",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    loc = resp.headers.get("location") or ""
    assert "save=fail" in loc
    assert "invalid_scope" in loc
    assert "tab=global" in loc


def test_post_settings_rejects_legacy_save_scope_all(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "arr_search_cooldown_minutes": "100",
                "log_retention_days": "90",
                "timezone": "UTC",
                "save_scope": "all",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    loc = resp.headers.get("location") or ""
    assert "invalid_scope" in loc
    assert "tab=global" in loc


def test_post_settings_async_rejects_invalid_save_scope_json(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={"save_scope": "all", "timezone": "UTC"},
            headers={"X-Fetcher-Settings-Async": "1"},
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": False, "tab": "global", "reason": "invalid_scope"}


def test_post_settings_invalid_scope_does_not_change_database(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.arr_search_cooldown_minutes = 7777
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "save_scope": "all",
                "arr_search_cooldown_minutes": "1111",
                "log_retention_days": "90",
                "timezone": "UTC",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.arr_search_cooldown_minutes == 7777

    asyncio.run(verify())


def test_get_settings_save_fail_banner_visible_on_sonarr_tab(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/settings?save=fail&reason=db_busy&tab=sonarr")
    assert r.status_code == 200
    html = r.text
    assert "settings-fetcher-save-fail" in html
    assert "database was busy" in html


def test_test_sonarr_post_does_not_mutate_app_settings_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")

    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.sonarr_url = "http://unchanged.example:8989"
            row.sonarr_api_key = ""
            await session.commit()

    asyncio.run(seed())

    async def _check_arr_health(self, *, url: str, api_key: str):
        from app.connection_test_service import ArrHealthCheckResult

        return ArrHealthCheckResult(ok=True, error_kind="none")

    monkeypatch.setattr("app.routers.settings.ConnectionTestService.check_arr_health", _check_arr_health)
    try:
        with _client(monkeypatch) as client:
            resp = client.post("/test/sonarr", follow_redirects=False)
        assert resp.status_code == 303
        assert "tab=sonarr" in (resp.headers.get("location") or "")

        async def verify_url() -> None:
            async with SessionLocal() as session:
                row = await _get_or_create_settings(session)
                assert row.sonarr_url == "http://unchanged.example:8989"

        asyncio.run(verify_url())
    finally:

        async def reset_url() -> None:
            async with SessionLocal() as session:
                row = await _get_or_create_settings(session)
                row.sonarr_url = ""
                await session.commit()

        asyncio.run(reset_url())


def test_global_save_updates_only_cooldown_retention_timezone(monkeypatch: pytest.MonkeyPatch) -> None:
    """Save global settings must not apply Sonarr/Radarr fields from the posted form."""

    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.sonarr_interval_minutes = 33
            row.radarr_interval_minutes = 44
            row.sonarr_max_items_per_run = 50
            row.arr_search_cooldown_minutes = 1440
            row.log_retention_days = 90
            row.timezone = "UTC"
            await session.commit()

    asyncio.run(seed())
    payload = {
        "sonarr_enabled": "true",
        "sonarr_url": "http://localhost:8989",
        "sonarr_api_key": "abc",
        "sonarr_search_missing": "true",
        "sonarr_search_upgrades": "true",
        "sonarr_max_items_per_run": "999",
        "sonarr_schedule_enabled": "false",
        "sonarr_schedule_start": "00:00",
        "sonarr_schedule_end": "23:59",
        "sonarr_interval_minutes": "99",
        "radarr_enabled": "true",
        "radarr_url": "http://localhost:7878",
        "radarr_api_key": "def",
        "radarr_search_missing": "true",
        "radarr_search_upgrades": "true",
        "radarr_remove_failed_imports": "false",
        "radarr_max_items_per_run": "888",
        "radarr_schedule_enabled": "false",
        "radarr_schedule_start": "00:00",
        "radarr_schedule_end": "23:59",
        "radarr_interval_minutes": "88",
        "arr_search_cooldown_minutes": "720",
        "log_retention_days": "120",
        "timezone": "Europe/Berlin",
        "save_scope": "global",
    }
    form: list[tuple[str, str]] = [(k, str(v)) for k, v in payload.items()]
    form.extend(_schedule_flag_pairs("sonarr_schedule"))
    form.extend(_schedule_flag_pairs("radarr_schedule"))
    encoded = urlencode(form)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    async def verify_db() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.sonarr_interval_minutes == 33
            assert row.radarr_interval_minutes == 44
            assert row.sonarr_max_items_per_run == 50
            assert row.arr_search_cooldown_minutes == 720
            assert row.log_retention_days == 120
            assert row.timezone == "Europe/Berlin"
            assert (row.sonarr_url or "").strip() == ""

    asyncio.run(verify_db())


def test_sonarr_save_preserves_radarr_interval_when_post_includes_wrong_radarr_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: Sonarr-only save must not let a bogus radarr_interval in the POST affect the DB."""

    async def seed() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.radarr_interval_minutes = 30
            row.sonarr_interval_minutes = 45
            await session.commit()

    asyncio.run(seed())
    payload = {
        "sonarr_enabled": "true",
        "sonarr_url": "http://localhost:8989",
        "sonarr_api_key": "abc",
        "sonarr_search_missing": "true",
        "sonarr_search_upgrades": "true",
        "sonarr_max_items_per_run": "50",
        "sonarr_schedule_enabled": "false",
        "sonarr_schedule_start": "00:00",
        "sonarr_schedule_end": "23:59",
        "sonarr_interval_minutes": "45",
        "radarr_enabled": "false",
        "radarr_url": "",
        "radarr_api_key": "",
        "radarr_search_missing": "true",
        "radarr_search_upgrades": "true",
        "radarr_remove_failed_imports": "false",
        "radarr_max_items_per_run": "50",
        "radarr_schedule_enabled": "false",
        "radarr_schedule_start": "00:00",
        "radarr_schedule_end": "23:59",
        "radarr_interval_minutes": "999",
        "arr_search_cooldown_minutes": "1440",
        "log_retention_days": "90",
        "timezone": "UTC",
        "save_scope": "sonarr",
    }
    form: list[tuple[str, str]] = [(k, str(v)) for k, v in payload.items()]
    form.extend(_schedule_flag_pairs("sonarr_schedule"))
    form.extend(_schedule_flag_pairs("radarr_schedule"))
    encoded = urlencode(form)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        page = client.get("/settings")
    html = page.text
    assert re.search(r'name="radarr_interval_minutes"[^>]*\bvalue="30"', html) or re.search(
        r'value="30"[^>]*name="radarr_interval_minutes"', html
    )

    async def verify_db() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.radarr_interval_minutes == 30

    asyncio.run(verify_db())


def test_sonarr_schedule_all_days_stays_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "sonarr_enabled": "true",
        "sonarr_url": "http://localhost:8989",
        "sonarr_api_key": "abc",
        "sonarr_search_missing": "true",
        "sonarr_search_upgrades": "true",
        "sonarr_max_items_per_run": "50",
        "sonarr_schedule_enabled": "true",
        "sonarr_schedule_start": "00:00",
        "sonarr_schedule_end": "23:59",
        "radarr_enabled": "false",
        "radarr_url": "",
        "radarr_api_key": "",
        "radarr_search_missing": "true",
        "radarr_search_upgrades": "true",
        "radarr_max_items_per_run": "50",
        "radarr_schedule_start": "00:00",
        "radarr_schedule_end": "23:59",
        "sonarr_interval_minutes": "60",
        "radarr_interval_minutes": "60",
        "radarr_schedule_enabled": "false",
        "arr_search_cooldown_minutes": "1440",
        "log_retention_days": "90",
        "timezone": "UTC",
        "save_scope": "sonarr",
    }
    form: list[tuple[str, str]] = [(k, str(v)) for k, v in payload.items()]
    form.extend(_schedule_flag_pairs("sonarr_schedule"))
    form.extend(_schedule_flag_pairs("radarr_schedule"))
    encoded = urlencode(form)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        page = client.get("/settings")
    html = page.text
    assert re.search(
        r'name="sonarr_schedule_enabled"[^>]*\bchecked\b|\bchecked\b[^>]*name="sonarr_schedule_enabled"',
        html,
    )
    assert len(re.findall(r'name="sonarr_schedule_(Mon|Tue|Wed|Thu|Fri|Sat|Sun)"', html)) == 7
    assert (
        len(
            re.findall(
                r'<input[^>]*name="sonarr_schedule_(Mon|Tue|Wed|Thu|Fri|Sat|Sun)"[^>]*\bchecked\b',
                html,
            )
        )
        == 7
    )
    assert "schedule-days-native" in html


def test_trimmer_schedule_all_days_stays_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "true"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "00:00"),
        ("emby_schedule_end", "23:59"),
        ("emby_max_items_scan", "2000"),
        ("emby_max_deletes_per_run", "25"),
        ("save_scope", "schedule"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        page = client.get("/trimmer/settings")
    html = page.text
    assert 'name="emby_schedule_enabled" checked' in html
    assert len(re.findall(r'name="emby_schedule_(Mon|Tue|Wed|Thu|Fri|Sat|Sun)"', html)) == 7
    assert (
        len(
            re.findall(
                r'<input[^>]*name="emby_schedule_(Mon|Tue|Wed|Thu|Fri|Sat|Sun)"[^>]*\bchecked\b',
                html,
            )
        )
        == 7
    )
    assert "schedule-days-native" in html
