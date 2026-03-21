"""Smoke + form tests for every main page and save endpoint (no live Emby/Sonarr)."""

from __future__ import annotations

from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient

from app.main import app


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown() -> None:
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
        "/emby/settings",
        "/emby/settings?saved=1",
        "/cleaner",
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
    assert data["app"] == "Grabby"


def test_dashboard_has_module_tabs(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert b"module-tab" in r.content
    assert b"Dashboard" in r.content


def test_settings_page_has_forms(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/settings")
    assert r.status_code == 200
    assert b"sonarr_url" in r.content
    assert b"radarr_url" in r.content


def test_emby_settings_has_content_criteria(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/emby/settings")
    assert r.status_code == 200
    html = r.text
    assert "Content Criteria" in html
    assert "People" in html
    assert "emby_rule_movie_people" in html
    assert "emby_rule_tv_people" in html


@pytest.mark.parametrize(
    "scope",
    ["sonarr_missing", "sonarr_upgrade", "radarr_missing", "radarr_upgrade"],
)
def test_post_api_arr_search_now(monkeypatch: pytest.MonkeyPatch, scope: str) -> None:
    from app.service_logic import RunResult

    seen: dict[str, str | None] = {"scope": None}

    async def _fake_run_once(session, *, arr_manual_scope=None):
        seen["scope"] = arr_manual_scope
        return RunResult(ok=True, message="done")

    monkeypatch.setattr("app.main.run_once", _fake_run_once)
    with _client(monkeypatch) as client:
        resp = client.post("/api/arr/search-now", json={"scope": scope})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "message": "done"}
    assert seen["scope"] == scope


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


def test_post_setup_wizard_step4_redirects_to_step5(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/setup/4",
            data={
                "wizard_action": "continue",
                "interval_minutes": "60",
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


def test_post_settings_save_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal Grabby settings form post."""
    payload = {
        "sonarr_enabled": "false",
        "sonarr_url": "",
        "sonarr_api_key": "",
        "sonarr_search_missing": "true",
        "sonarr_search_upgrades": "true",
        "sonarr_max_items_per_run": "50",
        "sonarr_schedule_enabled": "false",
        "sonarr_schedule_days": "Mon,Tue,Wed,Thu,Fri,Sat,Sun",
        "sonarr_schedule_start": "00:00",
        "sonarr_schedule_end": "23:59",
        "radarr_enabled": "false",
        "radarr_url": "",
        "radarr_api_key": "",
        "radarr_search_missing": "true",
        "radarr_search_upgrades": "true",
        "radarr_max_items_per_run": "50",
        "radarr_schedule_enabled": "false",
        "radarr_schedule_days": "Mon,Tue,Wed,Thu,Fri,Sat,Sun",
        "radarr_schedule_start": "00:00",
        "radarr_schedule_end": "23:59",
        "sonarr_interval_minutes": "0",
        "radarr_interval_minutes": "0",
        "arr_search_cooldown_minutes": "1440",
        "timezone": "UTC",
    }
    with _client(monkeypatch) as client:
        resp = client.post("/settings", data=payload, follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers.get("location", "").startswith("/settings")


def test_post_emby_connection_save(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/emby/settings/connection",
            data={
                "emby_enabled": "false",
                "emby_url": "http://localhost:8096",
                "emby_api_key": "test-key-not-real",
                "emby_user_id": "",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "/emby/settings" in resp.headers.get("location", "")


def test_post_cleaner_settings_full(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cleaner form: genres (multi), People credits, schedules."""
    # Use URL-encoded body so duplicate keys (genres, credit types) parse like a real browser.
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "false"),
        ("emby_schedule_days", "Mon,Tue,Wed,Thu,Fri,Sat,Sun"),
        ("emby_schedule_start", "09:00"),
        ("emby_schedule_end", "17:00"),
        ("emby_max_items_scan", "1500"),
        ("emby_max_deletes_per_run", "10"),
        ("emby_rule_movie_watched_rating_below", "3"),
        ("emby_rule_movie_unwatched_days", "30"),
        ("emby_rule_movie_genres", "Action"),
        ("emby_rule_movie_genres", "Drama"),
        ("emby_rule_movie_people", "Test Actor"),
        ("emby_rule_movie_people_credit_types", "Actor"),
        ("emby_rule_movie_people_credit_types", "Director"),
        ("emby_rule_tv_delete_watched", "true"),
        ("emby_rule_tv_genres", "Comedy"),
        ("emby_rule_tv_people", "Show Runner"),
        ("emby_rule_tv_people_credit_types", "Writer"),
        ("emby_rule_tv_unwatched_days", "14"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/emby/settings/cleaner",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=True,
        )
    assert resp.status_code == 200
    body = resp.text
    assert "saved=1" in str(resp.url) or "?saved=1" in str(resp.url)
    assert "Test Actor" in body
    assert "Show Runner" in body


def test_sonarr_schedule_all_days_stays_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "sonarr_enabled": "true",
        "sonarr_url": "http://localhost:8989",
        "sonarr_api_key": "abc",
        "sonarr_search_missing": "true",
        "sonarr_search_upgrades": "true",
        "sonarr_max_items_per_run": "50",
        "sonarr_schedule_enabled": "true",
        "sonarr_schedule_days": "Mon,Tue,Wed,Thu,Fri,Sat,Sun",
        "sonarr_schedule_start": "00:00",
        "sonarr_schedule_end": "23:59",
        "radarr_enabled": "false",
        "radarr_url": "",
        "radarr_api_key": "",
        "radarr_search_missing": "true",
        "radarr_search_upgrades": "true",
        "radarr_max_items_per_run": "50",
        "radarr_schedule_days": "Mon,Tue,Wed,Thu,Fri,Sat,Sun",
        "radarr_schedule_start": "00:00",
        "radarr_schedule_end": "23:59",
        "sonarr_interval_minutes": "0",
        "radarr_interval_minutes": "0",
        "arr_search_cooldown_minutes": "1440",
        "timezone": "UTC",
        "save_scope": "sonarr",
    }
    with _client(monkeypatch) as client:
        resp = client.post("/settings", data=payload, follow_redirects=False)
        assert resp.status_code == 303
        page = client.get("/settings")
    html = page.text
    assert 'name="sonarr_schedule_enabled" checked' in html
    assert 'name="sonarr_schedule_days" value="Mon,Tue,Wed,Thu,Fri,Sat,Sun"' in html


def test_cleaner_schedule_all_days_stays_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "true"),
        ("emby_schedule_days", "Mon,Tue,Wed,Thu,Fri,Sat,Sun"),
        ("emby_schedule_start", "00:00"),
        ("emby_schedule_end", "23:59"),
        ("emby_max_items_scan", "2000"),
        ("emby_max_deletes_per_run", "25"),
        ("save_scope", "global"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/emby/settings/cleaner",
            content=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        page = client.get("/emby/settings")
    html = page.text
    assert 'name="emby_schedule_enabled" checked' in html
    assert 'name="emby_schedule_days" value="Mon,Tue,Wed,Thu,Fri,Sat,Sun"' in html
