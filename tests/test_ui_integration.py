"""Smoke + form tests for every main page and save endpoint (no live Emby/Sonarr)."""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from urllib.parse import urlencode

import pytest
import httpx
from fastapi.testclient import TestClient

from app.db import SessionLocal, get_or_create_settings
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
    assert b'data-target="' in r.content


def _seed_setup_config(
    *,
    sonarr_url: str,
    sonarr_api_key: str,
    radarr_url: str,
    radarr_api_key: str,
    emby_url: str,
    emby_api_key: str,
) -> None:
    async def _seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_url = sonarr_url
            row.sonarr_api_key = sonarr_api_key
            row.radarr_url = radarr_url
            row.radarr_api_key = radarr_api_key
            row.emby_url = emby_url
            row.emby_api_key = emby_api_key
            await session.commit()

    asyncio.run(_seed())


def test_setup_wizard_visible_when_setup_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_setup_config(
        sonarr_url="",
        sonarr_api_key="",
        radarr_url="",
        radarr_api_key="",
        emby_url="",
        emby_api_key="",
    )
    with _client(monkeypatch) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert b"Run setup wizard" in r.content
    assert b"Setup</span>" in r.content


def test_setup_wizard_hidden_when_setup_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_setup_config(
        sonarr_url="http://localhost:8989",
        sonarr_api_key="sonarr-key",
        radarr_url="http://localhost:7878",
        radarr_api_key="radarr-key",
        emby_url="http://localhost:8096",
        emby_api_key="emby-key",
    )
    with _client(monkeypatch) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert b"Run setup wizard" not in r.content
    assert b"Setup</span>" not in r.content
    # Settings must remain accessible.
    with _client(monkeypatch) as client2:
        s = client2.get("/settings")
    assert s.status_code == 200
    assert b"sonarr_url" in s.content


def test_setup_wizard_reappears_if_setup_becomes_incomplete_again(monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_setup_config(
        sonarr_url="http://localhost:8989",
        sonarr_api_key="sonarr-key",
        radarr_url="http://localhost:7878",
        radarr_api_key="radarr-key",
        emby_url="http://localhost:8096",
        emby_api_key="emby-key",
    )
    with _client(monkeypatch) as client:
        r_complete = client.get("/")
    assert b"Run setup wizard" not in r_complete.content

    _seed_setup_config(
        sonarr_url="",
        sonarr_api_key="",
        radarr_url="",
        radarr_api_key="",
        emby_url="",
        emby_api_key="",
    )
    with _client(monkeypatch) as client2:
        r_incomplete = client2.get("/")
    assert b"Run setup wizard" in r_incomplete.content
    assert b"Setup</span>" in r_incomplete.content


def test_setup_wizard_page_left_nav_visibility_tracks_setup_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_setup_config(
        sonarr_url="",
        sonarr_api_key="",
        radarr_url="",
        radarr_api_key="",
        emby_url="",
        emby_api_key="",
    )
    with _client(monkeypatch) as client:
        r = client.get("/setup/1")
    assert r.status_code == 200
    assert b"Setup</span>" in r.content

    _seed_setup_config(
        sonarr_url="http://localhost:8989",
        sonarr_api_key="sonarr-key",
        radarr_url="http://localhost:7878",
        radarr_api_key="radarr-key",
        emby_url="http://localhost:8096",
        emby_api_key="emby-key",
    )
    with _client(monkeypatch) as client2:
        r2 = client2.get("/setup/1")
    assert r2.status_code == 200
    assert b"Setup</span>" not in r2.content


def test_settings_page_has_forms(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/settings")
    html = r.text
    assert r.status_code == 200
    assert b"sonarr_url" in r.content
    assert b"radarr_url" in r.content
    assert b"section-trimmer" not in r.content
    assert b"Trimmer settings" in r.content
    assert b"name=\"sonarr_cleanup_corrupt\"" in r.content
    assert b"name=\"sonarr_failed_import_remove_from_client\"" in r.content
    assert b"name=\"radarr_failed_import_remove_from_client\"" in r.content
    assert b"name=\"radarr_cleanup_corrupt\"" in r.content
    assert b"name=\"sonarr_cleanup_import_failed\"" in r.content
    assert b"name=\"radarr_cleanup_import_failed\"" in r.content
    assert html.count("re-add the queue row on sync.") == 2
    assert html.count('id="sonarr-panel-connection"') == 1
    assert html.count('id="radarr-panel-connection"') == 1
    assert html.count('id="sonarr-panel-search-cleanup"') == 1
    assert html.count('id="radarr-panel-search-cleanup"') == 1
    assert html.count("Search and cleanup</h3>") == 2
    assert html.count('id="sonarr-panel-limits"') == 1
    assert html.count('id="radarr-panel-limits"') == 1
    assert html.count("Limits and schedule</h3>") == 2
    assert html.count("settings-arr-panels") == 2
    assert html.count("Search behavior") == 0
    assert html.count("Failed import cleanup interval (minutes)") == 2
    assert html.count("How often to run the cleanup check when at least one remove option is on.") == 2
    assert html.count("Run limits") == 0
    assert "each Sonarr run removes" not in html
    assert "each Radarr run removes" not in html

    assert html.count("Runs searches on this interval.") == 2
    assert "How often Sonarr runs are due" not in html
    assert "How often Radarr runs are due" not in html


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


def test_settings_backup_restore_upgrade_are_global_only(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/settings")
    html = r.text
    assert 'data-settings-panel="global"' in html
    assert 'id="section-global"' in html
    assert 'id="log-viewer-hub"' in html
    assert 'id="section-global-backup"' in html
    assert 'id="updates-card"' in html
    assert "Backup &amp; restore" in html
    assert "Software updates" in html


@pytest.mark.parametrize(
    "url,needle",
    [
        ("/settings?saved=1&tab=global", "Global settings saved."),
        ("/settings?saved=1&tab=sonarr", "Sonarr settings saved."),
        ("/settings?saved=1&tab=radarr", "Radarr settings saved."),
    ],
)
def test_settings_saved_message_is_scope_aware(monkeypatch: pytest.MonkeyPatch, url: str, needle: str) -> None:
    with _client(monkeypatch) as client:
        r = client.get(url)
    assert r.status_code == 200
    assert needle in r.text


def test_settings_security_access_control_shows_inline_saved_banner(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/settings?saved=1&tab=security")
    assert r.status_code == 200
    assert "Access control saved." in r.text


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


def test_refiner_audio_dropdowns_include_ordered_languages(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/refiner/settings")
    assert r.status_code == 200
    html = r.text
    assert 'name="refiner_primary_audio_lang"' in html
    assert 'name="refiner_secondary_audio_lang"' in html
    assert 'name="refiner_tertiary_audio_lang"' in html
    assert 'name="refiner_default_audio_slot"' in html


def test_refiner_micro_helper_text_is_present(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/refiner/settings")
    assert r.status_code == 200
    html = r.text
    assert "Reads from the watched folder and writes finished files to the output folder." in html
    assert "With dry run off, originals are removed only after output is written successfully." in html
    assert "Keeps selected languages and removes unselected audio tracks." in html
    assert "Preferred languages (highest quality)" in html
    assert "Preferred languages (strict)" in html
    assert "Quality across all languages" in html
    assert 'trimmer-settings-section-tabs' in html
    assert "Watched folder check interval (seconds)" in html
    assert 'id="refiner-watched-folder-interval-sec"' in html
    assert "refiner-folders-interval-wrap" in html
    assert 'name="refiner_interval_seconds"' in html
    i_folders = html.index('id="refiner-folders"')
    i_interval = html.index("refiner-folders-interval-wrap")
    i_advanced = html.index("refiner-folders-advanced")
    i_sched = html.index('id="refiner-schedule"')
    assert i_folders < i_interval < i_advanced < i_sched
    assert 'href="#refiner-processing"' in html
    assert 'id="refiner-folders"' in html
    assert 'id="refiner-schedule"' in html
    assert "refiner-work" in html
    assert "refiner_default_work_folder_path" not in html
    assert "Checks the watched folder on this interval and processes ready files." in html
    assert "Limits processing to the selected days and times." in html
    assert "Enter the full folder path (e.g. F:\\Downloads\\Movies)" in html
    assert "Enter the destination folder for processed files" in html
    assert "Temporary working directory for processing" in html
    assert 'formaction="/refiner/settings/save?refiner_section=processing"' in html
    assert 'formaction="/refiner/settings/save?refiner_section=folders"' in html
    assert 'formaction="/refiner/settings/save?refiner_section=audio"' in html
    assert 'formaction="/refiner/settings/save?refiner_section=subtitles"' in html
    assert 'formaction="/refiner/settings/save?refiner_section=schedule"' in html


def test_refiner_saved_banner_uses_schedule_and_limits_wording(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/refiner/settings?saved=1&refiner_saved=schedule")
    assert r.status_code == 200
    assert "Refiner settings saved (Schedule &amp; limits)." in r.text


def test_refiner_fail_banner_includes_section_query(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/refiner/settings?save=fail&reason=db_busy&refiner_section=audio")
    assert r.status_code == 200
    assert "Could not save (Audio). Try again. (db_busy)" in r.text


def test_post_refiner_save_async_header_returns_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Refiner XHR path: JSON instead of 303 (same persistence as normal POST)."""
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_primary_audio_lang = "eng"
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        resp = client.post(
            "/refiner/settings/save?refiner_section=folders",
            data={
                "refiner_enabled": "true",
                "refiner_dry_run": "true",
                "refiner_primary_audio_lang": "eng",
                "refiner_secondary_audio_lang": "",
                "refiner_tertiary_audio_lang": "",
                "refiner_default_audio_slot": "primary",
                "refiner_audio_preference_mode": "preferred_langs_quality",
                "refiner_watched_folder": "D:\\incoming",
                "refiner_output_folder": "D:\\processed-async",
                "refiner_schedule_enabled": "false",
                "refiner_interval_seconds": "120",
                **{f"refiner_schedule_{d}": "0" for d in _WEEKDAYS},
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Fetcher-Refiner-Settings-Async": "1",
            },
        )
    assert resp.status_code == 200
    assert "application/json" in (resp.headers.get("content-type") or "").lower()
    assert resp.json() == {"ok": True, "section": "folders"}

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            assert row.refiner_interval_seconds == 120

    asyncio.run(verify())


def test_refiner_save_async_validation_returns_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Refiner async path: validation failure returns JSON with reason (no redirect)."""
    with _client(monkeypatch) as client:
        resp = client.post(
            "/refiner/settings/save?refiner_section=audio",
            data={
                "refiner_enabled": "true",
                "refiner_dry_run": "true",
                "refiner_primary_audio_lang": "",
                "refiner_secondary_audio_lang": "",
                "refiner_tertiary_audio_lang": "",
                "refiner_default_audio_slot": "primary",
                "refiner_audio_preference_mode": "preferred_langs_quality",
                "refiner_watched_folder": "D:\\incoming",
                "refiner_output_folder": "D:\\processed",
                "refiner_schedule_enabled": "false",
                "refiner_interval_seconds": "60",
                **{f"refiner_schedule_{d}": "0" for d in _WEEKDAYS},
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Fetcher-Refiner-Settings-Async": "1",
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert body["reason"] == "primary_audio_required"
    assert body["section"] == "audio"
    assert isinstance(body.get("message"), str) and len(body["message"]) > 20


def test_refiner_processing_save_async_allows_enable_without_audio_or_folders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Processing section: enable Refiner even when Audio/Folders are still incomplete."""
    with _client(monkeypatch) as client:
        resp = client.post(
            "/refiner/settings/save?refiner_section=processing",
            data={
                "refiner_enabled": "true",
                "refiner_dry_run": "true",
                "refiner_primary_audio_lang": "",
                "refiner_secondary_audio_lang": "",
                "refiner_tertiary_audio_lang": "",
                "refiner_default_audio_slot": "primary",
                "refiner_audio_preference_mode": "preferred_langs_quality",
                "refiner_watched_folder": "",
                "refiner_output_folder": "",
                "refiner_schedule_enabled": "false",
                "refiner_interval_seconds": "60",
                **{f"refiner_schedule_{d}": "0" for d in _WEEKDAYS},
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Fetcher-Refiner-Settings-Async": "1",
            },
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "section": "processing"}


def test_refiner_folders_save_async_rejects_missing_paths_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/refiner/settings/save?refiner_section=folders",
            data={
                "refiner_enabled": "true",
                "refiner_dry_run": "true",
                "refiner_primary_audio_lang": "eng",
                "refiner_secondary_audio_lang": "",
                "refiner_tertiary_audio_lang": "",
                "refiner_default_audio_slot": "primary",
                "refiner_audio_preference_mode": "preferred_langs_quality",
                "refiner_watched_folder": "",
                "refiner_output_folder": "",
                "refiner_schedule_enabled": "false",
                "refiner_interval_seconds": "60",
                **{f"refiner_schedule_{d}": "0" for d in _WEEKDAYS},
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Fetcher-Refiner-Settings-Async": "1",
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert body["reason"] == "watched_output_required"
    assert body["section"] == "folders"
    assert isinstance(body.get("message"), str) and "folder" in body["message"].lower()


def test_refiner_readiness_banner_when_enabled_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_primary_audio_lang = ""
            row.refiner_watched_folder = ""
            row.refiner_output_folder = ""
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/refiner/settings")
    assert r.status_code == 200
    assert "refiner-readiness-banner" in r.text
    assert "not ready" in r.text.lower()


def test_refiner_readiness_brief_api_json(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_primary_audio_lang = ""
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        resp = client.get("/api/refiner/readiness-brief")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("enabled") is True
    assert isinstance(body.get("issues"), list)
    assert len(body["issues"]) >= 1


def test_refiner_settings_page_has_syncable_banner_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/refiner/settings")
    assert r.status_code == 200
    assert 'id="refiner-banner-off"' in r.text
    assert 'id="refiner-banner-readiness"' in r.text


def test_refiner_dry_run_save_does_not_modify_emby_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.emby_dry_run = True
            row.refiner_dry_run = True
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        resp = client.post(
            "/refiner/settings/save?refiner_section=folders",
            data={
                "refiner_enabled": "true",
                "refiner_dry_run": "false",
                "refiner_primary_audio_lang": "eng",
                "refiner_secondary_audio_lang": "",
                "refiner_tertiary_audio_lang": "",
                "refiner_default_audio_slot": "primary",
                "refiner_audio_preference_mode": "best_available",
                "refiner_watched_folder": "D:\\incoming",
                "refiner_output_folder": "D:\\processed",
                "refiner_schedule_enabled": "false",
                "refiner_interval_seconds": "60",
                **{f"refiner_schedule_{d}": "0" for d in _WEEKDAYS},
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
    assert resp.status_code in (302, 303)
    loc = resp.headers.get("location") or ""
    assert "refiner/settings" in loc and "saved=1" in loc and "refiner_saved=folders" in loc and "#refiner-folders" in loc

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            assert row.emby_dry_run is True
            assert row.refiner_dry_run is False
            assert row.refiner_audio_preference_mode == "preferred_langs_quality"
            assert row.refiner_interval_seconds == 60

    asyncio.run(verify())


def test_trimmer_dry_run_save_does_not_modify_refiner_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.emby_dry_run = True
            row.refiner_dry_run = False
            await session.commit()

    asyncio.run(seed())
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "false"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "00:00"),
        ("emby_schedule_end", "23:59"),
        ("emby_interval_minutes", "60"),
        ("emby_max_items_scan", "100"),
        ("emby_max_deletes_per_run", "5"),
        ("save_scope", "schedule"),
    ]
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner",
            content=urlencode(pairs),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
    assert resp.status_code in (302, 303)

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            assert row.emby_dry_run is False
            assert row.refiner_dry_run is False

    asyncio.run(verify())


def test_refiner_is_not_embedded_in_trimmer_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/trimmer/settings")
    assert r.status_code == 200
    assert "Refiner settings" not in r.text
    assert 'name="refiner_primary_audio_lang"' not in r.text


def test_refiner_page_is_separate_from_trimmer(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/refiner/settings")
    assert r.status_code == 200
    assert "Refiner settings" in r.text
    assert "Trimmer settings" in r.text


def test_refiner_overview_page_exists_and_has_tabs(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_interval_seconds = 45
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/refiner")
    assert r.status_code == 200
    assert "Refiner overview" in r.text
    assert "45s" in r.text
    assert 'href="/refiner"' in r.text
    assert 'href="/refiner/settings"' in r.text


def test_trimmer_settings_section_visibility_script_registered() -> None:
    js = Path("app/static/app.js").read_text(encoding="utf-8")
    assert "function initTrimmerSettingsSectionTabs()" in js
    assert "initTrimmerSettingsSectionTabs();" in js


def test_trimmer_page_uses_overview_wording(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/trimmer")
    assert r.status_code == 200
    assert "Trimmer overview" in r.text
    assert "Trimmer review" not in r.text
    assert "Overview" in r.text
    assert "Rules in Trimmer settings" not in r.text


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
    app_name = "Sonarr" if scope.startswith("sonarr_") else "Radarr"
    flavor = "missing" if scope.endswith("_missing") else "upgrade"
    assert resp.json() == {
        "ok": True,
        "queued": False,
        "message": f"Manual {flavor} search sent to {app_name} successfully.",
    }
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
        "message": (
            "Radarr rejected the immediate manual upgrade search; Fetcher queued a full automation "
            "pass instead. Check Activity in a moment."
        ),
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
        "sonarr_retry_delay_minutes": "1440",
        "radarr_retry_delay_minutes": "1440",
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
        "sonarr_retry_delay_minutes": "1440",
        "radarr_retry_delay_minutes": "1440",
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
    loc = resp.headers.get("location") or ""
    assert "/trimmer/settings" in loc
    assert "trimmer_saved=connection" in loc


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
    assert resp.json() == {"ok": True, "section": "connection", "save_scope": "connection"}


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
    assert resp.json() == {"ok": True, "section": "schedule", "save_scope": "schedule"}

    async def verify_interval() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            assert row.emby_interval_minutes == 95

    asyncio.run(verify_interval())


def test_post_trimmer_cleaner_save_scope_from_query_when_missing_from_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Query ``trimmer_save_scope`` must work when the submitter omits ``save_scope`` (async / edge browsers)."""
    pairs: list[tuple[str, str]] = [
        ("emby_dry_run", "true"),
        ("emby_schedule_enabled", "false"),
        *_schedule_flag_pairs("emby_schedule"),
        ("emby_schedule_start", "10:00"),
        ("emby_schedule_end", "18:00"),
        ("emby_max_items_scan", "2000"),
        ("emby_max_deletes_per_run", "25"),
        ("emby_interval_minutes", "93"),
    ]
    encoded = urlencode(pairs)
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner?trimmer_section=schedule&trimmer_save_scope=schedule",
            content=encoded,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Fetcher-Trimmer-Settings-Async": "1",
            },
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "section": "schedule", "save_scope": "schedule"}

    async def verify_interval() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            assert row.emby_interval_minutes == 93

    asyncio.run(verify_interval())


def test_trimmer_cleaner_validation_async_returns_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """422 on Trimmer cleaner with async header must be JSON (not HTML) so the UI shows a real error."""
    with _client(monkeypatch) as client:
        resp = client.post(
            "/trimmer/settings/cleaner?trimmer_section=people",
            data={
                "save_scope": "tv",
                "emby_interval_minutes": "not-an-int",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Fetcher-Trimmer-Settings-Async": "1",
            },
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": False, "section": "people", "reason": "invalid", "save_scope": "tv"}


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
    assert resp.json() == {"ok": False, "section": "schedule", "reason": "invalid_scope", "save_scope": "sonarr"}


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
    assert resp.json()["save_scope"] == "global"


def test_post_trimmer_cleaner_legacy_global_does_not_mutate_db(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
            row = await get_or_create_settings(session)
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
    assert resp.json()["save_scope"] == "all"


def test_post_trimmer_cleaner_save_scope_all_does_not_mutate_db(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
            row = await get_or_create_settings(session)
            assert row.emby_interval_minutes == 33
            assert row.emby_rule_movie_watched_rating_below == 5
            assert row.emby_rule_tv_unwatched_days == 20

    asyncio.run(verify())


def test_trimmer_schedule_save_does_not_update_rule_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
            row = await get_or_create_settings(session)
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
            row = await get_or_create_settings(session)
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
            row = await get_or_create_settings(session)
            assert row.emby_rule_tv_unwatched_days == 25
            assert row.emby_rule_movie_watched_rating_below == 8
            assert row.emby_rule_movie_unwatched_days == 15

    asyncio.run(verify())


def test_trimmer_movies_save_does_not_update_tv_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
            row = await get_or_create_settings(session)
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
            row = await get_or_create_settings(session)
            row.emby_interval_minutes = 77
            row.emby_dry_run = True
            await session.commit()

    asyncio.run(seed())
    payload = {
        "sonarr_retry_delay_minutes": "720",
        "radarr_retry_delay_minutes": "720",
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
            row = await get_or_create_settings(session)
            assert row.emby_interval_minutes == 77
            assert row.emby_dry_run is True

    asyncio.run(verify())


def test_trimmer_schedule_save_does_not_mutate_sonarr_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
            row = await get_or_create_settings(session)
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
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_retry_delay_minutes = 17
            row.radarr_retry_delay_minutes = 23
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "sonarr_retry_delay_minutes": "2000",
                "radarr_retry_delay_minutes": "2000",
                "log_retention_days": "90",
                "timezone": "UTC",
                "save_scope": "global",
            },
            headers={"X-Fetcher-Settings-Async": "1"},
        )
    assert resp.status_code == 200
    assert "application/json" in (resp.headers.get("content-type") or "")
    assert resp.json() == {"ok": True, "tab": "global"}

    async def verify_retry_delay_unchanged() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            assert row.sonarr_retry_delay_minutes == 17
            assert row.radarr_retry_delay_minutes == 23

    asyncio.run(verify_retry_delay_unchanged())


def test_post_settings_rejects_missing_save_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "sonarr_retry_delay_minutes": "100",
                "radarr_retry_delay_minutes": "100",
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
                "sonarr_retry_delay_minutes": "100",
                "radarr_retry_delay_minutes": "100",
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
            row = await get_or_create_settings(session)
            row.sonarr_retry_delay_minutes = 7777
            row.radarr_retry_delay_minutes = 7777
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        resp = client.post(
            "/settings",
            data={
                "save_scope": "all",
                "sonarr_retry_delay_minutes": "1111",
                "radarr_retry_delay_minutes": "1111",
                "log_retention_days": "90",
                "timezone": "UTC",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            assert row.sonarr_retry_delay_minutes == 7777
            assert row.radarr_retry_delay_minutes == 7777

    asyncio.run(verify())


def test_get_settings_save_fail_banner_visible_on_sonarr_tab(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/settings?save=fail&reason=db_busy&tab=sonarr")
    assert r.status_code == 200
    html = r.text
    assert "settings-fetcher-save-fail" in html
    assert "Could not save (Sonarr). Try again. (db_busy)" in html


def test_test_sonarr_post_does_not_mutate_app_settings_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_JWT_SECRET", "test-jwt-secret-for-pytest-only")

    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
                row = await get_or_create_settings(session)
                assert row.sonarr_url == "http://unchanged.example:8989"

        asyncio.run(verify_url())
    finally:

        async def reset_url() -> None:
            async with SessionLocal() as session:
                row = await get_or_create_settings(session)
                row.sonarr_url = ""
                await session.commit()

        asyncio.run(reset_url())


def test_global_save_updates_only_retention_timezone(monkeypatch: pytest.MonkeyPatch) -> None:
    """Save global settings must not apply Sonarr/Radarr fields from the posted form."""

    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_interval_minutes = 33
            row.radarr_interval_minutes = 44
            row.sonarr_max_items_per_run = 50
            row.sonarr_retry_delay_minutes = 1440
            row.radarr_retry_delay_minutes = 1440
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
        "radarr_max_items_per_run": "888",
        "radarr_schedule_enabled": "false",
        "radarr_schedule_start": "00:00",
        "radarr_schedule_end": "23:59",
        "radarr_interval_minutes": "88",
        "sonarr_retry_delay_minutes": "720",
        "radarr_retry_delay_minutes": "720",
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
            row = await get_or_create_settings(session)
            assert row.sonarr_interval_minutes == 33
            assert row.radarr_interval_minutes == 44
            assert row.sonarr_max_items_per_run == 50
            assert row.sonarr_retry_delay_minutes == 1440
            assert row.radarr_retry_delay_minutes == 1440
            assert row.log_retention_days == 90
            assert row.timezone == "Europe/Berlin"
            assert (row.sonarr_url or "").strip() == ""

    asyncio.run(verify_db())


def test_sonarr_save_preserves_radarr_interval_when_post_includes_wrong_radarr_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: Sonarr-only save must not let a bogus radarr_interval in the POST affect the DB."""

    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
        "radarr_max_items_per_run": "50",
        "radarr_schedule_enabled": "false",
        "radarr_schedule_start": "00:00",
        "radarr_schedule_end": "23:59",
        "radarr_interval_minutes": "999",
        "sonarr_retry_delay_minutes": "1440",
        "radarr_retry_delay_minutes": "1440",
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
            row = await get_or_create_settings(session)
            assert row.radarr_interval_minutes == 30

    asyncio.run(verify_db())


def test_sonarr_granular_cleanup_saves_without_touching_radarr_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_cleanup_corrupt = False
            row.radarr_cleanup_corrupt = True
            await session.commit()

    asyncio.run(seed())
    payload = {
        "save_scope": "sonarr",
        "sonarr_enabled": "false",
        "sonarr_url": "",
        "sonarr_api_key": "",
        "sonarr_search_missing": "true",
        "sonarr_search_upgrades": "true",
        "sonarr_cleanup_corrupt": "true",
        "sonarr_max_items_per_run": "50",
        "sonarr_interval_minutes": "60",
        "sonarr_retry_delay_minutes": "15",
        "sonarr_schedule_start": "00:00",
        "sonarr_schedule_end": "23:59",
    }
    with _client(monkeypatch) as client:
        resp = client.post("/settings", data=payload, follow_redirects=False)
    assert resp.status_code == 303

    async def verify() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            assert row.sonarr_cleanup_corrupt is True
            assert row.radarr_cleanup_corrupt is True

    asyncio.run(verify())


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
        "sonarr_retry_delay_minutes": "1440",
        "radarr_retry_delay_minutes": "1440",
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
