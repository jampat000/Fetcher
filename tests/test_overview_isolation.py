"""Refiner vs Trimmer overview: compact configured-state summaries; semantic isolation; shared visuals only."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from app.db import SessionLocal, get_or_create_settings
from app.main import app
from app.models import AppSettings
from app.routers import refiner as refiner_router
from app.routers import trimmer as trimmer_router
from app.time_util import utc_now_naive
from app.trimmer_service import (
    TRIMMER_REVIEW_ERROR_MISSING_CONNECTION,
    TrimmerReviewResult,
    TrimmerReviewService,
)


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


def test_refiner_and_trimmer_overview_builders_are_not_shared() -> None:
    assert refiner_router.build_refiner_overview_config.__module__ == "app.routers.refiner"
    assert trimmer_router.build_trimmer_overview_config.__module__ == "app.routers.trimmer"


def test_trimmer_overview_page_avoids_refiner_overview_css_classes(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.emby_schedule_enabled = False
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/trimmer")
    assert r.status_code == 200
    assert "refiner-overview-value--muted" not in r.text
    assert "refiner-overview-section-title" not in r.text
    assert "refiner-overview-grid" not in r.text
    assert "refiner-overview-path" not in r.text
    assert "overview-value--muted" in r.text


def test_trimmer_overview_compact_configured_state_only(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.emby_schedule_enabled = False
            row.emby_rule_movie_unwatched_days = 30
            row.emby_rule_tv_unwatched_days = 14
            row.emby_user_id = "user-abc"
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/trimmer")
    assert r.status_code == 200
    assert "trimmer-overview-body" in r.text
    assert "m-grid trimmer-overview-config-grid" not in r.text
    assert "overview-section--recent" in r.text
    assert "30 days" in r.text
    assert "14 days" in r.text
    assert "user-abc" in r.text
    assert "Future episodes monitored" in r.text
    assert "Episodes unmonitored after trim" in r.text
    assert "Delete watched movies" in r.text
    for needle in (
        "Status",
        "Connection",
        "Scope",
        "Rules",
        "TV handling",
        "Schedule",
        "Recent activity",
        "Run interval",
        "Last scan",
    ):
        assert needle in r.text
    assert "Emby connection" not in r.text
    assert "Emby user" not in r.text
    assert 'class="label">User</div>' in r.text

    assert "Failed-import cleanup" not in r.text
    assert "Sonarr cleanup" not in r.text
    assert "Scan summary" not in r.text
    assert "Matched (this page)" not in r.text

    body_start = r.text.index("trimmer-overview-body")
    tail = r.text[body_start:]
    assert tail.index("Status") < tail.index("Connection") < tail.index("Scope") < tail.index("Rules")
    assert tail.index("Rules") < tail.index("TV handling") < tail.index("Schedule")
    assert tail.index("Schedule") < tail.index("Recent activity")


def test_trimmer_validation_error_not_inside_overview_card(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _seed_trimmer_enabled_connection_missing() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.emby_enabled = True
            row.emby_url = ""
            row.emby_api_key = ""
            await session.commit()

    asyncio.run(_seed_trimmer_enabled_connection_missing())

    async def _fake_review(_self, _settings, *, run_emby_scan: bool) -> TrimmerReviewResult:
        return TrimmerReviewResult(error=TRIMMER_REVIEW_ERROR_MISSING_CONNECTION)

    monkeypatch.setattr(TrimmerReviewService, "build_review", _fake_review)
    with _client(monkeypatch) as client:
        r = client.get("/trimmer")
    assert r.status_code == 200
    assert "Connection required" in r.text
    assert "Configure a media server URL and API key" in r.text
    assert "trimmer-validation-notice" in r.text
    assert "trimmer-page-top-notices" in r.text
    i_notice = r.text.index("trimmer-validation-notice")
    i_summary = r.text.index("trimmer-overview-summary-card")
    i_actions = r.text.index("trimmer-overview-actions")
    i_validation = r.text.index("trimmer-page-validation-and-results")
    assert i_notice < i_summary, "connection notice should appear above the overview card"
    assert "Connection required" not in r.text[i_summary:i_actions]
    assert "Connection required" not in r.text[i_actions:i_validation]
    assert r.text.index("trimmer-page-top-notices") < i_summary
    assert 'class="ink-link" href="/trimmer/settings#trimmer-connection"' in r.text
    assert 'class="ink-link" href="/activity?app=trimmer"' in r.text


def test_build_trimmer_recent_activity_summary_trimmed_owned() -> None:
    now = utc_now_naive()
    s = AppSettings()
    assert trimmer_router.build_trimmer_recent_activity_summary(s, now=now) == "No runs yet"
    s.emby_last_run_at = now - timedelta(minutes=3)
    out = trimmer_router.build_trimmer_recent_activity_summary(s, now=now)
    assert out.startswith("Last scan ")
    assert trimmer_router.build_trimmer_recent_activity_summary.__module__ == "app.routers.trimmer"


def test_refiner_overview_unchanged_after_trimmer_overview_redesign(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/refiner")
    assert r.status_code == 200
    assert "refiner-overview-body" in r.text
    assert "trimmer-overview-body" not in r.text
    assert "Open Refiner settings" not in r.text


def test_refiner_overview_compact_configured_state(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.movie_refiner_interval_seconds = 45
            row.refiner_watched_folder = "D:\\Media\\in"
            row.refiner_output_folder = "D:\\Media\\out"
            row.refiner_schedule_enabled = True
            row.refiner_schedule_days = "Mon"
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/refiner")
    assert r.status_code == 200
    assert "refiner-overview-body" in r.text
    assert "m-grid refiner-overview-grid" not in r.text
    assert "overview-section--recent" in r.text
    assert "activity-list" not in r.text
    i0 = r.text.index("refiner-overview-body")
    i1 = r.text.index("overview-activity-log-link")
    overview_chunk = r.text[i0:i1]
    assert "Artwork" not in overview_chunk
    for needle in (
        "Watched folder",
        "Output folder",
        "Work folder",
        "Scan interval",
        "45s",
        "Audio tracks",
        "Subtitles",
        "Source cleanup",
        "Schedule window",
        "Last scan",
        "Recent activity",
    ):
        assert needle in r.text

    body_start = r.text.index("refiner-overview-body")
    tail = r.text[body_start:]
    i_status = tail.index("Status")
    i_folders = tail.index("Folders")
    i_audio = tail.index("Audio")
    i_subs = tail.index("Subtitles")
    i_proc = tail.index("Processing")
    i_sched = tail.index("Schedule")
    i_recent = tail.index("Recent activity")
    assert i_status < i_folders < i_audio < i_subs < i_proc < i_sched < i_recent
    assert 'class="ink-link" href="/activity?app=refiner"' in r.text


def test_trimmer_page_no_heading_helper_prose(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/trimmer")
    assert r.status_code == 200
    assert "Rules in Trimmer settings" not in r.text


def test_overview_activity_log_links_are_tool_scoped() -> None:
    from pathlib import Path

    ref = Path("app/templates/refiner.html").read_text(encoding="utf-8")
    tri = Path("app/templates/trimmer.html").read_text(encoding="utf-8")
    assert '/activity?app=refiner"' in ref
    assert '/activity?app=trimmer"' in tri
    assert '/activity?app=emby"' not in tri
    assert 'class="ink-link"' in ref
    assert 'class="ink-link"' in tri
    assert "overview-activity-log-link" in ref
    assert "overview-activity-log-link" in tri


def test_activity_js_supports_app_query_deep_link() -> None:
    from pathlib import Path

    js = Path("app/static/app.js").read_text(encoding="utf-8")
    i0 = js.index("function initActivityFilterPills")
    i1 = js.index("function initSettingsPageCollapses", i0)
    block = js[i0:i1]
    assert "applyActivityPillFilter" in block
    assert "applyActivityPillFilterFromLocationOrDom" in block
    i_lo = js.index("function applyActivityPillFilterFromLocationOrDom")
    i_hi = js.index("function installActivityFeedClickDelegationOnce", i_lo)
    deep_link = js[i_lo:i_hi]
    assert '"refiner"' in deep_link
    assert '"trimmer"' in deep_link
    assert "emby" not in deep_link
    assert 'raw === "emby"' not in js


def test_activity_page_accepts_app_query(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r_ref = client.get("/activity?app=refiner")
        r_tri = client.get("/activity?app=trimmer")
        r_unknown = client.get("/activity?app=emby")
    assert r_ref.status_code == 200
    assert r_tri.status_code == 200
    assert r_unknown.status_code == 200
    assert "Activity" in r_ref.text


def test_activity_template_trimmer_pill_uses_trimmer_token() -> None:
    from pathlib import Path

    html = Path("app/templates/activity.html").read_text(encoding="utf-8")
    assert 'data-pill-filter="trimmer"' in html
    assert 'data-pill-filter="emby"' not in html


def test_trimmer_rules_collapsed_when_no_active_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.emby_rule_watched_rating_below = 0
            row.emby_rule_unwatched_days = 0
            row.emby_rule_movie_watched_rating_below = 0
            row.emby_rule_movie_unwatched_days = 0
            row.emby_rule_tv_delete_watched = False
            row.emby_rule_tv_unwatched_days = 0
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/trimmer")
    assert r.status_code == 200
    assert "No active rules" in r.text


def test_refiner_recent_activity_summary_states(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/refiner")
    assert r.status_code == 200
    assert any(
        x in r.text
        for x in ("Processing now", "Last scan ", "No runs yet")
    )


def test_refiner_and_trimmer_share_only_overview_visual_primitives() -> None:
    """Both use generic overview-section--recent; neither template uses the other's feature-prefixed section hooks."""
    # Static check: refiner template should not reference trimmer-overview-*
    from pathlib import Path

    refiner_tpl = Path("app/templates/refiner.html").read_text(encoding="utf-8")
    trimmer_tpl = Path("app/templates/trimmer.html").read_text(encoding="utf-8")
    assert "trimmer-overview" not in refiner_tpl
    assert "refiner-overview-body" not in trimmer_tpl
    assert "overview-section--recent" in refiner_tpl
    assert "overview-section--recent" in trimmer_tpl
