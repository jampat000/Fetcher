"""Activity page search/filter: server scoping, URL params, no dashboard regression."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from app.db import SessionLocal
from app.main import app
from app.models import ActivityLog, RefinerActivity
from app.web_common import (
    filter_activity_display_for_search,
    normalize_activity_tab_query,
)


def test_normalize_activity_tab_query_aliases() -> None:
    assert normalize_activity_tab_query(None) == "all"
    assert normalize_activity_tab_query("tv") == "sonarr"
    assert normalize_activity_tab_query("movies") == "radarr"
    assert normalize_activity_tab_query("refiner") == "refiner"


def test_filter_activity_display_for_search_case_insensitive_substring() -> None:
    rows: list[dict] = [
        {"primary_label": "TV · Missing search · 2 episodes", "detail_lines": ["Show Alpha"], "activity_tab_scope": "sonarr"},
        {"primary_label": "Other", "detail_lines": [], "activity_tab_scope": "sonarr"},
    ]
    out = filter_activity_display_for_search(rows, "alpha")
    assert len(out) == 1
    assert "Alpha" in (out[0].get("detail_lines") or [""])[0]


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


def test_activity_page_load_with_tab_and_search(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            from sqlalchemy import delete

            await session.execute(delete(RefinerActivity))
            await session.execute(delete(ActivityLog))
            session.add(
                ActivityLog(
                    app="sonarr",
                    kind="missing",
                    count=2,
                    detail="UniqueQueryTokenAAA\nSecond line",
                )
            )
            session.add(
                ActivityLog(
                    app="radarr",
                    kind="upgrade",
                    count=1,
                    detail="UniqueQueryTokenBBB",
                )
            )
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/activity?app=sonarr&q=TokenAAA")
    assert r.status_code == 200
    assert "UniqueQueryTokenAAA" in r.text
    assert "UniqueQueryTokenBBB" not in r.text
    assert 'value="TokenAAA"' in r.text
    assert "activity-search-input" in r.text


def test_activity_refiner_tab_plus_search_excludes_other_apps(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            from sqlalchemy import delete

            await session.execute(delete(RefinerActivity))
            await session.execute(delete(ActivityLog))
            session.add(
                ActivityLog(
                    app="radarr",
                    kind="upgrade",
                    count=1,
                    detail="ZZZ_ShouldNotAppearInRefinerSearchZZZ",
                )
            )
            session.add(
                RefinerActivity(
                    file_name="movie.mkv",
                    media_title="Risen Eagle",
                    status="success",
                    size_before_bytes=100,
                    size_after_bytes=90,
                    audio_tracks_before=2,
                    audio_tracks_after=1,
                    subtitle_tracks_before=0,
                    subtitle_tracks_after=0,
                )
            )
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/activity?app=refiner&q=movie")
    assert r.status_code == 200
    assert "ZZZ_ShouldNotAppearInRefinerSearchZZZ" not in r.text
    assert "movie" in r.text.lower()
    assert 'data-activity-tab-scope="refiner"' in r.text
    assert "activity-refiner-compare-details" in r.text


def test_dashboard_home_does_not_require_activity_search_params(monkeypatch: pytest.MonkeyPatch) -> None:
    with _client(monkeypatch) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert "dashboard-activity-live-root" in r.text


def test_activity_js_search_and_poll_use_token_stale_guard() -> None:
    from pathlib import Path

    js = Path("app/static/app.js").read_text(encoding="utf-8")
    assert "function activityFeedPollUrl" in js
    assert "fetcherBumpActivityFeedLatestToken" in js
    assert "pollToken === fetcherPeekActivityFeedLatestToken()" in js
    assert "token === fetcherPeekActivityFeedLatestToken()" in js
