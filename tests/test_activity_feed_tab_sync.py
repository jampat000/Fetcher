"""Activity page tab scope: server filter + live poll URL stay aligned with the selected pill."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.db import SessionLocal
from app.main import app
from app.models import ActivityLog, RefinerActivity
from app.web_common import (
    ACTIVITY_TAB_REFINER,
    ACTIVITY_TAB_SONARR,
    ACTIVITY_TAB_TRIMMER,
    activity_display_row,
    activity_log_tab_scope,
    filter_activity_display_for_tab,
    merge_activity_feed,
    normalize_activity_tab_query,
    refiner_activity_display_row,
)


def test_normalize_activity_tab_query_aliases() -> None:
    assert normalize_activity_tab_query(None) == "all"
    assert normalize_activity_tab_query("tv") == "sonarr"
    assert normalize_activity_tab_query("movies") == "radarr"
    assert normalize_activity_tab_query("refiner") == "refiner"


def test_filter_tab_trimmer_excludes_refiner_rows() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    now = ts
    logs = [
        ActivityLog(
            id=1,
            job_run_id=1,
            created_at=ts,
            app="trimmer",
            kind="trimmed",
            count=2,
            detail="t",
        ),
    ]
    refiners = [
        RefinerActivity(
            id=1,
            file_name="a.mkv",
            status="processing",
            size_before_bytes=1,
            size_after_bytes=1,
            audio_tracks_before=1,
            audio_tracks_after=1,
            subtitle_tracks_before=0,
            subtitle_tracks_after=0,
            created_at=ts,
        ),
    ]
    merged = merge_activity_feed(logs, refiners, "UTC", now, limit=50)
    trimmer_only = filter_activity_display_for_tab(merged, "trimmer")
    assert len(trimmer_only) == 1
    assert trimmer_only[0].get("activity_type") == "log"


def test_activity_page_refiner_tab_excludes_tv_search_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            from sqlalchemy import delete

            await session.execute(delete(RefinerActivity))
            await session.execute(delete(ActivityLog))
            session.add(
                ActivityLog(
                    app="sonarr",
                    kind="missing",
                    count=3,
                    detail="TV_ONLY_MARKER",
                )
            )
            session.add(
                RefinerActivity(
                    file_name="only.mkv",
                    media_title="Only",
                    status="processing",
                    size_before_bytes=1,
                    size_after_bytes=1,
                    audio_tracks_before=1,
                    audio_tracks_after=1,
                    subtitle_tracks_before=0,
                    subtitle_tracks_after=0,
                )
            )
            await session.commit()

    asyncio.run(seed())

    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    with TestClient(app) as client:
        r = client.get("/activity?app=refiner")
    assert r.status_code == 200
    assert "TV_ONLY_MARKER" not in r.text
    assert 'data-activity-app="sonarr"' not in r.text
    assert 'data-activity-tab-scope="refiner"' in r.text
    assert 'class="pill active" data-pill-filter="refiner"' in r.text or 'pill active" data-pill-filter="refiner"' in r.text


def test_activity_page_trimmer_tab_excludes_refiner_while_processing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Trimmer tab must not show Refiner rows even when a file is actively processing."""
    async def seed() -> None:
        async with SessionLocal() as session:
            from sqlalchemy import delete

            await session.execute(delete(RefinerActivity))
            await session.execute(delete(ActivityLog))
            session.add(
                ActivityLog(
                    app="trimmer",
                    kind="trimmed",
                    count=1,
                    detail="TRIMMER_TAB_MARKER",
                )
            )
            session.add(
                RefinerActivity(
                    file_name="live.mkv",
                    media_title="Live",
                    status="processing",
                    size_before_bytes=1,
                    size_after_bytes=1,
                    audio_tracks_before=1,
                    audio_tracks_after=1,
                    subtitle_tracks_before=0,
                    subtitle_tracks_after=0,
                )
            )
            await session.commit()

    asyncio.run(seed())

    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    with TestClient(app) as client:
        r = client.get("/activity?app=trimmer")
    assert r.status_code == 200
    assert "TRIMMER_TAB_MARKER" in r.text
    assert 'data-activity-app="refiner"' not in r.text
    assert 'data-refiner-live="1"' not in r.text
    assert 'class="pill' in r.text and "trimmer" in r.text


def test_activity_js_live_poll_uses_scoped_feed_url() -> None:
    js = Path("app/static/app.js").read_text(encoding="utf-8")
    assert "function activityFeedPollUrl" in js
    i = js.index('replaceLiveRegionFromUrl(\n            "#activity-live-root"')
    block = js[i : i + 450]
    assert "activityFeedPollUrl()," in block or "activityFeedPollUrl()" in block
    assert ',\n            "/activity",' not in block


def test_activity_js_syncs_url_on_pill_filter() -> None:
    js = Path("app/static/app.js").read_text(encoding="utf-8")
    assert "function syncActivityTabUrl" in js
    i0 = js.index("function applyActivityPillFilter(filter)")
    i1 = js.index("function applyActivityPillFilterFromUrl", i0)
    block = js[i0:i1]
    assert "syncActivityTabUrl" in block
    assert "#activity-feed-pills" in block


def test_activity_js_deep_link_covers_tv_movies() -> None:
    js = Path("app/static/app.js").read_text(encoding="utf-8")
    i0 = js.index("function applyActivityPillFilterFromUrl")
    i1 = js.index("function initActivityFilterPills", i0)
    block = js[i0:i1]
    assert 'raw === "sonarr"' in block or 'raw === "tv"' in block
    assert 'raw === "radarr"' in block or 'raw === "movies"' in block
    assert 'filterKey = "all"' in block


def test_activity_log_tab_scope_classifier() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    assert (
        activity_log_tab_scope(
            ActivityLog(id=1, job_run_id=1, created_at=ts, app="sonarr", kind="missing", count=1, detail="")
        )
        == ACTIVITY_TAB_SONARR
    )
    e = ActivityLog(
        id=2,
        job_run_id=1,
        created_at=ts,
        app="refiner",
        kind="refiner",
        status="ok",
        count=1,
        detail="Refiner (scheduled): processed=1 unchanged=0 dry_run_items=0 errors=0",
    )
    row = activity_display_row(e, "UTC")
    assert row["activity_tab_scope"] == ACTIVITY_TAB_REFINER
    r = RefinerActivity(
        id=9,
        file_name="x.mkv",
        status="processing",
        size_before_bytes=1,
        size_after_bytes=1,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=ts,
    )
    rr = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 13, 0, 0))
    assert rr["activity_tab_scope"] == ACTIVITY_TAB_REFINER


def test_activity_template_emits_tab_scope_attributes(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed() -> None:
        async with SessionLocal() as session:
            from sqlalchemy import delete

            await session.execute(delete(RefinerActivity))
            await session.execute(delete(ActivityLog))
            session.add(
                ActivityLog(
                    app="sonarr",
                    kind="missing",
                    count=10,
                    detail="Episodes",
                )
            )
            await session.commit()

    asyncio.run(seed())

    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    with TestClient(app) as client:
        r = client.get("/activity")
    assert r.status_code == 200
    assert 'data-activity-tab-scope="sonarr"' in r.text


def test_macro_includes_tab_scope_for_trimmer_log() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(
        id=1,
        job_run_id=1,
        created_at=ts,
        app="trimmer",
        kind="trimmed",
        count=1,
        detail="x",
    )
    row = activity_display_row(e, "UTC")
    assert row["activity_tab_scope"] == ACTIVITY_TAB_TRIMMER
