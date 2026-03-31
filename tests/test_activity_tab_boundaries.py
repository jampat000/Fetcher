"""Activity page tab scope: canonical classification + no cross-tab leakage (server dict + HTML markers)."""

from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from app.db import SessionLocal
from app.main import app
from app.models import ActivityLog, RefinerActivity
from app.web_common import (
    ACTIVITY_TAB_ALL_ONLY,
    ACTIVITY_TAB_RADARR,
    ACTIVITY_TAB_REFINER,
    ACTIVITY_TAB_SONARR,
    ACTIVITY_TAB_TRIMMER,
    activity_display_row,
    activity_log_tab_scope,
    filter_activity_display_for_tab,
    merge_activity_feed,
    refiner_activity_display_row,
)


def test_activity_log_tab_scope_sonarr_radarr_trimmer() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    assert (
        activity_log_tab_scope(
            ActivityLog(id=1, job_run_id=1, created_at=ts, app="sonarr", kind="missing", count=2, detail="")
        )
        == ACTIVITY_TAB_SONARR
    )
    assert (
        activity_log_tab_scope(
            ActivityLog(id=2, job_run_id=1, created_at=ts, app="radarr", kind="upgrade", count=1, detail="")
        )
        == ACTIVITY_TAB_RADARR
    )
    assert (
        activity_log_tab_scope(
            ActivityLog(id=3, job_run_id=1, created_at=ts, app="trimmer", kind="trimmed", count=3, detail="")
        )
        == ACTIVITY_TAB_TRIMMER
    )


def test_activity_log_tab_scope_refiner_batch_only_when_kind_refiner() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    batch = ActivityLog(
        id=1,
        job_run_id=1,
        created_at=ts,
        app="refiner",
        kind="refiner",
        status="ok",
        count=1,
        detail="Refiner (scheduled): processed=1 unchanged=0 dry_run_items=0 errors=0",
    )
    assert activity_log_tab_scope(batch) == ACTIVITY_TAB_REFINER
    weird = ActivityLog(
        id=2,
        job_run_id=1,
        created_at=ts,
        app="refiner",
        kind="error",
        count=0,
        detail="x",
    )
    assert activity_log_tab_scope(weird) == ACTIVITY_TAB_ALL_ONLY


def test_activity_log_tab_scope_service_and_unknown_app_are_all_only() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    assert (
        activity_log_tab_scope(
            ActivityLog(
                id=1,
                job_run_id=1,
                created_at=ts,
                app="service",
                kind="error",
                status="failed",
                count=0,
                detail="boom",
            )
        )
        == ACTIVITY_TAB_ALL_ONLY
    )
    assert (
        activity_log_tab_scope(
            ActivityLog(id=2, job_run_id=1, created_at=ts, app="", kind="missing", count=0, detail="")
        )
        == ACTIVITY_TAB_ALL_ONLY
    )


def test_activity_display_row_includes_tab_scope_matching_classifier() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    e = ActivityLog(id=1, job_run_id=1, created_at=ts, app="sonarr", kind="missing", count=10, detail="")
    row = activity_display_row(e, "UTC")
    assert row["activity_tab_scope"] == ACTIVITY_TAB_SONARR


def test_merge_activity_feed_mixed_scopes_no_cross_contamination() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    now = ts
    logs = [
        ActivityLog(id=1, job_run_id=1, created_at=ts, app="sonarr", kind="missing", count=10, detail=""),
        ActivityLog(
            id=2,
            job_run_id=1,
            created_at=ts,
            app="refiner",
            kind="refiner",
            status="ok",
            count=1,
            detail="Refiner (scheduled): processed=1 unchanged=0 dry_run_items=0 errors=0",
        ),
    ]
    refiners = [
        RefinerActivity(
            id=1,
            file_name="a.mkv",
            status="success",
            size_before_bytes=1,
            size_after_bytes=1,
            audio_tracks_before=1,
            audio_tracks_after=1,
            subtitle_tracks_before=0,
            subtitle_tracks_after=0,
            created_at=ts,
        )
    ]
    merged = merge_activity_feed(logs, refiners, "UTC", now, limit=50)
    scopes = {m.get("activity_tab_scope") for m in merged}
    assert ACTIVITY_TAB_SONARR in scopes
    assert ACTIVITY_TAB_REFINER in scopes
    assert len(merged) == 3
    for m in merged:
        assert m.get("activity_tab_scope") in (
            ACTIVITY_TAB_SONARR,
            ACTIVITY_TAB_REFINER,
        )


def test_refiner_file_row_tab_scope_is_refiner() -> None:
    ts = datetime(2026, 1, 1, 12, 0, 0)
    r = RefinerActivity(
        id=9,
        file_name="x.mkv",
        status="success",
        size_before_bytes=1,
        size_after_bytes=1,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=ts,
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 13, 0, 0))
    assert row["activity_tab_scope"] == ACTIVITY_TAB_REFINER


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


def test_activity_page_html_emits_tab_scope_attributes(monkeypatch: pytest.MonkeyPatch) -> None:
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
            session.add(
                ActivityLog(
                    app="refiner",
                    kind="refiner",
                    status="ok",
                    count=1,
                    detail="Refiner (scheduled): processed=1 unchanged=0 dry_run_items=0 errors=0",
                )
            )
            session.add(
                RefinerActivity(
                    file_name="clip.mkv",
                    media_title="Clip",
                    status="success",
                    size_before_bytes=100,
                    size_after_bytes=100,
                    audio_tracks_before=1,
                    audio_tracks_after=1,
                    subtitle_tracks_before=0,
                    subtitle_tracks_after=0,
                )
            )
            await session.commit()

    asyncio.run(seed())
    with _client(monkeypatch) as client:
        r = client.get("/activity")
    assert r.status_code == 200
    assert 'data-activity-tab-scope="sonarr"' in r.text
    assert 'data-activity-tab-scope="refiner"' in r.text
    assert r.text.count('data-activity-tab-scope="refiner"') >= 2


def test_activity_js_reapplies_pill_after_live_swap() -> None:
    from pathlib import Path

    js = Path("app/static/app.js").read_text(encoding="utf-8")
    assert "function currentActivityPillFilterFromDom" in js
    assert "hub.dataset.fetcherPillDelegation === \"1\"" in js or "hub.dataset.fetcherPillDelegation === '1'" in js
    assert "applyActivityPillFilter(currentActivityPillFilterFromDom" in js
    assert "function activityRowTabScope" in js
    assert "data-activity-tab-scope" in js or "activity-tab-scope" in js


def test_activity_js_live_swap_restores_refiner_compare_after_lucide() -> None:
    """Lucide runs inside polishActivityRowsAfterLiveSwap; open <details> must restore after icons mutate DOM."""
    from pathlib import Path

    js = Path("app/static/app.js").read_text(encoding="utf-8")
    i_act = js.find('querySelector("#activity-live-root")')
    assert i_act != -1
    block = js[i_act : i_act + 900]
    assert "polishActivityRowsAfterLiveSwap(actRoot" in block
    assert "restoreRefinerCompareDetailsOpen(actRoot" in block
    assert block.index("polishActivityRowsAfterLiveSwap(actRoot") < block.index(
        "restoreRefinerCompareDetailsOpen(actRoot"
    )
    i_dash = js.find('querySelector("#dashboard-activity-live-root")')
    assert i_dash != -1
    block_d = js[i_dash : i_dash + 700]
    assert block_d.index("polishActivityRowsAfterLiveSwap(dashRoot") < block_d.index(
        "restoreRefinerCompareDetailsOpen(dashRoot"
    )


def test_activity_js_click_delegation_leaves_refiner_compare_details_native() -> None:
    from pathlib import Path

    js = Path("app/static/app.js").read_text(encoding="utf-8")
    assert 'closest(".activity-refiner-compare-details")' in js
    i0 = js.index("function installActivityFeedClickDelegationOnce")
    i1 = js.index("function snapshotRefinerCompareDetailsOpen", i0)
    deleg = js[i0:i1]
    assert deleg.index('closest(".activity-refiner-compare-details")') < deleg.index(
        "closest(\".activity-detail-toggle\")"
    )


def test_filter_activity_display_for_tab_refiner_excludes_sonarr_rows() -> None:
    rows: list[dict] = [
        {"activity_tab_scope": ACTIVITY_TAB_SONARR, "primary_label": "TV search"},
        {"activity_tab_scope": ACTIVITY_TAB_REFINER, "primary_label": "Refiner file"},
    ]
    out = filter_activity_display_for_tab(rows, ACTIVITY_TAB_REFINER)
    assert len(out) == 1
    assert out[0]["primary_label"] == "Refiner file"


def test_activity_js_live_poll_uses_activity_feed_poll_url() -> None:
    from pathlib import Path

    js = Path("app/static/app.js").read_text(encoding="utf-8")
    assert "function activityFeedPollUrl" in js
    assert '\n            activityFeedPollUrl(),\n            () => {' in js


def test_activity_page_refiner_query_excludes_fetcher_search_rows_in_html(monkeypatch: pytest.MonkeyPatch) -> None:
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
                    detail="TV search activity must not appear on Refiner tab",
                )
            )
            session.add(
                RefinerActivity(
                    file_name="only.mkv",
                    media_title="Only",
                    status="success",
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
    with _client(monkeypatch) as client:
        r = client.get("/activity?app=refiner")
    assert r.status_code == 200
    assert "TV search activity must not appear on Refiner tab" not in r.text
    assert 'data-activity-app="sonarr"' not in r.text
    assert 'data-activity-tab-scope="refiner"' in r.text
