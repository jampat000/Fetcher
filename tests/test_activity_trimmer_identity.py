"""Trimmer activity app token (trimmer), migration from legacy activity_log rows, dashboard overview layout."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.dashboard_service import trimmer_connection_status_display
from app.migrations import migrate
from app.main import app
from app.models import ActivityLog, AppSettings, AppSnapshot, Base


def test_dashboard_overview_row_order_and_no_system_section(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    with TestClient(app) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert "dashboard-connection-card" not in r.text
    assert "dashboard-system-section" not in r.text
    assert "dash-trimmer-connection-type" not in r.text
    assert "dash-trimmer-connection-status" not in r.text
    assert "Trimmer media server slot" not in r.text
    i0 = r.text.index('id="dashboard-overview"')
    tail = r.text[i0:]
    i_arr = tail.index("dashboard-overview-arr-row")
    i_tools = tail.index("dashboard-overview-tools-row")
    i_ref = tail.index("dashboard-refiner-tool-card")
    i_trim = tail.index("dashboard-trimmer-tool-card")
    assert i_arr < i_tools < i_ref < i_trim


def test_dashboard_automation_and_overview_use_summary_tile_pattern(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    with TestClient(app) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert "dash-summary-tile" in r.text
    assert "dash-summary-meta-row" in r.text
    assert "Latest event" not in r.text
    assert "data-automation-card=\"refiner\"" in r.text
    i_son = r.text.index("data-automation-card=\"sonarr\"")
    i_rad = r.text.index("data-automation-card=\"radarr\"")
    i_ref = r.text.index("data-automation-card=\"refiner\"")
    i_trim = r.text.index("data-automation-card=\"trimmer\"")
    assert i_son < i_rad < i_ref < i_trim
    assert "Refiner is off. Enable it in settings to use dry run or live processing." in r.text


def test_dashboard_trimmer_tool_shows_media_note_when_connection_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default pytest DB has no media URL/key → system status Not configured → tool card shows one-line note."""
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    with TestClient(app) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert "Requires a configured media server connection" in r.text


def test_dashboard_trimmer_tool_card_avoids_emby_product_name(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    with TestClient(app) as client:
        r = client.get("/")
    assert r.status_code == 200
    i0 = r.text.index("dashboard-trimmer-tool-card")
    i1 = r.text.index('class="section dashboard-activity-section"')
    chunk = r.text[i0:i1]
    assert "Emby" not in chunk


def test_migrate_035_rewrites_activity_log_emby_to_trimmer(tmp_path: Path) -> None:
    db_path = tmp_path / "trimmer_mig.sqlite"
    url = f"sqlite+aiosqlite:///{db_path.as_posix().replace(chr(92), '/')}"
    engine = create_async_engine(url)

    async def _run() -> None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            session.add(
                ActivityLog(
                    app="emby",
                    kind="trimmed",
                    status="ok",
                    count=1,
                    detail="",
                )
            )
            await session.commit()
        await migrate(engine)
        async with factory() as session:
            rows = (await session.execute(select(ActivityLog))).scalars().all()
            assert len(rows) == 1
            assert rows[0].app == "trimmer"
        await engine.dispose()

    import asyncio

    asyncio.run(_run())


def test_trimmer_connection_status_display_variants() -> None:
    empty = AppSettings()
    assert trimmer_connection_status_display(empty, None) == ("Trimmer", "Not configured")

    configured = AppSettings()
    configured.emby_url = "http://localhost:8096"
    configured.emby_api_key = "secret"
    assert trimmer_connection_status_display(configured, None) == ("Trimmer", "Configured")

    snap_ok = AppSnapshot(
        app="emby",
        ok=True,
        status_message="OK",
        missing_total=0,
        cutoff_unmet_total=0,
    )
    assert trimmer_connection_status_display(configured, snap_ok) == ("Trimmer", "Connected")

    snap_bad = AppSnapshot(
        app="emby",
        ok=False,
        status_message="fail",
        missing_total=0,
        cutoff_unmet_total=0,
    )
    assert trimmer_connection_status_display(configured, snap_bad) == ("Trimmer", "Not connected")
