"""Tests for automatic DB pruning (log / snapshot tables)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.migrations import migrate
from app.models import ActivityLog, AppSettings, AppSnapshot, ArrActionLog, Base, JobRunLog
from app.service_logic import prune_old_records


async def _session_factory() -> tuple[async_sessionmaker[AsyncSession], object]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await migrate(engine)
    return async_sessionmaker(engine, expire_on_commit=False), engine


@pytest.fixture
def prune_session():
    async def _setup():
        return await _session_factory()

    factory, engine = asyncio.run(_setup())
    yield factory, engine
    asyncio.run(engine.dispose())


ANCHOR = datetime(2026, 6, 15, 12, 0, 0)


async def _seed_settings(
    session: AsyncSession,
    *,
    sonarr_retry_delay_minutes: int = 1440,
    radarr_retry_delay_minutes: int = 1440,
    log_retention_days: int = 90,
) -> None:
    row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
    if row is None:
        row = AppSettings()
        session.add(row)
    row.sonarr_retry_delay_minutes = sonarr_retry_delay_minutes
    row.radarr_retry_delay_minutes = radarr_retry_delay_minutes
    row.log_retention_days = log_retention_days
    await session.commit()


def test_activity_job_snapshot_older_than_retention_deleted(prune_session, monkeypatch: pytest.MonkeyPatch) -> None:
    factory, _ = prune_session
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: ANCHOR)

    async def run():
        async with factory() as session:
            await _seed_settings(session, log_retention_days=90)
            old = ANCHOR - timedelta(days=100)
            recent = ANCHOR - timedelta(days=10)
            session.add(
                ActivityLog(
                    created_at=old,
                    app="sonarr",
                    kind="missing",
                    status="ok",
                    count=1,
                    detail="",
                )
            )
            session.add(
                ActivityLog(
                    created_at=recent,
                    app="radarr",
                    kind="missing",
                    status="ok",
                    count=2,
                    detail="",
                )
            )
            session.add(JobRunLog(started_at=old, ok=True, message="old"))
            session.add(JobRunLog(started_at=recent, ok=True, message="new"))
            session.add(
                AppSnapshot(
                    created_at=old,
                    app="sonarr",
                    ok=True,
                    status_message="",
                    missing_total=0,
                    cutoff_unmet_total=0,
                )
            )
            session.add(
                AppSnapshot(
                    created_at=recent,
                    app="radarr",
                    ok=True,
                    status_message="",
                    missing_total=0,
                    cutoff_unmet_total=0,
                )
            )
            await session.commit()

        async with factory() as session:
            await prune_old_records(session)
            await session.commit()

        async with factory() as session:
            ac = (await session.execute(select(func.count()).select_from(ActivityLog))).scalar_one()
            jc = (await session.execute(select(func.count()).select_from(JobRunLog))).scalar_one()
            sc = (await session.execute(select(func.count()).select_from(AppSnapshot))).scalar_one()
            kept_act = (
                await session.execute(select(ActivityLog).where(ActivityLog.created_at == recent))
            ).scalars().first()
            return int(ac), int(jc), int(sc), kept_act

    ac, jc, sc, kept = asyncio.run(run())
    assert ac == 1
    assert jc == 1
    assert sc == 1
    assert kept is not None
    assert kept.count == 2


def test_arr_action_log_older_than_retry_delay_doubled_deleted(prune_session, monkeypatch: pytest.MonkeyPatch) -> None:
    factory, _ = prune_session
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: ANCHOR)

    async def run():
        async with factory() as session:
            await _seed_settings(session, sonarr_retry_delay_minutes=60, radarr_retry_delay_minutes=60)
            # window = 120 minutes
            stale = ANCHOR - timedelta(minutes=121)
            fresh = ANCHOR - timedelta(minutes=30)
            session.add(
                ArrActionLog(
                    created_at=stale,
                    app="sonarr",
                    action="missing",
                    item_type="episode",
                    item_id=1,
                )
            )
            session.add(
                ArrActionLog(
                    created_at=fresh,
                    app="sonarr",
                    action="missing",
                    item_type="episode",
                    item_id=2,
                )
            )
            await session.commit()

        async with factory() as session:
            await prune_old_records(session)
            await session.commit()

        async with factory() as session:
            n = (await session.execute(select(func.count()).select_from(ArrActionLog))).scalar_one()
            row2 = (
                await session.execute(select(ArrActionLog).where(ArrActionLog.item_id == 2))
            ).scalars().first()
            return int(n), row2

    n, row2 = asyncio.run(run())
    assert n == 1
    assert row2 is not None


def test_arr_action_log_uses_max_retry_delay_window(prune_session, monkeypatch: pytest.MonkeyPatch) -> None:
    factory, _ = prune_session
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: ANCHOR)

    async def run():
        async with factory() as session:
            await _seed_settings(session, sonarr_retry_delay_minutes=1, radarr_retry_delay_minutes=120)
            # max retry delay is 120; prune window = 240 minutes.
            stale = ANCHOR - timedelta(minutes=241)
            fresh = ANCHOR - timedelta(minutes=100)
            session.add(
                ArrActionLog(
                    created_at=stale,
                    app="radarr",
                    action="upgrade",
                    item_type="movie",
                    item_id=9,
                )
            )
            session.add(
                ArrActionLog(
                    created_at=fresh,
                    app="radarr",
                    action="upgrade",
                    item_type="movie",
                    item_id=10,
                )
            )
            await session.commit()

        async with factory() as session:
            await prune_old_records(session)
            await session.commit()

        async with factory() as session:
            n = (await session.execute(select(func.count()).select_from(ArrActionLog))).scalar_one()
            return int(n)

    assert asyncio.run(run()) == 1


def test_prune_failure_does_not_raise(prune_session, monkeypatch: pytest.MonkeyPatch) -> None:
    factory, _ = prune_session
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: ANCHOR)

    async def run():
        async with factory() as session:
            await _seed_settings(session)
            orig = session.execute
            n = 0

            async def wrapped(*args, **kwargs):
                nonlocal n
                n += 1
                if n >= 2:
                    raise RuntimeError("simulated DB failure")
                return await orig(*args, **kwargs)

            session.execute = wrapped  # type: ignore[method-assign]
            await prune_old_records(session)

    asyncio.run(run())


def test_log_retention_days_below_minimum_uses_seven_days_for_prune(prune_session, monkeypatch: pytest.MonkeyPatch) -> None:
    factory, _ = prune_session
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: ANCHOR)

    async def run():
        async with factory() as session:
            await _seed_settings(session, log_retention_days=3)
            # Stored 3 but prune clamps to 7 — record 8 days old should be pruned
            session.add(
                ActivityLog(
                    created_at=ANCHOR - timedelta(days=8),
                    app="sonarr",
                    kind="missing",
                    status="ok",
                    count=1,
                    detail="",
                )
            )
            session.add(
                ActivityLog(
                    created_at=ANCHOR - timedelta(days=5),
                    app="sonarr",
                    kind="missing",
                    status="ok",
                    count=2,
                    detail="",
                )
            )
            await session.commit()

        async with factory() as session:
            await prune_old_records(session)
            await session.commit()

        async with factory() as session:
            n = (await session.execute(select(func.count()).select_from(ActivityLog))).scalar_one()
            kept = (
                await session.execute(select(ActivityLog).where(ActivityLog.count == 2))
            ).scalars().first()
            return int(n), kept

    n, kept = asyncio.run(run())
    assert n == 1
    assert kept is not None
