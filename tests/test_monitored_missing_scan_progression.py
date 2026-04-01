"""Monitored missing scan + cooldown: full-library selection progresses beyond narrow ``/wanted/missing``."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import ArrActionLog, Base
from app.service_logic import radarr_select_monitored_missing_with_cooldown, sonarr_select_monitored_missing_with_cooldown


async def _session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return async_sessionmaker(engine, expire_on_commit=False), engine


@pytest.fixture
def scan_session():
    factory, engine = asyncio.run(_session_factory())
    yield factory
    asyncio.run(engine.dispose())


def test_sonarr_scan_skips_cooled_ids_and_advances(scan_session) -> None:
    """Wider pool: first batch ids on cooldown, next run takes the following eligible ids (same stable order)."""
    SessionMaker = scan_session
    now = datetime(2026, 3, 25, 10, 0, 0)

    class _Fake:
        async def series(self):
            return [{"id": 1}]

        async def episodes_for_series(self, *, series_id: int):
            return [
                {"id": 10 + i, "monitored": True, "hasFile": False, "seriesId": 1}
                for i in range(8)
            ]

    async def run_first():
        async with SessionMaker() as session:
            session.add(
                ArrActionLog(
                    created_at=now - timedelta(minutes=30),
                    app="sonarr",
                    action="missing",
                    item_type="episode",
                    item_id=10,
                )
            )
            session.add(
                ArrActionLog(
                    created_at=now - timedelta(minutes=30),
                    app="sonarr",
                    action="missing",
                    item_type="episode",
                    item_id=11,
                )
            )
            await session.commit()

        async with SessionMaker() as session:
            ids, recs, total = await sonarr_select_monitored_missing_with_cooldown(
                _Fake(),
                session,
                limit=3,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids, recs, total

    ids1, _, total1 = asyncio.run(run_first())
    assert total1 == 8
    assert ids1 == [12, 13, 14]

    async def run_second():
        # 30 minutes later: first-run dispatch (12–14) plus seeded (10–11) remain inside a 60-minute window.
        async with SessionMaker() as session:
            ids, _, total = await sonarr_select_monitored_missing_with_cooldown(
                _Fake(),
                session,
                limit=3,
                cooldown_minutes=60,
                now=now + timedelta(minutes=30),
            )
            await session.commit()
            return ids, total

    ids2, total2 = asyncio.run(run_second())
    assert total2 == 8
    assert ids2 == [15, 16, 17]


def test_radarr_scan_skips_cooled_ids_and_advances(scan_session) -> None:
    SessionMaker = scan_session
    now = datetime(2026, 3, 25, 11, 0, 0)

    class _Fake:
        async def movies(self):
            return [{"id": 100 + i, "title": f"M{i}", "monitored": True, "hasFile": False} for i in range(6)]

    async def seed():
        async with SessionMaker() as session:
            session.add(
                ArrActionLog(
                    created_at=now - timedelta(minutes=10),
                    app="radarr",
                    action="missing",
                    item_type="movie",
                    item_id=100,
                )
            )
            await session.commit()

    asyncio.run(seed())

    async def run_once():
        async with SessionMaker() as session:
            ids, recs, total = await radarr_select_monitored_missing_with_cooldown(
                _Fake(),
                session,
                limit=4,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids, recs, total

    ids, recs, total = asyncio.run(run_once())
    assert total == 6
    assert 100 not in ids
    assert ids == [101, 102, 103, 104]
    assert len(recs) == 4

    async def count_logs():
        async with SessionMaker() as session:
            q = await session.execute(select(ArrActionLog).where(ArrActionLog.app == "radarr"))
            return len(q.scalars().all())

    assert asyncio.run(count_logs()) == 5
