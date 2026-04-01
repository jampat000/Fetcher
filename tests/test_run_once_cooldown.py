"""Tests for Arr cooldown filtering and wanted-queue pagination (real SQLite session)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import ArrActionLog, Base
from app.service_logic import _filter_ids_by_cooldown, paginate_wanted_for_search


async def _session_factory() -> tuple[async_sessionmaker[AsyncSession], object]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return async_sessionmaker(engine, expire_on_commit=False), engine


@pytest.fixture
def cooldown_session():
    """In-memory DB with schema; disposes engine after test."""

    async def _setup():
        return await _session_factory()

    factory, engine = asyncio.run(_setup())
    yield factory, engine
    asyncio.run(engine.dispose())


def test_filter_ids_excludes_ids_inside_cooldown_window(cooldown_session) -> None:
    SessionMaker, _ = cooldown_session
    now = datetime(2025, 3, 1, 12, 0, 0)
    window_start = now - timedelta(minutes=60)

    async def run():
        async with SessionMaker() as session:
            session.add(
                ArrActionLog(
                    created_at=window_start + timedelta(minutes=10),
                    app="sonarr",
                    action="missing",
                    item_type="episode",
                    item_id=101,
                )
            )
            await session.commit()

        async with SessionMaker() as session:
            out = await _filter_ids_by_cooldown(
                session,
                app="sonarr",
                action="missing",
                item_type="episode",
                ids=[101, 102, 103],
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return out

    allowed = asyncio.run(run())
    assert allowed == [102, 103]


def test_filter_ids_includes_ids_outside_cooldown_window(cooldown_session) -> None:
    SessionMaker, _ = cooldown_session
    now = datetime(2025, 3, 1, 12, 0, 0)

    async def run():
        async with SessionMaker() as session:
            session.add(
                ArrActionLog(
                    created_at=now - timedelta(minutes=120),
                    app="radarr",
                    action="upgrade",
                    item_type="movie",
                    item_id=501,
                )
            )
            await session.commit()

        async with SessionMaker() as session:
            out = await _filter_ids_by_cooldown(
                session,
                app="radarr",
                action="missing",
                item_type="movie",
                ids=[501],
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return out

    allowed = asyncio.run(run())
    assert allowed == [501]


def test_filter_ids_max_apply_limits_returned_and_logged_rows(cooldown_session) -> None:
    SessionMaker, _ = cooldown_session
    now = datetime(2025, 3, 2, 9, 0, 0)

    async def run():
        async with SessionMaker() as session:
            out = await _filter_ids_by_cooldown(
                session,
                app="sonarr",
                action="missing",
                item_type="episode",
                ids=[1, 2, 3, 4],
                cooldown_minutes=60,
                now=now,
                max_apply=2,
            )
            await session.commit()
            return out

    allowed = asyncio.run(run())
    assert allowed == [1, 2]

    async def count_logs():
        async with SessionMaker() as session:
            q = await session.execute(select(func.count()).select_from(ArrActionLog))
            return int(q.scalar_one())

    assert asyncio.run(count_logs()) == 2


def test_paginate_stops_when_limit_reached(cooldown_session) -> None:
    SessionMaker, _ = cooldown_session
    now = datetime(2025, 4, 1, 10, 0, 0)

    class FakeArrClient:
        def __init__(self) -> None:
            self.pages_requested: list[tuple[int, int]] = []

        async def wanted_missing(self, *, page: int, page_size: int) -> dict:
            self.pages_requested.append((page, page_size))
            if page == 1:
                return {
                    "totalRecords": 500,
                    "records": [{"episodeId": 10 + i} for i in range(50)],
                }
            if page == 2:
                return {
                    "totalRecords": 500,
                    "records": [{"episodeId": 60 + i} for i in range(50)],
                }
            return {"totalRecords": 500, "records": []}

    async def run():
        client = FakeArrClient()
        async with SessionMaker() as session:
            ids, recs, total = await paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=3,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return client, ids, recs, total

    client, ids, recs, total = asyncio.run(run())
    assert ids == [10, 11, 12]
    assert len(recs) == 3
    assert total == 500
    assert client.pages_requested[0][0] == 1
    assert len(client.pages_requested) == 1


def test_paginate_respects_cooldown_across_pages(cooldown_session) -> None:
    SessionMaker, _ = cooldown_session
    now = datetime(2025, 4, 10, 8, 0, 0)

    class FakeArrClient:
        def __init__(self) -> None:
            self.pages_requested: list[int] = []

        async def wanted_missing(self, *, page: int, page_size: int) -> dict:
            self.pages_requested.append(page)
            if page == 1:
                return {"totalRecords": 200, "records": [{"episodeId": 1}, {"episodeId": 2}]}
            if page == 2:
                return {"totalRecords": 200, "records": [{"episodeId": 3}, {"episodeId": 4}]}
            return {"totalRecords": 200, "records": []}

    async def run():
        async with SessionMaker() as session:
            session.add(
                ArrActionLog(
                    created_at=now - timedelta(minutes=5),
                    app="sonarr",
                    action="missing",
                    item_type="episode",
                    item_id=2,
                )
            )
            await session.commit()

        client = FakeArrClient()
        async with SessionMaker() as session:
            ids, recs, total = await paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=3,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return client, ids, recs, total

    client, ids, recs, total = asyncio.run(run())
    assert 2 not in ids
    assert len(ids) == 3
    assert set(ids) == {1, 3, 4}
    assert total == 200
    assert 2 in client.pages_requested
