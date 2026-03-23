"""Contract tests for ``_paginate_wanted_for_search`` (page walk, totals, limits, ordering, cooldown).

Performance overlap/prefetch must not land without updating these assertions.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Literal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import ArrActionLog, Base
from app.service_logic import (
    _PAGINATE_WANTED_FOR_SEARCH_MAX_PAGES,
    _paginate_wanted_for_search,
)


async def _session_factory() -> tuple[async_sessionmaker[AsyncSession], object]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return async_sessionmaker(engine, expire_on_commit=False), engine


@pytest.fixture
def paginate_session():
    async def _setup():
        return await _session_factory()

    factory, engine = asyncio.run(_setup())
    yield factory, engine
    asyncio.run(engine.dispose())


def _make_paging_client(
    *,
    kind: Literal["missing", "cutoff"],
    pages: dict[int, dict[str, Any]],
) -> Any:
    """Return a fake ArrClient; ``pages`` maps 1-based page -> API payload."""

    class _Fake:
        def __init__(self) -> None:
            self.pages_requested: list[tuple[int, int]] = []

        async def wanted_missing(self, *, page: int, page_size: int) -> dict[str, Any]:
            self.pages_requested.append((page, page_size))
            if kind != "missing":
                raise AssertionError("wanted_missing called for cutoff kind")
            return dict(pages.get(page, {"totalRecords": 0, "records": []}))

        async def wanted_cutoff_unmet(self, *, page: int, page_size: int) -> dict[str, Any]:
            self.pages_requested.append((page, page_size))
            if kind != "cutoff":
                raise AssertionError("wanted_cutoff_unmet called for missing kind")
            return dict(pages.get(page, {"totalRecords": 0, "records": []}))

    return _Fake()


def test_page_walk_order_three_pages(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 1, 12, 0, 0)
    pages = {
        1: {"totalRecords": 300, "records": [{"episodeId": 10 + i} for i in range(3)]},
        2: {"totalRecords": 999, "records": [{"episodeId": 20 + i} for i in range(3)]},
        3: {"totalRecords": 999, "records": [{"episodeId": 30 + i} for i in range(3)]},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, recs, total = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=7,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids, recs, total

    ids, recs, total = asyncio.run(run())
    assert [p for p, _ in client.pages_requested] == [1, 2, 3]
    assert ids == [10, 11, 12, 20, 21, 22, 30]
    assert [r["episodeId"] for r in recs] == ids
    assert total == 300


def test_total_records_taken_only_from_page_1(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 2, 8, 0, 0)
    pages = {
        1: {"totalRecords": 42, "records": [{"episodeId": 1}]},
        2: {"totalRecords": 99999, "records": [{"episodeId": 2}]},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            _, _, total = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=2,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return total

    assert asyncio.run(run()) == 42


def test_early_exit_empty_records_page_1(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 3, 9, 0, 0)
    pages = {1: {"totalRecords": 77, "records": []}}
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, recs, total = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=5,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids, recs, total

    ids, recs, total = asyncio.run(run())
    # limit=5 → page_size = min(100, max(50, limit)) == 50
    assert client.pages_requested == [(1, 50)]
    assert ids == []
    assert recs == []
    assert total == 77


def test_early_exit_empty_records_after_nonempty_page(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 4, 10, 0, 0)
    pages = {
        1: {"totalRecords": 10, "records": [{"episodeId": 1}]},
        2: {"totalRecords": 10, "records": []},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, _, _ = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=10,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids

    assert asyncio.run(run()) == [1]
    assert [p for p, _ in client.pages_requested] == [1, 2]


def test_early_exit_when_limit_reached_stops_extra_pages(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 5, 11, 0, 0)
    pages = {
        1: {"totalRecords": 1000, "records": [{"episodeId": i} for i in range(1, 51)]},
        2: {"totalRecords": 1000, "records": [{"episodeId": i} for i in range(51, 101)]},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, recs, _ = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=5,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids, recs

    ids, recs = asyncio.run(run())
    assert ids == [1, 2, 3, 4, 5]
    assert [r["episodeId"] for r in recs] == ids
    assert client.pages_requested[0][0] == 1
    assert len(client.pages_requested) == 1


def test_max_pages_cap(monkeypatch: pytest.MonkeyPatch, paginate_session) -> None:
    assert _PAGINATE_WANTED_FOR_SEARCH_MAX_PAGES == 250
    monkeypatch.setattr(
        "app.service_logic._PAGINATE_WANTED_FOR_SEARCH_MAX_PAGES",
        3,
        raising=True,
    )
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 6, 12, 0, 0)
    pages = {
        1: {"totalRecords": 1000, "records": [{"episodeId": i} for i in range(1, 11)]},
        2: {"totalRecords": 1000, "records": [{"episodeId": i} for i in range(11, 21)]},
        3: {"totalRecords": 1000, "records": [{"episodeId": i} for i in range(21, 31)]},
        4: {"totalRecords": 1000, "records": [{"episodeId": i} for i in range(31, 41)]},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, _, _ = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=100,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids

    ids = asyncio.run(run())
    assert [p for p, _ in client.pages_requested] == [1, 2, 3]
    assert ids == list(range(1, 31))
    assert len(ids) == 30


def test_duplicate_episode_id_across_pages_first_wins_in_output(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 7, 13, 0, 0)
    pages = {
        1: {"totalRecords": 5, "records": [{"episodeId": 1}, {"episodeId": 2}]},
        2: {"totalRecords": 5, "records": [{"episodeId": 2}, {"episodeId": 3}]},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, _, _ = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=10,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids

    assert asyncio.run(run()) == [1, 2, 3]


def test_page_all_duplicates_advances_and_continues(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 8, 14, 0, 0)
    pages = {
        1: {"totalRecords": 5, "records": [{"episodeId": 1}]},
        2: {"totalRecords": 5, "records": [{"episodeId": 1}]},
        3: {"totalRecords": 5, "records": [{"episodeId": 2}]},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, _, _ = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=2,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids

    assert asyncio.run(run()) == [1, 2]
    assert [p for p, _ in client.pages_requested] == [1, 2, 3]


def test_cooldown_filters_some_ids_max_apply_fetches_next_page(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 9, 15, 0, 0)

    async def seed():
        async with SessionMaker() as session:
            session.add(
                ArrActionLog(
                    created_at=now - timedelta(minutes=5),
                    app="sonarr",
                    action="missing",
                    item_type="episode",
                    item_id=10,
                )
            )
            await session.commit()

    asyncio.run(seed())

    pages = {
        1: {"totalRecords": 50, "records": [{"episodeId": 10}, {"episodeId": 11}]},
        2: {"totalRecords": 50, "records": [{"episodeId": 12}, {"episodeId": 13}]},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, recs, total = await _paginate_wanted_for_search(
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
            return ids, recs, total

    ids, recs, total = asyncio.run(run())
    assert 10 not in ids
    assert ids == [11, 12, 13]
    assert [r["episodeId"] for r in recs] == ids
    assert total == 50
    assert [p for p, _ in client.pages_requested] == [1, 2]


def test_radarr_wanted_cutoff_movieId_shape(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 10, 16, 0, 0)
    pages = {
        1: {"totalRecords": 2, "records": [{"movieId": 9001, "title": "A"}, {"movieId": 9002}]},
        2: {"totalRecords": 2, "records": []},
    }
    client = _make_paging_client(kind="cutoff", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, recs, total = await _paginate_wanted_for_search(
                client,
                session,
                kind="cutoff",
                id_keys=("movieId", "id"),
                item_type="movie",
                app="radarr",
                action="upgrade",
                limit=5,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids, recs, total

    ids, recs, total = asyncio.run(run())
    assert [p for p, _ in client.pages_requested] == [1, 2]
    assert ids == [9001, 9002]
    assert recs[0]["title"] == "A"
    assert total == 2


def test_sonarr_id_key_fallback_when_episodeId_missing(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 11, 17, 0, 0)
    pages = {
        1: {
            "totalRecords": 1,
            "records": [{"id": 55, "title": "edge"}],
        },
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            ids, recs, total = await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=5,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()
            return ids, recs, total

    ids, recs, total = asyncio.run(run())
    assert ids == [55]
    assert recs[0]["id"] == 55
    assert total == 1


def test_arr_action_log_written_for_allowed_ids_only(paginate_session) -> None:
    SessionMaker, _ = paginate_session
    now = datetime(2025, 6, 12, 18, 0, 0)
    pages = {
        1: {"totalRecords": 3, "records": [{"episodeId": 1}, {"episodeId": 2}]},
    }
    client = _make_paging_client(kind="missing", pages=pages)

    async def run():
        async with SessionMaker() as session:
            await _paginate_wanted_for_search(
                client,
                session,
                kind="missing",
                id_keys=("episodeId", "id"),
                item_type="episode",
                app="sonarr",
                action="missing",
                limit=2,
                cooldown_minutes=60,
                now=now,
            )
            await session.commit()

    asyncio.run(run())

    async def count():
        async with SessionMaker() as session:
            q = await session.execute(select(func.count()).select_from(ArrActionLog))
            return int(q.scalar_one())

    assert asyncio.run(count()) == 2
