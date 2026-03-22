from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.models import AppSettings


def default_data_dir() -> Path:
    base = Path.home() / "AppData" / "Local" / "Fetcher"
    base.mkdir(parents=True, exist_ok=True)
    return base


def db_path() -> Path:
    """SQLite file path. Set ``FETCHER_DEV_DB_PATH`` before importing ``app.db`` for a separate dev database."""
    override = (os.environ.get("FETCHER_DEV_DB_PATH") or "").strip()
    if override:
        p = Path(override).expanduser()
        try:
            p = p.resolve()
        except OSError:
            p = Path(override).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    return default_data_dir() / "fetcher.db"


def create_engine() -> AsyncEngine:
    return create_async_engine(f"sqlite+aiosqlite:///{db_path().as_posix()}", future=True)


engine = create_engine()
SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


async def _get_or_create_settings(session: AsyncSession) -> AppSettings:
    row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
    if row:
        return row
    row = AppSettings()
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row

