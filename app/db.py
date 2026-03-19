from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def default_data_dir() -> Path:
    base = Path.home() / "AppData" / "Local" / "Grabby"
    base.mkdir(parents=True, exist_ok=True)
    return base


def db_path() -> Path:
    return default_data_dir() / "app.db"


def create_engine() -> AsyncEngine:
    return create_async_engine(f"sqlite+aiosqlite:///{db_path().as_posix()}", future=True)


engine = create_engine()
SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session

