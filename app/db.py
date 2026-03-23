from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path

from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.models import AppSettings, AppSnapshot

logger = logging.getLogger(__name__)

# aiosqlite: ``timeout`` is the SQLite busy-handler wait, in seconds (complements ``PRAGMA busy_timeout``).
_SQLITE_CONNECT_TIMEOUT_S = 10.0
# SQLite ``busy_timeout`` PRAGMA is in milliseconds.
_SQLITE_BUSY_TIMEOUT_MS = 10_000


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
    eng = create_async_engine(
        f"sqlite+aiosqlite:///{db_path().as_posix()}",
        future=True,
        connect_args={"timeout": _SQLITE_CONNECT_TIMEOUT_S},
    )
    _register_sqlite_pragmas(eng)
    return eng


def _register_sqlite_pragmas(engine: AsyncEngine) -> None:
    """Apply WAL + sane sync + busy wait on every new SQLite connection (scheduler + HTTP concurrency)."""

    @event.listens_for(engine.sync_engine, "connect")
    def _on_sqlite_connect(dbapi_connection: object, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS};")
            cursor.execute("PRAGMA journal_mode;")
            jm = cursor.fetchone()
            if jm and str(jm[0]).upper() != "WAL":
                logger.warning(
                    "SQLite journal_mode is %r (WAL unavailable on this path/volume). "
                    "Concurrent access may see more 'database is locked' errors.",
                    jm[0],
                )
        finally:
            cursor.close()


engine = create_engine()
SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Yield one :class:`AsyncSession` per request; always closed in ``finally`` (success or exception)."""
    session = SessionLocal()
    try:
        yield session
    finally:
        await session.close()


async def _get_or_create_settings(session: AsyncSession) -> AppSettings:
    row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
    if row:
        return row
    row = AppSettings()
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


async def fetch_latest_app_snapshots(session: AsyncSession) -> dict[str, AppSnapshot | None]:
    """Latest AppSnapshot row per app in one query (sonarr / radarr / emby)."""
    subq = (
        select(AppSnapshot.app, func.max(AppSnapshot.id).label("mx"))
        .where(AppSnapshot.app.in_(("sonarr", "radarr", "emby")))
        .group_by(AppSnapshot.app)
        .subquery()
    )
    rows = (
        (
            await session.execute(
                select(AppSnapshot).join(
                    subq,
                    (AppSnapshot.app == subq.c.app) & (AppSnapshot.id == subq.c.mx),
                )
            )
        )
        .scalars()
        .all()
    )
    out: dict[str, AppSnapshot | None] = {"sonarr": None, "radarr": None, "emby": None}
    for row in rows:
        if row.app in out:
            out[row.app] = row
    return out

