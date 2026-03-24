from __future__ import annotations

import logging
import os
import shutil
import sys
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


def _windows_program_data_fetcher_dir() -> Path:
    return Path(os.environ.get("PROGRAMDATA", r"C:\ProgramData")) / "Fetcher"


def _legacy_windows_sqlite_path() -> Path:
    """Pre-canonical default: ``%USERPROFILE%\\AppData\\Local\\Fetcher\\fetcher.db`` (e.g. LocalSystem profile)."""
    return Path.home() / "AppData" / "Local" / "Fetcher" / "fetcher.db"


def _migrate_legacy_sqlite_if_needed(canonical_db: Path, legacy_db: Path) -> None:
    """Copy legacy DB into the canonical folder once if canonical file is missing. Never overwrites canonical."""
    if canonical_db.is_file():
        return
    if not legacy_db.is_file():
        return
    canonical_db.parent.mkdir(parents=True, exist_ok=True)
    legacy_dir = legacy_db.parent
    try:
        for name in ("fetcher.db", "fetcher.db-wal", "fetcher.db-shm"):
            src = legacy_dir / name
            if src.is_file():
                shutil.copy2(src, canonical_db.parent / name)
    except OSError as e:
        logger.exception(
            "Could not migrate SQLite from %s to %s (%s). Stop the Fetcher service so files are not locked, then restart.",
            legacy_db,
            canonical_db,
            e,
        )
        raise RuntimeError(
            f"SQLite migration failed ({legacy_db} -> {canonical_db}). "
            "Stop the Fetcher service and retry."
        ) from e
    logger.info(
        "Migrated SQLite from legacy path %s to canonical path %s (one-time copy; runtime uses canonical only).",
        legacy_db,
        canonical_db,
    )


def _ensure_windows_frozen_sqlite_migrated() -> None:
    """One-time file copy before engine creation; skipped when dev path or FETCHER_DATA_DIR is set."""
    if sys.platform != "win32" or not getattr(sys, "frozen", False):
        return
    if (os.environ.get("FETCHER_DEV_DB_PATH") or "").strip():
        return
    if (os.environ.get("FETCHER_DATA_DIR") or "").strip():
        return
    canonical_db = _windows_program_data_fetcher_dir() / "fetcher.db"
    _migrate_legacy_sqlite_if_needed(canonical_db, _legacy_windows_sqlite_path())


def default_data_dir() -> Path:
    if sys.platform == "win32" and getattr(sys, "frozen", False):
        base = _windows_program_data_fetcher_dir()
        base.mkdir(parents=True, exist_ok=True)
        return base
    base = Path.home() / "AppData" / "Local" / "Fetcher"
    base.mkdir(parents=True, exist_ok=True)
    return base


def db_path() -> Path:
    """Resolve SQLite file path.

    Precedence:

    1. ``FETCHER_DEV_DB_PATH`` — full path to the database file (dev/tests/Docker).
    2. ``FETCHER_DATA_DIR`` — directory containing ``fetcher.db`` (recommended for fixed production layout).
    3. ``default_data_dir()`` / ``fetcher.db`` — frozen Windows builds use ``%ProgramData%\\Fetcher``
       after a one-time migration from the legacy profile path if needed (see module startup).
    """
    override = (os.environ.get("FETCHER_DEV_DB_PATH") or "").strip()
    if override:
        p = Path(override).expanduser()
        try:
            p = p.resolve()
        except OSError:
            p = Path(override).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    data_dir = (os.environ.get("FETCHER_DATA_DIR") or "").strip()
    if data_dir:
        root = Path(data_dir).expanduser()
        try:
            root = root.resolve()
        except OSError:
            root = Path(data_dir).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        return root / "fetcher.db"
    return default_data_dir() / "fetcher.db"


_ensure_windows_frozen_sqlite_migrated()


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

