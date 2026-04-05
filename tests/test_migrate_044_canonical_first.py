"""Regression: migration 044 must not copy stale legacy intervals over real canonical values."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.migrations import migrate
from app.models import AppSettings, Base


def _sqlite_supports_drop_column() -> bool:
    return sqlite3.sqlite_version_info >= (3, 35, 0)


async def _ensure_row(engine) -> None:
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        r = await s.execute(text("SELECT 1 FROM app_settings LIMIT 1"))
        if r.first() is None:
            s.add(AppSettings())
            await s.commit()


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_044_backfills_when_canonical_column_newly_added(tmp_path: Path) -> None:
    db = tmp_path / "legacy_only.sqlite"

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await _ensure_row(engine)
            # Strip one canonical column; keep legacy-style source for 044 to backfill.
            def _raw() -> None:
                con = sqlite3.connect(str(db))
                try:
                    con.execute(
                        "ALTER TABLE app_settings DROP COLUMN sonarr_search_interval_minutes"
                    )
                    con.execute(
                        "ALTER TABLE app_settings ADD COLUMN sonarr_interval_minutes "
                        "INTEGER NOT NULL DEFAULT 0"
                    )
                    con.execute(
                        "UPDATE app_settings SET sonarr_interval_minutes = 42 WHERE id = 1"
                    )
                    con.commit()
                finally:
                    con.close()

            _raw()
            await migrate(engine)
            async with engine.connect() as conn:
                cols = {
                    str(x[0])
                    for x in (
                        await conn.execute(
                            text("SELECT name FROM pragma_table_info('app_settings')")
                        )
                    ).fetchall()
                }
                assert "sonarr_search_interval_minutes" in cols
                assert "sonarr_interval_minutes" not in cols, "045 should drop legacy after 044"
                r = await conn.execute(
                    text(
                        "SELECT sonarr_search_interval_minutes FROM app_settings WHERE id = 1"
                    )
                )
                assert int(r.scalar()) == 42
        finally:
            await engine.dispose()

    asyncio.run(run())


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_044_preserves_canonical_when_legacy_reappears_with_stale_value(tmp_path: Path) -> None:
    """v4.0.8-style: saves updated canonical only; legacy can be wrong — must not clobber."""
    db = tmp_path / "mixed_stale_legacy.sqlite"

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await _ensure_row(engine)
            await migrate(engine)

            def _inject_stale_legacy() -> None:
                con = sqlite3.connect(str(db))
                try:
                    con.execute(
                        "ALTER TABLE app_settings ADD COLUMN emby_interval_minutes "
                        "INTEGER NOT NULL DEFAULT 60"
                    )
                    con.execute(
                        "UPDATE app_settings SET trimmer_interval_minutes = 120, "
                        "emby_interval_minutes = 50 WHERE id = 1"
                    )
                    con.commit()
                finally:
                    con.close()

            _inject_stale_legacy()
            await migrate(engine)

            async with engine.connect() as conn:
                cols = {
                    str(x[0])
                    for x in (
                        await conn.execute(
                            text("SELECT name FROM pragma_table_info('app_settings')")
                        )
                    ).fetchall()
                }
                assert "emby_interval_minutes" not in cols
                r = await conn.execute(
                    text(
                        "SELECT trimmer_interval_minutes FROM app_settings WHERE id = 1"
                    )
                )
                assert int(r.scalar()) == 120

            await migrate(engine)
            async with engine.connect() as conn:
                r2 = await conn.execute(
                    text(
                        "SELECT trimmer_interval_minutes FROM app_settings WHERE id = 1"
                    )
                )
                assert int(r2.scalar()) == 120
        finally:
            await engine.dispose()

    asyncio.run(run())


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_fresh_db_schema_42_no_legacy_interval_columns(tmp_path: Path) -> None:
    db = tmp_path / "fresh.sqlite"

    async def run() -> None:
        from app.schema_version import CURRENT_SCHEMA_VERSION

        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await _ensure_row(engine)
            await migrate(engine)
            legacy = (
                "sonarr_interval_minutes",
                "radarr_interval_minutes",
                "emby_interval_minutes",
                "failed_import_cleanup_interval_minutes",
                "refiner_interval_seconds",
                "sonarr_refiner_interval_seconds",
            )
            async with engine.connect() as conn:
                cols = {
                    str(x[0])
                    for x in (
                        await conn.execute(
                            text("SELECT name FROM pragma_table_info('app_settings')")
                        )
                    ).fetchall()
                }
                for c in legacy:
                    assert c not in cols
                r = await conn.execute(
                    text("SELECT schema_version FROM app_settings LIMIT 1")
                )
                assert int(r.scalar()) == CURRENT_SCHEMA_VERSION
            await migrate(engine)
            async with engine.connect() as conn:
                cols2 = {
                    str(x[0])
                    for x in (
                        await conn.execute(
                            text("SELECT name FROM pragma_table_info('app_settings')")
                        )
                    ).fetchall()
                }
                for c in legacy:
                    assert c not in cols2
        finally:
            await engine.dispose()

    asyncio.run(run())
