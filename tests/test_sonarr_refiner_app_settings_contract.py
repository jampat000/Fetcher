"""Sonarr Refiner ``app_settings`` repair contract matches ORM and SQLite default backfill."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.migrations import migrate
from app.models import AppSettings, Base
from app.sonarr_refiner_app_settings_contract import (
    SONARR_REFINER_APP_SETTINGS_EXPECTED_SQLITE_VALUES,
    SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS,
)


def _sqlite_supports_drop_column() -> bool:
    return sqlite3.sqlite_version_info >= (3, 35, 0)


def _strip_sonarr_refiner_columns(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    try:
        for col_name, _ in SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS:
            con.execute(f"ALTER TABLE app_settings DROP COLUMN {col_name}")
        con.commit()
    finally:
        con.close()


def test_sonarr_refiner_sqlite_specs_match_orm_columns() -> None:
    orm = {c.key for c in AppSettings.__table__.columns if c.key.startswith("sonarr_refiner_")}
    spec = {name for name, _ in SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS}
    assert orm == spec, (orm - spec, spec - orm)


def test_sonarr_refiner_expected_sqlite_values_cover_all_specs() -> None:
    spec_names = {n for n, _ in SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS}
    assert set(SONARR_REFINER_APP_SETTINGS_EXPECTED_SQLITE_VALUES.keys()) == spec_names


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_migrate_backfills_exact_sonarr_refiner_defaults(tmp_path: Path) -> None:
    db = tmp_path / "sonarr_refiner_defaults.sqlite"

    async def seed() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await migrate(engine)
        finally:
            await engine.dispose()

    async def add_row() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
            async with factory() as session:
                session.add(AppSettings())
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(seed())
    asyncio.run(add_row())
    _strip_sonarr_refiner_columns(db)

    async def repair() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            await migrate(engine)
        finally:
            await engine.dispose()

    asyncio.run(repair())

    con = sqlite3.connect(db)
    try:
        cur = con.execute(
            "SELECT "
            + ", ".join(name for name, _ in SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS)
            + " FROM app_settings ORDER BY id ASC LIMIT 1"
        )
        row = cur.fetchone()
        assert row is not None
        names = [name for name, _ in SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS]
        got = dict(zip(names, row, strict=True))
    finally:
        con.close()

    for col, expected in SONARR_REFINER_APP_SETTINGS_EXPECTED_SQLITE_VALUES.items():
        assert got[col] == expected, f"{col!r}: expected {expected!r}, got {got[col]!r}"


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_migrate_sonarr_refiner_defaults_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "sonarr_refiner_idempotent_defaults.sqlite"

    async def seed() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await migrate(engine)
        finally:
            await engine.dispose()

    async def add_row() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
            async with factory() as session:
                session.add(AppSettings())
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(seed())
    asyncio.run(add_row())
    _strip_sonarr_refiner_columns(db)

    async def twice() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            await migrate(engine)
            await migrate(engine)
            async with engine.connect() as conn:
                cols = ", ".join(n for n, _ in SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS)
                res = await conn.execute(text(f"SELECT {cols} FROM app_settings ORDER BY id ASC LIMIT 1"))
                row = res.fetchone()
                assert row is not None
                names = [n for n, _ in SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS]
                got = dict(zip(names, row, strict=True))
            for col, expected in SONARR_REFINER_APP_SETTINGS_EXPECTED_SQLITE_VALUES.items():
                assert got[col] == expected
        finally:
            await engine.dispose()

    asyncio.run(twice())
