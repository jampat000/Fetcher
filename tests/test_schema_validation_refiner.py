"""Startup Refiner schema enforcement (app_settings refiner_* columns)."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.migrations import _REFINER_APP_SETTINGS_SQLITE_SPECS, migrate
from app.models import Base
from app.schema_validation import (
    REQUIRED_REFINER_APP_SETTINGS_COLUMNS,
    validate_refiner_app_settings_schema,
)


def _sqlite_supports_drop_column() -> bool:
    return sqlite3.sqlite_version_info >= (3, 35, 0)


def _strip_refiner_columns_from_app_settings(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    try:
        for col_name, _ in _REFINER_APP_SETTINGS_SQLITE_SPECS:
            con.execute(f"ALTER TABLE app_settings DROP COLUMN {col_name}")
        con.commit()
    finally:
        con.close()


def test_validate_refiner_schema_raises_when_table_missing(tmp_path: Path) -> None:
    db = tmp_path / "empty.sqlite"
    sqlite3.connect(db).close()

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            with pytest.raises(RuntimeError, match="refiner|Refiner|database"):
                await validate_refiner_app_settings_schema(engine)
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_validate_refiner_schema_raises_when_columns_missing(tmp_path: Path) -> None:
    db = tmp_path / "incomplete.sqlite"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE app_settings (id INTEGER PRIMARY KEY, sonarr_url TEXT NOT NULL DEFAULT '')")
    con.commit()
    con.close()

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            with pytest.raises(RuntimeError) as exc_info:
                await validate_refiner_app_settings_schema(engine)
            msg = str(exc_info.value)
            assert "not supported" in msg.lower() or "requires" in msg.lower()
            for col in REQUIRED_REFINER_APP_SETTINGS_COLUMNS:
                assert col in msg
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_validate_refiner_schema_passes_when_columns_present(tmp_path: Path) -> None:
    db = tmp_path / "ok.sqlite"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE app_settings (id INTEGER PRIMARY KEY, "
        "refiner_enabled INTEGER NOT NULL DEFAULT 0, "
        "refiner_watched_folder TEXT NOT NULL DEFAULT '', "
        "refiner_output_folder TEXT NOT NULL DEFAULT '', "
        "refiner_work_folder TEXT NOT NULL DEFAULT '')"
    )
    con.commit()
    con.close()

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            await validate_refiner_app_settings_schema(engine)
        finally:
            await engine.dispose()

    asyncio.run(run())


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_migrate_repairs_app_settings_missing_refiner_columns_pre_upgrade_shape(
    tmp_path: Path,
) -> None:
    """Simulate an upgraded DB whose app_settings predates refiner_* columns (create_all + strip)."""
    db = tmp_path / "legacy.sqlite"

    async def seed_then_strip() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await migrate(engine)
        finally:
            await engine.dispose()

    asyncio.run(seed_then_strip())
    _strip_refiner_columns_from_app_settings(db)

    async def repair_and_validate() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            await migrate(engine)
            await validate_refiner_app_settings_schema(engine)
            async with engine.connect() as conn:
                res = await conn.execute(text("PRAGMA table_info(app_settings)"))
                names = {row[1] for row in res.fetchall()}
            for col_name, _ in _REFINER_APP_SETTINGS_SQLITE_SPECS:
                assert col_name in names, f"missing after repair: {col_name}"
        finally:
            await engine.dispose()

    asyncio.run(repair_and_validate())


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_migrate_refiner_column_repair_is_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "idempotent.sqlite"

    async def once() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await migrate(engine)
        finally:
            await engine.dispose()

    asyncio.run(once())
    _strip_refiner_columns_from_app_settings(db)

    async def twice() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            await migrate(engine)
            await migrate(engine)
            await validate_refiner_app_settings_schema(engine)
        finally:
            await engine.dispose()

    asyncio.run(twice())


def test_app_startup_succeeds_after_repair_of_missing_refiner_columns(tmp_path: Path) -> None:
    """Lifespan: create_all + migrate + ensure refiner columns; upgraded DB must not die on validate."""
    if not _sqlite_supports_drop_column():
        pytest.skip("SQLite 3.35+ required for DROP COLUMN")
    db = tmp_path / "startup_repair.sqlite"

    async def seed() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await migrate(engine)
        finally:
            await engine.dispose()

    asyncio.run(seed())
    _strip_refiner_columns_from_app_settings(db)

    repo_root = Path(__file__).resolve().parents[1]
    runner = tmp_path / "_startup_repair_probe.py"
    runner.write_text(
        textwrap.dedent(
            f"""
            import os
            import sys

            os.environ["FETCHER_DEV_DB_PATH"] = {str(db)!r}
            os.environ["FETCHER_JWT_SECRET"] = "0123456789abcdef0123456789abcdef"
            os.environ["FETCHER_CI_SMOKE"] = "1"

            from fastapi.testclient import TestClient
            from app.main import app

            with TestClient(app) as client:
                r = client.get("/healthz")
                if r.status_code != 200:
                    print(r.status_code, r.text, file=sys.stderr)
                    sys.exit(2)
            sys.exit(0)
            """
        ),
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)
    env["FETCHER_DEV_DB_PATH"] = str(db)
    env["FETCHER_JWT_SECRET"] = "0123456789abcdef0123456789abcdef"
    env["FETCHER_CI_SMOKE"] = "1"

    proc = subprocess.run(
        [sys.executable, str(runner)],
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
