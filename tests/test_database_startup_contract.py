"""Integration-style checks for :mod:`app.database_startup` and the real upgrade → validate contract."""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.database_startup import (
    _sqlite_db_paths_refer_to_same_file,
    run_schema_upgrade_phase,
    verify_sqlite_engine_matches_canonical_path,
)
from app.migrations import migrate
from app.models import Base
from app.refiner_app_settings_contract import REFINER_APP_SETTINGS_SQLITE_SPECS
from app.schema_validation import (
    REQUIRED_REFINER_APP_SETTINGS_COLUMNS,
    validate_refiner_app_settings_schema,
)
from tests.test_schema_validation_refiner import _strip_refiner_columns_from_app_settings


def _sqlite_supports_drop_column() -> bool:
    return sqlite3.sqlite_version_info >= (3, 35, 0)


def test_sqlite_db_paths_equivalent_same_file(tmp_path: Path) -> None:
    db = tmp_path / "fetcher.db"
    db.write_bytes(b"x")
    a = db
    b = Path(str(db).replace("\\", "/")) if "\\" in str(db) else Path(str(db))
    assert _sqlite_db_paths_refer_to_same_file(a, b)


def test_verify_sqlite_engine_mismatch_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_a = tmp_path / "engine_a.sqlite"
    db_b = tmp_path / "canonical_b.sqlite"
    sqlite3.connect(db_a).close()
    sqlite3.connect(db_b).close()
    eng = create_async_engine(f"sqlite+aiosqlite:///{db_a.as_posix()}")
    monkeypatch.setenv("FETCHER_DEV_DB_PATH", str(db_b))

    from app.database_resolution import compute_canonical_db_path

    canonical, _reason = compute_canonical_db_path()
    log = logging.getLogger("test_db_startup")

    try:
        with pytest.raises(RuntimeError, match="bound to|FETCHER_DEV_DB_PATH"):
            verify_sqlite_engine_matches_canonical_path(
                eng, canonical_db_file=canonical, log=log
            )
    finally:
        asyncio.run(eng.dispose())


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_run_schema_upgrade_phase_repairs_after_column_strip(tmp_path: Path) -> None:
    """Pool recycle + migrate/repair must leave pragma visible before strict validation."""
    db = tmp_path / "upgrade_phase.sqlite"

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

    async def upgrade_and_validate() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            log = logging.getLogger("test_upgrade_phase")
            await run_schema_upgrade_phase(engine, log=log)
            await validate_refiner_app_settings_schema(engine)
            async with engine.connect() as conn:
                res = await conn.execute(text("SELECT name FROM pragma_table_info('app_settings')"))
                names = {row[0] for row in res.fetchall()}
            for col_name, _ in REFINER_APP_SETTINGS_SQLITE_SPECS:
                assert col_name in names, col_name
        finally:
            await engine.dispose()

    asyncio.run(upgrade_and_validate())


@pytest.mark.skipif(not _sqlite_supports_drop_column(), reason="SQLite 3.35+ required for DROP COLUMN")
def test_packaged_like_lifespan_twice_idempotent(tmp_path: Path) -> None:
    """Subprocess imports ``app.main`` like the service/exe: two cold starts must succeed."""
    db = tmp_path / "lifespan_twice.sqlite"

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
    runner = tmp_path / "_lifespan_twice.py"
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

    for i in range(2):
        proc = subprocess.run(
            [sys.executable, str(runner)],
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert proc.returncode == 0, f"start {i + 1}: {proc.stdout + proc.stderr}"

    con = sqlite3.connect(db)
    try:
        cur = con.execute("SELECT name FROM pragma_table_info('app_settings')")
        names = {r[0] for r in cur.fetchall()}
    finally:
        con.close()
    for col in REQUIRED_REFINER_APP_SETTINGS_COLUMNS:
        assert col in names


def test_unsupported_no_app_settings_still_fails_clearly(tmp_path: Path) -> None:
    db = tmp_path / "no_settings.sqlite"
    sqlite3.connect(db).close()

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            with pytest.raises(RuntimeError, match="app_settings|refiner|database"):
                await validate_refiner_app_settings_schema(engine)
        finally:
            await engine.dispose()

    asyncio.run(run())
