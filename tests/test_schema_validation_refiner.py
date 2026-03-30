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
from sqlalchemy.ext.asyncio import create_async_engine

from app.schema_validation import (
    REQUIRED_REFINER_APP_SETTINGS_COLUMNS,
    validate_refiner_app_settings_schema,
)


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


def test_app_startup_fails_on_incomplete_db_without_refiner_columns(tmp_path: Path) -> None:
    """Fresh interpreter: engine binds to incomplete DB; lifespan must not start scheduler."""
    db = tmp_path / "incomplete_startup.sqlite"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE app_settings (id INTEGER PRIMARY KEY)")
    con.commit()
    con.close()

    repo_root = Path(__file__).resolve().parents[1]
    runner = tmp_path / "_startup_probe.py"
    runner.write_text(
        textwrap.dedent(
            f"""
            import os
            import sys

            os.environ["FETCHER_DEV_DB_PATH"] = {str(db)!r}
            os.environ["FETCHER_JWT_SECRET"] = "0123456789abcdef0123456789abcdef"

            from fastapi.testclient import TestClient
            from app.main import app

            try:
                with TestClient(app):
                    pass
            except RuntimeError as e:
                t = type(e).__name__ + ": " + str(e)
                if "refiner" in t.lower():
                    sys.exit(0)
                print(t, file=sys.stderr)
                sys.exit(2)
            print("expected RuntimeError, startup succeeded", file=sys.stderr)
            sys.exit(1)
            """
        ),
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)
    env["FETCHER_DEV_DB_PATH"] = str(db)
    env["FETCHER_JWT_SECRET"] = "0123456789abcdef0123456789abcdef"

    proc = subprocess.run(
        [sys.executable, str(runner)],
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
