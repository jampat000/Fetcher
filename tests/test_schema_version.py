"""``app_settings.schema_version`` must equal ``CURRENT_SCHEMA_VERSION``."""

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

from app.schema_validation import validate_app_settings_schema_version
from app.schema_version import CURRENT_SCHEMA_VERSION


def _write_minimal_app_settings(path: Path, schema_version: int) -> None:
    con = sqlite3.connect(path)
    con.execute(
        """
        CREATE TABLE app_settings (
            id INTEGER PRIMARY KEY,
            refiner_enabled INTEGER NOT NULL DEFAULT 0,
            refiner_watched_folder TEXT NOT NULL DEFAULT '',
            refiner_output_folder TEXT NOT NULL DEFAULT '',
            refiner_work_folder TEXT NOT NULL DEFAULT '',
            schema_version INTEGER NOT NULL
        );
        """
    )
    con.execute(
        "INSERT INTO app_settings (id, refiner_enabled, refiner_watched_folder, "
        "refiner_output_folder, refiner_work_folder, schema_version) VALUES (1, 0, '', '', '', ?)",
        (schema_version,),
    )
    con.commit()
    con.close()


def test_validate_schema_version_accepts_current(tmp_path: Path) -> None:
    db = tmp_path / "v.sqlite"
    _write_minimal_app_settings(db, CURRENT_SCHEMA_VERSION)

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            await validate_app_settings_schema_version(engine)
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_validate_schema_version_rejects_lower(tmp_path: Path) -> None:
    wrong = int(CURRENT_SCHEMA_VERSION) - 2
    db = tmp_path / "low.sqlite"
    _write_minimal_app_settings(db, wrong)

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            with pytest.raises(RuntimeError, match="exactly|requires"):
                await validate_app_settings_schema_version(engine)
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_validate_schema_version_rejects_higher(tmp_path: Path) -> None:
    wrong = int(CURRENT_SCHEMA_VERSION) + 7
    db = tmp_path / "high.sqlite"
    _write_minimal_app_settings(db, wrong)

    async def run() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{db.as_posix()}")
        try:
            with pytest.raises(RuntimeError, match="exactly|requires"):
                await validate_app_settings_schema_version(engine)
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_subprocess_startup_succeeds_with_matching_schema_version(tmp_path: Path) -> None:
    db = tmp_path / "startup_ok.sqlite"
    repo_root = Path(__file__).resolve().parents[1]
    runner = tmp_path / "_startup_ok.py"
    runner.write_text(
        textwrap.dedent(
            f"""
            import asyncio
            import os
            import sys

            os.environ["FETCHER_DEV_DB_PATH"] = {str(db)!r}
            os.environ["FETCHER_JWT_SECRET"] = "0123456789abcdef0123456789abcdef"

            async def prepare() -> None:
                from app.db import engine
                from app.migrations import migrate
                from app.models import Base

                async with engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                await migrate(engine)

            asyncio.run(prepare())

            from fastapi.testclient import TestClient
            from app.main import app

            with TestClient(app) as client:
                r = client.get("/healthz")
            sys.exit(0 if r.status_code == 200 else 1)
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


def test_subprocess_startup_fails_on_schema_version_too_high(tmp_path: Path) -> None:
    db = tmp_path / "startup_high.sqlite"
    wrong = int(CURRENT_SCHEMA_VERSION) + 5
    repo_root = Path(__file__).resolve().parents[1]
    runner = tmp_path / "_startup_high.py"
    runner.write_text(
        textwrap.dedent(
            f"""
            import asyncio
            import os
            import sys

            os.environ["FETCHER_DEV_DB_PATH"] = {str(db)!r}
            os.environ["FETCHER_JWT_SECRET"] = "0123456789abcdef0123456789abcdef"

            async def tamper() -> None:
                from app.db import SessionLocal, _get_or_create_settings, engine
                from app.migrations import migrate
                from app.models import Base

                async with engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                await migrate(engine)
                async with SessionLocal() as session:
                    row = await _get_or_create_settings(session)
                    row.schema_version = {wrong}
                    await session.commit()

            asyncio.run(tamper())

            from fastapi.testclient import TestClient
            from app.main import app

            try:
                with TestClient(app):
                    pass
            except RuntimeError as e:
                if "schema" in str(e).lower() or "version" in str(e).lower():
                    sys.exit(0)
                raise
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


def test_subprocess_startup_fails_on_schema_version_too_low(tmp_path: Path) -> None:
    db = tmp_path / "startup_low.sqlite"
    wrong = int(CURRENT_SCHEMA_VERSION) - 1
    repo_root = Path(__file__).resolve().parents[1]
    runner = tmp_path / "_startup_low.py"
    runner.write_text(
        textwrap.dedent(
            f"""
            import asyncio
            import os
            import sys

            os.environ["FETCHER_DEV_DB_PATH"] = {str(db)!r}
            os.environ["FETCHER_JWT_SECRET"] = "0123456789abcdef0123456789abcdef"

            async def tamper() -> None:
                from app.db import SessionLocal, _get_or_create_settings, engine
                from app.migrations import migrate
                from app.models import Base

                async with engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                await migrate(engine)
                async with SessionLocal() as session:
                    row = await _get_or_create_settings(session)
                    row.schema_version = {wrong}
                    await session.commit()

            asyncio.run(tamper())

            async def check() -> None:
                from app.db import engine
                from app.schema_validation import validate_app_settings_schema_version

                await validate_app_settings_schema_version(engine)

            try:
                asyncio.run(check())
            except RuntimeError as e:
                if "schema" in str(e).lower() or "version" in str(e).lower():
                    sys.exit(0)
                raise
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
