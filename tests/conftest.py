"""Pytest configuration: isolated DB path before app imports, schema + auth seed, auth dependency override."""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

import pytest

# Before any test module does `from app.main import app` / `from app.db import engine`.
_fd, _TEST_DB_PATH = tempfile.mkstemp(prefix="fetcher-pytest-", suffix=".sqlite")
os.close(_fd)
try:
    Path(_TEST_DB_PATH).unlink(missing_ok=True)
except OSError:
    pass
os.environ["FETCHER_DEV_DB_PATH"] = _TEST_DB_PATH

def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    """Keep README screenshot regen out of default collection (``pytest tests/``, etc.).

    Explicit ``pytest tests/e2e/test_readme_screenshots.py`` bypasses this (initial path);
    that path uses ``skipif`` in the module for a clear message when REGEN is unset.
    """
    if (os.environ.get("REGEN_README_SCREENSHOTS") or "").strip():
        return None
    if collection_path.name == "test_readme_screenshots.py":
        return True
    return None


@pytest.fixture(scope="session", autouse=True)
def _init_fetcher_test_database() -> None:
    from app.auth import hash_password
    from app.db import SessionLocal, _get_or_create_settings, engine
    from app.migrations import migrate
    from app.models import Base
    from app.time_util import utc_now_naive

    async def setup() -> None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await migrate(engine)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.auth_password_hash = hash_password("testpass12")
            row.auth_session_secret = "0123456789abcdef" * 4
            row.auth_username = "admin"
            row.auth_ip_allowlist = ""
            row.updated_at = utc_now_naive()
            await session.commit()

    asyncio.run(setup())
    yield
    try:
        os.unlink(_TEST_DB_PATH)
    except OSError:
        pass


@pytest.fixture(autouse=True)
def _dependency_override_require_auth(request: pytest.FixtureRequest) -> None:
    from app.auth import require_api_auth, require_auth
    from app.main import app

    if request.node.get_closest_marker("no_auth_override"):
        yield
        return

    async def _ok() -> None:
        return None

    async def _ok_api() -> None:
        return None

    app.dependency_overrides[require_auth] = _ok
    app.dependency_overrides[require_api_auth] = _ok_api
    yield
    app.dependency_overrides.pop(require_auth, None)
    app.dependency_overrides.pop(require_api_auth, None)


@pytest.fixture(autouse=True)
def _dependency_override_require_csrf(request: pytest.FixtureRequest) -> None:
    from app.auth import require_csrf
    from app.main import app

    if request.node.get_closest_marker("real_csrf"):
        yield
        return

    async def _ok_csrf() -> None:
        return None

    app.dependency_overrides[require_csrf] = _ok_csrf
    yield
    app.dependency_overrides.pop(require_csrf, None)
