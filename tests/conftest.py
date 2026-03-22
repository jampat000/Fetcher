"""Pytest configuration: isolated DB path before app imports, schema + auth seed, auth dependency override."""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

import pytest

# Before any test module does `from app.main import app` / `from app.db import engine`.
_fd, _TEST_DB_PATH = tempfile.mkstemp(prefix="grabby-pytest-", suffix=".sqlite")
os.close(_fd)
try:
    Path(_TEST_DB_PATH).unlink(missing_ok=True)
except OSError:
    pass
os.environ["GRABBY_DEV_DB_PATH"] = _TEST_DB_PATH


@pytest.fixture(scope="session", autouse=True)
def _init_grabby_test_database() -> None:
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
    from app.auth import require_auth
    from app.main import app

    if request.node.get_closest_marker("no_auth_override"):
        yield
        return

    async def _ok() -> None:
        return None

    app.dependency_overrides[require_auth] = _ok
    yield
    app.dependency_overrides.pop(require_auth, None)
