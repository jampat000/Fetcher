"""E2E fixtures: real HTTP server for Playwright (separate process, own SQLite)."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from tests.e2e.constants import E2E_AUTH_PASSWORD, E2E_AUTH_USERNAME

REPO_ROOT: Path = Path(__file__).resolve().parents[2]


def _pick_loopback_port() -> int:
    """Ephemeral port so E2E does not attach to an unrelated process on a fixed dev port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _init_e2e_database(db_path: Path) -> None:
    """Create schema + seed password so /setup/1 and authenticated routes work."""
    path_json = json.dumps(str(db_path.resolve()))
    user_json = json.dumps(E2E_AUTH_USERNAME)
    pass_json = json.dumps(E2E_AUTH_PASSWORD)
    script = f"""
import asyncio
import os
os.environ["FETCHER_DEV_DB_PATH"] = {path_json}
from app.auth import hash_password
from app.db import SessionLocal, get_or_create_settings, engine
from app.migrations import migrate
from app.models import Base
from app.time_util import utc_now_naive

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await migrate(engine)
    async with SessionLocal() as s:
        r = await get_or_create_settings(s)
        r.auth_password_hash = hash_password({pass_json})
        r.auth_session_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        r.auth_username = {user_json}
        r.updated_at = utc_now_naive()
        await s.commit()

asyncio.run(main())
"""
    subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(REPO_ROOT),
        check=True,
        env={**os.environ},
    )


@pytest.fixture(scope="session")
def e2e_server() -> str:
    fd, raw = tempfile.mkstemp(prefix="fetcher-e2e-", suffix=".sqlite3")
    os.close(fd)
    db_path = Path(raw)
    try:
        db_path.unlink(missing_ok=True)
    except OSError:
        pass

    _init_e2e_database(db_path)

    port = _pick_loopback_port()
    base = f"http://127.0.0.1:{port}"

    # Do not inherit FETCHER_RESET_AUTH=1 from the developer shell — startup would clear the seeded password.
    env = {
        **os.environ,
        "FETCHER_DEV_DB_PATH": str(db_path.resolve()),
        "FETCHER_RESET_AUTH": "0",
    }
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    try:
        deadline = time.time() + 30
        last_exc: BaseException | None = None
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/healthz", timeout=1) as r:
                    if r.status != 200:
                        continue
                    payload = json.loads(r.read().decode("utf-8", errors="replace"))
                    if payload.get("app") == "Fetcher" and (payload.get("status") or "").lower() == "ok":
                        break
            except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError, ValueError) as e:
                last_exc = e
                time.sleep(0.25)
        else:
            proc.terminate()
            pytest.fail(f"E2E server did not become ready: {last_exc!r}")
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
        try:
            db_path.unlink(missing_ok=True)
        except OSError:
            pass
