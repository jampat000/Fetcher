"""Set Fetcher web UI username/password on the dev SQLite file (one-shot).

Default database matches ``scripts/dev-start.ps1`` when ``FETCHER_DEV_DB_PATH`` is unset:
``%TEMP%\\fetcher-dev.sqlite3``.

Usage (from repo root)::

    .\\.venv\\Scripts\\python.exe scripts/dev-set-login.py
    .\\.venv\\Scripts\\python.exe scripts/dev-set-login.py MyPassword --username admin

``FETCHER_JWT_SECRET`` must be set for imports that touch the app stack; this script sets a
dev-only placeholder if it is missing (same idea as local pytest).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import secrets
import sys
import tempfile
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _ensure_dev_env() -> None:
    if not (os.environ.get("FETCHER_DEV_DB_PATH") or "").strip():
        os.environ["FETCHER_DEV_DB_PATH"] = str(Path(tempfile.gettempdir()) / "fetcher-dev.sqlite3")
    if not (os.environ.get("FETCHER_JWT_SECRET") or "").strip():
        os.environ["FETCHER_JWT_SECRET"] = "0123456789abcdef0123456789abcdef"


async def _run(username: str, password: str) -> None:
    from app.db import SessionLocal, get_or_create_settings, db_path, engine
    from app.migrations import migrate
    from app.models import Base
    from app.security_utils import hash_password
    from app.time_util import utc_now_naive

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await migrate(engine)
    async with SessionLocal() as session:
        row = await get_or_create_settings(session)
        row.auth_username = username.strip() or "admin"
        row.auth_password_hash = hash_password(password)
        if not (row.auth_session_secret or "").strip():
            row.auth_session_secret = secrets.token_hex(32)
        row.updated_at = utc_now_naive()
        await session.commit()

    u = username.strip() or "admin"
    print(f"Sign-in updated for database: {db_path()}")
    print(f"  Username: {u}")
    print("  Password: what you passed on the command line, or the default 'dev' if you used defaults.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Set Fetcher dev SQLite login.")
    parser.add_argument(
        "password",
        nargs="?",
        default="dev",
        help="Password to store (default: dev)",
    )
    parser.add_argument("--username", default="admin", help="Username (default: admin)")
    args = parser.parse_args()

    _ensure_dev_env()

    try:
        asyncio.run(_run(args.username, args.password))
    except Exception as e:  # noqa: BLE001
        print(f"Error: {e}", file=sys.stderr)
        return 1
    print("")
    print("Open the dev server (e.g. http://127.0.0.1:8766) and sign in with the credentials above.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
