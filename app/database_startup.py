"""Canonical SQLite startup upgrade path: migrate, pool recycle, repair, then strict validation.

Contract (production):

1. :func:`app.db.db_path` is the single source of truth for which file is authoritative.
2. The async :class:`~sqlalchemy.ext.asyncio.AsyncEngine` must point at that same file (set when
   ``app.db`` is first imported — env vars must be stable before import).
3. ``CREATE TABLE IF NOT EXISTS`` / ORM ``create_all``, then :func:`app.migrations.migrate`.
4. Recycle the connection pool and run :func:`app.migrations.repair_refiner_app_settings_columns`
   again so no pooled connection keeps a stale view of ``app_settings``.
5. Strict checks in :mod:`app.schema_validation` always run an idempotent repair first, then assert.

Unsupported states (ambiguous multi-DB, empty canonical + multiple legacy files) are rejected in
:mod:`app.database_resolution` with explicit errors — no silent DB switching.
"""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import unquote

from sqlalchemy.ext.asyncio import AsyncEngine

from app.migrations import migrate, repair_refiner_app_settings_columns

logger = logging.getLogger(__name__)


def verify_sqlite_engine_matches_canonical_path(
    engine: AsyncEngine, *, canonical_db_file: Path, log: logging.Logger
) -> None:
    """Raise ``RuntimeError`` if the engine was created against a different file than ``db_path()``."""
    raw = engine.url.database
    if not raw:
        raise RuntimeError(
            "SQLite engine URL has no database path; internal configuration error."
        )
    url_path = Path(unquote(str(raw))).resolve()
    want = canonical_db_file.resolve()
    if url_path != want:
        raise RuntimeError(
            f"SQLite engine is bound to {url_path}, but canonical db_path() is {want}. "
            "Environment variables that select the database (FETCHER_DEV_DB_PATH, FETCHER_DATA_DIR) "
            "must be set before the Fetcher process imports app.db. Restart the service or "
            "application after fixing the environment."
        )
    log.info("Startup: SQLite engine URL matches canonical database (%s)", want)


async def run_schema_upgrade_phase(engine: AsyncEngine, *, log: logging.Logger) -> None:
    """Run migrate, repair, dispose pool, repair again — all repair steps are idempotent."""
    log.info("Startup: schema upgrade phase — migrate() (includes refiner repair at end)")
    await migrate(engine)
    log.info("Startup: post-migrate refiner repair (idempotent)")
    await repair_refiner_app_settings_columns(engine)
    log.info("Startup: recycling database connection pool after schema upgrade")
    await engine.dispose()
    log.info("Startup: post-pool-recycle refiner repair (idempotent)")
    await repair_refiner_app_settings_columns(engine)
    log.info("Startup: schema upgrade phase complete (committed)")
