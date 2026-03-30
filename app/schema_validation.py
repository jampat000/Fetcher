"""Fail-fast checks that ``app_settings`` matches this build (Refiner columns present)."""

from __future__ import annotations

import logging
from typing import FrozenSet

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.schema_version import CURRENT_SCHEMA_VERSION

logger = logging.getLogger(__name__)

# Minimum Refiner persistence surface required on app_settings.
REQUIRED_REFINER_APP_SETTINGS_COLUMNS: FrozenSet[str] = frozenset(
    {
        "refiner_enabled",
        "refiner_watched_folder",
        "refiner_output_folder",
        "refiner_work_folder",
    }
)

_RUNTIME_ERROR_DETAIL = (
    "This Fetcher build requires a complete app_settings table including Refiner columns "
    "(refiner_*). The database file is missing required columns. "
    "Use a current SQLite database from this build, or remove the file and start fresh. "
    "Missing column(s): {missing}."
)


async def validate_refiner_app_settings_schema(engine: AsyncEngine) -> None:
    """Raise ``RuntimeError`` if ``app_settings`` is missing required ``refiner_*`` columns.

    Call after ``create_all`` and :func:`app.migrations.migrate` (which repairs missing
    ``refiner_*`` columns on SQLite ``app_settings`` before this check).
    """
    if engine.dialect.name != "sqlite":
        logger.error(
            "Refiner schema validation failed: expected SQLite, got dialect %r.",
            engine.dialect.name,
        )
        raise RuntimeError(
            f"Fetcher requires SQLite for database schema checks; got dialect {engine.dialect.name!r}."
        )

    async with engine.connect() as conn:
        res = await conn.execute(
            text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='app_settings' LIMIT 1")
        )
        if res.fetchone() is None:
            missing = ", ".join(sorted(REQUIRED_REFINER_APP_SETTINGS_COLUMNS))
            logger.error(
                "Refiner schema validation failed: app_settings table missing."
            )
            raise RuntimeError(
                _RUNTIME_ERROR_DETAIL.format(missing=missing),
            )

        res = await conn.execute(text("PRAGMA table_info(app_settings)"))
        cols = {row[1] for row in res.fetchall()}

    missing_set = REQUIRED_REFINER_APP_SETTINGS_COLUMNS - cols
    if missing_set:
        missing_list = ", ".join(sorted(missing_set))
        logger.error(
            "Refiner schema validation failed: missing app_settings columns: %s.",
            missing_list,
        )
        raise RuntimeError(_RUNTIME_ERROR_DETAIL.format(missing=missing_list))


async def validate_app_settings_schema_version(engine: AsyncEngine) -> None:
    """Raise ``RuntimeError`` unless ``app_settings.schema_version`` equals :data:`CURRENT_SCHEMA_VERSION`."""
    if engine.dialect.name != "sqlite":
        logger.error(
            "Schema version check failed: expected SQLite, got dialect %r.",
            engine.dialect.name,
        )
        raise RuntimeError(
            f"Fetcher requires SQLite for database schema checks; got dialect {engine.dialect.name!r}."
        )

    exp = int(CURRENT_SCHEMA_VERSION)
    async with engine.connect() as conn:
        res = await conn.execute(
            text("SELECT schema_version FROM app_settings ORDER BY id ASC LIMIT 1")
        )
        row = res.first()

    if row is None:
        logger.error(
            "Schema version mismatch: expected %s, found no app_settings row.",
            exp,
        )
        raise RuntimeError(
            f"This Fetcher build requires database schema version exactly {exp}, "
            "but app_settings has no rows. Restore a valid database or remove the DB file to reinitialize."
        )

    actual = row[0]
    if actual is None:
        logger.error("Schema version mismatch: expected %s, found NULL", exp)
        raise RuntimeError(
            f"This Fetcher build requires database schema version exactly {exp}. "
            f"Found NULL (unset). Restore a valid database or remove the DB file to reinitialize."
        )

    actual_i = int(actual)
    if actual_i != exp:
        logger.error("Schema version mismatch: expected %s, found %s", exp, actual_i)
        raise RuntimeError(
            f"This Fetcher build requires database schema version exactly {exp}. "
            f"Found {actual_i}. Use a database file from this Fetcher build or restore a compatible backup."
        )
