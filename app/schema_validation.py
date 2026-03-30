"""Fail-fast checks that ``app_settings`` matches this build (Refiner columns present).

Runs **after** :func:`app.database_startup.run_schema_upgrade_phase` on startup (repair is idempotent).
Contributor rules: ``app/schema_upgrade_contract.py``; product contract: ``docs/DATABASE-SCHEMA-CONTRACT.md``.
"""

from __future__ import annotations

import logging
from typing import FrozenSet

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.migrations import repair_refiner_app_settings_columns
from app.refiner_app_settings_contract import REFINER_APP_SETTINGS_SQLITE_SPECS
from app.schema_version import CURRENT_SCHEMA_VERSION

logger = logging.getLogger(__name__)

# All ``refiner_*`` columns enforced by strict validation (same set as SQLite repair DDL).
REQUIRED_REFINER_APP_SETTINGS_COLUMNS: FrozenSet[str] = frozenset(
    name for name, _ in REFINER_APP_SETTINGS_SQLITE_SPECS
)

_RUNTIME_ERROR_DETAIL = (
    "This Fetcher build requires a complete app_settings table including Refiner columns "
    "(refiner_*). The database file is missing required columns. "
    "Use a current SQLite database from this build, or remove the file and start fresh. "
    "Missing column(s): {missing}."
)


async def _app_settings_column_names(engine: AsyncEngine) -> tuple[bool, set[str]]:
    """Return ``(table_exists, column_names)`` for ``app_settings`` on SQLite."""
    async with engine.connect() as conn:
        res = await conn.execute(
            text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='app_settings' LIMIT 1")
        )
        if res.fetchone() is None:
            return False, set()
        res = await conn.execute(text("SELECT name FROM pragma_table_info('app_settings')"))
        names = {str(row[0]) for row in res.fetchall() if row[0] is not None}
        return True, names


async def validate_refiner_app_settings_schema(engine: AsyncEngine) -> None:
    """Strict check after upgrade: always run idempotent repair, then require full refiner surface.

    Call only after :func:`app.database_startup.run_schema_upgrade_phase` (or equivalent migrate +
    pool recycle). This function does not assume migrate already repaired — it runs repair again
    so validation never observes a repairable gap without attempting DDL first.
    """
    if engine.dialect.name != "sqlite":
        logger.error(
            "Refiner schema validation failed: expected SQLite, got dialect %r.",
            engine.dialect.name,
        )
        raise RuntimeError(
            f"Fetcher requires SQLite for database schema checks; got dialect {engine.dialect.name!r}."
        )

    logger.info(
        "Startup: strict validation — Refiner app_settings (idempotent repair, then assert)"
    )
    await repair_refiner_app_settings_columns(engine)

    exists, cols = await _app_settings_column_names(engine)
    if not exists:
        missing = ", ".join(sorted(REQUIRED_REFINER_APP_SETTINGS_COLUMNS))
        logger.error("Refiner schema validation failed: app_settings table missing after repair.")
        raise RuntimeError(_RUNTIME_ERROR_DETAIL.format(missing=missing))

    missing_set = REQUIRED_REFINER_APP_SETTINGS_COLUMNS - cols
    if missing_set:
        missing_list = ", ".join(sorted(missing_set))
        logger.error(
            "Refiner schema validation failed: required refiner_* columns still missing after "
            "repair (unsupported DB shape or repair error): %s.",
            missing_list,
        )
        raise RuntimeError(_RUNTIME_ERROR_DETAIL.format(missing=missing_list))

    logger.info(
        "Startup: Refiner app_settings strict validation OK (%s columns).",
        len(REQUIRED_REFINER_APP_SETTINGS_COLUMNS),
    )


async def validate_app_settings_schema_version(engine: AsyncEngine) -> None:
    """Raise ``RuntimeError`` unless ``app_settings.schema_version`` equals :data:`CURRENT_SCHEMA_VERSION`."""
    logger.info(
        "Startup: strict validation — app_settings.schema_version == %s",
        int(CURRENT_SCHEMA_VERSION),
    )
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
    logger.info("Startup: app_settings.schema_version strict validation OK (%s)", exp)
