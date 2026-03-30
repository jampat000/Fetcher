"""Permanent contract: persisted schema changes ship with idempotent repair in ``migrate()`` + strict validation after.

**Rules for contributors**

1. Any new required column/table on existing tables must be added in ``app/migrations.py`` inside
   ``migrate()`` (or helpers it calls), using idempotent ``IF NOT EXISTS`` / ``_has_column`` patterns.
2. Bump ``app/schema_version.py`` ``CURRENT_SCHEMA_VERSION`` when the persisted contract changes,
   with a migration that backfills or defaults new fields.
3. Strict checks stay in ``app/schema_validation.py`` and run **after** ``migrate()`` in application
   startup (see ``app/main.py`` lifespan).
4. Add or extend tests under ``tests/test_schema_*`` and ``tests/test_refiner_app_settings_contract.py``
   (or equivalent) so missing migration steps fail CI.

Silent auto-migration of unknown third-party data is intentionally out of scope; repair steps are
narrow, logged via SQLAlchemy/logging, and must be safe to run repeatedly.
"""

from __future__ import annotations

# Migrations module must end ``migrate()`` with refiner column repair before validation.
REFINER_REPAIR_ENTRYPOINT = "_ensure_refiner_app_settings_columns"
