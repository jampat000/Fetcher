# Database schema contract (SQLite)

## Supported upgrade behavior

Fetcher upgrades **supported** older `fetcher.db` files **in place**:

1. **Canonical file** — exactly one authoritative path from `FETCHER_DEV_DB_PATH`, `FETCHER_DATA_DIR`, or the default data directory (see `app/database_resolution.py`). **Installed Windows service:** `FetcherService.xml` sets **`FETCHER_DATA_DIR=C:\ProgramData\Fetcher`** so LocalSystem never uses **`…\systemprofile\AppData\Local\Fetcher`** as an implicit second root. Packaged **frozen** builds **ignore** a `FETCHER_DATA_DIR` that points at that LocalSystem profile path and fall back to `%ProgramData%\Fetcher`. Ambiguous multi-database layouts **refuse to start** with an explicit error (no silent switching).
2. **Engine binding** — the SQLAlchemy engine is created when `app.db` is first imported and must use that same canonical path. If the environment selects a different file after import, startup fails with a clear message: set env vars **before** starting the process and restart.
3. **Upgrade steps** — `Base.metadata.create_all` (creates missing tables only), then `migrate()` (ordered, idempotent `ALTER` / data backfills), then **pool dispose** and another **idempotent** `repair_refiner_app_settings_columns` pass so pooled connections cannot see a stale `app_settings` definition.
4. **Strict validation** — Refiner checks **always** run repair again (idempotent), then assert every column in `REFINER_APP_SETTINGS_SQLITE_SPECS`. Then `app_settings.schema_version` must equal `CURRENT_SCHEMA_VERSION`.

## What is repaired automatically

- All steps in `app/migrations.migrate()` (Sonarr/Radarr/Emby/auth/activity/refiner_activity, etc.).
- Missing **`refiner_*`** columns on `app_settings` via `repair_refiner_app_settings_columns` (same DDL list as `app/refiner_app_settings_contract.py`).

Operators do **not** need to re-enter settings when repair is possible; defaults come from `ADD COLUMN … DEFAULT`.

## Legacy behavior: upgrade-only vs runtime

- **Upgrade-only:** Numbered `_migrate_*` functions, legacy path probes in `database_resolution` (Windows “wrong DB” detection), and one-time column drops (e.g. obsolete global Arr columns) exist **only** to move old files to the current model. They are not alternate runtime code paths after startup succeeds.
- **Runtime:** The app uses the **current** SQLAlchemy models (`app/models.py`) and the single SQLite file. No parallel “legacy mode” after a successful upgrade.

Obsolete columns that SQLite makes awkward to remove may remain on disk **unused**; the ORM and validation enforce the **current** column set.

## Unsupported / ambiguous cases

- **Multiple substantial databases** where the canonical file is unclear (packaged Windows policy).
- **Canonical file empty** but several legacy databases exist (ambiguous).
- **`app_settings` missing** or **required columns still missing after repair** (corrupt DB, read-only volume, or unsupported dump) — startup fails with logs naming the problem; this is not fixed by deleting user configuration on purpose — operators fix the file or restore backup.

## References

- Implementation: `app/database_startup.py`, `app/main.py` lifespan, `app/migrations.py`, `app/schema_validation.py`.
- JWT / service env: `README.md`, `docs/INSTALL-AND-OPERATIONS.md`.
