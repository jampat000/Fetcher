# Upgrade, database path, and auth (contract)

Short reference for **deterministic** upgrades: the app either starts with the right SQLite file and schema, repairs idempotently, or **exits with an explicit error** before doing harm.

## Canonical database path (single source of truth)

Resolution lives in `app/database_resolution.py` and is used by `app/db.py` (`db_path()`). Precedence:

1. **`FETCHER_DEV_DB_PATH`** — full path to the DB file (dev, pytest, optional Docker-style setups). **No** multi-file legacy/conflict checks (explicit override).
2. **`FETCHER_DATA_DIR`** — directory that must contain **`fetcher.db`** (created on first use if missing).
3. **Default** — `default_data_dir() / "fetcher.db"`:
   - **Packaged Windows** (`sys.frozen`): `%ProgramData%\Fetcher\fetcher.db`
   - **Otherwise**: `%LOCALAPPDATA%\Fetcher\fetcher.db`-style path under the user profile (`~/AppData/Local/Fetcher` on Unix-like layouts).

Startup logs the **exact path** and **why** it was chosen (`log_database_resolution_startup`).

## Packaged Windows: legacy vs canonical

Automatic **copy** from old locations was removed (see CHANGELOG 3.2.0). For **packaged** builds only, Fetcher **detects** a second substantial `fetcher.db` under:

- `%LOCALAPPDATA%\Fetcher\fetcher.db`
- `%ProgramData%\Fetcher\fetcher.db`

Behavior:

| Situation | Result |
| --- | --- |
| Canonical DB exists and is non-empty; another substantial DB exists elsewhere | **Stop** — message lists both paths; user must remove/rename one or set `FETCHER_DATA_DIR` to the intended folder. |
| Canonical missing/empty; **one** substantial legacy file exists | **Stop** — instructs to copy (service stopped) or point `FETCHER_DATA_DIR` at the folder that already has `fetcher.db`. |
| Canonical missing/empty; **several** substantial legacy files | **Stop** — ambiguous; user must choose one backup and one active path. |
| **Unfrozen** Windows (source/tests) | Legacy checks **skipped** so `FETCHER_DATA_DIR` can target an empty folder without colliding with a developer profile DB. |

## Schema repair and validation

- All upgrades go through **`migrate()`** in `app/migrations.py` (idempotent `ALTER` / column repair, including Refiner columns via `_ensure_refiner_app_settings_columns`).
- **Strict** checks in `app/schema_validation.py` run **after** `migrate()` on startup.
- Contributor rules: `app/schema_upgrade_contract.py`.

## Auth after reinstall / upgrade

- **Password hash present** in the DB in use → UI expects **login** for that database.
- **No password hash** → **setup** (`/setup/0`).
- **`FETCHER_RESET_AUTH=1`** clears credentials **on that start** and is logged at **warning** and **error**; remove it after recovery so it does not run every boot.

Startup logs an **auth diagnostic** line: `password_hash_configured=…` and `next_ui=login|setup(/setup/0)`.

## Installer / uninstall

- **Upgrade**: replaces files under `{app}`; **does not** remove `%ProgramData%\Fetcher` (DB and logs stay). See `installer/Fetcher.iss` comments and **INSTALL-AND-OPERATIONS.md**.
- **Uninstall**: removes installed application files; **ProgramData is not deleted** by the installer script so data can survive uninstall.

## Docker / Linux

- Typically set **`FETCHER_DATA_DIR`** (and **`FETCHER_JWT_SECRET`**). No Windows legacy paths; policy is a straight canonical file.

## If startup refuses with a database message

1. Read the **full** error text (Event Viewer / console / `fetcher.log`).
2. **Back up** every `fetcher.db` path the message lists.
3. Ensure **one** authoritative `fetcher.db` in the folder implied by `FETCHER_DATA_DIR` or the default path, **or** set `FETCHER_DATA_DIR` to that folder.
4. Restart the service.
