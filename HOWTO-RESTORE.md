# Fetcher: Backup & Restore

Fetcher stores your **settings** (Sonarr, Radarr, Emby, API keys, schedules) in a **local SQLite database** under your profile:

- **Windows (installed `Fetcher.exe` / service):** The database is **`%ProgramData%\Fetcher\fetcher.db`** (override with machine env **`FETCHER_DATA_DIR`** pointing at the folder that contains **`fetcher.db`**). Fetcher does **not** copy databases from other profile paths; move or restore **`fetcher.db`** yourself with the service stopped if you change machines or layouts.
- **Windows (run as your user / dev with shared DB):** `%LOCALAPPDATA%\Fetcher\fetcher.db` (e.g. `C:\Users\You\AppData\Local\Fetcher\fetcher.db`).
- **Override:** set machine env **`FETCHER_DATA_DIR`** to a folder; Fetcher uses **`fetcher.db`** inside it (see **`service/README.md`**).

## One-file backup (recommended)

Use the **Web UI** so **Sonarr**, **Radarr**, **Trimmer** (Emby), **Refiner**, and **web authentication** settings from the single **`app_settings`** database row are in **one JSON file** (API keys and secrets included).

1. Open **Fetcher** in the browser (for example `http://127.0.0.1:8765` or dev port `8766`).
2. Go to **Settings**.
3. Under **Backup & Restore**, click **Download Backup**.
4. Keep the file **private** (same as a password manager export). The JSON includes **auth data** (e.g. password hash, session secret)—treat it like a **password**.

**Backup file format:** Current exports use **`format_version`: `2`**, the **`fetcher_backup`** / **`fetcher_settings_v1`** header, **`supported_schema_version`**, and **`settings`** with **current `app_settings` column names** only. **Restore requires the same supported schema version and backup format** as the running build. **ISO-8601** datetime strings inside fields still parse. Any **unsupported** top-level key, **wrong `format_version`**, **obsolete global Arr field names** in `settings`, or **schema mismatch** causes restore to **fail with no DB changes**—export again from the same Fetcher build.

### Restore on a new install

1. Install or start **Fetcher** on the new machine.
2. Open **Settings** → **Backup & Restore** → **Restore from Backup**.
3. Choose the `.json` file, turn on the **confirmation** toggle, click **Restore from Backup**.
4. **Restart** the **Fetcher** Windows service (or the app) so the scheduler reloads.

**Note:** Import replaces the **`app_settings` row** only. **`activity_log`**, **`job_run_log`**, **`app_snapshot`**, **`refiner_activity`**, and **`arr_action_log`** are **not** in the JSON; copy **`fetcher.db`** if you need those.

## Full database copy (optional)

To clone **everything** in the database (including **Activity** tables), **stop Fetcher** (so the DB is not locked), then copy `fetcher.db` to a safe place. To restore, stop **Fetcher** and replace `fetcher.db` with your copy.

## Git / source folder

Your **project** folder (clone of this repo) is separate from the **runtime** database. Backing up only the repo does **not** back up `fetcher.db` unless you copy it separately or use the JSON export above.
