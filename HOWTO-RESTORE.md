# Fetcher: Backup & Restore

Fetcher stores your **settings** (Sonarr, Radarr, Emby, API keys, schedules) in a **local SQLite database** under your profile:

- **Windows (installed `Fetcher.exe` / service):** The canonical database is **`%ProgramData%\Fetcher\fetcher.db`**. On first start after upgrading from a build that used the service profile path (e.g. **`%SystemRoot%\System32\config\systemprofile\AppData\Local\Fetcher\fetcher.db`** for **Local System**), Fetcher performs a **one-time copy** into ProgramData if the canonical file is missing; it does **not** overwrite an existing canonical database. If that copy **succeeds**, the legacy **`fetcher.db`** (and any **`-wal`** / **`-shm`** files next to it) are **renamed** with suffix **`.fetcher-programdata-migration-archive`** so the old folder no longer looks like a second live database. If **both** a ProgramData **`fetcher.db`** and a legacy **`fetcher.db`** already existed before upgrade, migration does **not** run and legacy files are **left unchanged**—resolve manually (stop Fetcher, compare files, back up, then remove or rename the unwanted copy). After a successful copy, **`fetcher.db.migrated_from_legacy`** (next to ProgramData **`fetcher.db`**) records the legacy path, size, mtime, and UTC time. Legacy files are renamed **only** if that marker matches the in-process migration proof and both canonical and legacy main files still match the recorded metadata (**exact** **mtime_ns**). Otherwise logs **`Archive skipped: …`** with the reason. Check the **Fetcher** log for **`SQLite database path:`**, **`Migration marker created:`**, **`Migrated SQLite`**, **`Verified migrated DB — archiving legacy copy.`**, or skip lines. Normal restarts when ProgramData already holds the DB do not log a migration line unless **`FETCHER_LOG_LEVEL=DEBUG`**.
- **Windows (run as your user / dev with shared DB):** `%LOCALAPPDATA%\Fetcher\fetcher.db` (e.g. `C:\Users\You\AppData\Local\Fetcher\fetcher.db`).
- **Override:** set machine env **`FETCHER_DATA_DIR`** to a folder; Fetcher uses **`fetcher.db`** inside it (see **`service/README.md`**).

## One-file backup (recommended)

Use the **Web UI** so your **Fetcher** (Sonarr/Radarr) and **Trimmer** (Emby) settings are in **one JSON file** (API keys included).

1. Open **Fetcher** in the browser (for example `http://127.0.0.1:8765` or dev port `8766`).
2. Go to **Settings**.
3. Under **Backup & Restore**, click **Download Backup**.
4. Keep the file **private** (same as a password manager export). The JSON includes **auth data** (e.g. password hash, session secret)—treat it like a **password**.

**Backup file format:** Timestamps inside the JSON (for example `exported_at` and `updated_at`) use **dd-mm-yyyy** style strings for readability. Older backups that used **ISO-8601** datetime strings still **import** correctly. **`format_version`: `1`** backups still import; current exports use **`format_version`: `2`** (removed obsolete global Arr columns from the schema).

### Restore on a new install

1. Install or start **Fetcher** on the new machine.
2. Open **Settings** → **Backup & Restore** → **Restore from Backup**.
3. Choose the `.json` file, turn on the **confirmation** toggle, click **Restore from Backup**.
4. **Restart** the **Fetcher** Windows service (or the app) so the scheduler reloads.

**Note:** Import replaces **settings** only (**Fetcher** and **Trimmer**). **Activity** history is **not** in the JSON file.

## Full database copy (optional)

To clone **everything** in the database (including **Activity** tables), **stop Fetcher** (so the DB is not locked), then copy `fetcher.db` to a safe place. To restore, stop **Fetcher** and replace `fetcher.db` with your copy.

## Git / source folder

Your **project** folder (clone of this repo) is separate from the **runtime** database. Backing up only the repo does **not** back up `fetcher.db` unless you copy it separately or use the JSON export above.
