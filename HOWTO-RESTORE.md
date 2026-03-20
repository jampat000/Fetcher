# Grabby: Backup & Restore

Grabby stores your **settings** (Sonarr, Radarr, Emby, API keys, schedules) in a **local SQLite database** under your profile:

- **Windows:** `%LOCALAPPDATA%\Grabby\app.db` (usually `C:\Users\You\AppData\Local\Grabby\app.db`)

## One-file backup (recommended)

Use the **Web UI** so your **Grabby** (Sonarr/Radarr) and **Cleaner** (Emby) settings are in **one JSON file** (API keys included).

1. Open **Grabby** in the browser (for example `http://127.0.0.1:8765` or dev port `8766`).
2. Go to **Settings**.
3. Under **Backup & Restore**, click **Download Backup**.
4. Keep the file **private** (same as a password manager export).

**Backup file format:** Timestamps inside the JSON (for example `exported_at` and `updated_at`) use **dd-mm-yyyy** style strings for readability. Older backups that used **ISO-8601** datetime strings still **import** correctly.

### Restore on a new install

1. Install or start **Grabby** on the new machine.
2. Open **Settings** → **Backup & Restore** → **Restore from Backup**.
3. Choose the `.json` file, check the confirmation box, click **Restore from Backup**.
4. **Restart** the **Grabby** Windows service (or the app) so the scheduler reloads.

**Note:** Import replaces **settings** only (**Grabby** and **Cleaner**). **Activity** history is **not** in the JSON file.

## Full database copy (optional)

To clone **everything** in the database (including **Activity** tables), **stop Grabby** (so the DB is not locked), then copy `app.db` to a safe place. To restore, stop **Grabby** and replace `app.db` with your copy.

## Git / source folder

Your **project** folder (clone of this repo) is separate from the **runtime** database. Backing up only the repo does **not** back up `app.db` unless you copy it separately or use the JSON export above.
