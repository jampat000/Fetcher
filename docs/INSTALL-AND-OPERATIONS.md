# Installation, updates, and operations

This page complements the root **[README.md](../README.md)** with paths, env vars, and “what to do when something looks wrong.” It’s written for the **packaged Windows service** unless a section says otherwise.

---

## What gets installed where

- **Program Files** (typical): `Fetcher.exe`, WinSW / service wrapper, static pieces shipped by **`FetcherSetup.exe`**. Upgrading replaces these files.
- **ProgramData** (typical): **`fetcher.db`** (SQLite — settings, activity, job history, encrypted or plaintext API keys depending on config), and **`logs\fetcher.log`** (rotating application log). This is what you back up if you care about preserving state.

Override the data directory with **`FETCHER_DATA_DIR`** (machine env, service restart)—see **`service/README.md`**.

---

## First startup checklist

1. Install **`FetcherSetup.exe`**.
2. Start the **Fetcher** service. On first run, a stable JWT secret is created under **`%ProgramData%\Fetcher\machine-jwt-secret`** unless **`FETCHER_JWT_SECRET`** is already set (optional machine env override).
3. If you changed environment variables, restart the **Fetcher** Windows service.
4. Open **`http://127.0.0.1:8765`** and finish **setup** (password + at least one of Sonarr / Radarr / Emby configured to the app’s satisfaction).

Optional but recommended: set **`FETCHER_DATA_ENCRYPTION_KEY`** (Fernet) **before** you store production API keys, so new writes go encrypted. If you add it later, existing plaintext keys in the DB stay plaintext until re-saved from the UI (or you accept a one-time re-entry).

---

## Updates and migrations

- Run a newer **`FetcherSetup.exe`** over the existing install (or use **Settings → Software updates**). **ProgramData** is left in place; the app runs **SQLite migrations** on startup when the bundled schema version moves forward.
- **Database path, legacy detection, and auth expectations** after upgrade: see **[UPGRADE-AND-DATABASE.md](UPGRADE-AND-DATABASE.md)** (canonical path, packaged Windows checks, schema repair order).
- After upgrade: service **Running**, UI loads, **`/api/version`** matches what you installed.

**Upgrading from 3.0.x (Refiner Browse / companion era):**

| Location | What the installer does | If something is “left over” |
| --- | --- | --- |
| **`{app}`** (Program Files, application folder) | **Deletes** legacy **`FetcherCompanion.exe`** and companion **`.ps1`** files **before** copying the new build (`[InstallDelete]` / uninstall cleanup). | After upgrade, those filenames should be **gone**. If an old copy somehow remained, uninstall/reinstall or delete manually—Fetcher no longer ships or invokes them. |
| **Per-user profile** (Task Scheduler, Start Menu, Desktop shortcuts you added) | **No automatic removal.** Windows does not give a service installer reliable, safe access to every user’s tasks and shortcuts. | **Optional cleanup:** open **Task Scheduler** and remove tasks you created for the companion; remove **Start Menu** / Desktop shortcuts. **Harmless if ignored:** they may point at a removed binary or do nothing; they do not affect the current app. |

**Automation:** Refiner settings **`POST /refiner/settings/save`** uses form field names **`refiner_*`** only (see **CHANGELOG** `[Unreleased]`).

**Backup:** export **Settings → Backup & Restore** for a portable snapshot of the **`app_settings`** row (Sonarr/Radarr/Trimmer/Refiner/auth); the SQLite file remains the full source of truth including history tables. **JSON** must use **current format** (`format_version` **2**, **`fetcher_backup`**) and **`supported_schema_version`** = **`CURRENT_SCHEMA_VERSION`**. Anything else is **rejected** on import.

---

## Logs and health

- **Primary log:** `<database parent>/logs/fetcher.log` — default **`%ProgramData%\Fetcher\logs\fetcher.log`**.  
- **Override directory:** **`FETCHER_LOG_DIR`** (full path to a folder). Startup prints the resolved path in the log stream.
- **Health:** **`GET /healthz`** (no auth) for monitors. **`GET /api/version`** for build identity.

Wrapper logs under Program Files (`*.out.log` / `*.err.log`) are WinSW noise; the **Logs** page in the UI reads the **`FETCHER_LOG_DIR`** / default logs folder, not those.

---

## Environment variables (quick reference)

| Variable | Required? | Role |
| --- | --- | --- |
| **`FETCHER_JWT_SECRET`** | **Yes for dev/unfrozen** | Signs JWT access/refresh tokens. **Packaged:** if unset, **`machine-jwt-secret`** next to **`fetcher.db`** is used or created. Set this env var to override the file. |
| **`FETCHER_DATA_ENCRYPTION_KEY`** | No | Fernet key; when set, Arr API keys encrypted at rest in SQLite. When unset, plaintext + startup **warning**. |
| **`FETCHER_LOG_DIR`** | No | Directory for **`fetcher.log`**. |
| **`FETCHER_DATA_DIR`** | No | SQLite (and default logs) directory; see **`service/README.md`**. |
| **`FETCHER_DEV_DB_PATH`** | Dev only | Isolated DB for local **`scripts/dev-start.ps1`**. |
| **`FETCHER_RESET_AUTH`** | Recovery | **`1`** once to clear lockout—see **SECURITY.md**. |

Docker-specific vars are in **DOCKER.md**.

---

## Sonarr/Radarr failed-import cleanup (optional)

When enabled under **TV** / **Movies** settings, Fetcher deletes matching rows from Sonarr’s or Radarr’s **download queue** only. It calls the queue delete endpoint with **blocklist** on the first attempt and retries **without** blocklist if the first call fails. **Remove-from-client** is **not** requested. **Activity** shows an entry only when a delete actually succeeds.

---

## Common failure modes

- **Service starts then stops:** JWT resolution failed (check **`fetcher.log`** for the exact line), DB path locked by another process, or WinSW **`*.err.log`**. For JWT: ensure **`%ProgramData%\Fetcher\machine-jwt-secret`** is writable on first run, or set **`FETCHER_JWT_SECRET`** at machine scope.
- **“Encryption key” warning every start:** harmless if you accept plaintext keys; set **`FETCHER_DATA_ENCRYPTION_KEY`** to silence it and protect new writes.
- **Arr “connection failed”:** URL scheme/host/port, API key, and network path. Use in-app connection tests; confirm Sonarr/Radarr/Emby APIs are up outside Fetcher.

For auth lockout, see **SECURITY.md → Lockout recovery**.
