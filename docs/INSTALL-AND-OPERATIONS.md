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
2. Set **`FETCHER_JWT_SECRET`** (machine environment variable). **The service will not stay up without it.**
3. Restart the **Fetcher** Windows service.
4. Open **`http://127.0.0.1:8765`** and finish **setup** (password + at least one of Sonarr / Radarr / Emby configured to the app’s satisfaction).

Optional but recommended: set **`FETCHER_DATA_ENCRYPTION_KEY`** (Fernet) **before** you store production API keys, so new writes go encrypted. If you add it later, existing plaintext keys in the DB stay plaintext until re-saved from the UI (or you accept a one-time re-entry).

---

## Updates and migrations

- Run a newer **`FetcherSetup.exe`** over the existing install (or use **Settings → Software updates**). **ProgramData** is left in place; the app runs **SQLite migrations** on startup when the bundled schema version moves forward.
- After upgrade: service **Running**, UI loads, **`/api/version`** matches what you installed.

**Backup:** export **Settings → Backup & Restore** before major upgrades if you like a portable snapshot; the SQLite file is the full source of truth.

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
| **`FETCHER_JWT_SECRET`** | **Yes** | Signs JWT access/refresh tokens. Missing → process exits at startup. |
| **`FETCHER_DATA_ENCRYPTION_KEY`** | No | Fernet key; when set, Arr API keys encrypted at rest in SQLite. When unset, plaintext + startup **warning**. |
| **`FETCHER_LOG_DIR`** | No | Directory for **`fetcher.log`**. |
| **`FETCHER_DATA_DIR`** | No | SQLite (and default logs) directory; see **`service/README.md`**. |
| **`FETCHER_DEV_DB_PATH`** | Dev only | Isolated DB for local **`scripts/dev-start.ps1`**. |
| **`FETCHER_RESET_AUTH`** | Recovery | **`1`** once to clear lockout—see **SECURITY.md**. |

Docker-specific vars are in **DOCKER.md**.

---

## Common failure modes

- **Service starts then stops:** almost always missing **`FETCHER_JWT_SECRET`** or DB path locked by another process (e.g. dev server on the same file). Read **`fetcher.log`** or Event Viewer.
- **“Encryption key” warning every start:** harmless if you accept plaintext keys; set **`FETCHER_DATA_ENCRYPTION_KEY`** to silence it and protect new writes.
- **Arr “connection failed”:** URL scheme/host/port, API key, and network path. Use in-app connection tests; confirm Sonarr/Radarr/Emby APIs are up outside Fetcher.

For auth lockout, see **SECURITY.md → Lockout recovery**.
