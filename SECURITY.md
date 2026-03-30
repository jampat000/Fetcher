# Security policy

## Supported versions

We fix security issues in the **latest release** on the default branch (`master`). Use an up-to-date build:

- **Windows:** **`FetcherSetup.exe`** from [Releases](https://github.com/jampat000/Fetcher/releases/latest) (or in-app **Settings → Software updates** when enabled).  
- **Docker:** pull **`ghcr.io/jampat000/fetcher:latest`** (or the image tag matching the release you run) — see **[`docs/DOCKER.md`](docs/DOCKER.md)**.

## Application environment (JWT and encryption)

These are **runtime** requirements for the Fetcher process (Windows service, Docker, or dev). Full install context is in **[`README.md`](README.md)** and **[`docs/INSTALL-AND-OPERATIONS.md`](docs/INSTALL-AND-OPERATIONS.md)**.

### `FETCHER_JWT_SECRET` (required)

- Used only to sign **JWT access and refresh tokens** for the JSON API (`HS256`).
- **If unset or empty, Fetcher exits on startup**—by design, with an error message that names the variable.
- Generate a long random secret; set it in the **machine** environment (Windows service) or container env, then restart.

### `FETCHER_DATA_ENCRYPTION_KEY` (optional)

- If set to a valid **Fernet** key, Sonarr/Radarr/Emby API keys in SQLite are written **encrypted** (`enc:v1:` prefix).
- If **not** set, those values remain **plaintext** in `fetcher.db`, and startup emits a **warning** explaining that—so operators are not misled into thinking encryption is on.
- Losing or changing the key after data is encrypted prevents decryption; back up the key with the same care as the database.

## Reporting a vulnerability

Please **do not** open a public issue for unfixed security problems.

- Open a **private security advisory** on GitHub (*Security → Advisories → Report a vulnerability*), or  
- Email the maintainer with a clear description, steps to reproduce, and impact.

We aim to acknowledge reports within a few days and coordinate disclosure after a fix.

## Secrets and sensitive data

- **Personal access tokens** used only for automation (e.g. GitHub API to set branch protection) should be **revoked** after use or kept in a password manager with **minimum scopes** (`repo` / **Administration** on this repo only for fine-grained PATs). Never commit tokens or paste them into issues/chat.
- **API keys** (Sonarr, Radarr, Emby, etc.) belong in the app **Settings** / database—not in git, logs, or screenshots you share publicly.  
- A **settings backup** (`.json` from **Settings** → **Backup & Restore**) contains the same secrets as the database—store it **encrypted** or **offline**; never commit it or post it publicly.  
- When sharing **logs** or **bug reports**, redact URLs, tokens, hostnames, and paths you consider private.  
- The **shipped WinSW service** listens on **`0.0.0.0`** (all interfaces) so you can open the Web UI from other PCs on your **LAN** (e.g. `http://YOUR-SERVER:8765`). The Web UI uses **username + password** (bcrypt-hashed in SQLite). Optional **IP allowlist** (Settings → Security → Access Control: explicit IPs/CIDRs via Python’s **`ipaddress`** module) can skip sign-in for listed clients only—do not rely on it if the port is reachable from untrusted networks. Prefer **`127.0.0.1`** or a **reverse proxy with TLS + auth** for remote access. Use **Windows Firewall** to limit who can reach the port.
- **HTTP error lines** persisted in **Job history / logs** show the failing URL with **credential-like query parameters redacted** (for example Emby’s `api_key` on the query string). Still treat full log exports as sensitive.

## Threat model (what “secure enough” means here)

Fetcher targets a **single trusted operator** on the **same machine** or a **private LAN**. The packaged service defaults to **`0.0.0.0`** for LAN access; use **`127.0.0.1`** in the service arguments if you want loopback-only. Industry guidance (e.g. OWASP) still applies, but the **acceptable risk** is different than for a multi-tenant internet app.

| Area | In the intended model (localhost / trusted LAN) | If you expose the API/UI to the internet or untrusted networks |
|------|--------------------------------------------------|------------------------------------------------------------------|
| **Access control** | Sign-in + session cookie (or optional IP allowlist if you configure it). | **Do not** expose the app to the internet without **extra** authentication (e.g. reverse proxy) and narrow **firewall** rules. |
| **SSRF** | Setup / “test connection” endpoints request **URLs you supply** (Sonarr, Radarr, Emby, etc.). Abuse requires reaching those API routes. | High risk: an attacker could probe internal URLs. Keep Fetcher **off** public networks or **block** those routes at the proxy. |
| **CSRF** | Authenticated **POST** forms include a signed **`csrf_token`** (see `app/auth.py`). The session cookie is **HttpOnly** and **SameSite=Lax**. For broader exposure still use **network isolation** or **proxy auth**. |
| **In-app upgrade** | **`POST /api/updates/apply`** downloads the release **`FetcherSetup.exe`** from GitHub and runs a **silent** Inno install (stops/restarts the Windows service). Treat like any admin installer: only use on **trusted networks**; do not expose the Web UI to the internet without proxy auth. Forks can set **`FETCHER_UPDATES_REPO`** (`owner/repo`). The **update check** calls GitHub’s API; if you see **403** (rate limits or policy), set **`FETCHER_GITHUB_TOKEN`** (or **`GITHUB_TOKEN`**) to a **read-only** PAT with minimal scope—never commit it. |
| **Injection** | Data access uses **SQLAlchemy** ORM/API for normal queries; migrations use fixed table names. | Keep dependencies updated (`pip-audit` in CI). |
| **Secrets in storage** | Keys live in **SQLite** and **backup JSON** (documented above). **Without `FETCHER_DATA_ENCRYPTION_KEY`, Arr API keys are plaintext in SQLite**; optional Fernet key encrypts them at rest. | Encrypt backups, restrict file permissions; set **`FETCHER_DATA_ENCRYPTION_KEY`** if disk exposure matters. |

## Access control

Fetcher’s **`get_client_ip()`** only substitutes the **`X-Forwarded-For`** header when the **direct TCP peer** is already a **private** or **loopback** address; that is meant for trusted local or LAN paths, not for proving identity on the public internet. If Fetcher is reachable **through a reverse proxy**, a client may be able to send a **forged `X-Forwarded-For`** and appear to match the **IP allowlist**, bypassing that check. **If you run Fetcher behind a reverse proxy, leave the IP allowlist empty** and use the **proxy’s own authentication** (and tight network rules) instead of relying on Fetcher’s allowlist for security.

## Default branch (`master`) on GitHub

Do not rely on local git habits alone: protect **`master`** with **required PRs**, **required passing checks** (`Test / pytest`, `Test / pip-audit` — copy exact names from a PR), **no force-push**, and rules that match how you work (**classic branch protection** or **repository rulesets**). Fetcher’s release workflows and docs assume **`master`** is the default branch.

Step-by-step checklist: **[`.github/BRANCH_PROTECTION.md`](.github/BRANCH_PROTECTION.md)** · ruleset JSON: **[`.github/IMPORT-BRANCH-PROTECTION.md`](.github/IMPORT-BRANCH-PROTECTION.md)**. Solo maintainers may use **0** required approvals with **restrict updates** so merges still go through PRs without a second human.

## CI security checks

- **pip-audit** (job **`pip-audit`** in **`.github/workflows/ci.yml`**, workflow name **Test**) runs against `requirements.txt` on pushes and PRs targeting **`master`**.  

## Lockout recovery

If you **forget the Fetcher Web UI password** (or lock yourself out), you can clear credentials using an environment variable **on the Windows service** (or dev process):

1. **Stop** the **Fetcher** Windows service (or stop the dev server).
2. Set **`FETCHER_RESET_AUTH=1`** in the service environment (WinSW / Services → Fetcher → Properties, or your service wrapper’s `<env>` block), or export it in the shell before `python -m app.cli` / `dev-start.ps1`.
3. **Start** Fetcher again. On startup the app logs a **WARNING**: `Auth credentials reset via FETCHER_RESET_AUTH. Visit /setup/0 to set a new password.` **`auth_username`** is reset to **`admin`** and **`auth_password_hash`** is cleared.
4. Open the Web UI (e.g. **`/`** or **`/setup`**) — until a new password is saved you are sent to **`/setup/0`** to choose a username and password (minimum **8** characters). **`/login`** also redirects there while no password is set.
5. **Stop** the service again, **remove** **`FETCHER_RESET_AUTH`** from the environment (do not leave it set), then **restart** the service normally.

Leaving **`FETCHER_RESET_AUTH=1`** enabled would clear credentials on **every** startup — remove it after recovery.

## Supply chain

Prefer downloading **`FetcherSetup.exe`** from **official [GitHub Releases](https://github.com/jampat000/Fetcher/releases)** only.




