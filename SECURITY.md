# Security policy

## Supported versions

We fix security issues in the **latest release** on the default branch (`master`). Use an up-to-date build from [Releases](https://github.com/jampat000/Grabby/releases).

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
- The Web UI and service run **locally by default**; if you expose Grabby to a network, use a **reverse proxy with TLS**, **firewall rules**, and strong **authentication** at the proxy boundary.
- **HTTP error lines** persisted in **Job history / logs** show the failing URL with **credential-like query parameters redacted** (for example Emby’s `api_key` on the query string). Still treat full log exports as sensitive.

## Threat model (what “secure enough” means here)

Grabby targets a **single trusted operator on the same machine** (or private LAN), with the HTTP server bound to **loopback** in the shipped service config. Industry guidance (e.g. OWASP) still applies, but the **acceptable risk** is different than for a multi-tenant internet app.

| Area | In the intended model (localhost / trusted LAN) | If you expose the API/UI to the internet or untrusted networks |
|------|--------------------------------------------------|------------------------------------------------------------------|
| **Access control** | No in-app login is assumed necessary. | **Do not** expose the app without **authentication** (e.g. at a reverse proxy) and narrow **firewall** rules. |
| **SSRF** | Setup / “test connection” endpoints request **URLs you supply** (Sonarr, Radarr, Emby, etc.). Abuse requires reaching those API routes. | High risk: an attacker could probe internal URLs. Keep Grabby **off** public networks or **block** those routes at the proxy. |
| **CSRF** | Forms use POST **without CSRF tokens**; risk is low when only you use the browser on the same origin. | If the UI is reachable cross-site from a malicious page while you’re “logged in” to nothing (no session), risk is still lower than cookie-based apps, but prefer **network isolation** or **proxy auth**. |
| **In-app upgrade** | **`POST /api/updates/apply`** downloads the release **`GrabbySetup.exe`** from GitHub and runs a **silent** Inno install (stops/restarts the Windows service). Treat like any admin installer: only use on **trusted networks**; do not expose the Web UI to the internet without proxy auth. Forks can set **`GRABBY_UPDATES_REPO`** (`owner/repo`). |
| **Injection** | Data access uses **SQLAlchemy** ORM/API for normal queries; migrations use fixed table names. | Keep dependencies updated (`pip-audit` in CI). |
| **Secrets in storage** | Keys live in **SQLite** and **backup JSON** (documented above). | Encrypt backups, restrict file permissions. |

## Default branch (`master`) on GitHub

Do not rely on local git habits alone: in GitHub **Settings → Branches** (or **Rules → Rulesets**), protect **`master`** with **required PR**, **required passing checks** (`Test / pytest`, `Security / pip-audit` — copy exact names from a PR), **no force-push**, and (for strongest posture) **don’t allow admins to bypass**.

Step-by-step checklist: **[`.github/BRANCH_PROTECTION.md`](.github/BRANCH_PROTECTION.md)**.

## CI security checks

- **pip-audit** runs against `requirements.txt` on pushes and PRs to `master` / `main`.  

## Supply chain

Prefer downloading **`GrabbySetup.exe`** from **official [GitHub Releases](https://github.com/jampat000/Grabby/releases)** only.




