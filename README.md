# Fetcher

**Fetcher** is a **FastAPI** web app with a **glass-style** UI (**Inter**), a **SQLite** database, and a background scheduler. It ships as a **64-bit Windows service** (Inno Setup **`FetcherSetup.exe`**) and as an optional **Linux** image on **GitHub Container Registry** (**`ghcr.io/jampat000/fetcher`**). It automates **Sonarr** and **Radarr** searches and optionally applies **Emby Trimmer** rules (with dry-run support).

- Search for **missing** movies and episodes  
- Re-run **upgrade** searches until Arr reports **quality cutoff** is met (your Quality Profiles define “better”)  
- Optional **Emby Trimmer** to match and remove titles by rules — use **Trimmer** in the UI to scan and review  

**License:** [MIT](LICENSE) · **Security:** [SECURITY.md](SECURITY.md) · **Contributing:** [CONTRIBUTING.md](CONTRIBUTING.md)

## ⚡ Vibe coded

This is vibe coded.

I needed something like this, couldn’t find anything that did exactly what I wanted, and I can’t code — so I built it with a lot of help.

Not asking anyone to use it.

If you want to try it, go for it.

## Screenshots

| Dashboard | Settings |
| --- | --- |
| ![Dashboard](docs/screenshots/dashboard.png) | ![Settings](docs/screenshots/settings.png) |

| Trimmer settings | Activity |
| --- | --- |
| ![Trimmer settings](docs/screenshots/trimmer-settings.png) | ![Activity](docs/screenshots/activity.png) |

## Download (Windows)

**[FetcherSetup.exe (latest release)](https://github.com/jampat000/Fetcher/releases/latest/download/FetcherSetup.exe)**

- **64-bit Windows** only  
- Installs **Fetcher** as a **Windows service** and opens the web UI when setup finishes  

## Install and first run

1. Run **`FetcherSetup.exe`** (administrator prompt is normal).  
2. Open **`http://127.0.0.1:8765`** on the PC running the service, or **`http://<host>:8765`** from another device on your network. Allow **TCP 8765** through the firewall if needed.  
3. On first visit you complete **setup** (account, then Sonarr / Radarr / Emby and options). After that, sign in at **`/login`** when your session expires.  
4. Use **Fetcher settings** and **Trimmer** in the sidebar for ongoing configuration.  

### Required service environment variable (JWT signing)

Installed builds require **`FETCHER_JWT_SECRET`** at startup. Set it as a **persistent machine environment variable** (administrator PowerShell), then restart the service:

```powershell
[Environment]::SetEnvironmentVariable("FETCHER_JWT_SECRET","<your-32+char-random-secret>","Machine")
Restart-Service Fetcher
```

If this variable is missing, Fetcher intentionally fails fast on startup (no fallback signing secret).

### Logs (Windows service)

Application logs (exceptions, startup, Trimmer/settings saves, migration messages) are written to a **rotating file** next to your database:

- **Default:** `%ProgramData%\Fetcher\logs\fetcher.log` (and `fetcher.log.1`, … on rotation)
- **Override:** set machine env **`FETCHER_LOG_DIR`** to a folder path if you want logs elsewhere.

The web UI **Logs** page lists files from that same directory. WinSW may also write short **`*.out.log` / `*.err.log`** files under the install folder (Program Files); those are **wrapper-only** and can be removed on uninstall — they are not the main application log.

**Security:** Password-protected UI (bcrypt + session cookie). Optional **IP allowlist** for trusted networks. See **[`SECURITY.md`](SECURITY.md)** for reporting issues, API keys, downloads, and lockout recovery.  

**Updates:** **Settings → Software updates** can install a newer **`FetcherSetup.exe`**, or install manually from [Releases](https://github.com/jampat000/Fetcher/releases).  

## Docker (Linux / NAS / container hosts)

**Separate from Windows:** use **`FetcherSetup.exe`** above for the Windows service. Docker is an optional **Linux** image: build from this repo or pull from **GHCR** after each release (**Docker publish** runs with **Tag release**).

**[`docs/DOCKER.md`](docs/DOCKER.md)** — **`docker compose up -d --build`** from git, or:

```bash
docker pull ghcr.io/jampat000/fetcher:latest
```

Tags match **[Releases](https://github.com/jampat000/Fetcher/releases)** (e.g. **`v2.0.18`**). Open **`http://127.0.0.1:8765`**. The database persists in a volume under **`/data`**.

## Health checks (no login)

- **`GET /healthz`** — JSON status for uptime monitoring  
- **`GET /api/version`** — JSON app version  

## Repository layout

| Path | Purpose |
| --- | --- |
| `app/` | FastAPI app and scheduler |
| `service/` | Windows service (WinSW) configuration |
| `installer/` | Inno Setup → **`FetcherSetup.exe`** |
| `Dockerfile`, `docker-compose.yml` | Linux/container install (see **`docs/DOCKER.md`**) |
| `VERSION` | Release version (semver) |

Optional **`config.yaml`** next to **`Fetcher.exe`** can supply API keys instead of (or overriding) the database — see **`app/config.py`** / **`app/resolvers/api_keys.py`**. **`config.example.yaml`** is a template (copy to **`config.yaml`**, gitignored).

## Backup and restore

**Settings → Backup & Restore** exports Fetcher + Trimmer settings to one JSON file. Treat it as **sensitive** (includes auth material). More detail: **[`HOWTO-RESTORE.md`](HOWTO-RESTORE.md)**.

## Changelog

**[`CHANGELOG.md`](CHANGELOG.md)** — includes maintainer **Releasing** notes.

## More documentation

**[`docs/README.md`](docs/README.md)** — index of guides (Docker, **`gh`**, public repo, workspace).  
**[`HOWTO-RESTORE.md`](HOWTO-RESTORE.md)** — settings backup JSON (also linked from the index).

## Contributing

Pull requests are welcome against **`master`** (branch protection / rulesets apply — see **[`CONTRIBUTING.md`](CONTRIBUTING.md)**). That file covers workflow, tests, **Windows** and **Docker** release automation, and **`gh`** usage.

## License

**MIT** — see **[`LICENSE`](LICENSE)**.

## GitHub “About” (repository metadata)

These are **not** stored in git; set them under **Repository → ⚙ About** (or **Settings → General**) so the GitHub page matches the project.

| Field | Suggested value |
| --- | --- |
| **Description** | `Windows service + web UI for Sonarr/Radarr automation and optional Emby Trimmer. FastAPI, SQLite. Docker on GHCR.` |
| **Website** | `https://github.com/jampat000/Fetcher/releases/latest` |
| **Topics** | `sonarr`, `radarr`, `emby`, `fastapi`, `sqlite`, `windows-service`, `docker`, `automation`, `self-hosted` |

## Development (quick start)

```powershell
py -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
.\scripts\dev-start.ps1
```

Open the URL the script prints (default **`http://127.0.0.1:8766`**). The installed service uses **8765**; local dev uses a separate temp database so it does not lock the service DB. For tests, screenshots, packaging, and CI details, see **`CONTRIBUTING.md`** and **`requirements-dev.txt`**.
