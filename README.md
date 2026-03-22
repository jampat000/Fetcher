# Fetcher

**Never miss a release (and clean up old media).** — Windows Service + Web UI for **Sonarr**, **Radarr**, and **Emby**:

> **Why this exists:** This is a **vibe-coded** app — I had a concrete requirement (Arr automation + optional Emby cleanup on Windows, as a service, with a UI that matched how I work) and nothing public really fit. If it helps you, great; if not, no hard feelings. It’s shipped because *I* needed it to exist.


- Search for **missing** movies/episodes
- Re-trigger searches to **upgrade** existing items until the Arr app reports the **quality cutoff** is met (your Quality Profiles still decide what “better” means)
- Optionally run **Emby Trimmer rules** (dry-run supported) to delete old/low-rated content. The **Trimmer** tab opens right away; use **Scan Emby for matches** when you want to pull your library and see what fits your rules (big libraries can take a bit).

## Screenshots

These are **current UI** captures (Playwright against a local server). To refresh them after UI changes:

```powershell
pip install -r requirements-dev.txt
py -m playwright install chromium
$env:REGEN_README_SCREENSHOTS='1'
pytest tests/e2e/test_readme_screenshots.py -v
```

### Dashboard

![Fetcher Dashboard](docs/screenshots/dashboard.png)

### Settings

![Fetcher Settings](docs/screenshots/settings.png)

### Trimmer Settings

![Fetcher Trimmer Settings](docs/screenshots/trimmer-settings.png)

### Activity

![Fetcher Activity](docs/screenshots/activity.png)

## Download (Windows installer)

**[Download FetcherSetup.exe (latest GitHub Release)](https://github.com/jampat000/Fetcher/releases/latest/download/FetcherSetup.exe)**

**Version note:** **2.0.0** and **2.0.1** are still part of the 2.x line (rebrand + CI smoke hardening), but the first **GitHub Release** whose **Windows installer** was built and smoke-tested cleanly on Actions is **2.0.2** (fixed `uvloop` on Windows + stricter packaging script errors). Use **Latest** above for a known-good installer; details in [`CHANGELOG.md`](CHANGELOG.md).

- Requires **64-bit Windows**.
- The installer deploys **Fetcher** as a **Windows Service** (WinSW) and opens the Web UI when setup finishes.

## Install & first run

1. Run **`FetcherSetup.exe`** and complete the wizard (admin prompt is normal for a service).
2. Open **`http://127.0.0.1:8765`** on the server (default port), or **`http://<server-ip>:8765`** from another PC on your LAN if the service listens on all interfaces (default in `service/FetcherService.xml` is **`0.0.0.0`**). Allow **TCP 8765** in **Windows Defender Firewall** on the Fetcher machine if browsers on other devices cannot connect.
3. **First browser visit (new or upgraded install):** if no password is stored yet, any visit to the main UI sends you to **`/setup/0`** (**Setup step 1 of 6 — account**): choose username and password (minimum **8** characters). You are **not** left on Sign-in with no way forward. Continue through **Sonarr**, **Radarr**, **Emby**, **Schedule & timezone**, then the final **You’re all set** screen (or use **Skip** on later steps — not on account). After that, use **Sign in** at **`/login`** when your session expires (cookie lasts **7 days**).
4. You can return to **Setup** in the sidebar anytime, or use **Fetcher Settings** / **Trimmer Settings** for detail. **Trimmer** rules and scans are under **Trimmer** / **Trimmer Settings**.

**Security:** The Web UI is **password-protected** (bcrypt + signed session cookie). Optional **IP allowlist** (Settings → Security → Access Control) can skip sign-in for specific IPs/CIDRs—use only on trusted networks; see [`SECURITY.md`](SECURITY.md). **Forgot password?** See **Lockout recovery** in [`SECURITY.md`](SECURITY.md) (**`FETCHER_RESET_AUTH=1`** on the service).

**Upgrading** from a build **without** sign-in: after the update, your **first** Web UI visit goes to **account setup** (`/setup/0`). **Sonarr / Radarr / Emby** settings in the database are **unchanged**; you only add a password (and can re-run the rest of the wizard or use **Settings** as before).

Upgrading an existing install: **Settings → Software updates** can run the latest **`FetcherSetup.exe`** silently (Windows service install), or follow **[`service/UPGRADE.md`](service/UPGRADE.md)** for manual steps. The update check uses GitHub’s **REST API** first; if your IP hits **API rate limits** (403), Fetcher falls back to **github.com** (`/releases/latest` / Atom) so checks usually still work without a token. For heavy use or private repos, set **`FETCHER_GITHUB_TOKEN`** (read-only PAT) on that PC — see [`SECURITY.md`](SECURITY.md). Optional: **`FETCHER_UPDATES_CACHE_SECONDS`** (default **900**) controls how long a successful check is cached.

Version is shown in the sidebar of the Web UI (`v…` next to the clock). It matches the repo **`VERSION`** file or your **release tag** when built in CI.

### Monitoring / observability

- **`GET /healthz`** — JSON: `status`, `app`, **`version`** (use for load balancers / uptime checks). **No login required.**
- **`GET /api/version`** — JSON: `app`, **`version`** (lightweight automation). **No login required.**
- Logs go to the **process console**; when running under **WinSW**, see the service wrapper logs in `service/README.md`. For long-running hosts, configure **log rotation** at the OS or service-manager level if log files grow large.

## Security

See **[`SECURITY.md`](SECURITY.md)** (reporting issues, handling API keys, official downloads).

GitHub Actions runs **pip-audit** on dependencies for the default branch. **Protect `master`** in repo settings — see **[`.github/BRANCH_PROTECTION.md`](.github/BRANCH_PROTECTION.md)**.

## What’s in this repo

- `app/`: FastAPI web app + background scheduler
- `service/`: WinSW (Windows Service Wrapper) config for running the packaged app as a Windows service
- `installer/`: Inno Setup script to produce `FetcherSetup.exe`
- `VERSION`: current release version (semver) for the app + installer metadata
- **`config.example.yaml`** → copy to **`config.yaml`** (gitignored) to supply **Sonarr / Radarr / Emby API keys** without storing them in SQLite; optional **YAML values override** DB for outbound API calls (see **`app/config.py`** / **`app/resolvers/api_keys.py`**). Packaged builds look for **`config.yaml`** next to **`Fetcher.exe`** first.
- `docs/`: maintainer guides — **[public repo checklist](docs/PUBLIC-REPO-CHECKLIST.md)**, **[audit log after local checks](docs/PUBLIC-REPO-AUDIT.md)**

## License

This project is licensed under the **MIT License** — see [`LICENSE`](LICENSE).

## Contributing

**Pull requests** into **`master`** (branch protection + CI). See **[`CONTRIBUTING.md`](CONTRIBUTING.md)**.

## Backup & Restore

Export **Fetcher** and **Trimmer** settings to **one JSON file** from **Settings** → **Backup & Restore** (move PCs, reinstall, keep API keys). The backup includes **auth fields** (e.g. password hash, session secret)—treat the file like a **password**. Details: **[`HOWTO-RESTORE.md`](HOWTO-RESTORE.md)**.

## Changelog

See [`CHANGELOG.md`](CHANGELOG.md), including maintainer **Releasing** steps.

### Ship a new version to GitHub (maintainers)

You **do not** need a **`dev`** branch on GitHub — keep “dev” **local** (your machine, `dev-start.ps1`, temp DB). What GitHub needs for a **Release** is: a commit whose **`VERSION`** file is bumped → workflow creates **`vX.Y.Z`** → **Build installer** runs on that tag.

**Habit:** on a **named branch** (e.g. **`release/v1.0.40`** from **`origin/master`**), bump **`VERSION`** + **`CHANGELOG.md`**, commit, then:

**Date:** For each **`## [X.Y.Z] - YYYY-MM-DD`** line, use the machine **system date** at ship time (Windows PowerShell: **`Get-Date -Format yyyy-MM-dd`**). Do not guess the calendar day.

```powershell
.\scripts\ship-release.ps1
```

That **pushes your current branch** to **`origin`** and runs **`gh workflow run "Tag release (from VERSION)" --ref <that-branch>`**, which creates the tag (if missing) and starts the Windows build + Release. Requires **[GitHub CLI](https://cli.github.com/)** (`gh auth login`).

Pushing to **`master`** / **`main`** with a **`VERSION`** change can also auto-run the same workflow; **`ship-release.ps1`** is for release branches without waiting on a merge.

Merge **`release/v…` → `master`** via PR when you want the default branch updated (branch protection).

## Prereqs (dev)

- Python (via the `py` launcher)
- **SQLite 3.35+** (bundled with current CPython builds) — migrations use **`ALTER TABLE … DROP COLUMN`** to remove obsolete settings columns.
- **E2E tests** (`tests/e2e/`): install dev deps, then **once** download Chromium:  
  `pip install -r requirements-dev.txt` → `py -m playwright install chromium`  
  (GitHub Actions uses `playwright install --with-deps chromium` on Ubuntu.)

## Run locally (dev)

```powershell
cd C:\Users\User\fetcher
py -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
.\scripts\dev-start.ps1
```

Then open the URL printed by the script (default `http://127.0.0.1:8766`).

**Security card (Settings):** the **Security** block at the top of **Fetcher Settings** has an in-page **subnav** (Account, Change Username, Change Password, Access control). You can also deep-link while testing, e.g. **`http://127.0.0.1:8766/settings#security-password`** (use your dev port if different).

**Troubleshooting “Settings doesn’t work” in dev**

1. **URL:** Open the **exact** URL `dev-start.ps1` prints (usually **`http://127.0.0.1:8766`**). **`http://localhost:8766`** can fail (IPv6 vs IPv4) or use a **different cookie jar** than `127.0.0.1`, so you look “signed out” on Settings after logging in on the other host.
2. **Port:** **8766** = dev (this repo). **8765** = installed service — it will **not** show your local code edits.
3. **Auth:** Visiting **`/settings`** without a session sends you to **Sign in** with a **`next=`** return URL; after a successful login you should land back on **Settings**. If you always end up on **Setup**, your dev DB has **no password** yet — finish **step 1 (account)** on **`/setup/0`**, or delete `%TEMP%\fetcher-dev.sqlite3` and start clean.

**Development database:** `scripts/dev-start.ps1` sets **`FETCHER_DEV_DB_PATH`** to **`%TEMP%\fetcher-dev.sqlite3`** by default (`app/db.py`) so the dev server does not lock the same **`fetcher.db`** as the installed service. Use **`-SharedAppDb`** only when you intentionally want **`%LocalAppData%\Fetcher\fetcher.db`**—**stop the Fetcher service first** to avoid SQLite “database is locked” errors.

### Testing the Web UI in dev (auth + setup)

1. Start **`.\scripts\dev-start.ps1`** and open the printed URL (e.g. **`http://127.0.0.1:8766`**).
2. **Fresh dev DB** (default path above): you’ll land on **`/setup/0`** → **step 1 of 6** is **account** (set username + password, 8+ chars). Finish or skip later wizard steps; then browse the app (**you’ll be signed in** after setup until the session expires).
3. **Start over:** stop the server, delete **`%TEMP%\fetcher-dev.sqlite3`**, start again → new **Setup** from step 1.
4. **Lockout / forgot password in dev:** set environment variable **`FETCHER_RESET_AUTH=1`** for the **same** shell or process that runs uvicorn, start once, then **remove** it—see **[`SECURITY.md` → Lockout recovery](SECURITY.md)**.
5. **`pytest`** (unit/integration) uses a **separate temp database** and **overrides `require_auth`** and **`require_csrf`** in **`tests/conftest.py`** (tests marked **`real_csrf`** use the real CSRF dependency). Automated tests do **not** use your dev browser session. **E2E** (`tests/e2e`) may need a real login flow depending on how those tests are written.
6. **CSRF:** After sign-in, every protected form includes a hidden **`csrf_token`** (and **`<meta name="csrf-token">`** for **`getCSRFToken()`**). Reload stale tabs if a POST returns **403** (“Invalid or expired CSRF token”). Manual **`curl`** / API clients posting **`application/x-www-form-urlencoded`** must send that field too (copy from **View source** or the meta tag)—**JSON** endpoints such as **`POST /api/arr/search-now`** are unchanged.

### Port **8765** vs **8766** (Simple Browser)

| URL | What it is |
|-----|------------|
| **`http://127.0.0.1:8765`** | The **installed** Fetcher (**Windows service**). This is the packaged **`Fetcher.exe`** from your last **`FetcherSetup.exe`**. It does **not** pick up edits you make in the git repo. |
| **`http://127.0.0.1:8766`** (or whatever `dev-start.ps1` prints) | **Development** server running **source code** from this folder (`uvicorn`). Use this to see UI/code changes immediately. |

`dev-start.ps1` **stops whatever is listening** on the dev port (default **8766**) so you can keep using the same URL. If a process cannot be stopped (permissions / ghost listener), run **`.\scripts\dev-start.ps1 -TryElevatedKill`** once to get a **UAC** prompt, or use **Administrator** PowerShell: `Get-NetTCPConnection -LocalPort 8766 -State Listen | Stop-NetTCPConnection -Confirm:$false`.

**If you only ever open 8765:** rebuild with **`packaging\build.ps1`**, run a new **`FetcherSetup.exe`**, or use **Settings → Software updates** to get a release that includes the feature.

**To use port 8765 for dev** (same URL you’re used to): stop the **Fetcher** service in `services.msc`, then from the repo run  
`.\scripts\dev-start.ps1 -PreferredPort 8765`  
so nothing else is listening on 8765.

If **Simple Browser** still looks stuck after a change, reload the tab or open the page in Chrome/Edge.

### Browser smoke tests (optional)

```powershell
pip install -r requirements-dev.txt
py -m playwright install chromium
pytest tests/e2e -q
```

## Packaging (exe)

```powershell
cd C:\Users\User\fetcher
.\packaging\build.ps1 -Clean
```

The output executable will be placed under `dist/`.

## Service install (WinSW)

After building the exe, copy:

- `dist\Fetcher\Fetcher.exe` (name may vary based on spec)
- `service\FetcherService.xml`
- **Installer (`installer\build.ps1`):** WinSW is bundled as **`installer\bin\WinSW.exe`** and copied to **`service\winsw.exe`** (no download during build). **Manual service folder:** get `winsw.exe` separately; see **`service/README.md`**.

Then run (admin PowerShell):

```powershell
cd <folder-with-winsw-and-xml-and-exe>
.\winsw.exe install
.\winsw.exe start
```

## Installer (local build)

This builds a **single all-in-one installer EXE** that bundles the app + WinSW and installs/starts the Windows Service.

Prereq: install Inno Setup (so `ISCC.exe` exists), or pass **`-InstallInnoSetupIfMissing`** for a silent per-user install into `installer\_inno\`.

Build:

```powershell
cd C:\Users\User\fetcher
.\installer\build.ps1 -Clean -InstallInnoSetupIfMissing
# Optional explicit version for Inno metadata:
# .\installer\build.ps1 -Clean -InstallInnoSetupIfMissing -Version 1.2.3
```

Output: `installer\output\FetcherSetup.exe`

**Version resolution:** `-Version` if set → else **`GITHUB_REF_NAME`** on Actions when it looks like `v1.2.3` → else repo **`VERSION`** file → else `0.0.0-dev`.

### Optional: code signing (Authenticode)

To improve SmartScreen / enterprise trust, sign **`FetcherSetup.exe`** with a **code-signing certificate** (PFX):

**Locally (PowerShell):**

```powershell
$env:INSTALLER_SIGN_PFX = "C:\path\to\codesign.pfx"
$env:INSTALLER_SIGN_PASSWORD = "your-pfx-password"
.\scripts\sign-installer.ps1 -InstallerPath ".\installer\output\FetcherSetup.exe"
```

**GitHub Actions:** add repository **variable** `ENABLE_CODE_SIGNING` = `true` and **secrets** `WINDOWS_PFX_BASE64` (base64 of the PFX file bytes) and `WINDOWS_PFX_PASSWORD`. The **Build installer** workflow runs `scripts\sign-installer.ps1` after the compile step when the variable is set.

## CI (GitHub Actions)

- **Test**: **pytest** on **Ubuntu** for every push / PR (`.github/workflows/test.yml`).
- **Security**: **pip-audit** on `requirements.txt` (`.github/workflows/security.yml`).
- **Build installer**: on **Windows**, PyInstaller → Inno → **smoke test** (start `Fetcher.exe`, hit `/healthz`) → artifact — **only** when you push a **`v*`** tag or run the workflow **manually** (`workflow_dispatch`). Ordinary branch/PR pushes do **not** trigger it (saves minutes and notification noise). **Tags** `v*` also run the **Release** job (`.github/workflows/build-installer.yml`).

On **`v*`** tag push or **Actions → Build installer → Run workflow**, the job runs on `windows-latest` and uploads **`installer/output/FetcherSetup.exe`**.

- Open **Actions** → the run you care about → **Artifacts** → **FetcherSetup**.
- Pushing a **tag** matching `v*` (e.g. `v1.2.3`) **prepares** a release: the build finishes and uploads the artifact, then the **release** job **pauses** until someone approves it (see below). After approval, it creates/updates the **GitHub Release** and attaches `FetcherSetup.exe` (release notes use `.github/release.yml` categories when auto-generated).

### Approve before publishing a release 

So you can inspect the workflow / artifact before anything goes on the **Releases** page:

1. Repo **Settings** → **Environments** → create **`release`** (or open it after the first tagged run).
2. Under **Environment protection rules**, add **Required reviewers** (and optional wait timer).
3. Push tag `v*`: when the release job starts, GitHub shows **Review deployments**; approve there to publish.

This does **not** block `git push` itself—only the **release** step on GitHub. To produce an installer for a commit without tagging, use **Actions → Build installer → Run workflow** and pick the branch; or build locally with `.\installer\build.ps1`. **Note:** If you run the workflow against a **tag** ref, GitHub uses that **tag commit’s** workflow file — a tag on an **old** SHA can build but **not** publish **Releases**; see **CHANGELOG → Releasing** and **`docs/GITHUB-CLI.md`**.

### Dependency updates

[**Dependabot**](.github/dependabot.yml) opens weekly PRs for **pip** and **GitHub Actions** dependencies. 


