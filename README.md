# Grabby

**Never miss a release (and clean up old media).** — Windows Service + Web UI that integrates with **Sonarr**, **Radarr**, and **Emby** to:

- Search for **missing** movies/episodes
- Re-trigger searches to **upgrade** existing items until the Arr app reports the **quality cutoff** is met (your Quality Profiles still decide what “better” means)
- Optionally run **Emby cleanup rules** (dry-run supported) to delete old/low-rated content

## Download (Windows installer)

**[Download GrabbySetup.exe (latest GitHub Release)](https://github.com/jampat000/Grabby/releases/latest/download/GrabbySetup.exe)**

- Requires **64-bit Windows**.
- The installer deploys **Grabby** as a **Windows Service** (WinSW) and opens the Web UI when setup finishes.

## Install & first run

1. Run **`GrabbySetup.exe`** and complete the wizard (admin prompt is normal for a service).
2. Open **`http://127.0.0.1:8765`** in your browser (default service port).
3. Go to **Settings** and add your **Sonarr**, **Radarr**, and/or **Emby** URLs and API keys.

Version is shown in the sidebar of the Web UI (`v…` next to the clock). It matches the repo **`VERSION`** file or your **release tag** when built in CI.

### Monitoring / observability

- **`GET /healthz`** — JSON: `status`, `app`, **`version`** (use for load balancers / uptime checks).
- **`GET /api/version`** — JSON: `app`, **`version`** (lightweight automation).
- Logs go to the **process console**; when running under **WinSW**, see the service wrapper logs in `service/README.md`. For long-running hosts, configure **log rotation** at the OS or service-manager level if log files grow large.

## Security

See **[`SECURITY.md`](SECURITY.md)** (reporting issues, handling API keys, official downloads).

GitHub Actions runs **pip-audit** on dependencies and **CodeQL** on Python code for the default branch.

## What’s in this repo

- `app/`: FastAPI web app + background scheduler
- `service/`: WinSW (Windows Service Wrapper) config for running the packaged app as a Windows service
- `installer/`: Inno Setup script to produce `GrabbySetup.exe`
- `VERSION`: current release version (semver) for the app + installer metadata

## License

This project is licensed under the **MIT License** — see [`LICENSE`](LICENSE).

## Backup & restore

Export **all settings** to **one JSON file** from **Settings → Backup & restore** (move PCs, reinstall, keep API keys). Details: **[`HOWTO-RESTORE.md`](HOWTO-RESTORE.md)**.

## Changelog

See [`CHANGELOG.md`](CHANGELOG.md).

## Prereqs (dev)

- Python (via the `py` launcher)

## Run locally (dev)

```powershell
cd C:\Users\User\grabby
py -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
.\scripts\dev-start.ps1
```

Then open the URL printed by the script (default `http://127.0.0.1:8766`).

Why `8766` by default: the installed Windows service app often runs on `8765`, which can hide source-code UI changes in Simple Browser.

If **Simple Browser** (or another embedded preview) still looks stuck after an update, use **Open in browser** / Chrome/Edge, or run **Developer: Reload Webview** / reload the Simple Browser tab so it picks up new `app.js`.

## Packaging (exe)

```powershell
cd C:\Users\User\grabby
.\packaging\build.ps1 -Clean
```

The output executable will be placed under `dist/`.

## Service install (WinSW)

After building the exe, copy:

- `dist\Grabby\Grabby.exe` (name may vary based on spec)
- `service\GrabbyService.xml`
- `service\winsw.exe` (download separately; see `service/README.md`)

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
cd C:\Users\User\grabby
.\installer\build.ps1 -Clean -InstallInnoSetupIfMissing
# Optional explicit version for Inno metadata:
# .\installer\build.ps1 -Clean -InstallInnoSetupIfMissing -Version 1.2.3
```

Output: `installer\output\GrabbySetup.exe`

**Version resolution:** `-Version` if set → else **`GITHUB_REF_NAME`** on Actions when it looks like `v1.2.3` → else repo **`VERSION`** file → else `0.0.0-dev`.

### Optional: code signing (Authenticode)

To improve SmartScreen / enterprise trust, sign **`GrabbySetup.exe`** with a **code-signing certificate** (PFX):

**Locally (PowerShell):**

```powershell
$env:INSTALLER_SIGN_PFX = "C:\path\to\codesign.pfx"
$env:INSTALLER_SIGN_PASSWORD = "your-pfx-password"
.\scripts\sign-installer.ps1 -InstallerPath ".\installer\output\GrabbySetup.exe"
```

**GitHub Actions:** add repository **variable** `ENABLE_CODE_SIGNING` = `true` and **secrets** `WINDOWS_PFX_BASE64` (base64 of the PFX file bytes) and `WINDOWS_PFX_PASSWORD`. The **Build installer** workflow runs `scripts\sign-installer.ps1` after the compile step when the variable is set.

## CI (GitHub Actions)

- **Test**: **pytest** on **Ubuntu** for every push / PR (`.github/workflows/test.yml`).
- **Security**: **pip-audit** on `requirements.txt` (`.github/workflows/security.yml`).
- **CodeQL**: Python static analysis (`.github/workflows/codeql.yml`), weekly schedule + on push/PR to `master` / `main`.
- **Build installer**: on **Windows**, PyInstaller → Inno → **smoke test** (start `Grabby.exe`, hit `/healthz`) → artifact; **tags** `v*` also publish a **Release** (`.github/workflows/build-installer.yml`).

On **push** (any branch), **pull requests**, and **manual run** (`workflow_dispatch`), **Build installer** runs on `windows-latest`: PyInstaller bundle → WinSW → Inno Setup → **`installer/output/GrabbySetup.exe`**.

- Open **Actions** → latest run → **Artifacts** → download **GrabbySetup** (good for PR / branch review before merge).
- Pushing a **tag** matching `v*` (e.g. `v1.2.3`) **prepares** a release: the build finishes and uploads the artifact, then the **release** job **pauses** until someone approves it (see below). After approval, it creates/updates the **GitHub Release** and attaches `GrabbySetup.exe` (release notes use `.github/release.yml` categories when auto-generated).

### Approve before publishing a release

So you can inspect the workflow / artifact before anything goes on the **Releases** page:

1. Repo **Settings** → **Environments** → create **`release`** (or open it after the first tagged run).
2. Under **Environment protection rules**, add **Required reviewers** (and optional wait timer).
3. Push tag `v*`: when the release job starts, GitHub shows **Review deployments**; approve there to publish.

This does **not** block `git push` itself—only the **release** step on GitHub. To confirm locally before any push, use your usual branch/PR review; the PR build artifact is the installer for that commit.

### Dependency updates

[**Dependabot**](.github/dependabot.yml) opens weekly PRs for **pip** and **GitHub Actions** dependencies.
