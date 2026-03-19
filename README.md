# Grabby

**Never miss a release (and clean up old media).** — Windows Service + Web UI that integrates with **Sonarr**, **Radarr**, and **Emby** to:

- Search for **missing** movies/episodes
- Re-trigger searches to **upgrade** existing items until the Arr app reports the **quality cutoff** is met (your Quality Profiles still decide what “better” means)
- Optionally run **Emby cleanup rules** (dry-run supported) to delete old/low-rated content

## What’s in this repo

- `app/`: FastAPI web app + background scheduler
- `service/`: WinSW (Windows Service Wrapper) config for running the packaged app as a Windows service
- `installer/`: Inno Setup script (optional) to produce a friendly Windows installer

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

## Installer (optional)

This builds a **single all-in-one installer EXE** that bundles the app + WinSW and installs/starts the Windows Service.

Prereq: install Inno Setup (so `ISCC.exe` exists).

Build:

```powershell
cd C:\Users\User\grabby
.\installer\build.ps1 -Clean
```

Output: `installer\output\GrabbySetup.exe`

## CI (GitHub Actions)

On **push** (any branch), **pull requests**, and **manual run** (`workflow_dispatch`), the workflow **Build installer** runs on `windows-latest`: PyInstaller bundle → WinSW → Inno Setup → **`installer/output/GrabbySetup.exe`**.

- Open **Actions** → latest run → **Artifacts** → download **GrabbySetup** (good for PR / branch review before merge).
- Pushing a **tag** matching `v*` (e.g. `v1.2.3`) **prepares** a release: the build finishes and uploads the artifact, then the **release** job **pauses** until someone approves it (see below). After approval, it creates/updates the **GitHub Release** and attaches `GrabbySetup.exe` (release notes are auto-generated).

### Approve before publishing a release

So you can inspect the workflow / artifact before anything goes on the **Releases** page:

1. Repo **Settings** → **Environments** → create **`release`** (or open it after the first tagged run).
2. Under **Environment protection rules**, add **Required reviewers** (and optional wait timer).
3. Push tag `v*`: when the release job starts, GitHub shows **Review deployments**; approve there to publish.

This does **not** block `git push` itself—only the **release** step on GitHub. To confirm locally before any push, use your usual branch/PR review; the PR build artifact is the installer for that commit.

