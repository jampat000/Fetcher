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
.\.venv\Scripts\uvicorn app.main:app --host 127.0.0.1 --port 8765
```

Then open `http://127.0.0.1:8765`.

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

