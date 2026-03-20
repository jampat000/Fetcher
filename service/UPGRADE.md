# Upgrading the Grabby Windows service

Use this when you have a **new build** (e.g. from GitHub Releases) and want the **installed service** on **`127.0.0.1:8765`** to match what you run in development (**`8766`** in `scripts/dev-start.ps1` is only for local source runs).

**Easiest (GUI):** Web UI → **Settings** → **Software updates** → **Upgrade automatically** (downloads `GrabbySetup.exe` and runs it silently; service restarts). Same end result as Option A.

## Before you start

- Grab the new **`GrabbySetup.exe`** from [Releases](https://github.com/jampat000/Grabby/releases/latest), **or** build locally with `packaging/build.ps1` and use the output folder.
- Optionally export settings: Web UI → **Settings** → **Backup & Restore** (see [`HOWTO-RESTORE.md`](../HOWTO-RESTORE.md)).

## Option A — Re-run the installer (simplest)

1. **Stop** the **Grabby** service (Services.msc → Grabby → Stop), or from an elevated prompt:  
   `sc stop Grabby`
2. Run the new **`GrabbySetup.exe`** and complete the wizard. It should replace **`Grabby.exe`** and the **`_internal`** folder next to it (typical install dir: `C:\Program Files\Grabby`).
3. **Start** the service again:  
   `sc start Grabby`
4. Open **`http://127.0.0.1:8765`** and confirm the sidebar version matches the release.

## Option B — Manual file swap

1. **Stop** the **Grabby** service (`sc stop Grabby`).
2. Under the install directory (same folder as **`Grabby.exe`**):
   - Replace **`Grabby.exe`**
   - Replace the entire **`_internal`** directory from the new build (do not mix old/new `_internal` with a new exe).
3. If you use **WinSW** and only the wrapper XML changed, merge edits from [`GrabbyService.xml`](GrabbyService.xml) (e.g. `--host` / `--port`) into your live XML, then `Grabby.exe restart` or restart the service from Services.
4. **Start** the service: `sc start Grabby`.

## After upgrading

- If schedules behave oddly after a big jump in versions, **restart** the service once.
- Dev port **`8766`** vs service **`8765`**: both are normal; the service uses the arguments in `GrabbyService.xml` / installer defaults.

See also **[`service/README.md`](README.md)** for logs and WinSW troubleshooting.
