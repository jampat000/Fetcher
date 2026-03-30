# Clean build and release (Windows installer + Docker)

## Before any release build

1. **Delete generated outputs** (do not commit these; `.gitignore` blocks them):

   ```powershell
   Remove-Item -Recurse -Force dist, build, installer\output -ErrorAction SilentlyContinue
   Remove-Item -Force *.zip, *.sha256, FetcherSetup.exe -ErrorAction SilentlyContinue
   ```

2. Confirm nothing stale remains: no `dist/`, `build/`, `installer/output/`, root `*.zip` / `*.sha256`.

## Windows: `FetcherSetup.exe` (PyInstaller + Inno Setup)

From repo root (same as CI `build-installer` workflow):

- **FFmpeg:** Set `FETCHER_FFMPEG_BIN_DIR` to a folder containing `ffmpeg.exe` and `ffprobe.exe`, **or** install both on `PATH`, **or** for a quick local test only use `packaging\build.ps1 -AllowPathFallback` (Refiner in the frozen app may then rely on PATH).

```powershell
.\installer\build.ps1 -Clean -InstallInnoSetupIfMissing
```

- **`-Clean`** removes `installer\output`, `dist`, and `build` first.
- **Output:** `installer\output\FetcherSetup.exe` (do not commit).
- **Bundled UI:** only `app/templates` and `app/static` from this repo (`packaging/fetcher.spec` `datas=`).

PyInstaller-only (no installer):

```powershell
.\packaging\build.ps1 -Clean
# dist\Fetcher\Fetcher.exe
```

### Verify the frozen folder (before Inno / shipping)

- Under `dist\Fetcher\` you should see only `Fetcher.exe` and `_internal\`.
- UI files should match repo `app\templates` and `app\static` under `_internal\app\…`.
- There should be no `tests\`, `docs\`, `packaging\`, root-level `*.zip` / `*.sha256`, or PyInstaller `build\` mixed into `dist\`.

On **Windows PowerShell 5.1**, avoid piping the build script (`*>` into `Out-File` / `Tee-Object`): `pip` writes notices to stderr and the pipeline can surface that as a terminating error. Run `.\packaging\build.ps1` directly, or capture logs another way.

## Docker (Linux image)

Context must not include `dist/` or `build/` (see `.dockerignore`). From repo root:

```bash
docker build -t fetcher:local .
```

- **Output:** image only; no zip committed.
- **App code:** `COPY app ./app` — current source only.

## What must never be committed

- `dist/`, `build/`, `installer/output/`
- `FetcherSetup.exe`, `*.zip`, `*.sha256`
- `packaging/ffmpeg-bin/`, `packaging/ffmpeg-download/` (local staging)

## Canonical version

- **Single source:** repo-root **`VERSION`** (semver `X.Y.Z`, one line).
- **Read at runtime:** `app/version_info.get_app_version()` (dev: file on disk; frozen: bundled `VERSION` via PyInstaller `datas`).
- **Installer:** `installer\build.ps1` / CI read **`VERSION`** for **Inno** `MyAppVersion`.
- **Docker tags:** **`docker-publish.yml`** tags images from **`VERSION`** at the checked-out ref (`:X.Y.Z`, `:X.Y`, `:latest` on release).
- **Git tag:** **`v` + contents of `VERSION`** (created by **Tag release**).

## Canonical GitHub Actions (one map)

| Workflow | Role |
|----------|------|
| **Test** (`ci.yml`) | **Canonical CI:** **`pytest`** on all pushes/PRs; **`pip-audit`** + **`docker build`** verify on **`master`** and PRs **into** **`master`** (plus **`workflow_dispatch`**). **Not** a release producer. |
| **Tag release (from VERSION)** | On **`VERSION`** change merged to **`master`**, or manual/`ship-release.ps1`: creates **`vX.Y.Z`** if missing, dispatches **Build installer** (ref = tag) and **Docker publish** (ref = default branch, `checkout_ref` = tag). |
| **Build installer** | PyInstaller + Inno → **`FetcherSetup.exe`**; uploads artifact; on **tag** ref, **release** job attaches **`FetcherSetup.exe`** to GitHub Releases. |
| **Docker publish** | Build + push image to **GHCR**; same **`VERSION`**-based tags as above. |

**Recovery / manual runs:** see **`CHANGELOG.md` → Releasing** and **`docs/GITHUB-CLI.md`** (installer ref trap vs Docker `checkout_ref`).

**Local ship helper:** `.\scripts\ship-release.ps1` — push + dispatch **Tag release** from your current branch.

## CI implementation note

**Build installer** on GitHub runs `installer\build.ps1 -Clean -InstallInnoSetupIfMissing` with FFmpeg downloaded and pinned in the workflow env. Same entrypoint as a full local release build when FFmpeg is staged.

## GitHub Releases (what belongs on each tag)

- **Canonical Windows asset:** **`FetcherSetup.exe`** only — this is what **Build installer** attaches via the **release** job.
- **Avoid** extra **`Fetcher-v*-windows-dist.zip`** / **`.sha256`** files on the same release unless you document a separate consumer; they duplicate the Windows install surface and confuse **Latest**.
- After a release ships, keep **`master`** aligned: **`VERSION`** on the default branch should match the line you intend to maintain (merge the release PR, or bump **`VERSION`** on **`master`** in a follow-up PR) so **Tag release (from VERSION)** and docs stay honest.
