# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **Contributing / governance:** [`CONTRIBUTING.md`](CONTRIBUTING.md); [`.github/BRANCH_PROTECTION.md`](.github/BRANCH_PROTECTION.md), [`.github/IMPORT-BRANCH-PROTECTION.md`](.github/IMPORT-BRANCH-PROTECTION.md), [`.github/rulesets/master-middle-ground.json`](.github/rulesets/master-middle-ground.json), [`.github/branch-protection-classic-master.json`](.github/branch-protection-classic-master.json); PR template; [`.github/CODEOWNERS`](.github/CODEOWNERS); [`scripts/protect-master-branch.ps1`](scripts/protect-master-branch.ps1).
- **Log hygiene:** [`app/log_sanitize.py`](app/log_sanitize.py) redacts credential-like query params (and userinfo) from URLs before persisting HTTP error lines in job logs; [`tests/test_log_sanitize.py`](tests/test_log_sanitize.py).

### Changed

- [`SECURITY.md`](SECURITY.md): PAT hygiene, threat model, default-branch notes; [`README.md`](README.md): contributing + branch protection pointer; **Dependabot** dependency PRs labeled `dependencies`.
- **Dependencies:** `python-multipart` **0.0.22** (CVE-2026-24486), `starlette` **0.52.1** (CVE-2025-54121, CVE-2025-62727), `fastapi` **0.129.2** (compatible Starlette range).

## [1.0.6] - 2026-03-22

### Added
- **First-run setup wizard** (`/setup`): guided steps for Sonarr, Radarr, Emby (with **Test connection** via JSON API), schedule interval & timezone; final **Next steps** screen with links to Grabby Settings, Cleaner Settings, and Cleaner.
- **Setup** sidebar entry; dashboard CTA when no stack URLs are configured; **dismissible** dashboard banners (stored in `localStorage`).
- **API:** `POST /api/setup/test-sonarr`, `test-radarr`, `test-emby` for wizard tests.
- **Cleaner:** default **`GET /cleaner`** no longer scans Emby (fast sidebar); use **`Scan Emby for matches`** (`/cleaner?scan=1`; legacy `?preview=1` still accepted).
- **Service upgrades:** [`service/UPGRADE.md`](service/UPGRADE.md) for replacing the Windows install / exe.
- Playwright **E2E smoke tests** ([`tests/e2e/`](tests/e2e/)) against a live uvicorn process (`healthz`, setup step 1, Cleaner page).
- **Settings:** expandable **Setup wizard vs this page vs Cleaner** guide; **Backup** download filename uses **dd-mm-yyyy**.

### Changed
- **Backup JSON:** human-readable **dd-mm-yyyy** datetime strings (`exported_at`, settings columns); **ISO-8601 strings from older backups still import**.
- **Dates in UI:** sidebar clock, activity, and logs use **dd-mm-yyyy**-style display.
- **FastAPI:** **lifespan** context for startup/shutdown (replaces deprecated `@app.on_event`).
- **Templates:** **Starlette**-style `TemplateResponse(request, name, context)` (no deprecation warning).
- **`datetime.utcnow()`** replaced with **`utc_now_naive()`** ([`app/time_util.py`](app/time_util.py)) for ORM and scheduler use.
- **CI Test workflow:** install **Playwright Chromium** before `pytest`.
- **Build installer workflow:** runs on **`v*`** tags and **manual** `workflow_dispatch` only (no longer on every branch/PR push).
- **Backup & Restore:** one JSON file for all **Grabby** and **Cleaner** settings; export metadata `includes` clarifies scope.
- **`/healthz`** includes **`version`**; **`GET /api/version`** added.
- Windows CI smoke: start packaged **`Grabby.exe`**, probe **`/healthz`**.
- **pip-audit** (`security.yml`), **CodeQL** (`codeql.yml`); **`SECURITY.md`**.

## [1.0.5] - 2026-03-21

### Added
- `VERSION` file for app, installer metadata, and Web UI sidebar version.
- `LICENSE` (MIT), `CHANGELOG.md`, Dependabot (pip + Actions), `.github/release.yml`.
- CI **Test** workflow (`pytest` on Ubuntu); optional installer **Authenticode** signing (`scripts/sign-installer.ps1`).
- `RunOnceId` on Inno `[UninstallRun]` entries.

### Changed
- README: download link, install/first-run, signing and CI docs.
- Installer reads version from `-Version`, `GITHUB_REF_NAME` (`v*`), or `VERSION`.

## [1.0.4] - 2025-03-20

### Changed
- CI: dedupe concurrent installer builds for the same commit.
- Installer `AppVersion` follows `VERSION` / release tag.
- Tracked `packaging/grabby.spec` so GitHub Actions can build the installer.

## [1.0.3] - 2025-03-20

### Fixed
- PyInstaller/Inno CI failure: `grabby.spec` was gitignored and missing on runners.

## Releasing (maintainers)

1. Update this file: move **`[Unreleased]`** items under a new **`[X.Y.Z] - YYYY-MM-DD`** heading, then keep **`[Unreleased]`** empty (or note pending work).
2. Bump **`VERSION`** to match the release.
3. Commit; tag **`vX.Y.Z`**; push commits **and** tags (installer/release workflows often key off **`v*`**).
4. Follow **GitHub Actions** / environment rules for approving production releases if configured.

[Unreleased]: https://github.com/jampat000/Grabby/compare/v1.0.6...HEAD
[1.0.6]: https://github.com/jampat000/Grabby/compare/v1.0.5...v1.0.6
[1.0.5]: https://github.com/jampat000/Grabby/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/jampat000/Grabby/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/jampat000/Grabby/releases/tag/v1.0.3
