# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [1.0.16] - 2026-03-20

### Added

- **Activity status tracking:** each activity event now records `Success` or `Failed` and surfaces failed run entries in Activity/Dashboard.

### Changed

- **Sonarr activity labels:** episode-level entries now prefer TV show name + episode code/title for clearer context.
- **Activity model/migration:** `activity_log` gains a `status` column (`ok`/`failed`) with backward-compatible migration.

## [1.0.15] - 2026-03-20

### Added

- **Activity detail logging:** per-run entries now include item-level context (movie titles, Sonarr episode labels, and Emby cleanup item names) instead of count-only summaries.

### Changed

- **Activity UI:** Dashboard and Activity pages now show detail lines under each event when available.
- **Data model/migration:** `activity_log` gains a `detail` text column with backward-compatible migration.

## [1.0.14] - 2026-03-20

### Changed

- **Cleaner -> Sonarr anti-boomerang:** after live TV deletes, Sonarr is now unmonitored at the **episode level** (`/api/v3/episode/monitor`) instead of whole-series unmonitor.
- **Matching logic:** TV delete candidates map from Emby to Sonarr using `Tvdb` first, then `title+year`; season/series deletes expand to all matching episode IDs.

## [1.0.13] - 2026-03-20

### Fixed

- **Schedules:** selecting all schedule days no longer reverts unexpectedly; schedule-day columns are stored as `TEXT` and migration widens legacy strict DB schemas.
- **Tests:** added regression coverage to ensure Grabby + Cleaner schedules stay enabled with all 7 days selected.

## [1.0.11] - 2026-03-20

### Fixed

- **Sonarr:** `grabby-missing` / `grabby-upgrade` tags now apply to **series** via `PUT /api/v3/series/editor` (Sonarr has no episode-level tag editor; the old path caused `HTTPStatusError`, often 404).

### Changed

- **Logs:** Sonarr/Radarr tag-apply warnings include **HTTP status, hint, and response snippet** when the API returns an error (`format_http_error_detail`).

## [1.0.10] - 2026-03-20

### Added

- **Settings → Software Updates:** **Check for Updates** button (explicit refresh; still auto-checks on load).

### Changed

- **Grabby Settings / Cleaner Settings:** scoped **Save … Settings** actions (Sonarr, Radarr, global Grabby; Cleaner global + content criteria for TV/Movies) so you do not have to save the whole page at once.
- **Cleaner Settings:** headings (**Emby Cleaner Settings**, **Global Cleaner Settings**, **Content Criteria Settings**) and layout aligned with those saves.

## [1.0.9] - 2026-03-20

### Added

- **Settings → Software updates:** checks **GitHub Releases** against the installed version; **Upgrade automatically** downloads `GrabbySetup.exe` and runs it **silently** (Windows installed build only). Optional env: `GRABBY_UPDATES_REPO`, `GRABBY_ALLOW_DEV_UPGRADE`, `GET /api/updates/check`, `POST /api/updates/apply`.

## [1.0.8] - 2026-03-20

### Added

- **Dashboard — Automation:** last run summary (time, OK/fail, short message) and **next scheduler tick** (interval + note about per-app schedule windows).
- **Cleaner Settings:** prominent **Dry run** vs **Live delete** banners; muted banner when **Emby Cleaner** is disabled.

### Changed

- **Naming:** user-facing **Emby cleanup** wording → **Emby Cleaner** (templates, messages, docs). Internal activity kind `cleanup` unchanged.
- **Reliability:** Sonarr/Radarr (**ArrClient**) and **Emby** HTTP calls use **retries with backoff** on transient errors (connection/timeouts, 429/502/503/504).
- **Logs / snapshots:** HTTP failures append short **hints** for common status codes (401/403/404, etc.).
- **CI:** removed CodeQL workflow for private-repo plan compatibility (keep `pytest` + `pip-audit`).
- **Docs:** `SECURITY.md`, branch-protection docs, and import JSON updated to require only supported checks (`Test / pytest`, `Security / pip-audit`).

## [1.0.7] - 2026-03-20

### Removed

- **Settings:** expandable “Setup wizard vs this page vs Cleaner” explainer (redundant); wizard “tip” on the final step that pointed to it.

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
- **Settings:** **Backup** download filename uses **dd-mm-yyyy**.

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

[Unreleased]: https://github.com/jampat000/Grabby/compare/v1.0.8...HEAD
[1.0.8]: https://github.com/jampat000/Grabby/compare/v1.0.7...v1.0.8
[1.0.7]: https://github.com/jampat000/Grabby/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/jampat000/Grabby/compare/v1.0.5...v1.0.6
[1.0.5]: https://github.com/jampat000/Grabby/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/jampat000/Grabby/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/jampat000/Grabby/releases/tag/v1.0.3




