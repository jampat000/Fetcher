# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [1.0.41] - 2026-03-20

### Changed

- **Sidebar:** Navigation uses **Lucide** icons (`data-lucide`) loaded from jsDelivr; replaces Unicode glyph placeholders. Related **`.sidebar-icon`** sizing in **`app.css`**.
- **Dashboard:** Sonarr/Radarr overview panels share **`app/templates/macros/arr_panel.html`** to reduce duplicated template markup.
- **Sign-in:** Login layout styles moved from inline attributes to **`.login-wrap`**, **`.login-card`**, and related classes in **`app/static/app.css`**.
- **CI / releasing:** **`scripts/ship-release.ps1`** — ASCII hyphen in error message so **PowerShell** parses the script reliably on all locales/encodings.

## [1.0.40] - 2026-03-22

### Changed

- **CI / releasing:** **`scripts/ship-release.ps1`** — push your current branch and dispatch **Tag release (from VERSION)**; auto-tag on push only for **`master`** / **`main`** (no remote **`dev`** required). Includes merge of **#55** so the packaged build matches **`master`** tip.

## [1.0.39] - 2026-03-24

### Added

- **Database pruning:** **`prune_old_records`** during scheduled/Grabby runs removes stale **`arr_action_log`** rows (window from **`arr_search_cooldown_minutes`**, or 48h when cooldown is 0) and **`activity_log`**, **`job_run_log`**, **`app_snapshot`** rows older than **`log_retention_days`** (clamped 7–3650). Failures are logged only. Unit tests: **`tests/test_pruning.py`**.

### Security

- **CSRF protection** for state-changing **HTML form** POSTs: signed tokens (**`itsdangerous.TimestampSigner`**, 1-hour validity) bound to the session user (or IP-allowlist account). **`require_csrf`** on **`/settings`**, **`/settings/auth/*`**, **`/settings/backup/import`**, **`/emby/settings`**, **`/emby/settings/connection`**, **`/emby/settings/cleaner`**, **`/test/sonarr`**, **`/test/radarr`**, **`/test/emby`**, **`/test/emby-form`**, and **`POST /setup/{step}`** for steps **1–5** (step **0** exempt). Excludes **`/login`**, JSON APIs (**`/api/arr/search-now`**, **`/api/setup/test-*`**), and wizard **`fetch()`** tests. Layout **`<meta name="csrf-token">`**, global **`getCSRFToken()`**, hidden fields on templates; **`tests/test_csrf.py`** with **`real_csrf`** marker; other tests override **`require_csrf`** in **`tests/conftest.py`**.

### Changed

- **CI:** **Tag release (from VERSION)** also runs when **`VERSION`** changes on branch **`dev`** (creates **`vX.Y.Z`** and dispatches **Build installer**), so **“bump and ship”** can target **`dev`** without waiting on **`master`**.
- **README (dev):** Document **`require_csrf`** test override and CSRF behavior for manual POSTs.

## [1.0.38] - 2026-03-23

### Added

- **`tests/test_auth_next_redirect.py`:** Covers **`sanitize_next_param`**, login **`next`** redirect, and open-redirect rejection.

### Changed

- **Settings → Security:** In-page **subnav** (Account, **Change Username**, **Change Password**, Access control) with fragment anchors; card layout for account vs access control; headings and copy state that username and password are changed separately.
- **Sign-in:** Unauthenticated visits redirect to **`/login?next=…`** (safe, same-origin paths only) so after login you return to the requested page (e.g. **Settings**).
- **POST `/settings`**, **`POST /emby/settings`**, **`POST /emby/settings/connection`**, **`POST /emby/settings/cleaner`:** **`SQLAlchemyError`** → **`save=fail&reason=db_error`**; **`ValueError`** → **`reason=invalid`**; other exceptions → **`logger.exception`**, session **rollback**, **`reason=error`**; **`GRABBY_LOG_LEVEL=DEBUG`** re-raises after logging.

### Fixed

- **`scripts/dev-start.ps1`:** PowerShell parse errors from **`[...]`** inside double-quoted strings (brackets escaped or single-quoted).

### Documentation

- **README** + **`scripts/dev-start.ps1`:** **`127.0.0.1` vs `localhost`**, dev vs service port, **Settings** auth troubleshooting.

## [1.0.37] - 2026-03-22

### Fixed

- **Setup wizard:** After saving the account (step 0), the response now sets the **session cookie** so **Test connection** on Sonarr/Radarr/Emby steps can call **`/api/setup/test-*`** (those routes require auth).

### Changed

- **Settings → Security:** Clearer layout—**Account** (signed-in line, username then password forms), **Access Control** last with its own intro; removed duplicate “current username” line above the username field.

## [1.0.36] - 2026-03-22

### Changed

- **Access control:** Replaced **Bypass auth on local LAN** with an explicit **`auth_ip_allowlist`** (newline-separated IPs/CIDRs, validated with **`ipaddress`**). **`POST /settings/auth/access_control`** saves the list; invalid entries redirect with **`reason=invalid_ip`**. Migration **`_migrate_019`** converts **`auth_bypass_lan = 1`** to the three private IPv4 ranges and clears the flag. Startup still logs a warning if **`auth_bypass_lan`** is somehow **True** after migration.
- **IP allowlist:** Single-address loopback entries treat **IPv4 and IPv6 loopback** as equivalent (e.g. **`127.0.0.1`** matches a **`::1`** client and vice versa).

### Documentation

- **`SECURITY.md`:** New **Access control** section on **`X-Forwarded-For`** substitution (private/loopback peers only), spoofing risk behind a reverse proxy, and leaving the allowlist empty when using proxy auth.

## [1.0.35] - 2026-03-22

### Changed

- **Auth (critical):** **`require_auth`** now raises **`GrabbyAuthRequired`** with a **`RedirectResponse`** instead of returning it — FastAPI ignores **`Response`** objects returned from **`dependencies=[Depends(...)]`**, so Sign-in redirects previously did not run for protected routes.
- **Auth UX (upgrades + new installs):** Until **`auth_password_hash`** is set, protected pages and **`/login`** redirect to **`/setup/0`** instead of a non-working Sign-in. **LAN bypass** does not skip account setup (no passwordless LAN after migration). **`/logout`** sends you to **`/setup/0`** when no password is set. JSON/API requests without a password return **401** with **`setup_path`**. Setup wizard **step 0** shows an upgrade-specific banner when Sonarr/Radarr/Emby already look configured; **“Skip wizard → Settings”** is hidden on step 0 (Settings required sign-in).
- **E2E:** Playwright server uses a **temp SQLite** via **`GRABBY_DEV_DB_PATH`** and a one-shot DB init so **`/setup/1`** tests are reproducible.

### Documentation

- **README.md:** Install & first-run flow with **sign-in** and **Setup** (account + Arr/Emby); **dev** section for testing auth (`/setup/0`, reset dev DB, **`GRABBY_RESET_AUTH`**, **`pytest`** vs browser); **Backup** sensitivity; **`/healthz`** / **`/api/version`** noted as unauthenticated; **upgrade** note (first visit → account setup, Arr settings kept).
- **`service/README.md`**, **`HOWTO-RESTORE.md`**, **`SECURITY.md`:** Align with password flow and lockout recovery.

## [1.0.34] - 2026-03-26

### Changed

- **`app/main.py`:** Non-route helpers moved to **`app/constants.py`** (timezone / genre / credit option lists), **`app/form_helpers.py`** (URL + timezone + people-credit form helpers), and **`app/display_helpers.py`** (schedule display, local time formatting).
- **`services/`** removed; API key resolution lives under **`app/resolvers/`** (imports and **PyInstaller** `hiddenimports` updated).
- **`app/migrations.py`:** Startup migrations split into ordered **`_migrate_001_…`** … **`_migrate_017_…`** steps (same SQL and behavior as before).

### Added

- **`requirements.in`** + pip-compile workflow note in **`requirements.txt`**; **`pip-tools`** in **`requirements-dev.txt`**.
- Unit tests: **`tests/test_emby_cleaner.py`** (`evaluate_candidate`), **`tests/test_run_once_cooldown.py`** (`_filter_ids_by_cooldown`, `_paginate_wanted_for_search` with in-memory SQLite).

## [1.0.33] - 2026-03-25

### Changed

- **Schema cleanup:** Removed obsolete **`app_settings`** columns **`interval_minutes`**, global **`search_missing` / `search_upgrades`**, and **`max_items_per_run`** (per-app Sonarr/Radarr fields are the only source of truth). Startup migration copies values into per-app columns when safe, then **`DROP COLUMN`** (requires SQLite **3.35+**).
- **Backup:** **`format_version`** is now **`2`**; imports still accept **`1`**. Old global keys in v1 JSON are mapped onto per-app fields when needed.
- **Cleaner:** Only **`?scan=1`** (or other truthy **`scan`**) runs an Emby library pull — **`?preview=1`** is no longer honored.
- **Setup wizard (step 4):** form field renamed to **`run_interval_minutes`** (still sets Sonarr/Radarr/Emby run intervals).

### Fixed

- **`GRABBY_DEV_DB_PATH`:** **`app/db.py`** now honors this env var (documented since 1.0.29). **`scripts/dev-start.ps1`** sets it to **`%TEMP%\grabby-dev.sqlite3`** by default and adds **`-SharedAppDb`** to use the normal per-user data directory (same file as the installed service when unscoped).

### Added

- Optional root **`config.yaml`** (gitignored): **`SONARR_API_KEY`**, **`RADARR_API_KEY`**, **`EMBY_API_KEY`** loaded via **PyYAML** in **`app/config.py`**. **`app/resolvers/api_keys.py`** resolves keys for the scheduler, **`run_once`**, and connection tests (**YAML overrides DB** when set). Tracked template: **`config.example.yaml`**.

## [1.0.32] - 2026-03-24

### Changed

- **Installer build:** WinSW is **bundled** as **`installer/bin/WinSW.exe`** and staged with **`installer/setup.py`** (skips copy if **`service/winsw.exe`** already exists). **`installer/build.ps1`** no longer downloads WinSW from the network.

## [1.0.31] - 2026-03-23

### Security

- **Logging:** Root log level defaults to **WARNING** (override with **`GRABBY_LOG_LEVEL`**). **`SensitiveLogFilter`** + **`RedactingFormatter`** redact URLs, **`api_key` / `sonarr_key` / `token`**-style values, **Bearer** tokens, and **Authorization** headers in formatted log lines (including tracebacks). **`uvicorn`** CLI uses **`log_level=warning`**. Persisted job run / activity messages from HTTP errors and generic exceptions are passed through **`redact_sensitive_text`**.

## [1.0.30] - 2026-03-22

### Fixed

- **Software updates (Settings):** Update check is more resilient when GitHub’s API omits **`assets`** (still builds the conventional **`/releases/download/<tag>/GrabbySetup.exe`** URL). Asset name matching is **case-insensitive**. **Manual “Check for Updates”** sends **`?refresh=1`** to skip the 15‑minute in-memory cache. Failures log a warning server-side; the browser shows **HTTP status / non‑JSON** hints instead of a generic “could not reach” when the app returns an error page.

## [1.0.29] - 2026-03-22

### Fixed

- **Schedule weekdays:** Unchecking all days and saving no longer re-selects every day; **empty** `*_schedule_days` persists, **`in_window`** treats **no days** as never matching while the schedule is on, and the settings UI **`checked`** state follows the stored CSV.

### Changed

- **Schedule weekday defaults:** New **`app_settings`** rows and freshly added schedule columns default to **no days** (`""`); scheduler **`getattr`** fallbacks use **`""`**. Existing databases keep stored CSV until you save again.
- **Schedule weekday UI:** **7-column grid** (4 on narrow viewports) with **chip-style labels**; native checkboxes + **`:has(:checked)`** (no schedule JS).
- **Schedules (Grabby + Cleaner):** Per-day **`int = Form(0)`** fields (**`sonarr_schedule_Mon` … `_Sun`**, Radarr/Emby same); time `<select>` grids with orphan saved times; **`days_selected`** from the **raw DB** column. **No day checked** stores **`""`**; comma-only / invalid tokens still normalize to **full week** for legacy rows.
- **Dev server (`dev-start.ps1`):** default **`GRABBY_DEV_DB_PATH`** = **`%TEMP%\grabby-dev.sqlite3`** (optional **`-SharedAppDb`**). **`app/db.py`** honors **`GRABBY_DEV_DB_PATH`**.
- **App startup / SQLite:** **`create_all` + `migrate`** retry with backoff; **WAL** + **busy_timeout** where possible; settings **`POST`** uses **`_try_commit_and_reschedule`** and friendlier **`save=fail`** redirects (validation, DB busy, errors).
- **Typography (app UI):** **`--dash-fs-*`** / **`--dash-fw-*`** on **`.main`** and forms; consistent scale across Settings, Cleaner, Activity, Logs, Setup.
- **Dashboard → Automation / Overview:** Automation card shows **last run** + **next tick**; Overview **preview-stat** grids for Sonarr/Radarr (**Schedule Window** tile with days + friendly time). **Job logs:** skip lines include **minutes since last run**, interval, and **~minutes until eligible**.

## [1.0.28] - 2026-03-22

### Fixed

- **Web UI:** Cache-bust **`/static/app.css`** with **`?v=<app_version>`** (same as **`app.js`**) so browser upgrades don’t keep an old stylesheet after installing a new build — fixes oversized or “wrong” dashboard typography vs dev.
- **In-app upgrade / update check:** If **`GITHUB_TOKEN`** or **`GRABBY_GITHUB_TOKEN`** is set to an **invalid** value (common on dev/media PCs), the GitHub API returned **401** with **no** fallback. Grabby now **retries the API without** that header, then falls back to **web/Atom** like rate limits. **Installer downloads** from **`github.com/.../releases/download/...`** no longer send **`Authorization`**, so a bad global token won’t break **`GrabbySetup.exe`** downloads.

## [1.0.27] - 2026-03-20

### Changed

- **Dashboard (Overview):** One intro line (what the buttons do + schedule/cooldown) above **Missing** / **Upgrade** (standard **secondary** button typography) under **Sonarr** (TV) and **Radarr** (movies). Each is a **one-time** action that bypasses **schedule windows** and **run-interval** gates for that action only; **per-item cooldown** still applies. **Emby Cleaner** is not run. **`POST /api/arr/search-now`** JSON body: **`scope`** = `sonarr_missing` \| `sonarr_upgrade` \| `radarr_missing` \| `radarr_upgrade`.
- **Grabby Settings:** Removed **scheduler base interval** from **Global Settings**; wake cadence is the **minimum** of **Sonarr** and **Radarr** **run intervals** only (legacy **`interval_minutes`** column kept for backups). **Global** layout stacks **Arr search cooldown** and **timezone** with wrapping so they fit narrow windows.
- **Setup wizard (step 4):** The **run interval** field now seeds **Sonarr**, **Radarr**, and **Emby Cleaner** intervals together (no separate global base).

### Fixed

- **CI / releases:** Pushing a tag with the default **`GITHUB_TOKEN`** does **not** start other workflows, so **v1.0.26** could exist as a **git tag** while **GitHub Releases “Latest”** stayed on **v1.0.25** with no new **`GrabbySetup.exe`**. **Tag release** now **`workflow_dispatch`es** **Build installer** for the new tag; **Build installer** also publishes a release when run **manually** with ref = that **tag** (recover a missed build).

## [1.0.26] - 2026-03-21

### Fixed

- **Software updates / GitHub API rate limits:** On **403** or **429** from **`api.github.com`**, fall back to **`github.com/.../releases/latest`** and **`releases.atom`** so the check still works without a token. Cache successful lookups (**`GRABBY_UPDATES_CACHE_SECONDS`**, default **900**) to avoid burning the **60/hour** unauthenticated API quota.
- **Dev server (`dev-start.ps1`):** Frees the chosen port by stopping **every** listener PID (not just the first), uses **`taskkill`** when **`Stop-Process`** fails, optionally **`Stop-NetTCPConnection`**, and **`-TryElevatedKill`** for a one-time **UAC** kill attempt. Clearer errors if the port stays busy (ghost/stale listeners).

### Changed

- **CI / releasing:** When **`VERSION`** changes on **`master`** or **`main`**, **Tag release (from VERSION)** runs automatically, creates **`vX.Y.Z`** if missing, and pushes it — **Build installer** then runs on that tag (no local `git tag` / `git push`). **Actions → Tag release (from VERSION) → Run workflow** remains available to retry or tag without editing `VERSION` again.
- **Docs:** **[`docs/GITHUB-CLI.md`](docs/GITHUB-CLI.md)** (Windows **`gh`** PATH, **`gh auth login`**, merge/release commands) and **[`docs/PRUNE-OLD-RELEASES.md`](docs/PRUNE-OLD-RELEASES.md)**.

## [1.0.25] - 2026-03-21

### Fixed

- **Sonarr / Radarr run interval:** Stored **`0`** was only fixed once when `arr_interval_defaults_applied` was added; saving the form or old DBs could keep **`0`**. Run intervals now enforce **minimum 1** in the UI, **coerce legacy 0 → 60** on every save (Pydantic) and **on every startup** (migration), so the fields show real minutes (default **60**), not **`0`**.
- **Software updates / GitHub:** Update check uses a proper **`User-Agent`** (version + repo URL), optional **`GRABBY_GITHUB_TOKEN`** / **`GITHUB_TOKEN`** for rate limits or private repos, and clearer messages when GitHub returns **403** (includes API `message` when present).
- **Dev server:** **`scripts/dev-start.ps1`** frees the preferred port by stopping **any** process listening there (not only Python), using **`Get-NetTCPConnection`** instead of parsing `netstat`.

## [1.0.24] - 2026-03-21

### Fixed

- **Sonarr/Radarr run interval:** Existing installs that still had **0** stored now get a **one-time** DB update to **60** on startup (same as new defaults). **0** (“use scheduler base”) can still be set manually in Settings.

### Changed

- **Scheduler vs Emby Cleaner:** Grabby’s **wake interval** is the **minimum** of **Sonarr** and **Radarr** **run intervals** (under each app’s schedule). **Emby Cleaner** cadence is under **Cleaner Settings** (`emby_interval_minutes`). The legacy **`interval_minutes`** DB column remains for backups only — it is no longer shown in **Global Settings**.
- **Settings UI:** Run interval layout and **Global Settings** grid; **Sonarr/Radarr** defaults **60** (model + form); `placeholder="60"` on interval fields; **`arr_interval_defaults_applied`** one-time migration flag.

## [1.0.23] - 2026-03-20

### Fixed

- **Emby Cleaner + Sonarr:** Cleaner always **deletes Sonarr episode files** when a file exists (disk + Sonarr state) for matched TV items. Shows **still airing** (`status` not `ended`) then get those episodes **left monitored** so the season/show keeps grabbing new episodes. **Ended** series get those episodes **unmonitored** after delete once your Cleaner rules matched (watched / criteria).

### Changed

- **Settings UI:** Removed **Global run interval** from **Grabby Settings**; scheduler / **Emby Cleaner** run interval is edited under **Cleaner Settings → Global Cleaner Settings**. **Sonarr** / **Radarr run interval** moved under each app’s **schedule window** section (still **`0`** = use that shared scheduler interval). **Global Settings** section label; removed **Save All Grabby Settings**.

## [1.0.22] - 2026-03-21

### Added

- **Grabby scheduler — Sonarr / Radarr run intervals:** Under **Settings → Grabby scheduler**, optional **Run interval — Sonarr** and **Run interval — Radarr** (minutes). **`0`** uses the **Global run interval**. One scheduler wake runs at the **minimum** of global + configured Arr intervals; each app is skipped until its own interval has elapsed since the last run (Emby uses the global interval only).

## [1.0.21] - 2026-03-20

### Fixed

- **Arr search repeats:** cooldown now applies per **Sonarr/Radarr library item** (episode/movie id), not separately for “missing” vs “upgrade”, so the same title is not triggered twice in one run. **Arr search cooldown** is a dedicated setting (default **24 hours**), independent of scheduler interval—`0` restores the old “match run interval” behavior.
- **Wanted queue coverage:** Sonarr/Radarr missing and cutoff-unmet handling **walks multiple API pages** per run until “max items per run” is filled with items that pass cooldown (or the queue ends). Previously only **page 1** was used, so the same top titles were the only candidates forever — unlike Huntarr-style tools that batch through the full backlog.
- **Radarr/Sonarr IDs:** tolerate numeric ids returned as strings in Arr JSON when extracting episode/movie ids.

### Changed

- **Windows service (WinSW sample):** default bind address is **`0.0.0.0`** so the Web UI is reachable from other machines on the LAN (use firewall rules; UI has no built-in login).

## [1.0.20] - 2026-03-20

### Fixed

- **Arr search/upgrade loops:** added a per-item cooldown (`arr_action_log` + cooldown filtering) so Grabby does not keep re-triggering the same missing/upgrade search for the same movie/episode every scheduler tick.

## [1.0.19] - 2026-03-20

### Fixed

- **Activity formatting:** `Activity`/`Dashboard` detail text now uses valid block markup and updated CSS so multi-item details (TV show + episodes) are readable and wrap cleanly.

## [1.0.18] - 2026-03-20

### Fixed

- **Activity formatting:** `detail` is now multi-line (pre-line rendering) so TV show + episode/movie entries are readable instead of a single long line.

## [1.0.17] - 2026-03-20

### Fixed

- **Activity UI:** removed misleading success/failure badge (it did not represent “download/import succeeded” for Arr).

### Changed

- **Activity formatting:** improved separator formatting for multi-item details.
- **Sonarr TV labels:** improved TV show name detection so Activity details prefer show name over episode-only context.

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
- **Cleaner:** default **`GET /cleaner`** no longer scans Emby (fast sidebar); use **`Scan Emby for matches`** (`/cleaner?scan=1`).
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

## [1.0.4] - 2026-03-20

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
3. **Bump and ship (shortcut):** On a **release branch** (e.g. **`release/vX.Y.Z`**), run **`.\scripts\ship-release.ps1`** — pushes that branch to **`origin`** and dispatches **Tag release (from VERSION)** (creates **`vX.Y.Z`** if missing + **Build installer**). You do **not** need a **`dev`** branch on GitHub; local dev is **`dev-start.ps1`** only. Then open a **PR `release/v…` → `master`** so the default branch matches (**`master`** is protected).
4. **Classic path (merge first):** Commit on **`release/vX.Y.Z`** from **`origin/master`**, open **PR → `master`**, merge when checks pass. A push to **`master`** that changes **`VERSION`** also auto-runs **Tag release**. After merge: **`git switch master && git pull --ff-only`**, delete the release branch as needed.
5. Maintainers / Cursor agent: after **`git fetch origin master --tags`**, you may run **`gh workflow run build-installer.yml --repo jampat000/Grabby --ref vX.Y.Z`** to queue a build — **only** if **`vX.Y.Z`** points to the commit you intend to ship (see step **7** if the tag is stale). Often unnecessary if **Tag release** already dispatched.
6. If tagging did not run (e.g. workflow not merged yet), use **Actions → Tag release (from VERSION) → Run workflow**, or create the tag from **GitHub Releases**.
7. If a **tag** exists but **Releases → Latest** never updated (no **`GrabbySetup.exe`** for that tag), check that **`vX.Y.Z`** points to the commit you mean — run **`git fetch origin master --tags`**, then compare **`git rev-parse vX.Y.Z`** vs **`git rev-parse origin/master`**. **Manual** **Build installer** / **`gh workflow run … --ref vX.Y.Z`** uses the **workflow YAML from that tag’s commit** — an **old** tag SHA can **build** but **skip** **release**. **Fix:** move the tag to the correct commit and **re-push** the tag, **or** bump **`VERSION`** and release again, **or** **`gh release create`** + attach **`GrabbySetup.exe`** from a green run artifact.
8. Follow **GitHub Actions** / environment rules for approving production releases if configured.

[Unreleased]: https://github.com/jampat000/Grabby/compare/v1.0.34...HEAD
[1.0.34]: https://github.com/jampat000/Grabby/compare/v1.0.33...v1.0.34
[1.0.33]: https://github.com/jampat000/Grabby/compare/v1.0.32...v1.0.33
[1.0.32]: https://github.com/jampat000/Grabby/compare/v1.0.31...v1.0.32
[1.0.31]: https://github.com/jampat000/Grabby/compare/v1.0.30...v1.0.31
[1.0.30]: https://github.com/jampat000/Grabby/compare/v1.0.29...v1.0.30
[1.0.29]: https://github.com/jampat000/Grabby/compare/v1.0.28...v1.0.29
[1.0.28]: https://github.com/jampat000/Grabby/compare/v1.0.27...v1.0.28
[1.0.27]: https://github.com/jampat000/Grabby/compare/v1.0.26...v1.0.27
[1.0.26]: https://github.com/jampat000/Grabby/compare/v1.0.25...v1.0.26
[1.0.25]: https://github.com/jampat000/Grabby/compare/v1.0.24...v1.0.25
[1.0.8]: https://github.com/jampat000/Grabby/compare/v1.0.7...v1.0.8
[1.0.7]: https://github.com/jampat000/Grabby/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/jampat000/Grabby/compare/v1.0.5...v1.0.6
[1.0.5]: https://github.com/jampat000/Grabby/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/jampat000/Grabby/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/jampat000/Grabby/releases/tag/v1.0.3




