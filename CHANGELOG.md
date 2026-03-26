# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [2.4.13] - 2026-03-26

### Fixed

- **Trimmer / Sonarr TV cleanup:** After deleting watched episode files for continuing series, Fetcher now preserves Sonarr season monitoring for the affected season(s) so future episodes are not stranded (no broad TV-trimming redesign; ended series behavior unchanged).

### Maintenance

- **Tests:** Added focused coverage for continuing-series season-monitor preservation vs ended-series no-op.

## [2.4.12] - 2026-03-26

### Changed

- **Sonarr/Radarr failed-import cleanup:** Queue removal now calls the native API with **`blocklist=true`** on the same delete, so the associated release is blocklisted per Sonarr/Radarr (reduces immediate re-grab). Activity and run summaries use explicit wording: removed from queue; blocklist requested via Sonarr/Radarr API (no claim of independent verification).
- **Sonarr failed-import cleanup:** Delete failures are caught and reported like Radarr (**`failed-import queue remove failed`** + HTTP detail), with no misleading success Activity row.
- **Settings (Sonarr & Radarr):** Checkbox labels clarified for **search missing**, **cutoff-unmet upgrade search**, and **remove failed imports (queue + blocklist)**; helper text notes API blocklist on removal.

### Maintenance

- **Tests:** Radarr cleanup asserts **`blocklist=True`** on delete; added focused Sonarr failed-import cleanup tests. Default pytest suite and screenshot-regeneration opt-in behavior unchanged.

## [2.4.11] - 2026-03-26

### Maintenance

- Ensured internal tooling and non-runtime files are excluded from packaged artifacts
- Verified Dockerfile and build packaging for production correctness
- Confirmed module naming remains coherent and non-conflicting
- Re-validated authentication and security-sensitive paths with no behavior changes
- Tightened release hygiene and changelog clarity for maintenance consistency

## [2.4.10] - 2026-03-26

### Fixed
- **Dashboard automation cards:** Per-app hint text no longer shows the **“No line for this app in the last service run…”** fallback when the card already has a **last run** timestamp for that app (avoids contradicting **Succeeded** / recent-run state). When the message has no extra signal beyond the log line, the subtext stays **empty** instead of a generic “reported normally” line.

### Changed
- **Tests:** Coverage for automation subtext with and without per-app run evidence.

## [2.4.9] - 2026-03-26

### Added
- **Documentation:** **[`docs/INSTALL-AND-OPERATIONS.md`](docs/INSTALL-AND-OPERATIONS.md)** — Windows install paths, updates, logs, env var table, common failures.
- **Tests:** JWT/encryption unit coverage and dashboard live-queue resilience tests; API Bearer rejection cases (see test suite).

### Changed
- **README:** Rewritten for a clear on-ramp: what Fetcher does, features, **required `FETCHER_JWT_SECRET`** and **optional `FETCHER_DATA_ENCRYPTION_KEY`**, install/update/troubleshooting/security sections, honest project status.
- **SECURITY.md:** New **Application environment** section for JWT (required) and Fernet encryption (optional / plaintext warning); threat-model table notes plaintext storage without encryption key.
- **service/README.md:** Documents optional **`FETCHER_DATA_ENCRYPTION_KEY`** for WinSW configs.
- **docs index:** Links **INSTALL-AND-OPERATIONS**.
- **API auth:** JSON routes under **`app/routers/api.py`** use **`require_api_auth`** so a present **`Authorization: Bearer`** header is validated (invalid token → 401 with actionable message); session cookie still used when no Bearer header. Test **`conftest`** overrides **`require_api_auth`** alongside **`require_auth`** where tests bypass auth.
- **Structure:** Dashboard status and live Arr totals live in **`app/dashboard_service.py`**; **`app/web_common.py`** keeps settings/setup/activity helpers.

## [2.4.8] - 2026-03-26

### Fixed
- **Logs page noise:** Suppress duplicate historical **“Run summary not available yet.”** rows when a completed run summary exists within a short **started_at** window (display-layer dedupe only).
- **Activity/log empty rows:** Ensure user-visible Activity lines always include a primary label and non-empty detail fallback; persist non-empty **ActivityLog.detail** when redaction would otherwise leave it blank.

### Changed
- **Dashboard automation clarity:** Global strip uses **Active / Idle / Processing** only (no misleading system-wide “cooling down”); per-app cards show **retry-delay** context from the last service-run summary only for Sonarr, Radarr, and Trimmer.
- **Dashboard timing copy:** Last/next run lines include relative phrases (e.g. minutes ago / in N minutes) where available.
- **Manual search API messages:** Success, queued, and failure responses name **Sonarr or Radarr** and **missing vs upgrade** explicitly.
- **Setup wizard & connection copy:** Clearer field purposes, validation messages, and connection-test hints (URL, API key, network).
- **Sign-in messaging:** More actionable invalid-login and rate-limit text; clearer session-configuration error text.
- **Log file browser errors:** More specific messages for disallowed paths, missing files, and read failures.

## [2.4.7] - 2026-03-25

### Fixed
- **Sonarr/Radarr monitored-missing progression:** Scheduled and dashboard missing-search actions advance through the full monitored-missing universe (not a small subset), with cooldown/retry exclusion preventing immediate repeats.
- **Dashboard responsiveness/navigation:** Dashboard HTML render no longer blocks before the dashboard view appears; live totals continue via normal polling.
- **0-search activity explanations:** Missing-search runs that dispatch **0** searches now record a single clear summary reason in Activity.
- **Global-only Backup/Restore/Upgrade cleanup:** Backup, Restore, and Upgrade UI/handling remain clearly global-only.
- **Layout/alignment polish:** Targeted TV/Movies and setup-page spacing/alignment cleanup for consistent UI rhythm.
- **Scoped save feedback:** Save success/error messages reflect the actual scope/section that was changed.
- **Smart Setup Wizard visibility:** Setup Wizard is shown only while setup is incomplete (dashboard + left nav), and reappears automatically if configuration becomes incomplete again; visibility is derived from saved configuration state and is consistent across `/setup/{step}` pages.

### Changed
- **Retry policy model:** Replaced shared cooldown semantics with explicit per-app **Retry Delay (minutes)** settings for Sonarr and Radarr (minimum is enforced; no implicit 0 fallback).
- **Sonarr/Radarr parity improvements:** Normalized shared settings wording/helper text; added Sonarr support for **Remove failed imports from queue**.
- **Trimmer wording:** Renamed user-facing **Trimmer Review** to **Trimmer Overview**.
- **Final wording/consistency cleanup:** Tightened label/helper text wording to ensure shared Sonarr/Radarr controls remain interchangeable where intended.

## [2.4.6] - 2026-03-25

### Fixed

- **Dashboard navigation latency:** Dashboard HTML render no longer blocks on live Arr totals; expensive live counts load after the view is visible via normal polling.
- **0-search explanations:** Missing-search runs that dispatch **0** searches now record a single, clear summary reason in Activity (e.g., retry delay vs empty eligible pool).
- **Sonarr/Radarr wording parity:** Normalized shared settings helper text so Sonarr and Radarr read identically (including Run interval helper text).

## [2.4.5] - 2026-03-25

### Fixed

- **Sonarr/Radarr missing progression stability:** Kept monitored-missing search progression aligned to the full monitored missing universe with cooldown/retry exclusion so successive runs continue advancing instead of looping a small subset.
- **Dashboard first-load responsiveness:** Removed delayed zero-first paint for hero stats by rendering current values server-side immediately, then continuing live refresh updates.
- **Settings scope clarity:** Backup, Restore, and Upgrade controls are now clearly Global-only in structure/presentation with no ambiguous app-section crossover.

### Changed

- **Retry policy model:** Replaced shared Arr cooldown semantics with explicit per-app **Retry Delay (minutes)** settings for Sonarr and Radarr; removed shared fallback behavior.
- **Arr parity:** Added Sonarr support for **Remove failed imports from queue** with matching settings/persistence style alongside Radarr.
- **Trimmer wording:** Renamed user-facing **Trimmer Review** wording to **Trimmer Overview**.
- **Settings UX polish:** Added context-aware save/scope feedback so Global, Sonarr, Radarr, and Trimmer saves/errors reflect the section being changed.
- **Layout consistency:** Applied targeted TV/Movies and setup-page spacing/alignment polish for more consistent control rhythm without redesign.

## [2.4.4] - 2026-03-25

### Fixed

- **Sonarr/Radarr missing search progression:** Scheduled and dashboard **Search now** **missing** actions walk the full **monitored, no-file** library (same universe as inclusive missing counts), batch by existing cooldown, then dispatch **EpisodeSearch** / **MoviesSearch** in stable order. Selection no longer depends only on **`/api/v3/wanted/missing`**, which excludes some monitored-missing titles and caused the same small batch to be searched repeatedly while a larger missing pool existed. **Upgrade/cutoff** behavior is unchanged (**`/wanted/cutoff`** pagination and cooldown).

## [2.4.3] - 2026-03-25

### Fixed

- **Trimmer cleaner async save:** **`save_scope`** can be taken from query **`trimmer_save_scope`** (mirrored on each save button **`formaction`**) when the submitter field is missing from the multipart body (e.g. some Enter-to-submit / edge **`FormData`** cases), removing spurious **`invalid_scope`** and the misleading “reload the page” message. **`RequestValidationError`** on this route now returns **JSON** when **`X-Fetcher-Trimmer-Settings-Async`** is set so the UI does not mis-handle a **303 + HTML** as success.
- **Trimmer cleaner diagnostics:** **`invalid_scope`** now logs body vs query **`save_scope`** values at **WARNING** for support.

### Changed

- **Installed-build logging:** Rotating **`fetcher.log`** under **`<database directory>/logs`** (default **`%ProgramData%\Fetcher\logs`** next to **`fetcher.db`**). Override with **`FETCHER_LOG_DIR`**. Startup logs the resolved log file path; **Logs** (`/logs/file`) reads the same directory. **README** documents the layout and WinSW **`*.out.log` / `*.err.log`** vs application logs.
- **Windows installer:** **`Fetcher.iss`** documents upgrade vs uninstall data policy (**Program Files** binaries vs **ProgramData** DB/logs). **`[UninstallDelete]`** removes only **WinSW wrapper** **`*.out.log` / `*.err.log`** under **`{app}`** — **not** canonical user data.

## [2.4.2] - 2026-03-25

### Fixed

- **Dashboard hero “missing” counts:** Live Sonarr/Radarr **missing** tiles again use the same semantics as automation (**monitored** episodes/movies **without files**, **including unreleased / not yet available**), via **`_sonarr_missing_total_including_unreleased`** and **`_radarr_missing_total_including_unreleased`** from **`service_logic`** — not **`/wanted/missing` `totalRecords` alone** (narrower queue). **Cutoff-unmet** tiles still use **`/wanted/cutoff` `totalRecords`**. Live refresh stays **independent of scheduler run intervals**; wall-clock timeouts preserve **snapshot fallback** if Arr is slow or unreachable.

## [2.4.1] - 2026-03-25

### Fixed

- **Sonarr/Radarr search cooldown:** Wanted-queue pagination now prefers each app’s **internal** database id (**`id`** for Sonarr episodes and Radarr movies) before alternate keys (**`episodeId`** / **`movieId`**). Using a foreign key for cooldown while sending internal ids to Arr could leave **`ArrActionLog`** out of sync so the **same items were searched again** on the next tick.
- **Dashboard hero first paint:** Server-rendered hero tiles now use the same **merged** Sonarr/Radarr missing and cutoff-unmet counts as **`build_dashboard_status`** (live *arr **`totalRecords`** when reachable, otherwise snapshot fallback), instead of reading snapshot fields only for initial **`data-target`** values.

### Changed

- **Dashboard hero refresh:** Hero tiles continue to poll about every **10s**; live totals remain independent of scheduler run intervals.
- **Setup wizard save UX:** Optional **`X-Fetcher-Setup-Async`** on **`POST /setup/{step}`** returns JSON (**`ok`**, **`redirect`**, or errors) with the same **Saving…** / **Saved.** / warning **feedback** pattern as hardened settings; classic form POST + **303** remains for non-JS. **`POST /setup/0`** stays CSRF-exempt; account creation still sets the session cookie on success.
- **Trimmer cleaner save scope:** **`POST /trimmer/settings/cleaner`** accepts only **`save_scope`** **`schedule`**, **`tv`**, and **`movies`**. Each scope persists **only** its columns (schedule vs TV rules vs movie rules — no combined/broad write). Legacy **`global`** and catch-all **`all`** are **rejected** (**`invalid_scope`**, no DB write).

## [2.4.0] - 2026-03-25

### Fixed

- **Windows persistence (packaged builds):** Canonical SQLite path is **`%ProgramData%\Fetcher\fetcher.db`**. On first start after upgrade, if that file is missing but a legacy database exists under the service profile’s **`AppData\Local\Fetcher\fetcher.db`** (e.g. Local System under **`…\systemprofile\…`**), Fetcher performs a **one-time file copy** (including **`-wal`** / **`-shm`** sidecars when present) into ProgramData. If the canonical database **already exists**, migration **does not** run and nothing is overwritten or merged. Normal runtime does **not** probe legacy paths after this decision.

### Changed

- **Windows migration marker and legacy archive:** After a **verified** **`copy2`**, Fetcher writes **`fetcher.db.migrated_from_legacy`** next to the canonical DB (JSON: legacy path, **source_size**, **source_mtime_ns**, **migrated_at_utc_iso**). Legacy **`fetcher.db`** / **`-wal`** / **`-shm`** are **renamed** (suffix **`.fetcher-programdata-migration-archive`**) **only** when the on-disk marker matches in-process proof, canonical and legacy main files match that proof (**exact** size and **mtime_ns**), and overrides are not active; otherwise logs **`Archive skipped: …`**. Marker is **never** overwritten; retained for audit.
- **Operations:** **`FETCHER_DATA_DIR`** still overrides the data directory. Startup logs the resolved database path (**`SQLite database path:`**); when migration runs, logs **`Migrated SQLite from legacy path…`** and **`Migration marker created:`** when applicable.
- **Settings UX (summary for this release):** **Separate** Global, Sonarr, and Radarr **forms**; **async** saves without full-page refresh; **Saving…** / **Settings saved.** / error **feedback**; **scoped** persistence (global vs per-app); **consistent wording** for save and session-related messages.
- **Settings save contract:** **`POST /settings`** accepts only **`save_scope`** values **`global`**, **`sonarr`**, and **`radarr`**. Missing, empty, or any other value (including legacy **`all`**) fails with **`invalid_scope`** — no broad or cross-section write path.
- **Connection test clarity:** **Test Sonarr / Test Radarr** use **saved** URL and API key only; they do **not** update **`AppSettings`** configuration. They still append an **`AppSnapshot`** row so dashboard connection status stays accurate (same intentional behavior as before, now documented in code and UI copy).
- **Non-JS settings flows:** Save failures show the same **section-local** warning on **Global / Sonarr / Radarr** tabs (not only Global). **`invalid_scope`** redirects always use **`tab=global`** so the error message is visible. **Malformed-field** validation (handled as **`save=fail&reason=invalid`**) preserves **`tab=`** from **`save_scope`** when it is **`global`**, **`sonarr`**, or **`radarr`**; otherwise **`tab=global`**. **`POST /settings/auth`** validation failures redirect with **`tab=security`**. Test redirects include **`tab=`** alongside **`test=`** for consistent tab restoration.
- **In-place (fetch) errors:** Failed async Save/Test responses still apply **`tab=`** from JSON to the address bar so the active section matches the server.

## [2.3.17] - 2026-03-25

### Changed

- **Release:** Patch version bump for installer and release pipeline; no application behavior changes from v2.3.16.

## [2.3.16] - 2026-03-25

### Changed

- **Settings UX:** Global, Sonarr, and Radarr each have a dedicated form. Saves use in-page requests (no full reload), stay on the current tab, and show immediate **Saving…** / **Settings saved.** / error feedback.
- **Save scope:** Global save stores only cooldown, log retention, and timezone; Sonarr and Radarr saves remain independent. Section helper text states that only that tab is saved where it helps.
- **Copy:** Save success, failure, and non-JS redirect messages use consistent, plain language; session/CSRF errors suggest reloading and signing in again.

## [2.3.15] - 2026-03-25

### Changed

- **Dashboard hero metrics refresh:** Summary tiles (Sonarr/Radarr missing and upgrades) poll on independent staggered timers with debounced API calls; automation cards still refresh on their own cadence so counts are not delayed until the next full status tick.
- **Settings documentation:** Corrected global help text to match independent Sonarr/Radarr/Trimmer scheduler jobs; clarified that per-tab saves are required for each app’s run interval to persist.

## [2.3.14] - 2026-03-24

### Changed

- **Missing count semantics:** Sonarr and Radarr dashboard "missing" totals now include monitored items that are not yet available, matching Arr expectations for monitored missing counts.
- **Dashboard wording clarity:** Updated hero tile subtitles to concise, consistent phrasing for monitored missing counts.

## [2.3.13] - 2026-03-24

### Fixed

- **Manual Arr search reliability/consistency:** Improved immediate manual search triggering by selecting concrete wanted item IDs with per-app max-items limits, and kept activity detail output aligned with searched items.
- **Dashboard manual search UX:** Reduced stale status messaging after manual actions by refreshing/clearing dashboard feedback promptly.
- **Top dashboard label clarity:** Renamed/reordered summary tiles to explicit Sonarr/Radarr missing/upgrades labels for clearer at-a-glance context.

## [2.3.12] - 2026-03-24

### Fixed

- **Manual search activity detail parity:** Improved manual missing/upgrade activity entries to include meaningful wanted-item details and counts (matching scheduled-action readability).
- **Manual search status UX:** Refined dashboard manual-search status behavior to auto-clear after successful trigger + live refresh, reducing stale “triggered/queued” messages.

## [2.3.11] - 2026-03-24

### Fixed

- **Manual search immediacy:** Restored immediate Sonarr/Radarr command triggering for manual missing/upgrade actions so clicks are reflected on Arr side right away.
- **Live UI feedback:** Triggered immediate in-page dashboard/activity/log refresh after manual-search responses so users can see updates without waiting for the periodic poll cycle.

## [2.3.10] - 2026-03-24

### Fixed

- **Manual search responsiveness:** Changed Sonarr/Radarr manual search API to queue work in the background and return immediately so the UI no longer blocks on full orchestration runtime.
- **Manual search API contract:** Added explicit queued response semantics for manual search requests while preserving existing backend execution behavior.

## [2.3.9] - 2026-03-24

### Fixed

- **Live activity/log visibility:** Fixed live-updated Activity and Logs rows disappearing after refresh by reapplying entry animation classes to swapped-in rows.

## [2.3.8] - 2026-03-24

### Changed

- **Live status surfaces:** Added automatic live refresh behavior for dashboard recent activity, full Activity, and Logs views so run/search results appear without manual page reloads.
- **Automation/dashboard polish follow-up:** Included latest dashboard/automation UI refinements and related frontend alignment updates shipped together in this build.
- **Manual Arr search visibility:** Included manual Sonarr/Radarr search activity logging fixes so manual actions are reflected in Activity consistently.

## [2.3.7] - 2026-03-24

### Fixed

- **Manual Arr search activity logging:** Fixed Sonarr and Radarr manual searches so successful manual actions now always create Activity entries, including no-results/cooldown outcomes.

## [2.3.6] - 2026-03-24

### Changed

- **Automation 4-card redesign:** Rebuilt Automation as a compact 4-card layout (Sonarr, Radarr, Trimmer, Latest event) for faster status scanning.
- **Dashboard visual alignment:** Updated Automation cards to match the summary-card visual language and responsive grid behavior.
- **Readability polish:** Reduced vertical bulk and clarified primary/secondary status lines, including improved contextual latest-event presentation.

## [2.3.5] - 2026-03-24

### Changed

- **Automation alignment polish:** Refined fixed-width Last run/Next run label alignment so values line up consistently across Sonarr, Radarr, and Trimmer.
- **Spacing and hierarchy polish:** Tuned subsystem spacing and title emphasis for a tighter, clearer visual hierarchy.
- **Secondary readability polish:** Further muted pending next-run text and tightened Latest system event spacing for cleaner visual flow.

## [2.3.4] - 2026-03-24

### Changed

- **Automation card final polish:** Removed inner subsystem boxes and refined spacing for a cleaner, more intentional Sonarr/Radarr/Trimmer layout.
- **Hierarchy and readability:** Tightened visual hierarchy so subsystem titles are primary and Last run/Next run labels stay muted and aligned.
- **Empty-state messaging:** Replaced placeholder dashes with explicit states (`Not yet run`, `Pending`, `No activity yet`) for clearer, production-ready UX.
- **Latest system event presentation:** Kept the row secondary at the bottom and improved contextual readability for event + time + status.

## [2.3.3] - 2026-03-24

### Changed

- **Automation card polish:** Reworked dashboard Automation presentation into clear Sonarr/Radarr/Trimmer subsystem blocks with stronger hierarchy and cleaner spacing, including intentional empty-state rendering.
- **Independent scheduler alignment:** Kept per-subsystem Last/Next scheduling rows primary and moved the global row to a secondary **Latest system event** section.
- **Latest event context:** Latest system event now shows contextual label (`{Subsystem} • {Event name}`) with timestamp + status badge for clearer operational signal.

## [2.3.2] - 2026-03-24

### Fixed

- **Dashboard status population:** Fixed per-subsystem scheduler status payload/render alignment so Sonarr, Radarr, and Trimmer last/next run fields populate from matching backend keys and fallback timing data when in-memory scheduler next-run is unavailable.
- **Per-subsystem run outcome badges:** Added Sonarr/Radarr/Trimmer success/failure badges on each subsystem’s last-run row using latest per-app snapshot outcome.
- **Dashboard hierarchy alignment:** Demoted the generic run row to **Latest system event** so independent per-subsystem Last/Next status is the primary scheduling signal.

## [2.3.1] - 2026-03-24

### Changed

- **Schedule-window semantics clarification:** Kept interval cadence active per subsystem and clarified that schedule windows are a restriction layer only (when disabled, runs follow interval normally; when enabled, runs execute only inside the allowed window).
- **Scheduler status clarity:** Dashboard/API now expose and show per-subsystem last-run + next-run timing (Sonarr, Radarr, Trimmer), with generic latest event shown as secondary context.

## [2.3.0] - 2026-03-24

### Changed

- **Scheduler independence:** Replaced the shared minimum-interval automation tick with independent scheduler jobs for **Sonarr**, **Radarr**, and **Trimmer**. Each job now has its own cadence and next-run state, and scheduled runs execute only their own app scope.
- **Run cadence semantics:** Removed internal per-app run-interval suppression from `run_once` scheduled execution paths so scheduled cadence is owned by each scheduler job; manual Arr actions remain serialized safely but no longer re-phase normal scheduler timing.
- **Dashboard status:** Updated scheduler status payload/UI wiring to report independent next-run values for Sonarr, Radarr, and Trimmer.

## [2.2.0] - 2026-03-24

### Added

- **Radarr (opt-in):** **Remove failed imports from queue** — when enabled under Radarr settings, each automation run scans Radarr history for explicit **import failed** events, matches the **download queue** by exact **download ID** only, and removes the queue item when there is exactly one match (no title/fuzzy matching, no blocklist, no re-search). Successful removals are recorded in **Activity** with title and reason when available. No-match and ambiguous multi-match cases skip safely without deleting.

## [2.1.1] - 2026-03-24

### Fixed

- **Settings (Fetcher):** Global / Security / Sonarr / Radarr tab buttons switch panels again via shared **`initSettingsTabs()`** in **`app/static/app.js`**, **`.settings-tab-target`** visibility in **`app/static/app.css`**, **`aria-selected`** / **`is-active`** state, **`#section-*`** deep links, and **`history.replaceState`** on tab clicks (no scroll jump). Inline settings tab script removed (it matched every **`[data-settings-panel]`**, including non-tab slices, which broke panel toggling).

## [2.1.0] - 2026-03-24

### Added

- **Regression tests:** Contract coverage for Emby `items_for_user` paging/merge/stop semantics and for the Emby delete phase of `apply_emby_trimmer_live_deletes` (including duplicate candidate ids and partial-failure messaging).

### Changed

- **Performance — Emby library scan:** `EmbyClient.items_for_user` prefetches at most one next page while merging results (bounded overlap; same API params and output order).
- **Performance — Sonarr/Radarr wanted queues:** Wanted-queue pagination may prefetch the next page while cooldown filtering runs on the current page (strict page order preserved).
- **Performance — Sonarr trimmer apply:** On-disk episode file deletes use bounded concurrency with per-file failure aggregation (non-fail-fast).
- **Performance — Emby trimmer apply:** Emby `delete_item` calls use bounded concurrency; failures are aggregated (not fail-fast). Automation action lines report partial success as `Emby: deleted X item(s); Y failed — …` when applicable.

### Security / operations

- **Installed Windows service builds** now require a persistent **`FETCHER_JWT_SECRET`** at startup (JWT/session signing). For WinSW env pass-through and first-time service setup, see **2.0.24** release notes.

## [2.0.25] - 2026-03-24

### Changed

- **Dashboard automation status:** Aligned the Automation "Last run" timestamp and status pill vertically and kept success wording consistent ("Succeeded") between initial render and live dashboard polling updates.

## [2.0.24] - 2026-03-23

### Fixed

- **Installer/service runtime:** Added explicit WinSW env pass-through for **`FETCHER_JWT_SECRET`** and documented persistent service configuration so installed builds can start with required JWT configuration while preserving fail-fast behavior when unset.

## [2.0.23] - 2026-03-23

### Fixed

- **Packaging/dependencies:** Restored required runtime dependencies in release lockfiles (**`passlib[argon2,bcrypt]`**, **`slowapi`**) after lock refresh drift, and kept vulnerability remediation pins for **`cryptography`** and **`pyjwt`** so CI security checks pass and installable builds include auth/rate-limit runtime modules.
- **Release CI:** Set CI/runtime JWT test secrets for GitHub Actions smoke/pytest workflows so packaged startup health checks and test collection succeed with enforced `FETCHER_JWT_SECRET` startup validation.
- **Installer packaging:** Expanded PyInstaller hidden imports for auth/rate-limit runtime modules so frozen `Fetcher.exe` includes passlib/slowapi dependency trees required by startup paths.

## [2.0.20] - 2026-03-23

### Changed

- **Security/Auth:** Hardened JWT and token refresh handling, centralized auth orchestration, enforced env-based secret loading, added rate limiting, and expanded auth/security regression coverage.
- **Refactor:** Split Trimmer review/apply orchestration into focused services and extracted Sonarr/Radarr/Emby execution blocks from run orchestration into dedicated helpers without behavior changes.
- **Testing/Operations:** Added focused regression suites for trimmer routes, connection-testing flows, and service run orchestration to freeze behavior before further refactors.

## [2.0.19] - 2026-03-23

### Changed

- **Docs:** **README** (overview, GHCR **`docker pull`**, GitHub **About** copy-paste table), **CONTRIBUTING** (rulesets vs classic, solo approvals, **Tag release** → Windows + Docker), **SECURITY** (supported builds, CSRF tokens, **`master`** rules), **LICENSE** copyright years, **docs/README** index.

## [2.0.18] - 2026-03-23

### Changed

- **UI:** Design-system pass — primary **Save** on Security and Trimmer rule sections; **Activity** filter chips match **settings** tabs; **"+N more"** uses standard link blue; shared **`--font-mono`** for code/log text; dashboard **gc-title** markup aligned with other pages.

## [2.0.17] - 2026-03-23

### Changed

- **Docs:** First pass removed stale **Cursor** handoff, **public repo audit**, and **`ship-dev.ps1`**; aligned **public checklist**, **CONTRIBUTING**, **CHANGELOG** releasing, **README**, **HOWTO-RESTORE**, backup UI copy (**toggle** wording). **Second pass:** **`docs/README.md`** index; **`PRUNE-OLD-RELEASES`** merged into **`docs/GITHUB-CLI.md`**; compare-link footer trimmed to **v2.x** (older tags → **Releases**); **IMPORT-BRANCH-PROTECTION** documents **`protect-default-branch`** ruleset.
- **Performance / UI noise:** Dashboard **Activity** query loads **8** rows (was **30**) since only five render; **dashboard status** polling runs once on load, pauses while the tab is hidden, and resumes on focus; shared **httpx** client uses connection limits for better reuse; **Software updates** post-upgrade **`/healthz`** poll interval **3s** (was **2s**).

## [2.0.16] - 2026-03-23

### Added

- **GitHub Releases:** standard **Install** section (**Windows** + **Docker** `docker pull`) above the auto-generated changelog (**Build installer** workflow).

### Changed

- **UI:** **First-run setup** and **Trimmer** settings use the same **toggle switches** as **Backup & restore** and **Fetcher settings** (enable Sonarr/Radarr/Emby, dry run, people **credit roles**). **`toggle_checkbox_value`** macro for grouped toggles.

### Fixed

- **Release CI:** **Tag release** dispatches **Docker publish** and **Build installer**; **Docker publish** uses default-branch dispatch with **`checkout_ref`** so workflow YAML stays current while the image builds from the release tag (avoids the Actions **ref trap** on older tag commits).

## [2.0.15] - 2026-03-23

### Added

- **Docker:** **`Dockerfile`** (Python 3.11-slim, **`uvicorn`** on **`0.0.0.0:8765`**, persistent SQLite via **`FETCHER_DEV_DB_PATH=/data/fetcher.db`**, non-root user), **`docker-compose.yml`** with volume and **`/healthz`** healthcheck, **`.dockerignore`**, and **[`docs/DOCKER.md`](docs/DOCKER.md)**. **`README.md`** links Docker install.
- **CI:** **`.github/workflows/docker-build.yml`** verifies **`docker build`** on changes to container inputs.

## [2.0.14] - 2026-03-23

### Changed

- **Settings → Software updates (Apply upgrade):** **Upgrade now** and **Release notes** sit on one row (`.settings-commit-actions--row`) so the panel height stays steadier when those controls appear.

## [2.0.13] - 2026-03-23

### Added

- **Settings → Software updates:** After **Upgrade now** succeeds, the page **polls `/healthz`** and **reloads** when the new **version** is live (or when the reported version changes from before the upgrade), so the browser shows the upgraded app without a manual refresh. **`POST /api/updates/apply`** returns **`previous_version`** and **`target_version`** for the UI.

### Changed

- **In-app upgrade:** Silent **`FetcherSetup.exe`** invocation now includes **`/CLOSEAPPLICATIONS`** so **Inno Setup** can replace installed files while the **Fetcher** service restarts (aligned with a reliable manual silent install).

## [2.0.12] - 2026-03-23

### Fixed

- **`VERSION` / default branch:** After rapid merges of **release** and **refactor/security** pull requests, **`master`** could report **`2.0.9`** in **`VERSION`** while **2.0.10** settings/update work was already merged — bump to **2.0.12** so **`VERSION`**, **`/healthz`**, and the changelog match the combined tree.

## [2.0.11] - 2026-03-23

### Added

- **Security hardening:** Path-traversal guard for dashboard log-file reads now enforces resolved path containment and rejects escapes with **403**.

### Changed

- **Architecture:** Finalized modular FastAPI router structure and shared `httpx.AsyncClient` pooling lifecycle alignment.
- **Runtime security:** Persisted `ActivityLog.detail` and `JobRunLog.message` strings are sanitized through centralized sensitive-text redaction before DB writes.
- **Web defaults:** CLI host default remains loopback-only (`127.0.0.1`) and security headers (`X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`) are enforced via middleware.

## [2.0.10] - 2026-03-23

### Fixed

- **Settings → Software updates → Apply upgrade:** One **if/else** path so **Upgrade now** is never left hidden while the confirmation toggle is visible; **`apply_supported`** is honored only when JSON **`true`** (`===` in script). Added a short hint to turn the confirmation on, then use **Upgrade now**; when in-app apply is unavailable, the toggle is hidden and a **manual install** line points to **Download FetcherSetup.exe** under **Release status**.

### Changed

- **`app.css`:** **`.btn:disabled`** visibility; **Apply upgrade** primary button uses full width (up to **20rem**) in the commit panel.

## [2.0.9] - 2026-03-23

### Added

- **`.github/rulesets/protect-default-branch.json`:** Example **repository ruleset** (branch rules, not classic protection) for import or **`gh api …/rulesets`** — optional reference for **`master`** / default branch.

### Changed

- **Settings → Backup & restore:** **Choose backup file** uses app **button** styling; **Confirm restore** matches **Apply upgrade** (shared **`settings-step-*`** / **`settings-commit-*`** layout, **toggle** control, tinted commit panel).
- **Settings → Software updates → Apply upgrade:** Same confirm UI pattern as backup restore; **`scripts/dev-start.ps1`** sets **`FETCHER_ALLOW_DEV_UPGRADE=1`** by default so source runs can use **Upgrade now** (use **`-NoDevUpgrade`** to keep the toggle hidden). **Development** hint only shows when an update exists and in-app apply is still unavailable.

## [2.0.8] - 2026-03-23

### Added

- **`app/httpx_shared.py`:** One shared **`httpx.AsyncClient`** created in FastAPI **lifespan** and closed on shutdown; **Arr**, **Emby**, and **GitHub updates** reuse it for connection pooling.
- **`app/routers/`:** FastAPI **`APIRouter`** modules (**`auth`**, **`setup`**, **`api`**, **`dashboard`**, **`settings`**, **`trimmer`**) plus **`deps`**; shared UI helpers in **`app/web_common.py`**, **`app/branding.py`**, **`app/paths.py`**, **`app/ui_templates.py`**.

### Changed

- **`app/db.py`:** **`PRAGMA journal_mode=WAL`**, **`synchronous=NORMAL`**, **`busy_timeout`** (10s) on each SQLite connection; **`aiosqlite`** connect **`timeout`**; **`get_session`** uses **`try`/`finally`** with **`await session.close()`**. Warns if WAL is not active.
- **`app/main.py`:** Lifespan only (app, static, routers, exception handlers); scheduler shutdown uses **`wait=False`** with logging and **`try`/`except`**.
- **`app/scheduler.py`:** **`shutdown(*, wait=True)`** forwards **`wait`** to APScheduler (app uses **`wait=False`**).
- **`app/service_logic.py`** / **`app/arr_client.py`:** Broader type hints (**`dict[str, Any]`**, **`datetime`**, etc.).
- **`scripts/protect-master-branch.ps1`:** REST body includes **`restrictions: null`** for GitHub branch-protection API.

### Fixed

- **Tests:** Router monkeypatch targets and Emby paging tests pass a mock **`http_client`** when the shared httpx client is not initialized.

## [2.0.7] - 2026-03-23

### Changed

- **Fetcher settings:** After **Save**, redirects include **`tab=`** (Global / Sonarr / Radarr) so the same section stays open. **Security** access-control saves use **`tab=security`**. Scroll restore is skipped when **`tab=`** is present so it does not override the chosen panel.
- **Trimmer settings:** Save redirects append a section **hash** (**`#trimmer-connection`**, **`#trimmer-schedule`**, **`#trimmer-rules`**, **`#trimmer-people`**) via **`formaction`** query **`trimmer_section`**. Scroll restore is skipped when a hash is present.
- **Dashboard overview:** Sonarr/Radarr **Search** tile shows **Missing** and **Upgrades** **On**/**Off** in one metric cell (not two separate tiles).

## [2.0.6] - 2026-03-23

### Added

- **Activity / dashboard:** Expandable title lists — preview shows **5** lines then **+N more**; **click the row** or **+N more** / **Show less** to reveal the full list stored for that entry. Shared **`macros/activity_row.html`** for both pages.

### Changed

- **Activity log storage:** **`ActivityLog.detail`** stores **all** non-empty titles for a run (one per line), with a large safety cap only (~400k chars). Removed per-run line truncation in **`_detail_from_labels`** so new runs retain full titles for the expand UI.

### Fixed

- **Activity detail parsing:** Legacy synthetic **`+N more`** lines in stored detail are ignored when building the list so they are not shown as titles.

## [2.0.5] - 2026-03-23

### Changed

- **Activity / dashboard:** Sonarr/Radarr activity lines use **Missing search for N episodes/movies** and **Upgrade search for N episodes/movies** (full activity page and dashboard recent activity) for clearer grammar than **N … missing search** / **N … upgrade search**.

## [2.0.4] - 2026-03-23

### Changed

- **Activity:** Missing-queue lines use **missing search** (e.g. **2 movies missing search**) to match **upgrade search** wording.
- **Dashboard:** Removed redundant quick links (**Fetcher settings**, **Trimmer settings**, **Scan library**, **Run logs**); use the sidebar. Removed the extra **Trimmer settings** link from the disabled **Emby Trimmer** summary.

## [2.0.3] - 2026-03-23

### Added

- **Fetcher settings:** Tabbed layout (**Global**, **Security**, **Sonarr**, **Radarr**) — one section visible at a time; **Global** includes backup/restore and software updates; **Security** stays on separate forms. Initial tab respects **`tab=`**, **`sec=`**, **`import=`** (opens **Global**), **`test=`**, and hash anchors.
- **`fetch_latest_app_snapshots`** (**`app/db.py`**): single query for latest **Sonarr** / **Radarr** / **Emby** **`app_snapshot`** rows.

### Changed

- **UI copy:** Sentence-style capitalization for buttons, labels, and hints where it was inconsistent; aligned **Sonarr** / **Radarr** lead lines and **Dashboard** hero labels (**Sonarr** / **Radarr upgrades**); connection test snapshot messages use **Connection test succeeded** / **Connection test failed**.
- **Spacing:** Shared **`--space-*`** scale in **`app.css`**, **`.btn-row`** alignment, **`.hint`** rhythm with **`hint + .btn-row`**, utilities (**`lead-muted`**, **`gc-title--sm`**, **`settings-security-h3`**, etc.); removed inline **`style=`** from main templates.
- **`prune_old_records`:** Optional **`settings`** argument so **`run_once`** does not load **`AppSettings`** twice per tick.

### Fixed

- **Dashboard:** Removed duplicate **`AppSnapshot`** queries on the same request (HTML page + **`_build_dashboard_status`**).
- **HTTP status hints:** **401** hint uses lowercase **settings** in running text.

## [2.0.2] - 2026-03-23

### Added

- **Trimmer:** Top **Review** / **Settings** tabs (same style as Fetcher Settings) on **`/trimmer`** and **`/trimmer/settings`** via **`macros/trimmer_area.html`**. **Trimmer Settings** adds a second tab row — **Connection**, **Schedule & limits**, **TV & movie rules**, **People rules** — with anchored sections and striped cards.
- **Tests:** **`tests/test_form_helpers.py`** for URL normalization (reject autofill tokens like **`admin`** as whole-field “URLs”).

### Changed

- **Fetcher Settings** is **Fetcher-only** again (Security, Sonarr, Radarr, Global): removed the embedded Trimmer card; **Global** points to sidebar **Trimmer** → **Trimmer Settings**.
- **Setup wizard:** Separate **Sonarr**, **Radarr**, and **Emby Trimmer** run interval fields (aligned with full Settings); **example URLs** on Sonarr/Radarr/Emby steps; **`autocomplete="off"`** on wizard steps after account; **`setup_helpers.normalize_setup_url`** delegates to **`form_helpers._normalize_base_url`**.

## [2.0.1] - 2026-03-23

### Fixed

- **Windows dev:** **`scripts/stop-fetcher-dev.ps1`** frees typical dev ports (**8766–8770**) and stops stray **Fetcher `uvicorn app.main:app`** processes and orphan **`multiprocessing` `python.exe`** workers (parent PID gone). **`scripts/dev-start.ps1`** invokes it before binding so **`--reload`** no longer leaves **8766** stuck.

### Added

- **Docs:** **[`docs/WORKSPACE-FOLDER.md`](docs/WORKSPACE-FOLDER.md)** — use local folder **`Fetcher`** (not legacy **`grabby`**), **`fetcher.code-workspace`** for Cursor/VS Code, and **`scripts/rename-local-repo-folder.ps1`** to rename an existing clone.

## [2.0.0] - 2026-03-22

**First semver 2.x release** — rebrand from Grabby to Fetcher, Cleaner to Trimmer, full V5 UI, Windows installer via GitHub Actions, and CI/packaging fixes so the frozen app builds and smoke-tests cleanly on Windows.

### Changed

- **Rebrand:** Application identity is **Fetcher** across the app, docs, installer, Windows service, environment variables (**`FETCHER_*`**), session cookie (**`fetcher_session`**), default SQLite file (**`fetcher.db`**), backup JSON magic (**`fetcher_backup`**), and GitHub defaults (**`jampat000/Fetcher`**, **`FetcherSetup.exe`**).
- **Trimmer:** Emby maintenance UI and routes under **`/trimmer`** / **`/trimmer/settings`**; activity kind **`trimmed`** (stored rows migrated from the pre-rename kind on upgrade). **`POST /trimmer/settings/cleaner`** remains the form action for Trimmer-specific settings.
- **Fetcher Settings:** Horizontal **tabs** (Security, Sonarr, Radarr, Global) at the top; each section is its own **`card gc`** with **stripe** accent (**purple** / **blue** / **green** / **gray**). Collapsible panels removed — sections stay open for faster editing.
- **Dashboard:** Hero tiles for **TV Missing**, **Movies Missing**, **TV Upgrades**, and **Movie Upgrades**; Sonarr/Radarr overview cards no longer duplicate those queue counts; **Automation** and **Emby Trimmer** cards trimmed to essential status; **Recent activity** preview shows up to **5** rows.

### Fixed

- **Windows / CI installer build:** **`requirements.txt`** pinned **`uvloop`** without a platform marker. **`uvloop` does not install on Windows**, so **`pip install -r requirements.txt` failed** while **`packaging/build.ps1` did not check the exit code** — the venv was missing **`uvicorn`** and the frozen **`Fetcher.exe` crashed** at import. **`uvloop`** is now **`sys_platform != "win32"`**, and **`packaging/build.ps1`** fails fast if **`pip`** / **PyInstaller** errors.
- **CI — Build installer smoke:** Packaged **Fetcher.exe** smoke test could time out because the server does not accept connections until app **lifespan** finishes, while the **scheduler** could start **`run_once`** (Arr/Emby HTTP) before **/healthz** was reachable. The workflow sets **`FETCHER_CI_SMOKE=1`** and **`FETCHER_DEV_DB_PATH`** to a temp DB, uses **`WorkingDirectory`** = the one-folder bundle dir, waits up to **6 minutes**, and surfaces **exit code** if the process dies early. **`FETCHER_CI_SMOKE`** skips **`scheduler.start()`** in **`app/main.py`** (CI / smoke tests only — do not set on a real Windows service install).

## [1.0.44] - 2026-03-22

### Fixed

- **CHANGELOG:** **1.0.43** release header dated **2026-03-22** (maintainer **system date** on ship day), replacing an incorrect **2026-03-23** in published history.

## [1.0.43] - 2026-03-22

### Fixed

- **CHANGELOG:** **1.0.42** release header date corrected to **2026-03-22**.

## [1.0.42] - 2026-03-22

### Added

- **Settings:** Collapsible sections (**Sonarr**, **Radarr**, **Global Fetcher**, **Security**) with chevron headers, **`localStorage`** open/closed state, and URL-driven expansion after saves/tests (**`saved`**, **`save`**, **`test`**, **`sec`**).

### Changed

- **Trimmer:** **`Scan Emby for matches`** runs preview-only when **dry run** is on; when **dry run** is off, the same scan applies live deletes (shared **`apply_emby_trimmer_live_deletes`** in **`service_logic`**). Removed **`POST /trimmer/delete-matched`** and the separate delete button.
- **Activity** / **Dashboard:** Activity kind tags show **Quality Upgrade** and **Trimmer**.

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

- **Database pruning:** **`prune_old_records`** during scheduled/Fetcher runs removes stale **`arr_action_log`** rows (window from **`arr_search_cooldown_minutes`**, or 48h when cooldown is 0) and **`activity_log`**, **`job_run_log`**, **`app_snapshot`** rows older than **`log_retention_days`** (clamped 7–3650). Failures are logged only. Unit tests: **`tests/test_pruning.py`**.

### Security

- **CSRF protection** for state-changing **HTML form** POSTs: signed tokens (**`itsdangerous.TimestampSigner`**, 1-hour validity) bound to the session user (or IP-allowlist account). **`require_csrf`** on **`/settings`**, **`/settings/auth/*`**, **`/settings/backup/import`**, **`/trimmer/settings`**, **`/trimmer/settings/connection`**, **`/trimmer/settings/trimmer`**, **`/test/sonarr`**, **`/test/radarr`**, **`/test/emby`**, **`/test/emby-form`**, and **`POST /setup/{step}`** for steps **1–5** (step **0** exempt). Excludes **`/login`**, JSON APIs (**`/api/arr/search-now`**, **`/api/setup/test-*`**), and wizard **`fetch()`** tests. Layout **`<meta name="csrf-token">`**, global **`getCSRFToken()`**, hidden fields on templates; **`tests/test_csrf.py`** with **`real_csrf`** marker; other tests override **`require_csrf`** in **`tests/conftest.py`**.

### Changed

- **CI:** **Tag release (from VERSION)** also runs when **`VERSION`** changes on branch **`dev`** (creates **`vX.Y.Z`** and dispatches **Build installer**), so **“bump and ship”** can target **`dev`** without waiting on **`master`**.
- **README (dev):** Document **`require_csrf`** test override and CSRF behavior for manual POSTs.

## [1.0.38] - 2026-03-23

### Added

- **`tests/test_auth_next_redirect.py`:** Covers **`sanitize_next_param`**, login **`next`** redirect, and open-redirect rejection.

### Changed

- **Settings → Security:** In-page **subnav** (Account, **Change Username**, **Change Password**, Access control) with fragment anchors; card layout for account vs access control; headings and copy state that username and password are changed separately.
- **Sign-in:** Unauthenticated visits redirect to **`/login?next=…`** (safe, same-origin paths only) so after login you return to the requested page (e.g. **Settings**).
- **POST `/settings`**, **`POST /trimmer/settings`**, **`POST /trimmer/settings/connection`**, **`POST /trimmer/settings/trimmer`:** **`SQLAlchemyError`** → **`save=fail&reason=db_error`**; **`ValueError`** → **`reason=invalid`**; other exceptions → **`logger.exception`**, session **rollback**, **`reason=error`**; **`FETCHER_LOG_LEVEL=DEBUG`** re-raises after logging.

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

- **Auth (critical):** **`require_auth`** now raises **`FetcherAuthRequired`** with a **`RedirectResponse`** instead of returning it — FastAPI ignores **`Response`** objects returned from **`dependencies=[Depends(...)]`**, so Sign-in redirects previously did not run for protected routes.
- **Auth UX (upgrades + new installs):** Until **`auth_password_hash`** is set, protected pages and **`/login`** redirect to **`/setup/0`** instead of a non-working Sign-in. **LAN bypass** does not skip account setup (no passwordless LAN after migration). **`/logout`** sends you to **`/setup/0`** when no password is set. JSON/API requests without a password return **401** with **`setup_path`**. Setup wizard **step 0** shows an upgrade-specific banner when Sonarr/Radarr/Emby already look configured; **“Skip wizard → Settings”** is hidden on step 0 (Settings required sign-in).
- **E2E:** Playwright server uses a **temp SQLite** via **`FETCHER_DEV_DB_PATH`** and a one-shot DB init so **`/setup/1`** tests are reproducible.

### Documentation

- **README.md:** Install & first-run flow with **sign-in** and **Setup** (account + Arr/Emby); **dev** section for testing auth (`/setup/0`, reset dev DB, **`FETCHER_RESET_AUTH`**, **`pytest`** vs browser); **Backup** sensitivity; **`/healthz`** / **`/api/version`** noted as unauthenticated; **upgrade** note (first visit → account setup, Arr settings kept).
- **`service/README.md`**, **`HOWTO-RESTORE.md`**, **`SECURITY.md`:** Align with password flow and lockout recovery.

## [1.0.34] - 2026-03-26

### Changed

- **`app/main.py`:** Non-route helpers moved to **`app/constants.py`** (timezone / genre / credit option lists), **`app/form_helpers.py`** (URL + timezone + people-credit form helpers), and **`app/display_helpers.py`** (schedule display, local time formatting).
- **`services/`** removed; API key resolution lives under **`app/resolvers/`** (imports and **PyInstaller** `hiddenimports` updated).
- **`app/migrations.py`:** Startup migrations split into ordered **`_migrate_001_…`** … **`_migrate_017_…`** steps (same SQL and behavior as before).

### Added

- **`requirements.in`** + pip-compile workflow note in **`requirements.txt`**; **`pip-tools`** in **`requirements-dev.txt`**.
- Unit tests: **`tests/test_emby_trimmer.py`** (`evaluate_candidate`), **`tests/test_run_once_cooldown.py`** (`_filter_ids_by_cooldown`, `_paginate_wanted_for_search` with in-memory SQLite).

## [1.0.33] - 2026-03-25

### Changed

- **Schema migration:** Removed obsolete **`app_settings`** columns **`interval_minutes`**, global **`search_missing` / `search_upgrades`**, and **`max_items_per_run`** (per-app Sonarr/Radarr fields are the only source of truth). Startup migration copies values into per-app columns when safe, then **`DROP COLUMN`** (requires SQLite **3.35+**).
- **Backup:** **`format_version`** is now **`2`**; imports still accept **`1`**. Old global keys in v1 JSON are mapped onto per-app fields when needed.
- **Trimmer:** Only **`?scan=1`** (or other truthy **`scan`**) runs an Emby library pull — **`?preview=1`** is no longer honored.
- **Setup wizard (step 4):** form field renamed to **`run_interval_minutes`** (still sets Sonarr/Radarr/Emby run intervals).

### Fixed

- **`FETCHER_DEV_DB_PATH`:** **`app/db.py`** now honors this env var (documented since 1.0.29). **`scripts/dev-start.ps1`** sets it to **`%TEMP%\fetcher-dev.sqlite3`** by default and adds **`-SharedAppDb`** to use the normal per-user data directory (same file as the installed service when unscoped).

### Added

- Optional root **`config.yaml`** (gitignored): **`SONARR_API_KEY`**, **`RADARR_API_KEY`**, **`EMBY_API_KEY`** loaded via **PyYAML** in **`app/config.py`**. **`app/resolvers/api_keys.py`** resolves keys for the scheduler, **`run_once`**, and connection tests (**YAML overrides DB** when set). Tracked template: **`config.example.yaml`**.

## [1.0.32] - 2026-03-24

### Changed

- **Installer build:** WinSW is **bundled** as **`installer/bin/WinSW.exe`** and staged with **`installer/setup.py`** (skips copy if **`service/winsw.exe`** already exists). **`installer/build.ps1`** no longer downloads WinSW from the network.

## [1.0.31] - 2026-03-23

### Security

- **Logging:** Root log level defaults to **WARNING** (override with **`FETCHER_LOG_LEVEL`**). **`SensitiveLogFilter`** + **`RedactingFormatter`** redact URLs, **`api_key` / `sonarr_key` / `token`**-style values, **Bearer** tokens, and **Authorization** headers in formatted log lines (including tracebacks). **`uvicorn`** CLI uses **`log_level=warning`**. Persisted job run / activity messages from HTTP errors and generic exceptions are passed through **`redact_sensitive_text`**.

## [1.0.30] - 2026-03-22

### Fixed

- **Software updates (Settings):** Update check is more resilient when GitHub’s API omits **`assets`** (still builds the conventional **`/releases/download/<tag>/FetcherSetup.exe`** URL). Asset name matching is **case-insensitive**. **Manual “Check for Updates”** sends **`?refresh=1`** to skip the 15‑minute in-memory cache. Failures log a warning server-side; the browser shows **HTTP status / non‑JSON** hints instead of a generic “could not reach” when the app returns an error page.

## [1.0.29] - 2026-03-22

### Fixed

- **Schedule weekdays:** Unchecking all days and saving no longer re-selects every day; **empty** `*_schedule_days` persists, **`in_window`** treats **no days** as never matching while the schedule is on, and the settings UI **`checked`** state follows the stored CSV.

### Changed

- **Schedule weekday defaults:** New **`app_settings`** rows and freshly added schedule columns default to **no days** (`""`); scheduler **`getattr`** fallbacks use **`""`**. Existing databases keep stored CSV until you save again.
- **Schedule weekday UI:** **7-column grid** (4 on narrow viewports) with **chip-style labels**; native checkboxes + **`:has(:checked)`** (no schedule JS).
- **Schedules (Fetcher + Trimmer):** Per-day **`int = Form(0)`** fields (**`sonarr_schedule_Mon` … `_Sun`**, Radarr/Emby same); time `<select>` grids with orphan saved times; **`days_selected`** from the **raw DB** column. **No day checked** stores **`""`**; comma-only / invalid tokens still normalize to **full week** for legacy rows.
- **Dev server (`dev-start.ps1`):** default **`FETCHER_DEV_DB_PATH`** = **`%TEMP%\fetcher-dev.sqlite3`** (optional **`-SharedAppDb`**). **`app/db.py`** honors **`FETCHER_DEV_DB_PATH`**.
- **App startup / SQLite:** **`create_all` + `migrate`** retry with backoff; **WAL** + **busy_timeout** where possible; settings **`POST`** uses **`_try_commit_and_reschedule`** and friendlier **`save=fail`** redirects (validation, DB busy, errors).
- **Typography (app UI):** **`--dash-fs-*`** / **`--dash-fw-*`** on **`.main`** and forms; consistent scale across Settings, Trimmer, Activity, Logs, Setup.
- **Dashboard → Automation / Overview:** Automation card shows **last run** + **next tick**; Overview **preview-stat** grids for Sonarr/Radarr (**Schedule Window** tile with days + friendly time). **Job logs:** skip lines include **minutes since last run**, interval, and **~minutes until eligible**.

## [1.0.28] - 2026-03-22

### Fixed

- **Web UI:** Cache-bust **`/static/app.css`** with **`?v=<app_version>`** (same as **`app.js`**) so browser upgrades don’t keep an old stylesheet after installing a new build — fixes oversized or “wrong” dashboard typography vs dev.
- **In-app upgrade / update check:** If **`GITHUB_TOKEN`** or **`FETCHER_GITHUB_TOKEN`** is set to an **invalid** value (common on dev/media PCs), the GitHub API returned **401** with **no** fallback. Fetcher now **retries the API without** that header, then falls back to **web/Atom** like rate limits. **Installer downloads** from **`github.com/.../releases/download/...`** no longer send **`Authorization`**, so a bad global token won’t break **`FetcherSetup.exe`** downloads.

## [1.0.27] - 2026-03-20

### Changed

- **Dashboard (Overview):** One intro line (what the buttons do + schedule/cooldown) above **Missing** / **Upgrade** (standard **secondary** button typography) under **Sonarr** (TV) and **Radarr** (movies). Each is a **one-time** action that bypasses **schedule windows** and **run-interval** gates for that action only; **per-item cooldown** still applies. **Emby Trimmer** is not run. **`POST /api/arr/search-now`** JSON body: **`scope`** = `sonarr_missing` \| `sonarr_upgrade` \| `radarr_missing` \| `radarr_upgrade`.
- **Fetcher Settings:** Removed **scheduler base interval** from **Global Settings**; wake cadence is the **minimum** of **Sonarr** and **Radarr** **run intervals** only (legacy **`interval_minutes`** column kept for backups). **Global** layout stacks **Arr search cooldown** and **timezone** with wrapping so they fit narrow windows.
- **Setup wizard (step 4):** The **run interval** field now seeds **Sonarr**, **Radarr**, and **Emby Trimmer** intervals together (no separate global base).

### Fixed

- **CI / releases:** Pushing a tag with the default **`GITHUB_TOKEN`** does **not** start other workflows, so **v1.0.26** could exist as a **git tag** while **GitHub Releases “Latest”** stayed on **v1.0.25** with no new **`FetcherSetup.exe`**. **Tag release** now **`workflow_dispatch`es** **Build installer** for the new tag; **Build installer** also publishes a release when run **manually** with ref = that **tag** (recover a missed build).

## [1.0.26] - 2026-03-21

### Fixed

- **Software updates / GitHub API rate limits:** On **403** or **429** from **`api.github.com`**, fall back to **`github.com/.../releases/latest`** and **`releases.atom`** so the check still works without a token. Cache successful lookups (**`FETCHER_UPDATES_CACHE_SECONDS`**, default **900**) to avoid burning the **60/hour** unauthenticated API quota.
- **Dev server (`dev-start.ps1`):** Frees the chosen port by stopping **every** listener PID (not just the first), uses **`taskkill`** when **`Stop-Process`** fails, optionally **`Stop-NetTCPConnection`**, and **`-TryElevatedKill`** for a one-time **UAC** kill attempt. Clearer errors if the port stays busy (ghost/stale listeners).

### Changed

- **CI / releasing:** When **`VERSION`** changes on **`master`** or **`main`**, **Tag release (from VERSION)** runs automatically, creates **`vX.Y.Z`** if missing, and pushes it — **Build installer** then runs on that tag (no local `git tag` / `git push`). **Actions → Tag release (from VERSION) → Run workflow** remains available to retry or tag without editing `VERSION` again.
- **Docs:** **[`docs/GITHUB-CLI.md`](docs/GITHUB-CLI.md)** (Windows **`gh`** PATH, **`gh auth login`**, merge/release commands, pruning old releases).

## [1.0.25] - 2026-03-21

### Fixed

- **Sonarr / Radarr run interval:** Stored **`0`** was only fixed once when `arr_interval_defaults_applied` was added; saving the form or old DBs could keep **`0`**. Run intervals now enforce **minimum 1** in the UI, **coerce legacy 0 → 60** on every save (Pydantic) and **on every startup** (migration), so the fields show real minutes (default **60**), not **`0`**.
- **Software updates / GitHub:** Update check uses a proper **`User-Agent`** (version + repo URL), optional **`FETCHER_GITHUB_TOKEN`** / **`GITHUB_TOKEN`** for rate limits or private repos, and clearer messages when GitHub returns **403** (includes API `message` when present).
- **Dev server:** **`scripts/dev-start.ps1`** frees the preferred port by stopping **any** process listening there (not only Python), using **`Get-NetTCPConnection`** instead of parsing `netstat`.

## [1.0.24] - 2026-03-21

### Fixed

- **Sonarr/Radarr run interval:** Existing installs that still had **0** stored now get a **one-time** DB update to **60** on startup (same as new defaults). **0** (“use scheduler base”) can still be set manually in Settings.

### Changed

- **Scheduler vs Emby Trimmer:** Fetcher’s **wake interval** is the **minimum** of **Sonarr** and **Radarr** **run intervals** (under each app’s schedule). **Emby Trimmer** cadence is under **Trimmer Settings** (`emby_interval_minutes`). The legacy **`interval_minutes`** DB column remains for backups only — it is no longer shown in **Global Settings**.
- **Settings UI:** Run interval layout and **Global Settings** grid; **Sonarr/Radarr** defaults **60** (model + form); `placeholder="60"` on interval fields; **`arr_interval_defaults_applied`** one-time migration flag.

## [1.0.23] - 2026-03-20

### Fixed

- **Emby Trimmer + Sonarr:** Trimmer always **deletes Sonarr episode files** when a file exists (disk + Sonarr state) for matched TV items. Shows **still airing** (`status` not `ended`) then get those episodes **left monitored** so the season/show keeps grabbing new episodes. **Ended** series get those episodes **unmonitored** after delete once your Trimmer rules matched (watched / criteria).

### Changed

- **Settings UI:** Removed **Global run interval** from **Fetcher Settings**; scheduler / **Emby Trimmer** run interval is edited under **Trimmer Settings → Global Trimmer Settings**. **Sonarr** / **Radarr run interval** moved under each app’s **schedule window** section (still **`0`** = use that shared scheduler interval). **Global Settings** section label; removed **Save All Fetcher Settings**.

## [1.0.22] - 2026-03-21

### Added

- **Fetcher scheduler — Sonarr / Radarr run intervals:** Under **Settings → Fetcher scheduler**, optional **Run interval — Sonarr** and **Run interval — Radarr** (minutes). **`0`** uses the **Global run interval**. One scheduler wake runs at the **minimum** of global + configured Arr intervals; each app is skipped until its own interval has elapsed since the last run (Emby uses the global interval only).

## [1.0.21] - 2026-03-20

### Fixed

- **Arr search repeats:** cooldown now applies per **Sonarr/Radarr library item** (episode/movie id), not separately for “missing” vs “upgrade”, so the same title is not triggered twice in one run. **Arr search cooldown** is a dedicated setting (default **24 hours**), independent of scheduler interval—`0` restores the old “match run interval” behavior.
- **Wanted queue coverage:** Sonarr/Radarr missing and cutoff-unmet handling **walks multiple API pages** per run until “max items per run” is filled with items that pass cooldown (or the queue ends). Previously only **page 1** was used, so the same top titles were the only candidates forever — unlike Huntarr-style tools that batch through the full backlog.
- **Radarr/Sonarr IDs:** tolerate numeric ids returned as strings in Arr JSON when extracting episode/movie ids.

### Changed

- **Windows service (WinSW sample):** default bind address is **`0.0.0.0`** so the Web UI is reachable from other machines on the LAN (use firewall rules; UI has no built-in login).

## [1.0.20] - 2026-03-20

### Fixed

- **Arr search/upgrade loops:** added a per-item cooldown (`arr_action_log` + cooldown filtering) so Fetcher does not keep re-triggering the same missing/upgrade search for the same movie/episode every scheduler tick.

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

- **Activity detail logging:** per-run entries now include item-level context (movie titles, Sonarr episode labels, and Emby Trimmer item names) instead of count-only summaries.

### Changed

- **Activity UI:** Dashboard and Activity pages now show detail lines under each event when available.
- **Data model/migration:** `activity_log` gains a `detail` text column with backward-compatible migration.

## [1.0.14] - 2026-03-20

### Changed

- **Trimmer → Sonarr anti-boomerang:** after live TV deletes, Sonarr is now unmonitored at the **episode level** (`/api/v3/episode/monitor`) instead of whole-series unmonitor.
- **Matching logic:** TV delete candidates map from Emby to Sonarr using `Tvdb` first, then `title+year`; season/series deletes expand to all matching episode IDs.

## [1.0.13] - 2026-03-20

### Fixed

- **Schedules:** selecting all schedule days no longer reverts unexpectedly; schedule-day columns are stored as `TEXT` and migration widens legacy strict DB schemas.
- **Tests:** added regression coverage to ensure Fetcher + Trimmer schedules stay enabled with all 7 days selected.

## [1.0.11] - 2026-03-20

### Fixed

- **Sonarr:** `fetcher-missing` / `fetcher-upgrade` tags now apply to **series** via `PUT /api/v3/series/editor` (Sonarr has no episode-level tag editor; the old path caused `HTTPStatusError`, often 404).

### Changed

- **Logs:** Sonarr/Radarr tag-apply warnings include **HTTP status, hint, and response snippet** when the API returns an error (`format_http_error_detail`).

## [1.0.10] - 2026-03-20

### Added

- **Settings → Software Updates:** **Check for Updates** button (explicit refresh; still auto-checks on load).

### Changed

- **Fetcher Settings / Trimmer Settings:** scoped **Save … Settings** actions (Sonarr, Radarr, global Fetcher; Trimmer global + content criteria for TV/Movies) so you do not have to save the whole page at once.
- **Trimmer Settings:** headings (**Emby Trimmer Settings**, **Global Trimmer Settings**, **Content Criteria Settings**) and layout aligned with those saves.

## [1.0.9] - 2026-03-20

### Added

- **Settings → Software updates:** checks **GitHub Releases** against the installed version; **Upgrade automatically** downloads `FetcherSetup.exe` and runs it **silently** (Windows installed build only). Optional env: `FETCHER_UPDATES_REPO`, `FETCHER_ALLOW_DEV_UPGRADE`, `GET /api/updates/check`, `POST /api/updates/apply`.

## [1.0.8] - 2026-03-20

### Added

- **Dashboard — Automation:** last run summary (time, OK/fail, short message) and **next scheduler tick** (interval + note about per-app schedule windows).
- **Trimmer Settings:** prominent **Dry run** vs **Live delete** banners; muted banner when **Emby Trimmer** is disabled.

### Changed

- **Naming:** user-facing **Emby** maintenance wording → **Emby Trimmer** (templates, messages, docs). Internal activity tagging for Trimmer runs unchanged in this release (later releases use the **`trimmed`** kind).
- **Reliability:** Sonarr/Radarr (**ArrClient**) and **Emby** HTTP calls use **retries with backoff** on transient errors (connection/timeouts, 429/502/503/504).
- **Logs / snapshots:** HTTP failures append short **hints** for common status codes (401/403/404, etc.).
- **CI:** removed CodeQL workflow for private-repo plan compatibility (keep `pytest` + `pip-audit`).
- **Docs:** `SECURITY.md`, branch-protection docs, and import JSON updated to require only supported checks (`Test / pytest`, `Security / pip-audit`).

## [1.0.7] - 2026-03-20

### Removed

- **Settings:** expandable “Setup wizard vs this page vs Trimmer” explainer (redundant); wizard “tip” on the final step that pointed to it.

### Added

- **Contributing / governance:** [`CONTRIBUTING.md`](CONTRIBUTING.md); [`.github/BRANCH_PROTECTION.md`](.github/BRANCH_PROTECTION.md), [`.github/IMPORT-BRANCH-PROTECTION.md`](.github/IMPORT-BRANCH-PROTECTION.md), [`.github/rulesets/master-middle-ground.json`](.github/rulesets/master-middle-ground.json), [`.github/branch-protection-classic-master.json`](.github/branch-protection-classic-master.json); PR template; [`.github/CODEOWNERS`](.github/CODEOWNERS); [`scripts/protect-master-branch.ps1`](scripts/protect-master-branch.ps1).
- **Log hygiene:** [`app/log_sanitize.py`](app/log_sanitize.py) redacts credential-like query params (and userinfo) from URLs before persisting HTTP error lines in job logs; [`tests/test_log_sanitize.py`](tests/test_log_sanitize.py).

### Changed

- [`SECURITY.md`](SECURITY.md): PAT hygiene, threat model, default-branch notes; [`README.md`](README.md): contributing + branch protection pointer; **Dependabot** dependency PRs labeled `dependencies`.
- **Dependencies:** `python-multipart` **0.0.22** (CVE-2026-24486), `starlette` **0.52.1** (CVE-2025-54121, CVE-2025-62727), `fastapi` **0.129.2** (compatible Starlette range).

## [1.0.6] - 2026-03-22

### Added
- **First-run setup wizard** (`/setup`): guided steps for Sonarr, Radarr, Emby (with **Test connection** via JSON API), schedule interval & timezone; final **Next steps** screen with links to Fetcher Settings, Trimmer Settings, and Trimmer.
- **Setup** sidebar entry; dashboard CTA when no stack URLs are configured; **dismissible** dashboard banners (stored in `localStorage`).
- **API:** `POST /api/setup/test-sonarr`, `test-radarr`, `test-emby` for wizard tests.
- **Trimmer:** default **`GET /trimmer`** no longer scans Emby (fast sidebar); use **`Scan Emby for matches`** (`/trimmer?scan=1`).
- **Service upgrades:** [`service/UPGRADE.md`](service/UPGRADE.md) for replacing the Windows install / exe.
- Playwright **E2E smoke tests** ([`tests/e2e/`](tests/e2e/)) against a live uvicorn process (`healthz`, setup step 1, Trimmer page).
- **Settings:** **Backup** download filename uses **dd-mm-yyyy**.

### Changed
- **Backup JSON:** human-readable **dd-mm-yyyy** datetime strings (`exported_at`, settings columns); **ISO-8601 strings from older backups still import**.
- **Dates in UI:** sidebar clock, activity, and logs use **dd-mm-yyyy**-style display.
- **FastAPI:** **lifespan** context for startup/shutdown (replaces deprecated `@app.on_event`).
- **Templates:** **Starlette**-style `TemplateResponse(request, name, context)` (no deprecation warning).
- **`datetime.utcnow()`** replaced with **`utc_now_naive()`** ([`app/time_util.py`](app/time_util.py)) for ORM and scheduler use.
- **CI Test workflow:** install **Playwright Chromium** before `pytest`.
- **Build installer workflow:** runs on **`v*`** tags and **manual** `workflow_dispatch` only (no longer on every branch/PR push).
- **Backup & Restore:** one JSON file for all **Fetcher** and **Trimmer** settings; export metadata `includes` clarifies scope.
- **`/healthz`** includes **`version`**; **`GET /api/version`** added.
- Windows CI smoke: start packaged **`Fetcher.exe`**, probe **`/healthz`**.
- **pip-audit** (`security.yml`); **`SECURITY.md`**.

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
- Tracked `packaging/fetcher.spec` so GitHub Actions can build the installer.

## [1.0.3] - 2025-03-20

### Fixed
- PyInstaller/Inno CI failure: `fetcher.spec` was gitignored and missing on runners.

## Releasing (maintainers)

1. Update this file: move **`[Unreleased]`** items under a new **`[X.Y.Z] - YYYY-MM-DD`** heading (**use the machine system date at ship time** — Windows PowerShell: **`Get-Date -Format yyyy-MM-dd`**; do not guess), then keep **`[Unreleased]`** empty (or note pending work).
2. Bump **`VERSION`** to match the release.
3. **Merge-first (recommended):** Commit on **`release/vX.Y.Z`** from **`origin/master`**, open **PR → `master`**, merge when checks pass. A push to **`master`** that changes **`VERSION`** runs **Tag release (from VERSION)** — creates **`vX.Y.Z`** if missing, then dispatches **Build installer** and **Docker publish** (see **`.github/workflows/tag-release.yml`**). After merge: **`git switch master && git pull --ff-only`**, delete the remote release branch if you like.
4. **Shortcut:** **`.\scripts\ship-release.ps1`** on your release branch pushes **`origin`** and dispatches **Tag release** on that ref (useful before merge; still merge to **`master`** so **`VERSION`** matches the default branch).
5. After **`git fetch origin master --tags`**, you may run **`gh workflow run build-installer.yml --repo jampat000/Fetcher --ref vX.Y.Z`** — **only** if **`vX.Y.Z`** points to the commit you intend to ship (**ref trap:** workflow YAML comes from that tag’s commit). Prefer **Tag release** so **Docker publish** uses **`checkout_ref`** correctly. See **`.cursor/rules/github-installer-workflow-ref-trap.mdc`**.
6. If tagging did not run, use **Actions → Tag release (from VERSION) → Run workflow** on **`master`** (or your release branch). Avoid hand-creating tags only from the **Releases** UI unless you know the commit matches **`VERSION`**.
7. If a **tag** exists but **Releases → Latest** never updated (no **`FetcherSetup.exe`**), compare **`git rev-parse vX.Y.Z`** vs **`git rev-parse origin/master`**. An **old** tag SHA can **build** but **skip** the **release** job. **Fix:** move the tag, **or** bump **`VERSION`** and release again, **or** **`gh release create`** + attach **`FetcherSetup.exe`** from a green artifact.
8. Follow **GitHub Actions** / environment rules for approving production releases if configured.
9. **Compare links** at the end of this file list **recent v2.x** diffs. **v1.x** and older: **[GitHub Releases](https://github.com/jampat000/Fetcher/releases)**.

[Unreleased]: https://github.com/jampat000/Fetcher/compare/v2.4.11...HEAD
[2.4.11]: https://github.com/jampat000/Fetcher/compare/v2.4.10...v2.4.11
[2.4.10]: https://github.com/jampat000/Fetcher/compare/v2.4.9...v2.4.10
[2.4.9]: https://github.com/jampat000/Fetcher/compare/v2.4.8...v2.4.9
[2.4.8]: https://github.com/jampat000/Fetcher/compare/v2.4.7...v2.4.8
[2.4.7]: https://github.com/jampat000/Fetcher/compare/v2.4.6...v2.4.7
[2.4.6]: https://github.com/jampat000/Fetcher/compare/v2.4.5...v2.4.6
[2.4.5]: https://github.com/jampat000/Fetcher/compare/v2.4.4...v2.4.5
[2.4.4]: https://github.com/jampat000/Fetcher/compare/v2.4.3...v2.4.4
[2.4.3]: https://github.com/jampat000/Fetcher/compare/v2.4.2...v2.4.3
[2.4.2]: https://github.com/jampat000/Fetcher/compare/v2.4.1...v2.4.2
[2.4.1]: https://github.com/jampat000/Fetcher/compare/v2.4.0...v2.4.1
[2.4.0]: https://github.com/jampat000/Fetcher/compare/v2.3.17...v2.4.0
[2.3.17]: https://github.com/jampat000/Fetcher/compare/v2.3.16...v2.3.17
[2.3.16]: https://github.com/jampat000/Fetcher/compare/v2.3.15...v2.3.16
[2.3.15]: https://github.com/jampat000/Fetcher/compare/v2.3.14...v2.3.15
[2.3.14]: https://github.com/jampat000/Fetcher/compare/v2.3.13...v2.3.14
[2.3.13]: https://github.com/jampat000/Fetcher/compare/v2.3.12...v2.3.13
[2.3.12]: https://github.com/jampat000/Fetcher/compare/v2.3.11...v2.3.12
[2.3.11]: https://github.com/jampat000/Fetcher/compare/v2.3.10...v2.3.11
[2.3.10]: https://github.com/jampat000/Fetcher/compare/v2.3.9...v2.3.10
[2.3.9]: https://github.com/jampat000/Fetcher/compare/v2.3.8...v2.3.9
[2.3.8]: https://github.com/jampat000/Fetcher/compare/v2.3.7...v2.3.8
[2.3.7]: https://github.com/jampat000/Fetcher/compare/v2.3.6...v2.3.7
[2.3.6]: https://github.com/jampat000/Fetcher/compare/v2.3.5...v2.3.6
[2.3.5]: https://github.com/jampat000/Fetcher/compare/v2.3.4...v2.3.5
[2.3.4]: https://github.com/jampat000/Fetcher/compare/v2.3.3...v2.3.4
[2.3.3]: https://github.com/jampat000/Fetcher/compare/v2.3.2...v2.3.3
[2.3.2]: https://github.com/jampat000/Fetcher/compare/v2.3.1...v2.3.2
[2.3.1]: https://github.com/jampat000/Fetcher/compare/v2.3.0...v2.3.1
[2.3.0]: https://github.com/jampat000/Fetcher/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/jampat000/Fetcher/compare/v2.1.1...v2.2.0
[2.1.1]: https://github.com/jampat000/Fetcher/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/jampat000/Fetcher/compare/v2.0.25...v2.1.0
[2.0.25]: https://github.com/jampat000/Fetcher/compare/v2.0.24...v2.0.25
[2.0.24]: https://github.com/jampat000/Fetcher/compare/v2.0.23...v2.0.24
[2.0.23]: https://github.com/jampat000/Fetcher/compare/v2.0.20...v2.0.23
[2.0.20]: https://github.com/jampat000/Fetcher/compare/v2.0.19...v2.0.20
[2.0.19]: https://github.com/jampat000/Fetcher/compare/v2.0.18...v2.0.19
[2.0.18]: https://github.com/jampat000/Fetcher/compare/v2.0.17...v2.0.18
[2.0.17]: https://github.com/jampat000/Fetcher/compare/v2.0.16...v2.0.17
[2.0.16]: https://github.com/jampat000/Fetcher/compare/v2.0.15...v2.0.16
[2.0.15]: https://github.com/jampat000/Fetcher/compare/v2.0.14...v2.0.15
[2.0.14]: https://github.com/jampat000/Fetcher/compare/v2.0.13...v2.0.14
[2.0.13]: https://github.com/jampat000/Fetcher/compare/v2.0.12...v2.0.13
[2.0.12]: https://github.com/jampat000/Fetcher/compare/v2.0.11...v2.0.12
[2.0.11]: https://github.com/jampat000/Fetcher/compare/v2.0.10...v2.0.11
[2.0.10]: https://github.com/jampat000/Fetcher/compare/v2.0.9...v2.0.10
[2.0.9]: https://github.com/jampat000/Fetcher/compare/v2.0.8...v2.0.9
[2.0.8]: https://github.com/jampat000/Fetcher/compare/v2.0.7...v2.0.8
[2.0.7]: https://github.com/jampat000/Fetcher/compare/v2.0.6...v2.0.7
[2.0.6]: https://github.com/jampat000/Fetcher/compare/v2.0.5...v2.0.6
[2.0.5]: https://github.com/jampat000/Fetcher/compare/v2.0.4...v2.0.5
[2.0.4]: https://github.com/jampat000/Fetcher/compare/v2.0.3...v2.0.4
[2.0.3]: https://github.com/jampat000/Fetcher/compare/v2.0.2...v2.0.3
[2.0.2]: https://github.com/jampat000/Fetcher/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/jampat000/Fetcher/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/jampat000/Fetcher/compare/v1.0.44...v2.0.0
