# Cursor Handoff: Activity Logs + Anti-Boomerang Cooldown

## Where we left off
You were seeing Sonarr/Radarr keep trying to **upgrade/search the same movies/episodes over and over** on every scheduler tick.

We also improved the **Activity log UI** so it shows richer per-item context (TV show + episode/movie labels) with better formatting.

## Latest shipped change (pending merge/install)
The Arr “over and over” loop mitigation was shipped on **`v1.0.20`**.

- Release tag: `v1.0.20`
- Branch: `chore/release-v1.0.20`
- PR: https://github.com/jampat000/Grabby/pull/new/chore/release-v1.0.20

### What v1.0.20 does
- Adds an `arr_action_log` history table.
- When Grabby is about to call:
  - Sonarr `wanted_missing` / `wanted_cutoff_unmet` (episode searches/upgrade searches)
  - Radarr `wanted_missing` / `wanted_cutoff_unmet` (movie searches/upgrade searches)
  it filters out any episode/movie IDs already triggered within a cooldown window (see **`arr_search_cooldown_minutes`** in Settings).

Result: the same IDs won’t re-trigger every scheduler run until state changes.

## Key earlier fixes included in this workstream (for context)
1. **Schedule UX**
   - Schedule day selection + time inputs were fixed/improved.
2. **Activity logs**
   - `Activity`/`Dashboard` now display `detail` strings with readable multi-line formatting.
   - Sonarr TV labels prefer show name (fallback via `seriesId -> series.title` lookup).
3. **Cleaner boomerang prevention (episode-level)**
   - For live Emby TV deletes, Sonarr is unmonitored at the **episode** level (instead of series), so it won’t circle back as easily.

## What you should do on the media server next
1. Merge PR for `chore/release-v1.0.20` into `master` (or at least ensure the installer build for `v1.0.20` exists).
2. Install/upgrade using the `GrabbySetup.exe` produced for `v1.0.20`.
3. Restart the **Grabby Windows service** (or use WinSW restart).
4. Verify in the UI:
   - Activity/Dashboard: repeated “missing/upgrade search” entries should stop repeating for the exact same episode/movie IDs every tick.
   - Logs (optional): confirm you see “suppressed (cooldown)” for repeated triggers (if action-level logging is visible in your environment).

## Notes / assumptions
- If you are running the installed service (`GrabbySetup.exe`), edits in the git repo won’t affect it until you install a new release/installer.
- Cooldown only suppresses triggers; it does not prevent normal retries once the cooldown window passes and state is still “wanted missing/cutoff unmet”.

## Follow-up (post–v1.0.20)

Users still saw **Radarr** hammering the **same movies** because:

1. **Cooldown was tied to very short scheduler intervals** — items became eligible again almost every tick.
2. **Cooldown was keyed by `action`** (`missing` vs `upgrade`) — the same movie could get **two** `MoviesSearch` calls in **one** Grabby run if it appeared on both queues.

**Changes after that handoff:**

- **`arr_search_cooldown_minutes`** in Settings (default **1440** = 24h; **`0`** = use each app’s run interval as the cooldown basis).
- **`_filter_ids_by_cooldown`** ignores `action` when deciding suppression; `action` is still stored for auditing.
- Sample **`service/GrabbyService.xml`** uses **`--host 0.0.0.0`** for LAN Web UI access (+ firewall / SECURITY notes).

