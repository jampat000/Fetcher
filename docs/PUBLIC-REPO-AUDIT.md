# Public repo readiness — automated audit log

This file records what was **done automatically** in the repo and what **only you** can do on GitHub.

**Date:** 2026-03-20 (local audit)

---

## Done for you (in this workspace)

| Check | Result |
|--------|--------|
| **Secret-ish string scan** (`app/`, `tests/`, `scripts/`, `docs/`, `.github/` — not `.venv`) | Only **field names** and **test placeholders** (e.g. `test-key`, `secret` in test objects). **No real tokens** found in source. |
| **Tracked sensitive filenames** (`git ls-files` — `.env`, `.pem`, backups, `.db`) | **None** matched. |
| **Tracked `*.json`** | Only `.github/branch-protection-classic-master.json` and `.github/rulesets/master-middle-ground.json` (config templates, not app data). |
| **`.gitignore` hardened** | Added ignore rules for `.env`, `.env.*`, `grabby-settings-backup*.json`, `*settings-backup*.json` (optional `!.env.example` if you add a template later). |

---

## You must do (GitHub website — I cannot log in as you)

These require **your** browser and **owner** rights on the repository.

1. **Optional — history scan**  
   Install/run [gitleaks](https://github.com/gitleaks/gitleaks) or [trufflehog](https://github.com/trufflesecurity/trufflehog) locally, or rely on **GitHub secret scanning** after the repo is public.

2. **Branch protection**  
   Follow [`.github/BRANCH_PROTECTION.md`](../.github/BRANCH_PROTECTION.md) on the repo’s **Settings → Branches / Rules**.

3. **Create a Release**  
   **Releases → Draft a new release** → tag (e.g. `v1.0.9`) → attach **`GrabbySetup.exe`** → Publish.

4. **Change visibility**  
   **Settings → General → Danger Zone → Change repository visibility → Public**.

5. **Verify**  
   In an incognito window, open `https://github.com/<you>/<repo>/releases/latest`.  
   On Grabby: **Settings → Software Updates** should reach GitHub without **404** (if `GRABBY_UPDATES_REPO` matches this repo).

---

## If `git` is not in your PATH

Use **Git Bash** or full path, e.g.  
`"C:\Program Files\Git\bin\git.exe" -C C:\Users\User\grabby status`

---

After you finish the GitHub steps, you can delete this file or keep it as a record.
