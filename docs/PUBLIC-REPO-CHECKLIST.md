# Step-by-step: make the repo public (stay safe)

Use this when you want **GitHub Releases** and the **in-app update check** to work for everyone, without leaking secrets.

**Local audit already run in-repo:** see **[`PUBLIC-REPO-AUDIT.md`](PUBLIC-REPO-AUDIT.md)** for what was checked automatically vs what you must click on GitHub.

Work through the steps **in order**. Check each box when done.

---

## Step 1 — Decide what “public” means

- [ ] **Code + Releases public** — Anyone can see source and download `GrabbySetup.exe` from Releases. *(Typical for open source.)*
- [ ] **Code private, updates from another repo** — Keep coding private; use a **separate public repo** (or org repo) only for releases, and set **`GRABBY_UPDATES_REPO=owner/repo`** for shipped builds.

*Pick one path; the steps below assume “code + releases public.”*

---

## Step 2 — Scan for secrets (local machine)

In your repo root:

```powershell
cd C:\Users\User\grabby
```

- [ ] Search working tree for obvious secrets (pick one tool):

  **If you have [ripgrep](https://github.com/BurntSushi/ripgrep):**

  ```powershell
  rg -i "api[_-]?key|password|secret|token|x-api-key|bearer " --glob '!*.md'
  ```

  **Or in PowerShell (no extra install):**

  ```powershell
  Get-ChildItem -Recurse -File -Include *.py,*.ps1,*.json,*.yml,*.yaml,*.toml,*.iss |
    Select-String -Pattern "api.key|apikey|x-api-key|password|secret|token" -SimpleMatch:$false |
    Select-Object -First 50
  ```

  Review matches — many will be **variable names** or docs; you’re looking for **real credentials**.

- [ ] Confirm **no** `.env`, `*.pem`, `*_backup*.json`, or real Sonarr/Radarr/Emby keys are tracked:

  ```powershell
  git ls-files | findstr /i ".env pem backup json"
  ```

  If backup JSON or `.env` appears, **remove from git** (and add to `.gitignore` if missing), then continue to Step 4.

---

## Step 3 — Scan git history (important)

Old commits can still contain leaked keys.

- [ ] Run a secret scan you trust, e.g. [GitHub secret scanning](https://docs.github.com/en/code-security/secret-scanning) (after push), or tools like `gitleaks` / `trufflehog` locally.

- [ ] If you find **live** credentials in history: **rotate them** in Sonarr/Radarr/Emby (and anywhere else), then consider cleaning history (`git filter-repo`) or treating the repo as compromised for those keys.

---

## Step 4 — Git hygiene

- [ ] **`.gitignore`** includes things like: `.venv/`, `dist/`, `*.db`, local env files, exported settings backups.

- [ ] **No large accidental files** in history (optional): check repo size and big blobs.

---

## Step 5 — GitHub settings (before or right after going public)

On GitHub: **Settings** for the repository (and org if applicable).

- [ ] **Branch protection** on default branch (required checks, no force-push). See [`.github/BRANCH_PROTECTION.md`](../.github/BRANCH_PROTECTION.md).

- [ ] **Security → Code security** — enable what your plan allows (Dependabot alerts, etc.).

- [ ] **Security policy** — you already have [`SECURITY.md`](../SECURITY.md); confirm it’s accurate.

---

## Step 6 — Create at least one public Release

The in-app updater looks for **`GrabbySetup.exe`** on the **latest Release**.

- [ ] Build the installer (your usual pipeline, e.g. `packaging/build.ps1` + Inno).

- [ ] On GitHub: **Releases → Draft a new release** — tag (e.g. `v1.0.9`), title, short notes.

- [ ] **Attach** `GrabbySetup.exe` (exact name the app expects).

- [ ] Publish the release.

---

## Step 7 — Flip visibility to Public

GitHub: **Settings → General → Danger Zone → Change repository visibility → Public**.

- [ ] Read the warning (forks, stars, etc.).

- [ ] Confirm.

---

## Step 8 — After it’s public

- [ ] Open **`https://github.com/YOUR_USER/YOUR_REPO/releases/latest`** in a browser (logged out or incognito). You should see the release and asset.

- [ ] On a PC with Grabby: **Settings → Software Updates** should no longer show a **404** for that repo (if you use the default or set **`GRABBY_UPDATES_REPO`** correctly).

- [ ] Optional: add a **README** badge or “Download” link pointing at latest release.

---

## Reminders

| Safe on GitHub | Never in the repo |
|----------------|------------------|
| Source code, changelog, installer exe on Releases | Sonarr/Radarr/Emby API keys |
| `SECURITY.md`, issue templates | Settings backup JSON with keys |
| Public CI logs (avoid printing secrets) | Personal tokens in workflows (use **secrets**) |

**Grabby** stores operator keys in its **local database** on each machine — that stays off GitHub as long as you don’t commit backups or screenshots of Settings.

---

## If you get stuck on one step

Say which **step number** and what you see (e.g. “Step 6: asset name”, “Step 3: gitleaks output”). We can narrow it down.
