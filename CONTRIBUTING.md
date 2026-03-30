# Contributing

Thanks for helping improve Fetcher.

**Clone path:** Prefer **`%USERPROFILE%\Fetcher`** (see **[`docs/README.md`](docs/README.md)** / **[`docs/WORKSPACE-FOLDER.md`](docs/WORKSPACE-FOLDER.md)**).

## Workflow (protected `master`)

The default branch **`master`** is protected: changes land via **pull request**, **required CI checks** must pass, and **force-push** is blocked. Enforcement may be **classic branch protection** (*Settings → Branches*) or **repository rulesets** (*Settings → Rules → Rulesets*) — see **[`.github/BRANCH_PROTECTION.md`](.github/BRANCH_PROTECTION.md)** and **[`.github/IMPORT-BRANCH-PROTECTION.md`](.github/IMPORT-BRANCH-PROTECTION.md)**.

1. **Branch** from `master` (examples: `fix/thing`, `chore/docs`, `feat/whatever`), or use a **`release/vX.Y.Z`** branch for version bumps (maintainers).
2. **Commit** with clear messages.
3. Open a **pull request** into `master`.
4. Wait for **required checks** (e.g. **`Test / pytest`**, **`Test / pip-audit`** — use the exact names shown on a green PR).
5. If your rules require **approvals**, add one; **solo** setups often use **0** required approvals so you can merge your own PR without a second person.
6. **Merge** when green. If GitHub reports the base branch policy blocks merge (e.g. review rules), maintainers may use **`gh pr merge <n> --admin`** when appropriate.

**After merge:** a push to **`master`** that updates **`VERSION`** runs **Tag release (from VERSION)**, which creates tag **`vX.Y.Z`** (if missing) and dispatches **Build installer** (Windows **`FetcherSetup.exe`**) and **Docker publish** (**`ghcr.io/jampat000/fetcher`**). GitHub Actions release triggers are **`master`**-only for that path (there is no parallel **`main`** pipeline). See **Releasing** in **[`CHANGELOG.md`](CHANGELOG.md)** and **`docs/BUILD-AND-RELEASE.md`**.

## Local checks

```powershell
py -m pip install -r requirements.txt -r requirements-dev.txt
py -m playwright install chromium
py -m pytest -q
```

**Optional:** **`scripts/emby-api-inventory.py`** — ad-hoc Emby API exploration (not used by CI).

## Dependency updates

**Dependabot** opens weekly PRs for **pip** and **GitHub Actions**. Prefer merging them when **CI is green** (or adjust pins if something breaks).

## Security

- Do **not** commit API keys, backup JSON, or real `.env` files. See **[`SECURITY.md`](SECURITY.md)**.
- If you used a **personal access token** only to run `scripts/protect-master-branch.ps1` or the protection API once, **revoke** it when finished (*GitHub → Settings → Developer settings → Personal access tokens*).

## Releases

Maintainers: see **Releasing** at the bottom of **[`CHANGELOG.md`](CHANGELOG.md)** and the **`VERSION`** file.

**Typical path:** create **`release/vX.Y.Z`** from **`origin/master`**, bump **`VERSION`**, move **`[Unreleased]`** into **`[X.Y.Z] - YYYY-MM-DD`** (use the machine date, e.g. **`Get-Date -Format yyyy-MM-dd`** on Windows), update changelog compare links, **commit**, **push**, open **PR → `master`**, **merge**. That merge updates **`VERSION`** on **`master`** and triggers **Tag release (from VERSION)** → **`vX.Y.Z`**, **Build installer**, and **Docker publish**. No **`dev`** branch is required.

**Alternate:** from a release branch, **`.\scripts\ship-release.ps1`** pushes the branch and dispatches **Tag release** (requires **`gh auth login`**).

**GitHub CLI:** Install **`gh`**, run **`gh auth login`** — see **[`docs/GITHUB-CLI.md`](docs/GITHUB-CLI.md)**. If **`gh`** isn’t on **`PATH`**, use **`%ProgramFiles%\GitHub CLI\gh.exe`**.

**Ref note:** manual **`gh workflow run build-installer.yml --ref vX.Y.Z`** uses the workflow file **at that tag’s commit** — if the tag is stale vs **`master`**, fix the tag or merge first (see **`docs/GITHUB-CLI.md`** / workspace rules on the ref trap).

