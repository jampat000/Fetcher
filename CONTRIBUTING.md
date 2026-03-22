# Contributing

Thanks for helping improve Fetcher.

**Clone path:** Prefer **`%USERPROFILE%\Fetcher`** so the folder name matches the repo (see **[`docs/WORKSPACE-FOLDER.md`](docs/WORKSPACE-FOLDER.md)** if you still use an old name like **`grabby`**).

## Workflow (protected `master`)

This repo uses **classic branch protection** on **`master`**: pull requests, required CI checks, no force-push.

1. **Branch** from `master` (example: `fix/thing`, `chore/docs`, `feat/whatever`).
2. **Commit** with clear messages.
3. Open a **pull request** into `master`.
4. Wait for **required checks** (e.g. `Test / pytest`, `Security / pip-audit`).
5. **Approve** the PR if your branch rules require an approval (solo maintainers often self-approve).
6. **Merge** when green.

Docs: **[`.github/BRANCH_PROTECTION.md`](.github/BRANCH_PROTECTION.md)** · JSON/API: **[`.github/IMPORT-BRANCH-PROTECTION.md`](.github/IMPORT-BRANCH-PROTECTION.md)**

## Local checks

```powershell
py -m pip install -r requirements.txt -r requirements-dev.txt
py -m playwright install chromium
py -m pytest -q
```

## Dependency updates

**Dependabot** opens weekly PRs for **pip** and **GitHub Actions**. Prefer merging them when **CI is green** (or adjust pins if something breaks).

## Security

- Do **not** commit API keys, backup JSON, or real `.env` files. See **[`SECURITY.md`](SECURITY.md)**.
- If you used a **personal access token** only to run `scripts/protect-master-branch.ps1` or the protection API once, **revoke** it when finished (*GitHub → Settings → Developer settings → Personal access tokens*).

## GitHub cleanup (one-time, after enabling protection)

- Remove any **Rulesets** that GitHub said **won’t enforce** on a free private repo, if you now rely on **Settings → Branches** instead.
- Prefer **one** protection story so the team isn’t confused.

## Releases

Maintainers: see **Releasing** at the bottom of **[`CHANGELOG.md`](CHANGELOG.md)** and **`VERSION`**.

**Easiest path:** on a **release branch** (e.g. **`release/vX.Y.Z`**), bump **`VERSION`** + changelog, commit, then **`.\scripts\ship-release.ps1`** — pushes that branch to **`origin`** and dispatches **Tag release (from VERSION)** so **`vX.Y.Z`** is created (if missing) and **Build installer** runs. No **`dev`** branch on GitHub is required. See **`README.md`** → **Ship a new version to GitHub (maintainers)**.

A push to **`master`** or **`main`** that changes **`VERSION`** also auto-runs that workflow.

**GitHub CLI:** Install **`gh`**, run **`gh auth login`** (required for **`ship-release.ps1`**), then **`gh pr merge`** etc. — see **[`docs/GITHUB-CLI.md`](docs/GITHUB-CLI.md)**. If **`gh`** isn’t found, use **`%ProgramFiles%\GitHub CLI\gh.exe`**.

