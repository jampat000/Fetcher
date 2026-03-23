# GitHub CLI (`gh`) on Windows

Use **`gh`** to merge PRs, manage releases, and optionally delete old releases (see **Prune old releases** below).

## One-time setup

1. **PATH** — After installing, **close and reopen** your terminal (or Cursor). If `gh` is still not found, the default binary is usually:
   ```text
   %ProgramFiles%\GitHub CLI\gh.exe
   ```
2. **Login** (required once per machine/profile):
   ```powershell
   gh auth login
   ```
   Choose **GitHub.com**, **HTTPS**, and sign in (browser or token).

3. **Optional — token without browser** — Set **`GH_TOKEN`** (classic PAT with `repo` scope, or fine-grained with contents + pull requests).

## Common commands (Fetcher)

Run from any directory; use **`--repo jampat000/Fetcher`** if the folder isn’t this git repo.

```powershell
# Open PR for current branch
gh pr create --base master --title "..." --body "..."

# List / merge
gh pr list --repo jampat000/Fetcher
gh pr merge 35 --repo jampat000/Fetcher --merge

# After merge — sync local
cd $env:USERPROFILE\Fetcher   # or your clone — see WORKSPACE-FOLDER.md
git checkout master
git pull origin master

# Releases
gh release list --repo jampat000/Fetcher
gh release delete v1.0.10 --repo jampat000/Fetcher --yes
```

`gh pr merge` respects branch protection (required checks must pass).

## Build installer — `gh workflow run` and the tag ref

`gh workflow run build-installer.yml --repo jampat000/Fetcher --ref vX.Y.Z` runs **Build installer** using the **workflow file at the commit that `vX.Y.Z` points to** (and checks out that commit). It does **not** pull the workflow definition from **`master`** while building an old tag.

If **`vX.Y.Z`** predates a CI change (e.g. **release** job conditions), a run can **succeed** for **build** but **not** create a **GitHub Release**. **Before** relying on manual dispatch: `git fetch origin master --tags` and confirm the tag points to the commit you intend to ship; if not, **re-tag** on **`origin/master`** or ship a new **`VERSION`**.

**Docker publish:** `gh workflow run docker-publish.yml --repo jampat000/Fetcher --ref master -f checkout_ref=vX.Y.Z` — use when you need GHCR without re-tagging (workflow YAML from **`master`**, image from the tag commit).

See **`.cursor/rules/github-installer-workflow-ref-trap.mdc`** (agent-facing) and **CHANGELOG.md → Releasing**.

## Prune old releases (optional)

GitHub keeps every **Release** and **tag** until you remove them. For **Fetcher**, only delete versions you are sure nobody should install anymore.

- **Prefer keeping** several recent releases so users on older builds can still upgrade.
- **Deleting** a release removes **notes** and **`FetcherSetup.exe`** for that tag from the Releases UI (people who already downloaded keep the file).
- **In-app updates** need **at least one** good release with **`FetcherSetup.exe`** on **Latest**.

**Website:** **Releases** → open an old release → **⋯** → **Delete release** (optionally delete the tag if offered).

**CLI:**

```powershell
gh release list --repo jampat000/Fetcher
gh release delete v1.0.10 --repo jampat000/Fetcher --yes
```

**Delete a tag only** (after removing the release, if needed):

```powershell
git push https://github.com/jampat000/Fetcher.git :refs/tags/v1.0.10
```

Use a PAT with **`repo`** if not using **`gh`** credentials.
