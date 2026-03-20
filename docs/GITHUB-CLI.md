# GitHub CLI (`gh`) on Windows

Use **`gh`** to merge PRs, manage releases, and delete old releases from the terminal (see also [`PRUNE-OLD-RELEASES.md`](PRUNE-OLD-RELEASES.md)).

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

## Common commands (Grabby)

Run from any directory; use **`--repo jampat000/Grabby`** if the folder isn’t this git repo.

```powershell
# Open PR for current branch
gh pr create --base master --title "..." --body "..."

# List / merge
gh pr list --repo jampat000/Grabby
gh pr merge 35 --repo jampat000/Grabby --merge

# After merge — sync local
cd C:\Users\User\grabby
git checkout master
git pull origin master

# Releases
gh release list --repo jampat000/Grabby
gh release delete v1.0.10 --repo jampat000/Grabby --yes
```

`gh pr merge` respects branch protection (required checks must pass).
