<#
.SYNOPSIS
  Push the current branch to GitHub and run "Tag release (from VERSION)" on that branch.

.DESCRIPTION
  You do **not** need a **dev** branch on GitHub. Work locally (any branch name), bump **VERSION**,
  then push a **release branch** (e.g. **release/v1.0.40**) or use a branch that already exists on
  the remote.

  This script:
  1. **git push** so **origin** has your commit(s) with the new **VERSION**
  2. **gh workflow run "Tag release (from VERSION)" --ref &lt;current-branch&gt;** — reads **VERSION** on
     that ref, creates **vX.Y.Z** if missing, dispatches **Build installer**

  Auto-tag on push only runs when **VERSION** changes on **master** or **main** (see workflow).
  This dispatch works for **any** branch name you push — typical pattern: **release/vX.Y.Z** → script → tag + build.

  Prerequisites: **git**, **GitHub CLI** (**gh auth login**).

.EXAMPLE
  git switch -c release/v1.0.40 origin/master
  # edit VERSION + CHANGELOG, commit
  .\scripts\ship-release.ps1

.EXAMPLE
  .\scripts\ship-release.ps1 -Remote upstream
#>
param(
    [string]$Remote = "origin"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location -LiteralPath $repoRoot

if (-not (Test-Path -LiteralPath "VERSION")) {
    throw "VERSION file not found at repo root."
}

$null = Get-Command git -ErrorAction Stop
$null = Get-Command gh -ErrorAction Stop

$branch = git rev-parse --abbrev-ref HEAD
if ($branch -eq "HEAD") {
    throw "Detached HEAD — switch to a named branch (e.g. release/v1.0.40) before shipping."
}

$v = (Get-Content -LiteralPath "VERSION" -Raw).Trim()
Write-Host "Repo: $repoRoot"
Write-Host "Branch: $branch  |  VERSION: $v"
Write-Host ""
Write-Host "Pushing HEAD -> ${Remote}:$branch ..."
git push $Remote "HEAD:refs/heads/$branch"

Write-Host ""
Write-Host "Dispatching workflow: Tag release (from VERSION)  (ref: $branch) ..."
gh workflow run "Tag release (from VERSION)" --ref $branch

$slug = gh repo view --json nameWithOwner -q .nameWithOwner
Write-Host ""
Write-Host "Done. Actions: https://github.com/$slug/actions"
Write-Host "When the build finishes: https://github.com/$slug/releases"
Write-Host ""
Write-Host "Tip: merge your release branch into master via PR when ready (default branch / history)."
