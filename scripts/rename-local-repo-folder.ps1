#Requires -Version 5.1
<#
.SYNOPSIS
  Rename the repo root folder (e.g. an old name -> Fetcher) to match GitHub / product name.

.DESCRIPTION
  Close Cursor, VS Code, and any dev servers using this repo before running.
  Invoke from anywhere inside the git working tree.

.EXAMPLE
  .\scripts\rename-local-repo-folder.ps1
  .\scripts\rename-local-repo-folder.ps1 -NewFolderName fetcher
#>
param(
  [ValidateNotNullOrEmpty()]
  [string]$NewFolderName = "Fetcher"
)

$ErrorActionPreference = "Stop"

$root = git rev-parse --show-toplevel 2>$null
if (-not $root) {
  throw "Not inside a git repository. cd to your clone first (the folder that contains .git)."
}
$root = (Resolve-Path -LiteralPath $root).Path
$parent = Split-Path -Parent $root
$currentName = Split-Path -Leaf $root
$newPath = Join-Path $parent $NewFolderName

if ($currentName -eq $NewFolderName) {
  Write-Host "Already named '$NewFolderName'. Nothing to do."
  exit 0
}
if (Test-Path -LiteralPath $newPath) {
  throw @"
Target already exists: $newPath
Remove or rename that folder first, or use -NewFolderName with a different name.
"@
}

Write-Host @"
Renaming:
  $root
  -> $newPath
"@
Rename-Item -LiteralPath $root -NewName $NewFolderName
Write-Host @"

Done.
  Re-open Cursor/VS Code: $newPath
  Optional workspace file: $(Join-Path $newPath 'fetcher.code-workspace')
"@
