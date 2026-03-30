# Set Fetcher web login on the dev SQLite file (same default DB as dev-start.ps1 without -SharedAppDb).
# Usage: .\scripts\dev-set-login.ps1
#        .\scripts\dev-set-login.ps1 -Password "your-password" -Username admin

param(
  [string]$Username = "admin",
  [string]$Password = "dev"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $RepoRoot

if (!(Test-Path ".\.venv\Scripts\python.exe")) {
  throw "Missing .venv. Run: py -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}

if (-not $env:FETCHER_DEV_DB_PATH) {
  $env:FETCHER_DEV_DB_PATH = Join-Path ([System.IO.Path]::GetTempPath()) "fetcher-dev.sqlite3"
}
if (-not $env:FETCHER_JWT_SECRET) {
  $env:FETCHER_JWT_SECRET = "0123456789abcdef0123456789abcdef"
}

& .\.venv\Scripts\python.exe scripts/dev-set-login.py $Password --username $Username
