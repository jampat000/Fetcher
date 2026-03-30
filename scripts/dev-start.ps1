param(
  [string]$BindHost = "127.0.0.1",
  [int]$PreferredPort = 8766,
  [switch]$Reload = $true,
  # Offer UAC once if normal kill failed (helps when another user/admin owns the listener).
  [switch]$TryElevatedKill,
  # Use the same SQLite file as the installed service (%LocalAppData%\Fetcher\fetcher.db). Stop the service first to avoid locks.
  [switch]$SharedAppDb,
  # Do not set FETCHER_ALLOW_DEV_UPGRADE (Settings → Apply upgrade toggle stays off for source runs).
  [switch]$NoDevUpgrade
)

$ErrorActionPreference = "Stop"

# Always run from repo root (so .venv and app.main resolve even if you run .\scripts\dev-start.ps1 from elsewhere).
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $RepoRoot
Write-Host "Repo: $RepoRoot" -ForegroundColor DarkGray
Write-Host ""

# Isolated dev DB by default so uvicorn does not compete with the Windows service for the same fetcher.db.
if ($SharedAppDb) {
  Remove-Item Env:\FETCHER_DEV_DB_PATH -ErrorAction SilentlyContinue
  Write-Host 'Dev DB: (shared) default - %LocalAppData%\Fetcher\fetcher.db  [stop Fetcher service if SQLite is busy]' -ForegroundColor DarkYellow
} else {
  $devDb = Join-Path ([System.IO.Path]::GetTempPath()) "fetcher-dev.sqlite3"
  $env:FETCHER_DEV_DB_PATH = $devDb
  Write-Host "Dev DB: $devDb  (FETCHER_DEV_DB_PATH)  `[use -SharedAppDb for installed app database`]" -ForegroundColor DarkGray
  Write-Host "Forgot dev login? Run: .\scripts\dev-set-login.ps1  (username admin, password dev)" -ForegroundColor DarkGray
}
Write-Host ""

# Source runs are not "frozen"; without this, Settings hides the Apply upgrade toggle (see app/updates.py _apply_eligible).
if ($NoDevUpgrade) {
  Remove-Item Env:\FETCHER_ALLOW_DEV_UPGRADE -ErrorAction SilentlyContinue
  Write-Host "Dev: in-app upgrade off for this process (-NoDevUpgrade)." -ForegroundColor DarkGray
  Write-Host ""
} else {
  $env:FETCHER_ALLOW_DEV_UPGRADE = "1"
  Write-Host "Dev: in-app upgrade enabled (FETCHER_ALLOW_DEV_UPGRADE=1). Use -NoDevUpgrade to match restricted UI." -ForegroundColor DarkGray
  Write-Host ""
}

function Get-ProcessNameByPid {
  param([int]$ProcessId)
  try {
    return (Get-Process -Id $ProcessId -ErrorAction Stop).ProcessName
  } catch {
    return ""
  }
}

function Get-ListenerPids {
  param([int]$Port)
  try {
    return @(
      Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
        ForEach-Object { [int]$_.OwningProcess } |
        Where-Object { $_ -gt 0 } |
        Sort-Object -Unique
    )
  } catch {
    return @()
  }
}

function Test-PortListening {
  param([int]$Port)
  return (Get-ListenerPids -Port $Port).Count -gt 0
}

function Stop-ListenerProcesses {
  param([int]$Port)

  $pids = Get-ListenerPids -Port $Port
  if ($pids.Count -eq 0) {
    return
  }

  foreach ($listenerPid in $pids) {
    $procName = Get-ProcessNameByPid -ProcessId $listenerPid
    Write-Host "Freeing port $Port - stopping PID $listenerPid ($procName)..."
    $stopped = $false
    try {
      Stop-Process -Id $listenerPid -Force -ErrorAction Stop
      $stopped = $true
    } catch {
      # Process may be elevated / different session; taskkill sometimes succeeds when Stop-Process does not.
      $tk = Start-Process -FilePath "taskkill.exe" -ArgumentList @("/F", "/PID", "$listenerPid") -Wait -PassThru -WindowStyle Hidden
      if ($tk.ExitCode -eq 0) {
        $stopped = $true
      }
    }
    if (-not $stopped -and $TryElevatedKill) {
      Write-Host "Requesting elevated kill for PID $listenerPid (UAC prompt)..."
      $cmd = "try { Stop-Process -Id $listenerPid -Force -ErrorAction Stop } catch { taskkill /F /PID $listenerPid | Out-Null }"
      Start-Process powershell.exe -Verb RunAs -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $cmd) -Wait
    }
  }

  Start-Sleep -Milliseconds 500

  # Stale LISTEN rows (ghost OwningProcess): try resetting those TCP entries (may need admin).
  if (Test-PortListening -Port $Port) {
    try {
      Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
        Stop-NetTCPConnection -Confirm:$false -ErrorAction SilentlyContinue
    } catch {}
    Start-Sleep -Milliseconds 400
  }

  if (Test-PortListening -Port $Port) {
    Write-Host ""
    Write-Host "Port $Port is still in use. Common fixes:" -ForegroundColor Yellow
    Write-Host "  1) Close the other terminal running uvicorn / Fetcher dev, or reboot once." -ForegroundColor Yellow
    Write-Host "  2) Admin PowerShell: Get-NetTCPConnection -LocalPort $Port -State Listen | Stop-NetTCPConnection -Confirm:`$false" -ForegroundColor Yellow
    Write-Host "  3) Re-run: .\scripts\dev-start.ps1 -TryElevatedKill   (UAC prompt to kill the owning process)" -ForegroundColor Yellow
    Write-Host ""
    throw "Port $Port could not be freed. See messages above."
  }
}

if (!(Test-Path ".\.venv\Scripts\python.exe")) {
  throw "Missing .venv. Run: py -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}

# Clear stale uvicorn / multiprocessing listeners on common dev ports (Windows reload orphans).
& (Join-Path $PSScriptRoot "stop-fetcher-dev.ps1") -Quiet

Stop-ListenerProcesses -Port $PreferredPort

$reloadArgs = @()
if ($Reload) {
  $reloadArgs = @("--reload")
}

Write-Host "Starting Fetcher dev server..."
Write-Host "Open: http://$BindHost`:$PreferredPort"
Write-Host ""
Write-Host "TIP: Use this exact host in the browser. If http://localhost:$PreferredPort fails or sign-in works but Settings looks logged out, use 127.0.0.1 (cookies differ per host; localhost may use IPv6 only)."
Write-Host ""
Write-Host "NOTE: Port 8765 = installed Windows service (Fetcher.exe). Port 8766 = this dev server (source)."
Write-Host "      To use 8765 for dev: stop the Fetcher service first, then: .\scripts\dev-start.ps1 -PreferredPort 8765"
Write-Host ""

& .\.venv\Scripts\python.exe -m uvicorn app.main:app --host $BindHost --port $PreferredPort @reloadArgs
