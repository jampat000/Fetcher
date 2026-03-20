param(
  [string]$BindHost = "127.0.0.1",
  [int]$PreferredPort = 8766,
  [switch]$Reload = $true
)

$ErrorActionPreference = "Stop"

function Get-ProcessNameByPid {
  param([int]$ProcessId)
  try {
    return (Get-Process -Id $ProcessId -ErrorAction Stop).ProcessName
  } catch {
    return ""
  }
}

function Get-FirstListenerPid {
  param([int]$Port)
  # Prefer Get-NetTCPConnection (reliable on Windows) over parsing netstat.
  try {
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
      Select-Object -First 1
    if ($conn -and $null -ne $conn.OwningProcess) {
      return [int]$conn.OwningProcess
    }
  } catch {}
  return $null
}

if (!(Test-Path ".\.venv\Scripts\python.exe")) {
  throw "Missing .venv. Run: py -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}

# Avoid $pid* names: $PID is an automatic variable and breaks parsing (e.g. "$pidOnPreferred").
$listenerPid = Get-FirstListenerPid -Port $PreferredPort
if ($listenerPid) {
  $procName = Get-ProcessNameByPid -ProcessId $listenerPid
  Write-Host "Freeing port $PreferredPort - stopping PID $listenerPid ($procName)..."
  try {
    Stop-Process -Id $listenerPid -Force -ErrorAction Stop
  } catch {
    throw "Port $PreferredPort is in use by PID $listenerPid ($procName) and could not be stopped. Close that app or run as Administrator, or use -PreferredPort with a free port."
  }
  Start-Sleep -Milliseconds 400
}

$reloadArgs = @()
if ($Reload) {
  $reloadArgs = @("--reload")
}

Write-Host "Starting Grabby dev server..."
Write-Host "Open: http://$BindHost`:$PreferredPort"
Write-Host ""
Write-Host "NOTE: Port 8765 = installed Windows service (Grabby.exe). Port 8766 = this dev server (source)."
Write-Host "      To use 8765 for dev: stop the Grabby service first, then: .\scripts\dev-start.ps1 -PreferredPort 8765"
Write-Host ""

& .\.venv\Scripts\python.exe -m uvicorn app.main:app --host $BindHost --port $PreferredPort @reloadArgs
