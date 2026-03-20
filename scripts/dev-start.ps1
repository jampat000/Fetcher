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
  $line = netstat -ano | Select-String "LISTENING\s+(\d+)$" | Where-Object { $_.ToString() -match "[:\.]$Port\s+" } | Select-Object -First 1
  if (-not $line) {
    return $null
  }
  if ($line.ToString() -match "LISTENING\s+(\d+)$") {
    return [int]$Matches[1]
  }
  return $null
}

if (!(Test-Path ".\.venv\Scripts\python.exe")) {
  throw "Missing .venv. Run: py -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}

$pidOnPreferred = Get-FirstListenerPid -Port $PreferredPort
if ($pidOnPreferred) {
  $procName = Get-ProcessNameByPid -ProcessId $pidOnPreferred
  if ($procName -and $procName.ToLower().StartsWith("python")) {
    Write-Host "Stopping existing dev server on port $PreferredPort (PID $pidOnPreferred)..."
    Stop-Process -Id $pidOnPreferred -Force -ErrorAction Stop
    Start-Sleep -Milliseconds 400
  } else {
    throw "Port $PreferredPort is in use by PID $pidOnPreferred ($procName). Stop that app first, or run with a different -PreferredPort."
  }
}

$reloadArgs = @()
if ($Reload) {
  $reloadArgs = @("--reload")
}

Write-Host "Starting Grabby dev server..."
Write-Host "Open: http://$BindHost`:$PreferredPort"
Write-Host ""
Write-Host "NOTE: http://127.0.0.1:8765 is normally the INSTALLED Windows service (packaged exe)."
Write-Host "      It does NOT show git/source edits. Use the URL above for development."
Write-Host "      To develop ON port 8765: stop the Grabby service, then run this script with -PreferredPort 8765."
Write-Host ""

& .\.venv\Scripts\python.exe -m uvicorn app.main:app --host $BindHost --port $PreferredPort @reloadArgs
