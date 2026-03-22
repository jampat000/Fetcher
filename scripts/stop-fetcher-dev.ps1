#Requires -Version 5.1
<#
.SYNOPSIS
  Free typical Fetcher dev ports and stop stray uvicorn / orphan multiprocessing workers.

.DESCRIPTION
  On Windows, uvicorn --reload can leave python worker processes whose parent PID is gone;
  netstat may show LISTEN on a PID that tasklist cannot resolve. Clearing listener PIDs plus
  orphan multiprocessing children usually fixes stuck 8766 without a reboot.

.EXAMPLE
  .\scripts\stop-fetcher-dev.ps1
#>
param(
  [int[]]$Ports = @(8766, 8767, 8768, 8769, 8770),
  [switch]$Quiet,
  # Stops python.exe multiprocessing spawn workers whose parent process no longer exists.
  # Low risk on a dev box; disable if you run other Python multiprocessing jobs the same way.
  [switch]$OrphanMultiprocessing = $true
)

$ErrorActionPreference = "SilentlyContinue"

function Write-DevStopLog {
  param([string]$Message)
  if (-not $Quiet) {
    Write-Host $Message
  }
}

foreach ($port in $Ports) {
  $conns = @(Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue)
  foreach ($c in $conns) {
    $listenerPid = [int]$c.OwningProcess
    if ($listenerPid -le 0) {
      continue
    }
    Write-DevStopLog "Stopping listener PID $listenerPid on port $port..."
    Stop-Process -Id $listenerPid -Force -ErrorAction SilentlyContinue
    $null = Start-Process -FilePath "taskkill.exe" -ArgumentList @("/F", "/PID", "$listenerPid") -Wait -PassThru -WindowStyle Hidden
  }
}

Get-CimInstance Win32_Process -Filter "Name='python.exe'" | ForEach-Object {
  $cl = $_.CommandLine
  if ($null -eq $cl) {
    return
  }
  if ($cl -match "uvicorn\s+app\.main:app" -and $cl -match "Fetcher") {
    Write-DevStopLog "Stopping Fetcher uvicorn PID $($_.ProcessId)..."
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    $null = Start-Process -FilePath "taskkill.exe" -ArgumentList @("/F", "/PID", "$($_.ProcessId)") -Wait -PassThru -WindowStyle Hidden
  }
}

if ($OrphanMultiprocessing) {
  Get-CimInstance Win32_Process -Filter "Name='python.exe'" | ForEach-Object {
    $cl = $_.CommandLine
    if ($null -eq $cl -or $cl -notmatch "multiprocessing") {
      return
    }
    if ($cl -match "parent_pid=(\d+)") {
      $ppid = [int]$Matches[1]
      if (-not (Get-Process -Id $ppid -ErrorAction SilentlyContinue)) {
        Write-DevStopLog "Stopping orphan multiprocessing worker PID $($_.ProcessId) (parent $ppid gone)..."
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        $null = Start-Process -FilePath "taskkill.exe" -ArgumentList @("/F", "/PID", "$($_.ProcessId)") -Wait -PassThru -WindowStyle Hidden
      }
    }
  }
}

Start-Sleep -Milliseconds 400
