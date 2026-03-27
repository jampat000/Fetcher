<#!
Registers a scheduled task so FetcherCompanion.exe starts at user logon (interactive session).
Optionally registers HKCU\...\Run as a fallback startup path for the current user only
(skipped when -SkipHKCURun or when the shell is elevated, so HKCU targets the wrong profile).

Uses the current Windows identity for the task principal (Interactive, Limited — no elevation).

Requires: Fetcher installed (FetcherCompanion.exe path).

Example (installed layout):
  powershell -NoProfile -ExecutionPolicy Bypass -File Register-FetcherCompanionTask.ps1 `
    -CompanionExe "C:\Program Files\Fetcher\Fetcher\FetcherCompanion.exe"
#>
param(
  [Parameter(Mandatory = $true)]
  [string]$CompanionExe,
  [switch]$SkipHKCURun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-RunningElevated {
  try {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  } catch {
    return $false
  }
}

function Set-FetcherCompanionHKCURun {
  param(
    [Parameter(Mandatory = $true)]
    [string]$ExePath
  )
  $runKey = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
  if (-not (Test-Path -LiteralPath $runKey)) {
    New-Item -Path $runKey -Force | Out-Null
  }
  $quoted = '"' + $ExePath + '"'
  Set-ItemProperty -LiteralPath $runKey -Name "FetcherCompanion" -Value $quoted -Type String -Force
}

if (-not (Test-Path -LiteralPath $CompanionExe)) {
  throw "Companion executable not found: $CompanionExe"
}

$CompanionExe = (Resolve-Path -LiteralPath $CompanionExe).Path
$workDir = Split-Path -Parent $CompanionExe

$userId = [Security.Principal.WindowsIdentity]::GetCurrent().Name

$taskName = "Fetcher\Fetcher Companion"
$action = New-ScheduledTaskAction -Execute $CompanionExe -WorkingDirectory $workDir
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
  -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings `
  -Principal $principal -Force | Out-Null

Write-Host "Registered scheduled task: $taskName for $userId"
Write-Host "The companion will start at next logon. To run now: Start-ScheduledTask -TaskName '$taskName'"

$doHKCU = -not $SkipHKCURun
if ($doHKCU -and (Test-RunningElevated)) {
  Write-Host "Skipping HKCU Run (elevated shell would target the wrong user hive). Re-run this script without elevation, or rely on the logon task."
  $doHKCU = $false
}

if ($doHKCU) {
  Set-FetcherCompanionHKCURun -ExePath $CompanionExe
  Write-Host "Registered HKCU Run fallback: FetcherCompanion -> $CompanionExe"
}
