<#!
Registers a scheduled task so FetcherCompanion.exe starts at user logon (interactive session).
Run this script once per Windows user from that user's desktop session (not as a remote SYSTEM shell).
Requires: Fetcher installed (FetcherCompanion.exe path).

Example (installed layout):
  powershell -NoProfile -ExecutionPolicy Bypass -File Register-FetcherCompanionTask.ps1 `
    -CompanionExe "C:\Program Files\Fetcher\Fetcher\FetcherCompanion.exe"
#>
param(
  [Parameter(Mandatory = $true)]
  [string]$CompanionExe
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $CompanionExe)) {
  throw "Companion executable not found: $CompanionExe"
}

$CompanionExe = (Resolve-Path -LiteralPath $CompanionExe).Path
$workDir = Split-Path -Parent $CompanionExe

$taskName = "Fetcher\Fetcher Companion"
$action = New-ScheduledTaskAction -Execute $CompanionExe -WorkingDirectory $workDir
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
  -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings `
  -Principal $principal -Force | Out-Null

Write-Host "Registered scheduled task: $taskName"
Write-Host "The companion will start at next sign-in. To run now: Start-ScheduledTask -TaskName '$taskName'"
