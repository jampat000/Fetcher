<#!
Called by FetcherSetup (Inno Setup) after files are installed.
When the installer runs in a safe interactive context (not SYSTEM, not non-interactive
Session 0 service context), registers the per-user logon task for FetcherCompanion,
optionally HKCU Run (skipped when elevated — wrong HKCU hive), starts the companion,
and probes /health briefly. Writes one line per run to %ProgramData%\Fetcher\logs\companion-setup.log
#>
param(
  [Parameter(Mandatory = $true)]
  [string]$CompanionExe,
  [switch]$Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-SafeForAutomaticCompanionTaskRegistration {
  try {
    $sysSid = New-Object Security.Principal.SecurityIdentifier 'S-1-5-18'
    $cur = [Security.Principal.WindowsIdentity]::GetCurrent()
    if ($cur.User.Value -eq $sysSid.Value) {
      return $false
    }
  } catch {
    return $false
  }
  if (-not [Environment]::UserInteractive) {
    return $false
  }
  $sn = $env:SESSIONNAME
  if ($sn -and $sn.ToUpperInvariant() -eq 'SERVICES') {
    return $false
  }
  return $true
}

function Test-RunningElevated {
  try {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  } catch {
    return $false
  }
}

function Write-CompanionSetupLog {
  param([string]$Line)
  try {
    $dir = Join-Path $env:ProgramData "Fetcher\logs"
    if (-not (Test-Path -LiteralPath $dir)) {
      New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    $logFile = Join-Path $dir "companion-setup.log"
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -LiteralPath $logFile -Value "[$ts] $Line" -Encoding UTF8
  } catch {
    # best-effort only
  }
}

function Test-CompanionHealth {
  param([int]$TimeoutMs = 5000)
  $url = "http://127.0.0.1:8767/health"
  $deadline = [DateTime]::UtcNow.AddMilliseconds($TimeoutMs)
  while ([DateTime]::UtcNow -lt $deadline) {
    try {
      $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop
      if ($r.StatusCode -eq 200 -and $r.Content -match '"ok"\s*:\s*true') {
        return $true
      }
    } catch {
    }
    Start-Sleep -Milliseconds 350
  }
  return $false
}

try {
  if (-not (Test-Path -LiteralPath $CompanionExe)) {
    Write-CompanionSetupLog "SKIP: CompanionExe missing at $CompanionExe"
    if (-not $Quiet) { Write-Warning "Companion executable missing; skipping task registration." }
    exit 0
  }

  if (-not (Test-SafeForAutomaticCompanionTaskRegistration)) {
    Write-CompanionSetupLog "SKIP: unsafe context (SYSTEM, non-interactive, or SERVICES session)"
    if (-not $Quiet) {
      Write-Host "Automatic Fetcher Companion task registration skipped (installer not in an interactive user context, or running as SYSTEM)."
      Write-Host "After install: sign in as the Windows user who will use Browse, then run Start Menu — Register Fetcher Companion (folder picker)."
    }
    exit 0
  }

  $elev = Test-RunningElevated
  $regArgs = @{ CompanionExe = $CompanionExe }
  if ($elev) {
    $regArgs.SkipHKCURun = $true
  }

  & (Join-Path $PSScriptRoot "Register-FetcherCompanionTask.ps1") @regArgs

  if ($elev) {
    Write-CompanionSetupLog "REGISTER: scheduled_task=yes hkcu_run=skipped (elevated_installer_context)"
  } else {
    Write-CompanionSetupLog "REGISTER: scheduled_task=yes hkcu_run=registered (fallback_startup)"
  }

  $taskName = "Fetcher\Fetcher Companion"
  try {
    Start-ScheduledTask -TaskName $taskName -ErrorAction Stop
    Write-CompanionSetupLog "IMMEDIATE: Start-ScheduledTask invoked"
  } catch {
    try {
      $workDir = Split-Path -Parent $CompanionExe
      Start-Process -FilePath $CompanionExe -WorkingDirectory $workDir -WindowStyle Hidden -ErrorAction Stop
      Write-CompanionSetupLog "IMMEDIATE: Start-Process fallback invoked"
    } catch {
      Write-CompanionSetupLog "IMMEDIATE: start failed ($($_.Exception.Message))"
    }
  }

  $ok = Test-CompanionHealth -TimeoutMs 5000
  if ($ok) {
    Write-CompanionSetupLog "HEALTH: /health OK within probe window"
  } else {
    Write-CompanionSetupLog "HEALTH: /health not OK within probe window (Browse may need logoff/logon or Start Menu Register)"
  }
} catch {
  Write-CompanionSetupLog "ERROR: $($_.Exception.Message)"
  if (-not $Quiet) {
    Write-Warning "Fetcher Companion task registration failed (you can register manually from Start Menu): $($_.Exception.Message)"
  }
}

exit 0
