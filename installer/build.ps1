param(
  [switch]$Clean,
  [string]$IsccPath = "",
  [switch]$InstallInnoSetupIfMissing,
  # Semver only (e.g. 1.2.3). If empty: -Version from env, then repo VERSION file, then 0.0.0-dev
  [string]$Version = ""
)

function Resolve-GrabbyAppVersion {
  param(
    [string]$Explicit,
    [string]$Root
  )
  $v = $Explicit.Trim()
  if ($v) { return $v }

  $refName = $env:GITHUB_REF_NAME
  if ($refName -and $refName -match '^v(\d+\.\d+\.\d+)$') {
    return $Matches[1]
  }

  $vf = Join-Path $Root "VERSION"
  if (Test-Path -LiteralPath $vf) {
    $raw = (Get-Content -LiteralPath $vf -Raw).Trim()
    if ($raw -match '^(\d+\.\d+\.\d+)') {
      return $Matches[1]
    }
    return $raw
  }
  return "0.0.0-dev"
}

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if ($Clean) {
  if (Test-Path ".\\installer\\output") { Remove-Item ".\\installer\\output" -Recurse -Force }
  try { if (Test-Path ".\\dist") { Remove-Item ".\\dist" -Recurse -Force } } catch { Write-Warning "Could not fully clean dist/ (files may be in use). Continuing." }
  try { if (Test-Path ".\\build") { Remove-Item ".\\build" -Recurse -Force } } catch { Write-Warning "Could not fully clean build/ (files may be in use). Continuing." }
}

Write-Host "1) Building app (PyInstaller)..."
.\packaging\build.ps1

$distApp = Join-Path $root "dist\Grabby"
$distExe = Join-Path $distApp "Grabby.exe"
if (-not (Test-Path -LiteralPath $distExe)) {
  throw "PyInstaller output missing: $distExe (is packaging\grabby.spec committed and present on this machine/CI?)"
}

Write-Host "2) Staging WinSW (bundled installer/bin/WinSW.exe)..."
$py = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $py)) {
  throw "Python venv missing after app build: $py"
}
& $py ".\installer\setup.py"
if ($LASTEXITCODE -ne 0) {
  throw "installer/setup.py failed with exit code $LASTEXITCODE"
}

Write-Host "3) Compiling installer (Inno Setup)..."

# If provided, use explicit path first
$iscc = $null
if ($IsccPath -and (Test-Path -LiteralPath $IsccPath)) {
  $iscc = $IsccPath
}

# Otherwise try common Inno Setup locations, otherwise rely on PATH
$candidates = @(
  "$env:ProgramFiles(x86)\\Inno Setup 6\\ISCC.exe",
  "$env:ProgramFiles\\Inno Setup 6\\ISCC.exe",
  "ISCC.exe"
)

if (-not $iscc) {
foreach ($c in $candidates) {
  if ($c -eq "ISCC.exe") {
    try {
      $cmd = Get-Command ISCC.exe -ErrorAction Stop
      $iscc = $cmd.Source
      break
    } catch { }
  } elseif (Test-Path $c) {
    $iscc = $c
    break
  }
}
}

if (-not $iscc) {
  if ($InstallInnoSetupIfMissing) {
    Write-Host "ISCC.exe not found. Installing Inno Setup silently..."
    $ver = "6.7.1"
    $innoInstaller = ".\\installer\\_innosetup-$ver.exe"
    $url = "https://github.com/jrsoftware/issrc/releases/download/is-6_7_1/innosetup-$ver.exe"
    if (!(Test-Path -LiteralPath $innoInstaller)) {
      Invoke-WebRequest -Uri $url -OutFile $innoInstaller -UseBasicParsing
    }
    # Install per-user into repo-local folder (avoid admin requirement / PATH issues)
    $localInnoDir = (Resolve-Path ".\\installer").Path + "\\_inno"
    New-Item -ItemType Directory -Path $localInnoDir -Force | Out-Null
    & $innoInstaller /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /CURRENTUSER /DIR="$localInnoDir" | Out-Null

    $candidateLocal = Join-Path $localInnoDir "ISCC.exe"
    if (Test-Path -LiteralPath $candidateLocal) {
      $iscc = $candidateLocal
    }

    # re-detect
    if (-not $iscc) {
      foreach ($c in $candidates) {
        if ($c -eq "ISCC.exe") {
          try {
            $cmd = Get-Command ISCC.exe -ErrorAction Stop
            $iscc = $cmd.Source
            break
          } catch { }
        } elseif (Test-Path $c) {
          $iscc = $c
          break
        }
      }
    }
  }
}

if (-not $iscc) {
  throw "Inno Setup compiler (ISCC.exe) not found. Re-run with -IsccPath 'C:\path\to\ISCC.exe', or pass -InstallInnoSetupIfMissing to auto-install it."
}

$appVersion = Resolve-GrabbyAppVersion -Explicit $Version -Root $root
Write-Host "Inno AppVersion: $appVersion"
& $iscc "/DMyAppVersion=$appVersion" ".\\installer\\Grabby.iss"

Write-Host ""
Write-Host "Installer built under: installer\\output\\GrabbySetup.exe"

