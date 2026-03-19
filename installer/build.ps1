param(
  [switch]$Clean,
  [string]$IsccPath = "",
  [string]$WinSWVersion = "v2.12.0",
  [switch]$InstallInnoSetupIfMissing
)

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

Write-Host "2) Ensuring WinSW present..."
$winswPath = ".\\service\\winsw.exe"
if (!(Test-Path $winswPath)) {
  $url = "https://github.com/winsw/winsw/releases/download/$WinSWVersion/WinSW-x64.exe"
  Write-Host "Downloading WinSW from $url"
  Invoke-WebRequest -Uri $url -OutFile $winswPath -UseBasicParsing
} else {
  Write-Host "WinSW already present at $winswPath"
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

& $iscc ".\\installer\\Grabby.iss"

Write-Host ""
Write-Host "Installer built under: installer\\output\\GrabbySetup.exe"

