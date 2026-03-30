param(
  [switch]$Clean,
  [switch]$AllowPathFallback
)

$ErrorActionPreference = "Stop"
# pip/pyinstaller emit notices on stderr; PS 7.2+ would treat that as a terminating error with Stop.
if ($PSVersionTable.PSVersion -ge [version]"7.2") {
  $PSNativeCommandUseErrorActionPreference = $false
}
$repoRoot = Split-Path -Parent $PSScriptRoot

if ($Clean) {
  # If a previous packaged app is running, it can lock dist/ files on Windows.
  Get-Process -Name "Fetcher" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

  try { if (Test-Path ".\\build") { Remove-Item ".\\build" -Recurse -Force } } catch { Write-Warning "Could not fully clean build/ (files may be in use). Continuing." }
  if (Test-Path ".\\dist") {
    $ok = $false
    foreach ($i in 1..5) {
      try {
        Remove-Item ".\\dist" -Recurse -Force
        $ok = $true
        break
      } catch {
        Start-Sleep -Milliseconds (250 * $i)
      }
    }
    if (-not $ok) { throw "Failed to clean dist/. Ensure Fetcher is not running and try again." }
  }
}

if (!(Test-Path ".\\.venv\\Scripts\\python.exe")) {
  py -m venv .venv
}

.\.venv\Scripts\pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "pip install -r requirements.txt failed (exit $LASTEXITCODE)" }
.\.venv\Scripts\pip install pyinstaller
if ($LASTEXITCODE -ne 0) { throw "pip install pyinstaller failed (exit $LASTEXITCODE)" }

# Optional ffmpeg/ffprobe staging for packaged Windows Refiner support.
$ffStage = ".\packaging\ffmpeg-bin"
if (Test-Path $ffStage) { Remove-Item $ffStage -Recurse -Force }
New-Item -ItemType Directory -Path $ffStage | Out-Null

$ffSrc = @()
$envDir = ($env:FETCHER_FFMPEG_BIN_DIR -as [string])
if ($envDir -and (Test-Path (Join-Path $envDir "ffmpeg.exe")) -and (Test-Path (Join-Path $envDir "ffprobe.exe"))) {
  $ffSrc = @((Join-Path $envDir "ffmpeg.exe"), (Join-Path $envDir "ffprobe.exe"))
} else {
  try { $ffmpegCmd = (Get-Command ffmpeg -ErrorAction Stop).Source } catch { $ffmpegCmd = $null }
  try { $ffprobeCmd = (Get-Command ffprobe -ErrorAction Stop).Source } catch { $ffprobeCmd = $null }
  if ($ffmpegCmd -and $ffprobeCmd) { $ffSrc = @($ffmpegCmd, $ffprobeCmd) }
}
if ($ffSrc.Count -eq 2) {
  Copy-Item -LiteralPath $ffSrc[0] -Destination (Join-Path $ffStage "ffmpeg.exe") -Force
  Copy-Item -LiteralPath $ffSrc[1] -Destination (Join-Path $ffStage "ffprobe.exe") -Force
  Write-Host "Staged ffmpeg/ffprobe for packaged build."
} else {
  if ($AllowPathFallback) {
    Write-Warning "ffmpeg/ffprobe not staged. Packaged Refiner will fall back to PATH."
  } else {
    throw "ffmpeg/ffprobe not found for packaging. Set FETCHER_FFMPEG_BIN_DIR to a folder containing ffmpeg.exe and ffprobe.exe, or run with -AllowPathFallback for non-release local builds."
  }
}

.\.venv\Scripts\pyinstaller --noconfirm packaging\fetcher.spec
if ($LASTEXITCODE -ne 0) { throw "pyinstaller failed (exit $LASTEXITCODE)" }

Write-Host ""
Write-Host "Built: dist\\Fetcher\\Fetcher.exe"

