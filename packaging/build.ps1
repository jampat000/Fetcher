param(
  [switch]$Clean
)

$ErrorActionPreference = "Stop"

if ($Clean) {
  # If a previous packaged app is running, it can lock dist/ files on Windows.
  Get-Process -Name "Grabby" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

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
    if (-not $ok) { throw "Failed to clean dist/. Ensure Grabby is not running and try again." }
  }
}

if (!(Test-Path ".\\.venv\\Scripts\\python.exe")) {
  py -m venv .venv
}

.\.venv\Scripts\pip install -r requirements.txt
.\.venv\Scripts\pip install pyinstaller

.\.venv\Scripts\pyinstaller --noconfirm packaging\grabby.spec

Write-Host ""
Write-Host "Built: dist\\Grabby\\Grabby.exe"

