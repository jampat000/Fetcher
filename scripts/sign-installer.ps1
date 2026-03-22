# Optional: Authenticode-sign FetcherSetup.exe (SmartScreen / enterprise trust).
# Prereqs: Windows SDK signtool.exe, a code-signing PFX.
#
# Usage (interactive):
#   $env:INSTALLER_SIGN_PFX = "C:\certs\code.pfx"
#   $env:INSTALLER_SIGN_PASSWORD = "secret"
#   .\scripts\sign-installer.ps1 -InstallerPath ".\installer\output\FetcherSetup.exe"
#
# CI: set secrets WINDOWS_PFX_BASE64 (base64 of PFX bytes) and WINDOWS_PFX_PASSWORD.
# Workflow decodes to a temp file and sets INSTALLER_SIGN_PFX.

param(
  [Parameter(Mandatory)][string]$InstallerPath,
  [string]$TimestampUrl = "http://timestamp.digicert.com"
)

$ErrorActionPreference = "Stop"

$pfxPath = $env:INSTALLER_SIGN_PFX
$pfxPass = $env:INSTALLER_SIGN_PASSWORD
if (-not $pfxPass) { $pfxPass = $env:WINDOWS_PFX_PASSWORD }

if ($env:WINDOWS_PFX_BASE64 -and -not $pfxPath) {
  $bytes = [Convert]::FromBase64String($env:WINDOWS_PFX_BASE64)
  $pfxPath = Join-Path $env:TEMP ("fetcher-sign-" + [Guid]::NewGuid().ToString("n") + ".pfx")
  [IO.File]::WriteAllBytes($pfxPath, $bytes)
  Write-Host "Wrote temporary PFX for signing."
}

if (-not $pfxPath -or -not (Test-Path -LiteralPath $pfxPath)) {
  throw "Set INSTALLER_SIGN_PFX (path to .pfx) or WINDOWS_PFX_BASE64."
}
if (-not $pfxPass) {
  throw "Set INSTALLER_SIGN_PASSWORD or WINDOWS_PFX_PASSWORD for the PFX."
}
if (-not (Test-Path -LiteralPath $InstallerPath)) {
  throw "Installer not found: $InstallerPath"
}

$kitsRoot = "${env:ProgramFiles(x86)}\Windows Kits\10\bin"
if (-not (Test-Path -LiteralPath $kitsRoot)) {
  throw "Windows SDK / signtool not found under $kitsRoot. Install 'Windows SDK Signing Tools' or Visual Studio build tools."
}

$signtool = Get-ChildItem -Path $kitsRoot -Recurse -Filter "signtool.exe" -ErrorAction SilentlyContinue |
  Where-Object { $_.FullName -match '\\x64\\signtool\.exe$' } |
  Sort-Object FullName -Descending |
  Select-Object -First 1

if (-not $signtool) {
  throw "signtool.exe not found under $kitsRoot"
}

Write-Host "Signing with $($signtool.FullName) ..."
& $signtool.FullName sign /fd SHA256 /tr $TimestampUrl /td SHA256 /f $pfxPath /p $pfxPass $InstallerPath
Write-Host "Done."

if ($env:WINDOWS_PFX_BASE64 -and $pfxPath -like "*fetcher-sign-*") {
  Remove-Item -LiteralPath $pfxPath -Force -ErrorAction SilentlyContinue
}
