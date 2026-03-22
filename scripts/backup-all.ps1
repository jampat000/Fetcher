param(
  [string]$ProjectPath = "C:\Users\User\fetcher",
  [string]$TranscriptPath = "C:\Users\User\.cursor\projects\c-Users-User-fetcher\agent-transcripts",
  [string]$BackupRoot = "C:\Users\User\fetcher-backups"
)

$ErrorActionPreference = "Stop"

if (!(Test-Path -LiteralPath $ProjectPath)) {
  throw "Project path not found: $ProjectPath"
}

New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null

$ts = Get-Date -Format "dd-MM-yyyy-HHmmss"
$projectZip = Join-Path $BackupRoot ("fetcher-" + $ts + ".zip")
$transcriptZip = Join-Path $BackupRoot ("fetcher-transcripts-" + $ts + ".zip")

Write-Host "Creating project backup: $projectZip"
Compress-Archive -Path (Join-Path $ProjectPath "*") -DestinationPath $projectZip -CompressionLevel Optimal -Force

if (Test-Path -LiteralPath $TranscriptPath) {
  Write-Host "Creating transcript backup: $transcriptZip"
  Compress-Archive -Path (Join-Path $TranscriptPath "*") -DestinationPath $transcriptZip -CompressionLevel Optimal -Force
} else {
  Write-Warning "Transcript path not found, skipping transcript backup: $TranscriptPath"
}

Write-Host ""
Write-Host "Backup complete."
Write-Host "Project archive: $projectZip"
if (Test-Path -LiteralPath $transcriptZip) {
  Write-Host "Transcript archive: $transcriptZip"
}
