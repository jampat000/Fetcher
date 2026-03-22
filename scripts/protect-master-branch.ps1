<#
.SYNOPSIS
  Applies GitHub branch protection to `master` via the REST API.

.DESCRIPTION
  GitHub does not store branch rules in git — you must run this locally (or use the UI)
  with a token that can change branch protection.

  Token (pick one):
  - Classic PAT: repo scope (repo owner) or full repo access.
  - Fine-grained PAT: Repository access to Fetcher + Administration: Read and write.

  Create: GitHub avatar → Settings → Developer settings → Personal access tokens

.EXAMPLE
  cd C:\path\to\fetcher
  $env:GITHUB_TOKEN = 'ghp_....'   # MUST be in quotes — fine-grained tokens start with github_pat_
  & .\scripts\protect-master-branch.ps1

.EXAMPLE
  .\scripts\protect-master-branch.ps1 -SkipRequiredStatusChecks
  # If GitHub returns 422 because a check name has never run yet, use this, then add checks in the UI.
#>
[CmdletBinding()]
param(
    [string] $Owner = "",
    [string] $Repo = "",
    [string] $Branch = "master",
    [string] $Token = "",
    [switch] $SkipRequiredStatusChecks
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $Token) {
    if ($env:GITHUB_TOKEN) { $Token = $env:GITHUB_TOKEN }
    elseif ($env:GH_TOKEN) { $Token = $env:GH_TOKEN }
}

if (-not $Token) {
    Write-Error "Set GITHUB_TOKEN (or GH_TOKEN) or pass -Token. See comment header in this script."
}

if (-not $Owner -or -not $Repo) {
    try {
        $url = git config --get remote.origin.url 2>$null
        if ($url -match 'github\.com[:/]([^/]+)/([^/.]+)(?:\.git)?') {
            if (-not $Owner) { $Owner = $Matches[1] }
            if (-not $Repo) { $Repo = $Matches[2] }
        }
    } catch { }
}

if (-not $Owner -or -not $Repo) {
    Write-Error "Could not infer Owner/Repo from git. Pass -Owner jampat000 -Repo Fetcher"
}

$api = "https://api.github.com/repos/$Owner/$Repo/branches/$Branch/protection"
$headers = @{
    Authorization          = "Bearer $Token"
    Accept                 = "application/vnd.github+json"
    "X-GitHub-Api-Version" = "2022-11-28"
}

# Body matches .github/BRANCH_PROTECTION.md (classic protection).
# Check names must match the "Checks" tab on a PR exactly.
$checks = @(
    @{ context = "Test / pytest" },
    @{ context = "Security / pip-audit" }
)

$body = [ordered]@{
    enforce_admins                  = $true
    required_pull_request_reviews    = @{
        dismiss_stale_reviews           = $true
        require_code_owner_reviews       = $false
        required_approving_review_count  = 1
    }
    allow_force_pushes              = $false
    allow_deletions                 = $false
    required_conversation_resolution = $true
}

if (-not $SkipRequiredStatusChecks) {
    $body['required_status_checks'] = @{
        strict = $true
        checks = $checks
    }
} else {
    $body['required_status_checks'] = $null
}

# ConvertTo-Json: omit null required_status_checks if skipping would break API — GitHub may require non-null.
if ($SkipRequiredStatusChecks) {
    $body.Remove('required_status_checks')
}

$json = $body | ConvertTo-Json -Depth 10

Write-Host "PUT $api" -ForegroundColor Cyan
try {
    $response = Invoke-RestMethod -Uri $api -Method Put -Headers $headers -Body $json -ContentType 'application/json'
    Write-Host "Branch protection updated for $Owner/$Repo @ $Branch" -ForegroundColor Green
    $response | ConvertTo-Json -Depth 6
} catch {
    $err = $_.ErrorDetails.Message
    if (-not $err -and $_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $err = $reader.ReadToEnd()
    }
    Write-Host "Request body was:" -ForegroundColor Yellow
    Write-Host $json
    Write-Host ""
    Write-Host "API error: $err" -ForegroundColor Red
    if (-not $SkipRequiredStatusChecks) {
        Write-Host ""
        Write-Host "If GitHub says a required check is unknown, open a PR so all workflows run once," -ForegroundColor Yellow
        Write-Host "or re-run with: .\scripts\protect-master-branch.ps1 -SkipRequiredStatusChecks" -ForegroundColor Yellow
        Write-Host "then add required checks manually under Settings → Branches → master." -ForegroundColor Yellow
    }
    throw
}


