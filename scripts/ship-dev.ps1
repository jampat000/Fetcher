<#
.SYNOPSIS
  Deprecated — use ship-release.ps1

.DESCRIPTION
  **dev** does not need to exist on GitHub. Use **.\scripts\ship-release.ps1** instead: it pushes
  your **current branch** and triggers **Tag release (from VERSION)** on that ref.
#>
Write-Warning "ship-dev.ps1 is deprecated. Use: .\scripts\ship-release.ps1"
& "$PSScriptRoot\ship-release.ps1" @args
