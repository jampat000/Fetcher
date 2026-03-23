# Import branch protection (JSON)

Also read **[`CONTRIBUTING.md`](../CONTRIBUTING.md)** for day-to-day PR workflow now that `master` is protected.

## Which file?

| File | Use when |
|------|-----------|
| [`rulesets/master-middle-ground.json`](rulesets/master-middle-ground.json) | **Rulesets** UI **Import** or `POST /repos/{owner}/{repo}/rulesets`. **Note:** On **free private** repos GitHub often **does not enforce** rulesets until **GitHub Team**. |
| [`rulesets/protect-default-branch.json`](rulesets/protect-default-branch.json) | Same as rulesets import, but targets **`~DEFAULT_BRANCH`** instead of hard-coded **`master`**. |
| [`branch-protection-classic-master.json`](branch-protection-classic-master.json) | **Classic** protection via API: `PUT /repos/{owner}/{repo}/branches/master/protection`. **Works on typical free private repos.** Cannot be pasted into the Rulesets importer. |

## Import ruleset (GitHub UI)

1. **Settings → Rules → Rulesets → New ruleset** (or **New branch ruleset**).
2. Use **Import** / paste JSON if your UI offers it, or create via API:

```powershell
$env:GITHUB_TOKEN = 'your_token_with_repo_admin_scope'
$json = Get-Content -Raw .github/rulesets/master-middle-ground.json
Invoke-RestMethod -Method Post -Uri 'https://api.github.com/repos/jampat000/Fetcher/rulesets' `
  -Headers @{
    Authorization = "Bearer $env:GITHUB_TOKEN"
    Accept = 'application/vnd.github+json'
    'X-GitHub-Api-Version' = '2022-11-28'
  } -Body $json -ContentType 'application/json'
```

Replace `jampat000/Fetcher` if needed. Token needs **classic `repo` scope** or fine-grained **Administration: Read and write** on this repository.

## Apply classic protection (API — recommended on free private)

```powershell
$env:GITHUB_TOKEN = 'your_token'
$json = Get-Content -Raw .github/branch-protection-classic-master.json
Invoke-RestMethod -Method Put `
  -Uri 'https://api.github.com/repos/jampat000/Fetcher/branches/master/protection' `
  -Headers @{
    Authorization = "Bearer $env:GITHUB_TOKEN"
    Accept = 'application/vnd.github+json'
    'X-GitHub-Api-Version' = '2022-11-28'
  } -Body $json -ContentType 'application/json'
```

If GitHub returns **422** (unknown context), run CI once on `master` or open a PR so the required checks exist, then retry. Or temporarily remove `required_status_checks` in the JSON, apply, and add checks in **Settings → Branches**.

## Status check names

Must match the **Checks** tab on a PR exactly. Defaults for this repo:

- `Test / pytest`
- `Security / pip-audit`


