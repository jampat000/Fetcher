# Securing the `master` branch on GitHub

Branch protection is **not** stored in git—you turn it on in the GitHub UI (or **Repository rulesets** / API). This file is the **checklist** for this repo’s workflows.

**Repo path:** *Settings → Branches* (classic) or *Settings → Rules → Rulesets* (recommended for new setups).

---

## Middle ground (free private repo — ~2 minutes)

Use **classic branch protection**, not rulesets (rulesets often don’t enforce on free private repos).

1. Open **[Add branch protection rule](https://github.com/jampat000/Fetcher/settings/branch_protection_rules/new)**  
   (or **Fetcher → Settings → Branches → Add branch protection rule**).

2. **Branch name pattern:** `master`

3. Enable:

   | Setting | Turn on? |
   |---------|----------|
   | **Require a pull request before merging** | ✅ Yes — set **1** approval (*solo:* you **Approve** your own PRs). |
   | **Require status checks to pass** | ✅ Yes — enable **Require branches to be up to date before merging**. |
   | **Status checks** (search & add both checks) | `Test / pytest`, `Security / pip-audit` |
   | **Block force pushes** | ✅ Yes |
   | **Allow deletions** | ❌ Leave **off** / unchecked |

4. Optional but nice: **Dismiss stale pull request approvals when new commits are pushed** — ✅

5. **Do not allow bypassing:** turn **off** *“Allow repository administrators to bypass”* / *“Include administrators”* if you want the rules to apply to you too (strongest). If that’s annoying as solo maintainer, leave bypass **on** for admins.

6. **Save changes.**

7. **Delete** any useless **Rulesets** you created earlier (*Settings → Rules → Rulesets*) so you’re not confused by non-enforced rules.

If a status check name isn’t found, merge anything to `master` once or open a PR so workflows run, then edit the rule and add the check.

**JSON import / API bodies:** see **[`IMPORT-BRANCH-PROTECTION.md`](IMPORT-BRANCH-PROTECTION.md)** (`rulesets/master-middle-ground.json`, `rulesets/protect-default-branch.json`, `branch-protection-classic-master.json`).

---

## 0. One-shot API (fastest if you have a token)

From the repo root on your PC (needs **git** + **PowerShell**):

1. Create a **personal access token** with permission to change repo settings:  
   **Classic:** `repo` scope · **Fine-grained:** this repository + **Administration: Read and write**
2. Run:

```powershell
cd C:\path\to\fetcher
$env:GITHUB_TOKEN = 'paste_token_here'
& .\scripts\protect-master-branch.ps1
```

Use **single quotes** around the token. If you omit quotes, PowerShell tries to run the token as a command. Run from the **repo root** (folder that contains `scripts\`).

If GitHub returns **422** because a required check has never run on the repo, either open any PR so **Test / Security** run once and retry, **or**:

```powershell
.\scripts\protect-master-branch.ps1 -SkipRequiredStatusChecks
```

…then add the required checks in **Settings → Branches → `master`**.

---

## 1. Classic branch protection (Settings → Branches)

1. **Add rule** for branch name pattern: `master`  
   (If you use `main`, repeat or use a ruleset that covers both.)

2. Enable:

| Setting | Recommendation |
|--------|----------------|
| **Require a pull request before merging** | ✅ On |
| **Required approvals** | **1** (or more for teams) |
| **Dismiss stale pull request approvals when new commits are pushed** | ✅ On |
| **Require review from Code Owners** | Optional — useful with [CODEOWNERS](./CODEOWNERS); solo maintainers often **skip** this or self-approve. |
| **Require status checks to pass** | ✅ On |
| **Require branches to be up to date before merging** | ✅ On (reduces “green on stale base” merges) |
| **Require conversation resolution** | ✅ On (if you use review threads) |
| **Require signed commits** | Optional — stronger Supply chain / audit trail |
| **Require linear history** | Optional — preference |
| **Include administrators** | **Off** if you want rules to apply to repo admins too (strongest) |
| **Allow force pushes** | ❌ Off |
| **Allow deletions** | ❌ Off |

3. **Status checks to require**  
   Add every check that must be green before merge. Names must match what GitHub shows on a PR (**Checks** tab). For this repository they are usually:

   - `Test / pytest`
   - `Security / pip-audit`

   > **Tip:** Open any PR → **Checks** → copy the **exact** names from the list (GitHub is picky about spelling/spaces).

4. Save the rule.

---

## 2. Repository rulesets (Settings → Rules → Rulesets)

If you prefer rulesets (fine-grained, multiple targets):

- **Target branches:** `master` (and `main` if used).
- **Rules:** restrict updates, require PR, required checks (same two checks as above), block force-push, optional block deletions.
- **Bypass list:** empty for strongest posture, or emergency break-glass role only.

**Private repo on a free account:** GitHub may show *“Rulesets won’t be enforced … until you move to GitHub Team”*. In that case rulesets are **not** your answer—use **§1 Classic branch protection** (Settings → Branches) instead; it still protects `master` on typical free private repositories.

---

## 3. Account and org hygiene (outside the repo)

- **2FA** on all maintainer accounts (GitHub: *Settings → Password and authentication*).
- **Dependabot security updates** and **Secret scanning** / **push protection**: enable in *Settings → Code security* (availability depends on public/private and plan).
- **Actions:** *Settings → Actions → General* — prefer **“Read repository contents”** for the default `GITHUB_TOKEN` unless a workflow needs more (this repo’s workflows already scope permissions per-workflow where possible).

---

## 4. Optional: GitHub CLI

If you use `gh` and prefer API automation, generate rulesets from the UI once, or use `gh api` with a ruleset payload. The exact JSON depends on your org; start from GitHub’s docs for [Repository rules](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets).

---

## 5. After you enable protection

- All changes to `master` should go through a **PR**; bots (e.g. Dependabot) still open PRs and merge **only if** checks pass and approval policy is satisfied.
- If a required check name changes (workflow/job rename), update the branch rule or ruleset so merges don’t stall.



