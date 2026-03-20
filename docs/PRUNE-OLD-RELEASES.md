# Prune old GitHub releases (optional housekeeping)

GitHub keeps every **Release** and **tag** until you remove them. Deleting a release **does not** delete the git tag by default (and vice versa). For **Grabby**, only remove versions you are sure nobody should install anymore.

## Before you start

- **Prefer keeping** recent releases (e.g. last **5–10** tags) so users on older builds can still upgrade in-app or manually.
- **Deleting** a release removes **release notes** and **attached assets** (`GrabbySetup.exe`) for that tag from the Releases UI. Anyone who already downloaded the installer still has the file.
- You need **admin** on the repo and a way to authenticate (**GitHub web** or **`gh` CLI** logged in).

## Option A — GitHub website (safest / clearest)

1. Open **`https://github.com/jampat000/Grabby/releases`** (replace owner/repo if you use a fork).
2. Open an **old** release → **⋯** (or **Edit**) → **Delete release** → confirm.
3. Optionally remove the **tag** too (GitHub may offer this; if not, use Option B for tags).

Repeat per release. Do **not** delete the **latest** stable release you want people to install.

## Option B — GitHub CLI (`gh`)

Install: [GitHub CLI](https://cli.github.com/), then `gh auth login`.

```powershell
# List releases (newest first)
gh release list --repo jampat000/Grabby

# Delete one release (removes release + assets for that tag)
gh release delete v1.0.10 --repo jampat000/Grabby --yes
```

To **remove only the tag** (no release metadata) or clean up after deleting a release:

```powershell
git push https://github.com/jampat000/Grabby.git :refs/tags/v1.0.10
```

(Use a PAT with `repo` scope if not using `gh`’s credential helper.)

## Option C — GitHub API (advanced)

Use `DELETE /repos/{owner}/{repo}/releases/{release_id}` with a fine-grained or classic PAT (**Contents: Read and write** / **`repo`**). Easiest is to stick to **Option A** or **B**.

## In-app updates

The app reads **`VERSION`** / **`releases/latest`** (or the Atom feed). As long as **at least one good release** exists with **`GrabbySetup.exe`**, update checks keep working. Deleting **all** old releases except the current one is usually enough.
