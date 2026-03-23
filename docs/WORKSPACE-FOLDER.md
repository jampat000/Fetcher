# Local folder name: **`Fetcher`**

The GitHub repo is **[jampat000/Fetcher](https://github.com/jampat000/Fetcher)**. Use a clone folder named **`Fetcher`** so paths and editor titles stay obvious. If you still have an older clone under another name, use **`scripts/rename-local-repo-folder.ps1`** (see below).

## Recommended layout

Clone (or rename) so the **directory name** matches the product:

```powershell
cd $env:USERPROFILE
git clone https://github.com/jampat000/Fetcher.git Fetcher
cd Fetcher
```

Then in **Cursor / VS Code**:

- **File → Open Folder…** → choose **`…\Fetcher`**, **or**
- **File → Open Workspace from File…** → open **`fetcher.code-workspace`** in the repo root (sidebar label shows **Fetcher**).

## Rename an existing clone (any name → `Fetcher`)

1. **Close Cursor / VS Code** (and stop **`dev-start.ps1`** if it’s running).
2. From **any** shell **inside** the repo:

   ```powershell
   .\scripts\rename-local-repo-folder.ps1
   ```

   Default target name is **`Fetcher`**. Use **`-NewFolderName fetcher`** if you prefer lowercase.

3. Re-open the folder or **`fetcher.code-workspace`** from the **new** path.

If **`…\Fetcher`** already exists, pick another name or remove the empty folder first.

## “fatal: not a git repository”

That means the shell’s **current directory** is not inside the clone. `cd` to the folder that contains **`.git`**, then run `git` commands:

```powershell
cd $env:USERPROFILE\Fetcher
git status
```
