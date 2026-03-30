# Windows Service (WinSW)

This project uses **WinSW** (Windows Service Wrapper) to run the packaged `Fetcher` executable as a Windows Service.

## Get WinSW

Download WinSW (x64) and name it `winsw.exe`, then place it in the same folder as:

- `winsw.exe`
- `FetcherService.xml` (includes **`<workingdirectory>%BASE%</workingdirectory>`** for the PyInstaller one-folder layout)
- `Fetcher.exe` (your packaged app)

WinSW releases are available on GitHub (search “WinSW releases”).

## Install / Start (Admin PowerShell)

```powershell
.\winsw.exe install
.\winsw.exe start
```

## JWT secret (installed service)

Packaged **`Fetcher.exe`** resolves the JWT signing secret in this order:

1. **`FETCHER_JWT_SECRET`** if set in the process environment (e.g. machine-level variable — inherited by WinSW; no XML needed).
2. Otherwise **`machine-jwt-secret`** next to **`fetcher.db`** (default **`%ProgramData%\Fetcher\machine-jwt-secret`**), created on **first** successful start with a stable random value.

**Do not** add `<env name="FETCHER_JWT_SECRET" value="%FETCHER_JWT_SECRET%"/>` to WinSW: when the machine variable is missing, that expands to **empty** and prevents both inheritance and the file fallback.

Optional explicit secret:

```powershell
[Environment]::SetEnvironmentVariable("FETCHER_JWT_SECRET","<your-32+char-random-secret>","Machine")
.\winsw.exe restart
```

## Optional API key encryption (`FETCHER_DATA_ENCRYPTION_KEY`)

If set to a **Fernet** key, Sonarr/Radarr/Emby API keys are stored **encrypted** in SQLite. If unset, they remain **plaintext** and the app logs a **warning** at startup. Generate a key with:

`python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`

Add `<env name="FETCHER_DATA_ENCRYPTION_KEY" value="..."/>` next to the JWT entry if you use it. See root **`README.md`** and **`docs/INSTALL-AND-OPERATIONS.md`**.

## SQLite data directory (settings, activity, etc.)

By default, **packaged** Fetcher on Windows uses **`%ProgramData%\Fetcher\fetcher.db`**. Set machine env **`FETCHER_DATA_DIR`** to a folder if you want the database elsewhere (that folder must contain **`fetcher.db`**). There is **no** automatic copy from other profile locations—copy **`fetcher.db`** (and **`-wal`** / **`-shm`** if present) manually while Fetcher is **stopped** when moving data.

To use a different folder explicitly, set a **machine** environment variable and restart the service:

```powershell
[Environment]::SetEnvironmentVariable("FETCHER_DATA_DIR","C:\\ProgramData\\Fetcher","Machine")
```

Then copy your existing **`fetcher.db`** into that folder (with Fetcher **stopped**), or point **`FETCHER_DATA_DIR`** at the folder that already contains **`fetcher.db`**.

Add to **`winsw.xml`** next to the other `<env>` entries:

```xml
<env name="FETCHER_DATA_DIR" value="C:\ProgramData\Fetcher"/>
```

Startup logs include **`SQLite database path:`** with the resolved file.

## Listen address (LAN vs localhost)

`FetcherService.xml` passes **`--host`** to `Fetcher.exe`. **`0.0.0.0`** listens on all interfaces so you can use **`http://<this-pc-ip>:8765`** from other devices on your network. Use **`127.0.0.1`** if you want the Web UI only on this machine. Open **TCP 8765** in Windows Firewall when using `0.0.0.0`. The Web UI uses **username + password** (and optional **IP allowlist** in Settings); see root **`SECURITY.md`**.

## Stop / Uninstall

```powershell
.\winsw.exe stop
.\winsw.exe uninstall
```

