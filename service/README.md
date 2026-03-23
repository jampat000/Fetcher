# Windows Service (WinSW)

This project uses **WinSW** (Windows Service Wrapper) to run the packaged `Fetcher` executable as a Windows Service.

## Get WinSW

Download WinSW (x64) and name it `winsw.exe`, then place it in the same folder as:

- `winsw.exe`
- `FetcherService.xml`
- `Fetcher.exe` (your packaged app)

WinSW releases are available on GitHub (search “WinSW releases”).

## Install / Start (Admin PowerShell)

```powershell
.\winsw.exe install
.\winsw.exe start
```

## Required JWT secret for installed service

`Fetcher.exe` requires `FETCHER_JWT_SECRET` at startup. Configure it as a persistent machine env var, then restart the service:

```powershell
[Environment]::SetEnvironmentVariable("FETCHER_JWT_SECRET","<your-32+char-random-secret>","Machine")
.\winsw.exe restart
```

`FetcherService.xml` forwards this value to the process via:

```xml
<env name="FETCHER_JWT_SECRET" value="%FETCHER_JWT_SECRET%"/>
```

No fallback JWT secret is used; missing secret causes intentional fail-fast startup.

## Listen address (LAN vs localhost)

`FetcherService.xml` passes **`--host`** to `Fetcher.exe`. **`0.0.0.0`** listens on all interfaces so you can use **`http://<this-pc-ip>:8765`** from other devices on your network. Use **`127.0.0.1`** if you want the Web UI only on this machine. Open **TCP 8765** in Windows Firewall when using `0.0.0.0`. The Web UI uses **username + password** (and optional **IP allowlist** in Settings); see root **`SECURITY.md`**.

## Stop / Uninstall

```powershell
.\winsw.exe stop
.\winsw.exe uninstall
```

