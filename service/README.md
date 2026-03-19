# Windows Service (WinSW)

This project uses **WinSW** (Windows Service Wrapper) to run the packaged `Grabby` executable as a Windows Service.

## Get WinSW

Download WinSW (x64) and name it `winsw.exe`, then place it in the same folder as:

- `winsw.exe`
- `GrabbyService.xml`
- `Grabby.exe` (your packaged app)

WinSW releases are available on GitHub (search “WinSW releases”).

## Install / Start (Admin PowerShell)

```powershell
.\winsw.exe install
.\winsw.exe start
```

## Stop / Uninstall

```powershell
.\winsw.exe stop
.\winsw.exe uninstall
```

