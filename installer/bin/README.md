# Bundled WinSW

`WinSW.exe` here is the **x64** binary from the [WinSW releases](https://github.com/winsw/winsw/releases) (same payload as `WinSW-x64.exe`, renamed for a stable path). It is **not** downloaded at build time.

`installer/setup.py` copies this file to `service/winsw.exe` when building the Inno installer (skipped if `service/winsw.exe` already exists).
