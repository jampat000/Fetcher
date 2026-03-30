# Fetcher v3.0.7

This patch release fixes a Windows service companion auto-launch failure seen in production.

Highlights:
- Added fallback token acquisition from the active session's explorer.exe when WTSQueryUserToken fails
- Improved diagnostics for Windows session launch and companion health detection
- Keeps Docker/headless behavior unchanged and manual-entry only
