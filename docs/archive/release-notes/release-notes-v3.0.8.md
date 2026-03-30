# Fetcher v3.0.8

This patch release hardens Windows service companion auto-launch for Refiner folder Browse.

Highlights:
- Added generalized active-session process token fallback when WTSQueryUserToken and explorer.exe are unavailable
- Improved diagnostics for Windows token acquisition and companion launch
- Docker/headless behavior remains unchanged and manual-entry only
