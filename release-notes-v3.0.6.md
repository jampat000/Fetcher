# Fetcher v3.0.6

This patch release hardens Windows service companion startup for Refiner folder Browse.

Highlights:
- Fetcher service now uses a more deterministic Windows session launch path for FetcherCompanion
- Improved diagnostics for companion startup and health detection
- Better distinction between no active session, launch failure, and launched-but-not-healthy outcomes

Notes:
- This release is focused on Windows service reliability
- Docker/headless installs are unchanged and continue to use manual path entry for folder selection
