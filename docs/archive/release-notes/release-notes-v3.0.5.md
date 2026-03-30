# Fetcher v3.0.5

This patch release completes the Windows service folder Browse architecture.

Highlights:
- Fetcher service now automatically launches FetcherCompanion in the active logged-in Windows session when needed
- Browse works without manual companion launching in the normal Windows service case
- Existing fallback startup paths remain in place for resilience
- Docker/headless installs continue to use manual path entry by design

Notes:
- Windows service installs now prefer service-driven companion startup
- If policy or environment blocks interactive launch, existing fallback registration paths remain available
- Docker/headless installs do not support native folder Browse
