This patch release adds the proper Windows service-compatible folder browsing architecture.

Highlights:
- Added FetcherCompanion.exe for interactive folder browsing in the logged-in user session
- Kept Fetcher service headless and background-only
- Folder Browse now works correctly for service installs without privilege escalation
- Added companion registration support for Windows user logon

Important:

To enable Browse while Fetcher runs as a service, run:

**"Register Fetcher Companion (folder picker)"**

once from the logged-in Windows account.

This release does not change the core service model. It adds the correct companion process for interactive folder selection.
