# Security policy

## Supported versions

We fix security issues in the **latest release** on the default branch (`master`). Use an up-to-date build from [Releases](https://github.com/jampat000/Grabby/releases).

## Reporting a vulnerability

Please **do not** open a public issue for unfixed security problems.

- Open a **private security advisory** on GitHub (*Security → Advisories → Report a vulnerability*), or  
- Email the maintainer with a clear description, steps to reproduce, and impact.

We aim to acknowledge reports within a few days and coordinate disclosure after a fix.

## Secrets and sensitive data

- **API keys** (Sonarr, Radarr, Emby, etc.) belong in the app **Settings** / database—not in git, logs, or screenshots you share publicly.  
- A **settings backup** (`.json` from **Settings** → **Backup & Restore**) contains the same secrets as the database—store it **encrypted** or **offline**; never commit it or post it publicly.  
- When sharing **logs** or **bug reports**, redact URLs, tokens, hostnames, and paths you consider private.  
- The Web UI and service run **locally by default**; if you expose Grabby to a network, use a **reverse proxy with TLS**, **firewall rules**, and strong **authentication** at the proxy boundary.

## CI security checks

- **pip-audit** runs against `requirements.txt` on pushes and PRs to `master` / `main`.  
- **CodeQL** analyzes the Python codebase on the same triggers plus a weekly schedule.

## Supply chain

Prefer downloading **`GrabbySetup.exe`** from **official [GitHub Releases](https://github.com/jampat000/Grabby/releases)** only.
