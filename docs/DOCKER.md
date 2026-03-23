# Fetcher in Docker

Fetcher is a **FastAPI** app with a **SQLite** database. The container listens on **port 8765** and stores the DB file on a **volume** under `/data`.

## Requirements

- Docker 20.10+ (or Docker Desktop)
- Optional: Docker Compose v2 (`docker compose`)

## Quick start (Compose)

From the repository root:

```bash
docker compose up -d --build
```

Open **http://127.0.0.1:8765** and complete **Setup** (account, Sonarr/Radarr, etc.).

Stop:

```bash
docker compose down
```

Data is kept in the named volume `fetcher-data` until you run `docker compose down -v`.

## Build and run (Docker only)

```bash
docker build -t fetcher:latest .
docker run -d --name fetcher \
  -p 8765:8765 \
  -e FETCHER_DEV_DB_PATH=/data/fetcher.db \
  -v fetcher-data:/data \
  --restart unless-stopped \
  fetcher:latest
```

Create the volume once if it does not exist:

```bash
docker volume create fetcher-data
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| **`FETCHER_DEV_DB_PATH`** | **Required for persistence.** SQLite file path inside the container (e.g. `/data/fetcher.db`). The image sets this by default; override only if you use another mount path. |
| **`TZ`** | Host timezone for logs and scheduler windows (e.g. `Europe/London`). |
| **`FETCHER_LOG_LEVEL`** | e.g. `INFO`, `DEBUG` (default warning-style logging in uvicorn). |
| **`FETCHER_GITHUB_TOKEN`** | Optional, for GitHub API rate limits on **Software updates**. |
| **`FETCHER_UPDATES_REPO`** | Optional `owner/repo` if you track releases on a fork. |

Optional **`config.yaml`** (API keys) can be bind-mounted read-only at **`/app/config.yaml`** if you use that feature.

## Security notes

- The container binds **`0.0.0.0:8765`** so the UI is reachable from other hosts if you publish the port. Prefer **localhost-only** publishing (`127.0.0.1:8765:8765`) or put **HTTPS** and auth in front with a reverse proxy.
- **In-app Windows upgrade** does not apply inside Docker; deploy new images instead.

## Health check

`GET /healthz` returns JSON including **`version`** (from the `VERSION` file baked into the image).

## Updating to a new image

```bash
docker compose pull   # if you use a registry image
docker compose up -d --build
```

Or rebuild from git:

```bash
git pull
docker compose up -d --build
```
