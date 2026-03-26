# Fetcher — FastAPI + SQLite (Linux). Data: mount a volume on /data and set FETCHER_DEV_DB_PATH.
# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    FETCHER_DEV_DB_PATH=/data/fetcher.db

WORKDIR /app

COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY VERSION ./VERSION

RUN mkdir -p /data /app/logs \
    && useradd --create-home --uid 1000 --shell /usr/sbin/nologin fetcher \
    && chown -R fetcher:fetcher /app /data

USER fetcher

EXPOSE 8765

# LISTEN on all interfaces; use reverse proxy + TLS for production exposure.
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8765", "--log-level", "warning"]
