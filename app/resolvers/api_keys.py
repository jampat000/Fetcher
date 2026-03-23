"""Resolve Sonarr / Radarr / Emby API keys from env or encrypted DB fields."""

from __future__ import annotations

from typing import Literal

from app.models import AppSettings
from app.security_utils import decrypt_secret_from_storage, read_secret_env

ArrKind = Literal["sonarr", "radarr", "emby"]


def resolve_sonarr_api_key(settings: AppSettings) -> str:
    env_key = read_secret_env("FETCHER_SONARR_API_KEY", "SONARR_API_KEY")
    if env_key:
        return env_key
    return decrypt_secret_from_storage(settings.sonarr_api_key)


def resolve_radarr_api_key(settings: AppSettings) -> str:
    env_key = read_secret_env("FETCHER_RADARR_API_KEY", "RADARR_API_KEY")
    if env_key:
        return env_key
    return decrypt_secret_from_storage(settings.radarr_api_key)


def resolve_emby_api_key(settings: AppSettings, *, form: str | None = None) -> str:
    """If ``form`` is passed (e.g. unsaved Emby settings), non-empty form wins; else env, then DB."""
    if form is not None:
        fs = (form or "").strip()
        if fs:
            return fs
    env_key = read_secret_env("FETCHER_EMBY_API_KEY", "EMBY_API_KEY")
    if env_key:
        return env_key
    return decrypt_secret_from_storage(settings.emby_api_key)


def resolve_setup_api_key(raw: str, kind: ArrKind) -> str:
    """For setup-wizard JSON tests: use body key if set, else environment variables."""
    k = (raw or "").strip()
    if k:
        return k
    if kind == "sonarr":
        return read_secret_env("FETCHER_SONARR_API_KEY", "SONARR_API_KEY")
    if kind == "radarr":
        return read_secret_env("FETCHER_RADARR_API_KEY", "RADARR_API_KEY")
    return read_secret_env("FETCHER_EMBY_API_KEY", "EMBY_API_KEY")
