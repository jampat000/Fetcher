"""Resolve Sonarr / Radarr / Emby API keys: optional ``config.yaml`` overrides SQLite settings."""

from __future__ import annotations

from typing import Literal

from app.config import get_root_yaml_config
from app.models import AppSettings

ArrKind = Literal["sonarr", "radarr", "emby"]


def resolve_sonarr_api_key(settings: AppSettings) -> str:
    y = get_root_yaml_config().sonarr_api_key.strip()
    if y:
        return y
    return (settings.sonarr_api_key or "").strip()


def resolve_radarr_api_key(settings: AppSettings) -> str:
    y = get_root_yaml_config().radarr_api_key.strip()
    if y:
        return y
    return (settings.radarr_api_key or "").strip()


def resolve_emby_api_key(settings: AppSettings, *, form: str | None = None) -> str:
    """If ``form`` is passed (e.g. unsaved Emby settings), non-empty form wins; else YAML, then DB."""
    if form is not None:
        fs = (form or "").strip()
        if fs:
            return fs
    y = get_root_yaml_config().emby_api_key.strip()
    if y:
        return y
    return (settings.emby_api_key or "").strip()


def resolve_setup_api_key(raw: str, kind: ArrKind) -> str:
    """For setup-wizard JSON tests: use body key if set, else ``config.yaml``."""
    k = (raw or "").strip()
    if k:
        return k
    cfg = get_root_yaml_config()
    if kind == "sonarr":
        return cfg.sonarr_api_key.strip()
    if kind == "radarr":
        return cfg.radarr_api_key.strip()
    return cfg.emby_api_key.strip()
