"""Load optional root ``config.yaml`` (API keys and other local overrides)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class RootYamlConfig:
    """Values read from optional ``config.yaml`` at the repo / install root."""

    sonarr_api_key: str = ""
    radarr_api_key: str = ""
    emby_api_key: str = ""


def _dev_repo_root() -> Path:
    # app/config.py -> app -> repo root
    return Path(__file__).resolve().parent.parent


def _config_file_paths() -> list[Path]:
    """Prefer a config next to the packaged exe, then the development repo root."""
    paths: list[Path] = []
    if getattr(sys, "frozen", False):
        paths.append(Path(sys.executable).resolve().parent / "config.yaml")
    paths.append(_dev_repo_root() / "config.yaml")
    return paths


def _find_config_path() -> Path | None:
    for p in _config_file_paths():
        if p.is_file():
            return p
    return None


_config_mtime: float | None = None
_cached_yaml: RootYamlConfig | None = None


def _parse_yaml_payload(raw: Any) -> RootYamlConfig:
    if not isinstance(raw, dict):
        return RootYamlConfig()

    def _s(key: str) -> str:
        v = raw.get(key)
        if v is None:
            return ""
        return str(v).strip()

    return RootYamlConfig(
        sonarr_api_key=_s("SONARR_API_KEY"),
        radarr_api_key=_s("RADARR_API_KEY"),
        emby_api_key=_s("EMBY_API_KEY"),
    )


def get_root_yaml_config() -> RootYamlConfig:
    """Load ``config.yaml`` if present; cache until the file mtime changes."""
    global _config_mtime, _cached_yaml

    path = _find_config_path()
    if path is None:
        _config_mtime = None
        _cached_yaml = RootYamlConfig()
        return _cached_yaml

    mtime = path.stat().st_mtime
    if _cached_yaml is not None and _config_mtime == mtime:
        return _cached_yaml

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except (OSError, yaml.YAMLError):
        _cached_yaml = RootYamlConfig()
    else:
        _cached_yaml = _parse_yaml_payload(data)
    _config_mtime = mtime
    return _cached_yaml


def clear_root_yaml_config_cache() -> None:
    """Invalidate cache (e.g. for tests)."""
    global _config_mtime, _cached_yaml
    _config_mtime = None
    _cached_yaml = None
