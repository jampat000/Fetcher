"""Optional YAML-shaped config types (unused at runtime; secrets come from the environment)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RootYamlConfig:
    """Placeholder dataclass for historical imports."""

    sonarr_api_key: str = ""
    radarr_api_key: str = ""
    emby_api_key: str = ""


def get_root_yaml_config() -> RootYamlConfig:
    """Return empty keys; runtime uses environment variables only."""
    return RootYamlConfig()


def clear_root_yaml_config_cache() -> None:
    """No-op (no cache)."""
    return None
