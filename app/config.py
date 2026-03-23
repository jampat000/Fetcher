"""Deprecated config loader kept for backward compatibility.

Secrets are now sourced from environment variables only.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RootYamlConfig:
    """Legacy shape for callers that still import this module."""

    sonarr_api_key: str = ""
    radarr_api_key: str = ""
    emby_api_key: str = ""

def get_root_yaml_config() -> RootYamlConfig:
    """Always empty: environment variables replaced YAML for secrets."""
    return RootYamlConfig()


def clear_root_yaml_config_cache() -> None:
    """No-op kept for backward compatibility."""
    return None
