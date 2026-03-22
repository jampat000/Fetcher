"""Tests for optional root ``config.yaml`` loading and service key resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.config import RootYamlConfig, clear_root_yaml_config_cache, get_root_yaml_config
from app.models import AppSettings
from services.api_keys import (
    resolve_emby_api_key,
    resolve_radarr_api_key,
    resolve_setup_api_key,
    resolve_sonarr_api_key,
)


@pytest.fixture(autouse=True)
def _clear_yaml_cache() -> None:
    clear_root_yaml_config_cache()
    yield
    clear_root_yaml_config_cache()


def test_get_root_yaml_config_missing_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.config._find_config_path", lambda: None)
    clear_root_yaml_config_cache()
    assert get_root_yaml_config() == RootYamlConfig()


def test_get_root_yaml_config_loads_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "config.yaml"
    p.write_text(
        'SONARR_API_KEY: "s1"\nRADARR_API_KEY: "r1"\nEMBY_API_KEY: "e1"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr("app.config._find_config_path", lambda: p)
    clear_root_yaml_config_cache()
    cfg = get_root_yaml_config()
    assert cfg.sonarr_api_key == "s1"
    assert cfg.radarr_api_key == "r1"
    assert cfg.emby_api_key == "e1"


def test_resolve_sonarr_prefers_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "services.api_keys.get_root_yaml_config",
        lambda: RootYamlConfig(sonarr_api_key="from-yaml", radarr_api_key="", emby_api_key=""),
    )
    s = AppSettings()
    s.sonarr_api_key = "from-db"
    assert resolve_sonarr_api_key(s) == "from-yaml"


def test_resolve_sonarr_falls_back_to_db(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "services.api_keys.get_root_yaml_config",
        lambda: RootYamlConfig(),
    )
    s = AppSettings()
    s.sonarr_api_key = "db-only"
    assert resolve_sonarr_api_key(s) == "db-only"


def test_resolve_emby_form_overrides_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "services.api_keys.get_root_yaml_config",
        lambda: RootYamlConfig(emby_api_key="yaml-e"),
    )
    s = AppSettings()
    s.emby_api_key = "db-e"
    assert resolve_emby_api_key(s, form="form-e") == "form-e"
    assert resolve_emby_api_key(s, form="") == "yaml-e"
    assert resolve_emby_api_key(s, form=None) == "yaml-e"


def test_resolve_setup_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "services.api_keys.get_root_yaml_config",
        lambda: RootYamlConfig(sonarr_api_key="y-s"),
    )
    assert resolve_setup_api_key("", "sonarr") == "y-s"
    assert resolve_setup_api_key("  body  ", "sonarr") == "body"
