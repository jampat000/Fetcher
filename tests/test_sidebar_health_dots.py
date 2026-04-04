"""Sidebar health dot mapping (Arr snapshots + Emby / Refiner heuristics)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.web_common import sidebar_health_dots


def _empty_snaps() -> dict[str, None]:
    return {"sonarr": None, "radarr": None, "emby": None}


def test_emby_unknown_without_settings_even_if_snap_missing() -> None:
    assert sidebar_health_dots(_empty_snaps(), None)["emby"] == "unknown"


def test_emby_ok_when_enabled_url_and_key_env_without_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FETCHER_EMBY_API_KEY", "secret")
    settings = SimpleNamespace(emby_enabled=True, emby_url="http://emby", emby_api_key="")
    assert sidebar_health_dots(_empty_snaps(), settings)["emby"] == "ok"


def test_emby_unknown_when_enabled_but_no_url_and_no_env_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FETCHER_EMBY_API_KEY", raising=False)
    monkeypatch.delenv("EMBY_API_KEY", raising=False)
    settings = SimpleNamespace(emby_enabled=True, emby_url=" ", emby_api_key="")
    assert sidebar_health_dots(_empty_snaps(), settings)["emby"] == "unknown"


def test_emby_uses_snapshot_fail_over_config_heuristic() -> None:
    settings = SimpleNamespace(emby_enabled=True, emby_url="http://e", emby_api_key="")
    snap = SimpleNamespace(ok=False)
    snaps = {"sonarr": None, "radarr": None, "emby": snap}
    assert sidebar_health_dots(snaps, settings)["emby"] == "fail"


def test_emby_snapshot_ok_without_settings() -> None:
    snap = SimpleNamespace(ok=True)
    snaps = {"sonarr": None, "radarr": None, "emby": snap}
    assert sidebar_health_dots(snaps, None)["emby"] == "ok"
