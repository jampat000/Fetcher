"""Tests for GitHub release check and upgrade apply guards."""

from __future__ import annotations

import types
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app import updates as updates_mod
from app.updates import _apply_eligible, _tag_to_version


@pytest.fixture(autouse=True)
def _clear_updates_release_cache() -> None:
    updates_mod._release_payload_cache.clear()
    yield
    updates_mod._release_payload_cache.clear()


def _build_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_start() -> None:
        return None

    def _noop_shutdown() -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)
    return TestClient(app)


def test_tag_to_version_strips_v_prefix() -> None:
    v = _tag_to_version("v1.2.3")
    assert v is not None
    assert str(v) == "1.2.3"


def test_tag_to_version_invalid() -> None:
    assert _tag_to_version("") is None
    assert _tag_to_version("not-a-version") is None


def test_apply_eligible_allows_dev_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRABBY_ALLOW_DEV_UPGRADE", "1")
    monkeypatch.setattr("app.updates.sys", types.SimpleNamespace(platform="win32", frozen=False))
    assert _apply_eligible() is True


def test_api_updates_check_fresh_release(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_fetch(_repo: str, include_token: bool = True) -> dict[str, Any]:
        return {
            "tag_name": "v2.0.0",
            "html_url": "https://github.com/jampat000/Grabby/releases/tag/v2.0.0",
            "assets": [
                {
                    "name": "GrabbySetup.exe",
                    "browser_download_url": "https://github.com/jampat000/Grabby/releases/download/v2.0.0/GrabbySetup.exe",
                }
            ],
        }

    monkeypatch.setattr("app.updates._fetch_latest_release_payload", _fake_fetch)
    monkeypatch.setattr("app.updates._platform_ok", lambda: True)
    monkeypatch.setattr("app.updates.get_app_version", lambda: "1.0.0")

    with _build_client(monkeypatch) as client:
        r = client.get("/api/updates/check")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["update_available"] is True
    assert data["latest_version"] == "2.0.0"
    assert "GrabbySetup.exe" in (data.get("download_url") or "")


def test_api_updates_check_falls_back_when_api_rate_limited(monkeypatch: pytest.MonkeyPatch) -> None:
    req = httpx.Request("GET", "https://api.github.com/repos/x/y/releases/latest")
    resp = httpx.Response(403, json={"message": "API rate limit exceeded for 1.2.3.4."})
    err = httpx.HTTPStatusError("rate limited", request=req, response=resp)

    async def _api_fail(_repo: str, include_token: bool = True) -> dict[str, Any]:
        raise err

    async def _web_ok(_repo: str) -> dict[str, Any]:
        return {
            "tag_name": "v2.0.0",
            "html_url": "https://github.com/jampat000/Grabby/releases/tag/v2.0.0",
            "assets": [
                {
                    "name": "GrabbySetup.exe",
                    "browser_download_url": "https://github.com/jampat000/Grabby/releases/download/v2.0.0/GrabbySetup.exe",
                }
            ],
        }

    monkeypatch.setattr("app.updates._fetch_latest_release_payload", _api_fail)
    monkeypatch.setattr("app.updates._fetch_latest_without_api", _web_ok)
    monkeypatch.setattr("app.updates._platform_ok", lambda: True)
    monkeypatch.setattr("app.updates.get_app_version", lambda: "1.0.0")

    with _build_client(monkeypatch) as client:
        r = client.get("/api/updates/check")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["update_available"] is True
    assert data["latest_version"] == "2.0.0"


def test_api_updates_check_retries_api_without_token_on_401(monkeypatch: pytest.MonkeyPatch) -> None:
    req = httpx.Request("GET", "https://api.github.com/repos/x/y/releases/latest")
    resp401 = httpx.Response(401, request=req, json={"message": "Bad credentials"})
    err401 = httpx.HTTPStatusError("401", request=req, response=resp401)

    async def _fetch(_repo: str, include_token: bool = True) -> dict[str, Any]:
        if include_token:
            raise err401
        return {
            "tag_name": "v2.0.0",
            "html_url": "https://github.com/jampat000/Grabby/releases/tag/v2.0.0",
            "assets": [
                {
                    "name": "GrabbySetup.exe",
                    "browser_download_url": "https://github.com/jampat000/Grabby/releases/download/v2.0.0/GrabbySetup.exe",
                }
            ],
        }

    monkeypatch.setattr("app.updates._fetch_latest_release_payload", _fetch)
    monkeypatch.setattr("app.updates._platform_ok", lambda: True)
    monkeypatch.setattr("app.updates.get_app_version", lambda: "1.0.0")

    with _build_client(monkeypatch) as client:
        r = client.get("/api/updates/check")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["update_available"] is True


def test_installer_download_headers_skip_auth_for_github_releases_download() -> None:
    h = updates_mod._installer_download_headers(
        "https://github.com/jampat000/Grabby/releases/download/v1.0.27/GrabbySetup.exe"
    )
    assert "Authorization" not in h
    assert h.get("Accept") == "application/octet-stream"


def test_api_updates_check_synthetic_download_when_api_assets_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """GitHub sometimes returns a release without assets in the JSON; use conventional /releases/download/ URL."""

    async def _fake(_repo: str, include_token: bool = True) -> dict[str, Any]:
        return {
            "tag_name": "v2.0.0",
            "html_url": "https://github.com/jampat000/Grabby/releases/tag/v2.0.0",
            "assets": [],
        }

    monkeypatch.setattr("app.updates._fetch_latest_release_payload", _fake)
    monkeypatch.setattr("app.updates._platform_ok", lambda: True)
    monkeypatch.setattr("app.updates.get_app_version", lambda: "1.0.0")

    with _build_client(monkeypatch) as client:
        r = client.get("/api/updates/check")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["update_available"] is True
    assert data["asset_missing"] is False
    assert "releases/download/v2.0.0/GrabbySetup.exe" in (data.get("download_url") or "")


def test_api_updates_check_refresh_bypasses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    async def _fake(_repo: str, include_token: bool = True) -> dict[str, Any]:
        calls["n"] += 1
        return {
            "tag_name": "v2.0.0",
            "html_url": "https://github.com/jampat000/Grabby/releases/tag/v2.0.0",
            "assets": [
                {
                    "name": "GrabbySetup.exe",
                    "browser_download_url": "https://github.com/jampat000/Grabby/releases/download/v2.0.0/GrabbySetup.exe",
                }
            ],
        }

    monkeypatch.setattr("app.updates._fetch_latest_release_payload", _fake)
    monkeypatch.setattr("app.updates._platform_ok", lambda: True)
    monkeypatch.setattr("app.updates.get_app_version", lambda: "1.0.0")

    with _build_client(monkeypatch) as client:
        client.get("/api/updates/check")
        client.get("/api/updates/check")
        assert calls["n"] == 1
        client.get("/api/updates/check?refresh=1")
        assert calls["n"] == 2


def test_api_updates_check_up_to_date(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_fetch(_repo: str, include_token: bool = True) -> dict[str, Any]:
        return {
            "tag_name": "v1.0.0",
            "html_url": "https://github.com/x/y",
            "assets": [{"name": "GrabbySetup.exe", "browser_download_url": "https://example.com/a.exe"}],
        }

    monkeypatch.setattr("app.updates._fetch_latest_release_payload", _fake_fetch)
    monkeypatch.setattr("app.updates._platform_ok", lambda: True)
    monkeypatch.setattr("app.updates.get_app_version", lambda: "1.0.0")

    with _build_client(monkeypatch) as client:
        r = client.get("/api/updates/check")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["update_available"] is False


def test_api_updates_apply_rejects_when_not_frozen(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.updates.sys", types.SimpleNamespace(platform="win32", frozen=False))
    monkeypatch.delenv("GRABBY_ALLOW_DEV_UPGRADE", raising=False)

    with _build_client(monkeypatch) as client:
        r = client.post("/api/updates/apply")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert "frozen" in body["error"].lower()


def test_api_updates_apply_rejects_non_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.updates.sys", types.SimpleNamespace(platform="linux", frozen=True))

    with _build_client(monkeypatch) as client:
        r = client.post("/api/updates/apply")
    assert r.status_code == 200
    assert r.json()["ok"] is False
