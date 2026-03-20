from __future__ import annotations

import json

import pytest

from app.backup import (
    BACKUP_FORMAT_VERSION,
    BACKUP_MAGIC,
    apply_settings_dict,
    app_settings_to_plain,
    build_export_payload,
    export_json_bytes,
    parse_and_validate_settings_dict,
)
from app.models import AppSettings


def test_export_payload_structure() -> None:
    row = AppSettings(sonarr_url="http://sonarr.test", sonarr_api_key="secret")
    payload = build_export_payload(row)
    assert payload["grabby_backup"] == BACKUP_MAGIC
    assert payload["format_version"] == BACKUP_FORMAT_VERSION
    assert "exported_at" in payload
    assert payload["settings"]["sonarr_url"] == "http://sonarr.test"
    assert payload["settings"]["sonarr_api_key"] == "secret"


def test_roundtrip_plain_dict() -> None:
    row = AppSettings(
        sonarr_url="http://a",
        radarr_url="http://b",
        emby_max_items_scan=0,
        sonarr_enabled=True,
    )
    plain = app_settings_to_plain(row)
    row2 = AppSettings()
    apply_settings_dict(row2, plain)
    assert row2.sonarr_url == "http://a"
    assert row2.radarr_url == "http://b"
    assert row2.emby_max_items_scan == 0
    assert row2.sonarr_enabled is True


def test_parse_validate_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="Invalid JSON"):
        parse_and_validate_settings_dict(b"not json")
    with pytest.raises(ValueError, match="Not a Grabby"):
        parse_and_validate_settings_dict(json.dumps({"foo": 1}).encode())
    with pytest.raises(ValueError, match="format_version"):
        parse_and_validate_settings_dict(
            json.dumps({"grabby_backup": BACKUP_MAGIC, "format_version": 99, "settings": {}}).encode()
        )


def test_export_json_bytes_parse() -> None:
    row = AppSettings(emby_url="http://emby")
    raw = export_json_bytes(row)
    data = parse_and_validate_settings_dict(raw)
    assert data["emby_url"] == "http://emby"


def test_http_export_import_redirects_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    from app.main import app

    async def _noop_start() -> None:
        return None

    def _noop_shutdown() -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)

    with TestClient(app) as client:
        ex = client.get("/settings/backup/export")
    assert ex.status_code == 200
    assert json.loads(ex.text)["grabby_backup"] == BACKUP_MAGIC

    with TestClient(app) as client:
        imp = client.post(
            "/settings/backup/import",
            files={"file": ("grabby.json", ex.content, "application/json")},
            data={"confirm": "yes"},
        )
    assert imp.status_code == 303
    assert "import=ok" in (imp.headers.get("location") or "")
