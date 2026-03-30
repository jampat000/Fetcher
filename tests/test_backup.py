from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest

from app.backup import (
    BACKUP_FORMAT_VERSION,
    BACKUP_MAGIC,
    apply_settings_dict,
    app_settings_to_plain,
    build_export_payload,
    export_json_bytes,
    format_backup_datetime,
    parse_and_validate_settings_dict,
    parse_backup_datetime_string,
)
from sqlalchemy import inspect as sa_inspect

from app.models import AppSettings
from app.schema_version import CURRENT_SCHEMA_VERSION


def test_backup_datetime_format_and_parse_roundtrip() -> None:
    utc = datetime(2024, 6, 15, 10, 30, 5, tzinfo=timezone.utc)
    s = format_backup_datetime(utc)
    assert s == "15-06-2024 10:30:05"
    assert parse_backup_datetime_string(s) == utc
    iso = "2024-06-15T10:30:05+00:00"
    assert parse_backup_datetime_string(iso) == utc


def test_export_payload_structure() -> None:
    row = AppSettings(
        sonarr_url="http://sonarr.test",
        sonarr_api_key="secret",
        schema_version=CURRENT_SCHEMA_VERSION,
    )
    payload = build_export_payload(row)
    assert payload["fetcher_backup"] == BACKUP_MAGIC
    assert payload["format_version"] == BACKUP_FORMAT_VERSION
    assert payload["supported_schema_version"] == CURRENT_SCHEMA_VERSION
    assert payload["settings"]["schema_version"] == CURRENT_SCHEMA_VERSION
    assert "exported_at" in payload
    assert payload["includes"]["fetcher"] is True
    assert payload["includes"]["trimmer"] is True
    assert payload["includes"]["refiner"] is True
    assert payload["settings"]["sonarr_url"] == "http://sonarr.test"
    assert payload["settings"]["sonarr_api_key"] == "secret"


def test_export_includes_every_app_settings_column_except_id() -> None:
    row = AppSettings(schema_version=CURRENT_SCHEMA_VERSION)
    payload = build_export_payload(row)
    col_keys = {a.key for a in sa_inspect(AppSettings).mapper.column_attrs if a.key != "id"}
    for key in col_keys:
        assert key in payload["settings"], f"missing settings key: {key}"


def test_export_roundtrip_preserves_representative_tool_and_auth_fields() -> None:
    row = AppSettings(
        schema_version=CURRENT_SCHEMA_VERSION,
        sonarr_url="http://sonarr.example",
        radarr_url="http://radarr.example",
        emby_url="http://emby.example",
        refiner_enabled=True,
        refiner_watched_folder="/media/in",
        auth_username="backupuser",
    )
    raw = export_json_bytes(row)
    got = parse_and_validate_settings_dict(raw)
    assert got["sonarr_url"] == "http://sonarr.example"
    assert got["radarr_url"] == "http://radarr.example"
    assert got["emby_url"] == "http://emby.example"
    assert got["refiner_enabled"] is True
    assert got["refiner_watched_folder"] == "/media/in"
    assert got["auth_username"] == "backupuser"


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


def test_parse_rejects_format_version_1() -> None:
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "format_version": 1,
            "supported_schema_version": CURRENT_SCHEMA_VERSION,
            "settings": {"schema_version": CURRENT_SCHEMA_VERSION, "sonarr_url": "http://old"},
        }
    ).encode()
    with pytest.raises(ValueError, match="format_version"):
        parse_and_validate_settings_dict(raw)


def test_parse_rejects_foreign_backup_header_key() -> None:
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "grabby_backup": "grabby_settings_v1",
            "format_version": BACKUP_FORMAT_VERSION,
            "supported_schema_version": CURRENT_SCHEMA_VERSION,
            "settings": {"schema_version": CURRENT_SCHEMA_VERSION, "sonarr_url": "http://x"},
        }
    ).encode()
    with pytest.raises(ValueError, match="unsupported top-level key"):
        parse_and_validate_settings_dict(raw)


def test_parse_rejects_only_foreign_header_no_fetcher_magic() -> None:
    raw = json.dumps(
        {
            "grabby_backup": "grabby_settings_v1",
            "format_version": BACKUP_FORMAT_VERSION,
            "supported_schema_version": CURRENT_SCHEMA_VERSION,
            "settings": {"schema_version": CURRENT_SCHEMA_VERSION, "sonarr_url": "http://y"},
        }
    ).encode()
    with pytest.raises(ValueError, match="valid Fetcher settings backup"):
        parse_and_validate_settings_dict(raw)


def test_parse_rejects_removed_global_arr_keys_in_settings() -> None:
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "format_version": BACKUP_FORMAT_VERSION,
            "supported_schema_version": CURRENT_SCHEMA_VERSION,
            "settings": {
                "schema_version": CURRENT_SCHEMA_VERSION,
                "sonarr_url": "http://z",
                "search_missing": True,
            },
        }
    ).encode()
    with pytest.raises(ValueError, match="unsupported field name"):
        parse_and_validate_settings_dict(raw)


def test_parse_rejects_removed_global_keys_even_with_per_app_duplicates() -> None:
    """Obsolete global keys are never accepted, even if per-app keys are also present."""
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "format_version": BACKUP_FORMAT_VERSION,
            "supported_schema_version": CURRENT_SCHEMA_VERSION,
            "settings": {
                "schema_version": CURRENT_SCHEMA_VERSION,
                "sonarr_url": "http://z",
                "sonarr_search_missing": True,
                "search_missing": False,
            },
        }
    ).encode()
    with pytest.raises(ValueError, match="unsupported field name"):
        parse_and_validate_settings_dict(raw)


def test_parse_rejects_format_version_string_two() -> None:
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "format_version": "2",
            "supported_schema_version": CURRENT_SCHEMA_VERSION,
            "settings": {"schema_version": CURRENT_SCHEMA_VERSION, "sonarr_url": "http://s"},
        }
    ).encode()
    with pytest.raises(ValueError, match="format_version"):
        parse_and_validate_settings_dict(raw)


def test_parse_validate_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="Invalid JSON"):
        parse_and_validate_settings_dict(b"not json")
    with pytest.raises(ValueError, match="Fetcher settings backup"):
        parse_and_validate_settings_dict(json.dumps({"foo": 1}).encode())
    with pytest.raises(ValueError, match="format_version"):
        parse_and_validate_settings_dict(
            json.dumps({"fetcher_backup": BACKUP_MAGIC, "format_version": 99, "settings": {}}).encode()
        )


def test_parse_rejects_backup_without_schema_metadata() -> None:
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "format_version": BACKUP_FORMAT_VERSION,
            "settings": {"sonarr_url": "http://x"},
        }
    ).encode()
    with pytest.raises(ValueError, match="schema version metadata"):
        parse_and_validate_settings_dict(raw)


def test_parse_rejects_schema_version_lower_than_build() -> None:
    low = int(CURRENT_SCHEMA_VERSION) - 3
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "format_version": BACKUP_FORMAT_VERSION,
            "supported_schema_version": low,
            "settings": {"schema_version": low, "sonarr_url": "http://y"},
        }
    ).encode()
    with pytest.raises(ValueError, match="schema version"):
        parse_and_validate_settings_dict(raw)


def test_parse_rejects_schema_version_higher_than_build() -> None:
    high = int(CURRENT_SCHEMA_VERSION) + 4
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "format_version": BACKUP_FORMAT_VERSION,
            "supported_schema_version": high,
            "settings": {"schema_version": high, "sonarr_url": "http://z"},
        }
    ).encode()
    with pytest.raises(ValueError, match="schema version"):
        parse_and_validate_settings_dict(raw)


def test_parse_rejects_when_top_level_and_settings_schema_disagree() -> None:
    raw = json.dumps(
        {
            "fetcher_backup": BACKUP_MAGIC,
            "format_version": BACKUP_FORMAT_VERSION,
            "supported_schema_version": CURRENT_SCHEMA_VERSION,
            "settings": {
                "schema_version": int(CURRENT_SCHEMA_VERSION) - 1,
                "sonarr_url": "http://bad",
            },
        }
    ).encode()
    with pytest.raises(ValueError, match="disagree"):
        parse_and_validate_settings_dict(raw)


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

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)

    with TestClient(app) as client:
        ex = client.get("/settings/backup/export")
    assert ex.status_code == 200
    exported = json.loads(ex.text)
    assert exported["fetcher_backup"] == BACKUP_MAGIC
    assert exported["supported_schema_version"] == CURRENT_SCHEMA_VERSION
    assert exported["settings"]["schema_version"] == CURRENT_SCHEMA_VERSION
    assert exported["includes"]["refiner"] is True

    with TestClient(app) as client:
        imp = client.post(
            "/settings/backup/import",
            files={"file": ("fetcher.json", ex.content, "application/json")},
            data={"confirm": "yes"},
            follow_redirects=False,
        )
    assert imp.status_code == 303
    assert "import=ok" in (imp.headers.get("location") or "")


def test_import_schema_mismatch_does_not_modify_stored_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """parse runs before any row update; failed restore must not change the DB row."""
    from app.backup import import_settings_replace
    from app.db import SessionLocal, _get_or_create_settings

    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)

    marker = "http://fetcher-backup-guard-marker"

    async def seed_and_fail() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.sonarr_url = marker
            await session.commit()

        wrong = int(CURRENT_SCHEMA_VERSION) - 1
        bad = json.dumps(
            {
                "fetcher_backup": BACKUP_MAGIC,
                "format_version": BACKUP_FORMAT_VERSION,
                "supported_schema_version": wrong,
                "settings": {"schema_version": wrong, "sonarr_url": "http://would-be-applied"},
            }
        ).encode()

        async with SessionLocal() as session:
            with pytest.raises(ValueError, match="schema version"):
                await import_settings_replace(session, bad)

        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.sonarr_url == marker

    asyncio.run(seed_and_fail())


def test_import_removed_global_keys_does_not_modify_stored_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.backup import import_settings_replace
    from app.db import SessionLocal, _get_or_create_settings

    async def _noop_start() -> None:
        return None

    def _noop_shutdown(*_a: object, **_kw: object) -> None:
        return None

    monkeypatch.setattr("app.main.scheduler.start", _noop_start)
    monkeypatch.setattr("app.main.scheduler.shutdown", _noop_shutdown)

    marker = "http://fetcher-obsolete-json-key-guard"

    async def seed_and_fail() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.sonarr_url = marker
            await session.commit()

        bad = json.dumps(
            {
                "fetcher_backup": BACKUP_MAGIC,
                "format_version": BACKUP_FORMAT_VERSION,
                "supported_schema_version": CURRENT_SCHEMA_VERSION,
                "settings": {
                    "schema_version": CURRENT_SCHEMA_VERSION,
                    "sonarr_url": "http://would-be-applied",
                    "max_items_per_run": 99,
                },
            }
        ).encode()

        async with SessionLocal() as session:
            with pytest.raises(ValueError, match="unsupported field name"):
                await import_settings_replace(session, bad)

        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            assert row.sonarr_url == marker

    asyncio.run(seed_and_fail())
