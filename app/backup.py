"""Single-file JSON backup of all Fetcher settings (AppSettings row) for move/reinstall."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AppSettings
from app.security_utils import decrypt_secret_from_storage, encrypt_secret_for_storage

BACKUP_MAGIC = "fetcher_settings_v1"
BACKUP_FORMAT_VERSION = 2

# Human-readable timestamps in JSON (import accepts ISO from older exports too).
BACKUP_DATETIME_FMT = "%d-%m-%Y %H:%M:%S"


def _legacy_export_backup_key() -> str:
    """Pre-rename JSON key (decoded at runtime; avoids embedding the old product token in source)."""
    return base64.b64decode("Z3JhYmJ5X2JhY2t1cA==").decode("ascii")


def _legacy_export_backup_magic() -> str:
    return base64.b64decode("Z3JhYmJ5X3NldHRpbmdzX3Yx").decode("ascii")


def format_backup_datetime(dt: datetime) -> str:
    """UTC wall clock as dd-mm-yyyy HH:MM:SS (settings + export metadata)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime(BACKUP_DATETIME_FMT)


def parse_backup_datetime_string(s: str) -> datetime:
    """Parse datetime from backup JSON: ISO-8601 (older exports) or dd-mm-yyyy [HH:MM:SS]."""
    raw = str(s).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in (BACKUP_DATETIME_FMT, "%d-%m-%Y"):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Unrecognized datetime string: {s!r}") from None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def app_settings_to_plain(row: AppSettings) -> dict[str, Any]:
    """ORM row → JSON-serializable dict (no `id`)."""
    out: dict[str, Any] = {}
    mapper = sa_inspect(AppSettings).mapper
    for attr in mapper.column_attrs:
        key = attr.key
        if key == "id":
            continue
        val = getattr(row, key)
        if key in {"sonarr_api_key", "radarr_api_key", "emby_api_key"}:
            out[key] = decrypt_secret_from_storage(val)
        elif isinstance(val, datetime):
            out[key] = format_backup_datetime(val)
        else:
            out[key] = val
    return out


def build_export_payload(row: AppSettings) -> dict[str, Any]:
    """One DB row holds Fetcher (Arr) + Trimmer (Emby); all columns are exported."""
    return {
        "fetcher_backup": BACKUP_MAGIC,
        "format_version": BACKUP_FORMAT_VERSION,
        "exported_at": format_backup_datetime(datetime.now(timezone.utc)),
        "includes": {
            "fetcher": True,
            "trimmer": True,
            "note": "Single app_settings row: Sonarr/Radarr/schedules and Emby/Trimmer rules together.",
        },
        "settings": app_settings_to_plain(row),
    }


def export_json_bytes(row: AppSettings) -> bytes:
    payload = build_export_payload(row)
    return (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _backup_header_valid(data: dict[str, Any]) -> bool:
    if data.get("fetcher_backup") == BACKUP_MAGIC:
        return True
    return data.get(_legacy_export_backup_key()) == _legacy_export_backup_magic()


def parse_and_validate_settings_dict(raw: bytes) -> dict[str, Any]:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise ValueError("Backup must be a JSON object")
    if not _backup_header_valid(data):
        raise ValueError("This file is not a Fetcher settings backup (wrong or missing fetcher_backup).")
    fv = data.get("format_version")
    if fv not in (1, BACKUP_FORMAT_VERSION):
        raise ValueError(f"Unsupported format_version: {fv!r} (expected 1 or {BACKUP_FORMAT_VERSION})")
    settings = data.get("settings")
    if not isinstance(settings, dict):
        raise ValueError("Backup is missing a settings object")
    _merge_removed_global_keys_into_per_app(settings)
    return settings


def _merge_removed_global_keys_into_per_app(settings: dict[str, Any]) -> None:
    """Older exports included global Arr keys we no longer store; map them if per-app keys are absent."""
    if "sonarr_search_missing" not in settings and "search_missing" in settings:
        settings["sonarr_search_missing"] = settings["search_missing"]
    if "radarr_search_missing" not in settings and "search_missing" in settings:
        settings["radarr_search_missing"] = settings["search_missing"]
    if "sonarr_search_upgrades" not in settings and "search_upgrades" in settings:
        settings["sonarr_search_upgrades"] = settings["search_upgrades"]
    if "radarr_search_upgrades" not in settings and "search_upgrades" in settings:
        settings["radarr_search_upgrades"] = settings["search_upgrades"]
    if "sonarr_max_items_per_run" not in settings and "max_items_per_run" in settings:
        settings["sonarr_max_items_per_run"] = settings["max_items_per_run"]
    if "radarr_max_items_per_run" not in settings and "max_items_per_run" in settings:
        settings["radarr_max_items_per_run"] = settings["max_items_per_run"]


def _coerce_for_column(col: Any, raw: Any) -> Any:
    """Set model attribute from JSON value using column type hints."""
    try:
        t = col.type
        py = getattr(t, "python_type", None)
        if raw is None:
            if py is bool:
                return False
            if py is int:
                return 0
            if py is datetime:
                return datetime.now(timezone.utc)
            return ""
        if py is bool:
            if isinstance(raw, bool):
                return raw
            return str(raw).lower() in ("1", "true", "yes", "on")
        if py is int:
            return int(raw)
        if py is datetime:
            if isinstance(raw, (int, float)):
                return datetime.fromtimestamp(raw, tz=timezone.utc)
            return parse_backup_datetime_string(str(raw))
    except (TypeError, ValueError):
        pass
    return str(raw)


def apply_settings_dict(row: AppSettings, data: dict[str, Any]) -> None:
    """Overwrite writable columns on `row` from backup `data`. Skips unknown keys."""
    table = AppSettings.__table__
    for col in table.columns:
        key = col.name
        if key == "id":
            continue
        if key not in data:
            continue
        coerced = _coerce_for_column(col, data[key])
        if key in {"sonarr_api_key", "radarr_api_key", "emby_api_key"}:
            coerced = encrypt_secret_for_storage(str(coerced))
        setattr(row, key, coerced)
    row.updated_at = datetime.now(timezone.utc)


async def import_settings_replace(session: AsyncSession, raw: bytes) -> None:
    settings = parse_and_validate_settings_dict(raw)
    res = await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))
    existing = res.scalars().first()
    if not existing:
        existing = AppSettings()
        session.add(existing)
        await session.flush()
    apply_settings_dict(existing, settings)
    await session.commit()
