"""Single-file JSON backup of all Grabby settings (AppSettings row) for move/reinstall."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import inspect as orm_inspect

from app.models import AppSettings

BACKUP_MAGIC = "grabby_settings_v1"
BACKUP_FORMAT_VERSION = 1


def app_settings_to_plain(row: AppSettings) -> dict[str, Any]:
    """ORM row → JSON-serializable dict (no `id`)."""
    out: dict[str, Any] = {}
    mapper = orm_inspect(AppSettings).mapper
    for attr in mapper.column_attrs:
        key = attr.key
        if key == "id":
            continue
        val = getattr(row, key)
        if isinstance(val, datetime):
            out[key] = val.replace(tzinfo=timezone.utc).isoformat() if val.tzinfo is None else val.isoformat()
        else:
            out[key] = val
    return out


def build_export_payload(row: AppSettings) -> dict[str, Any]:
    return {
        "grabby_backup": BACKUP_MAGIC,
        "format_version": BACKUP_FORMAT_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "settings": app_settings_to_plain(row),
    }


def export_json_bytes(row: AppSettings) -> bytes:
    payload = build_export_payload(row)
    return (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def parse_and_validate_settings_dict(raw: bytes) -> dict[str, Any]:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise ValueError("Backup must be a JSON object")
    if data.get("grabby_backup") != BACKUP_MAGIC:
        raise ValueError("This file is not a Grabby settings backup (wrong or missing grabby_backup).")
    fv = data.get("format_version")
    if fv != BACKUP_FORMAT_VERSION:
        raise ValueError(f"Unsupported format_version: {fv!r} (expected {BACKUP_FORMAT_VERSION})")
    settings = data.get("settings")
    if not isinstance(settings, dict):
        raise ValueError("Backup is missing a settings object")
    return settings


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
            s = str(raw).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
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
        setattr(row, key, _coerce_for_column(col, data[key]))
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
