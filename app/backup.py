"""Single-file JSON backup of all Fetcher settings (AppSettings row) for move/reinstall."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AppSettings
from app.schema_version import CURRENT_SCHEMA_VERSION
from app.security_utils import decrypt_secret_from_storage, encrypt_secret_for_storage

logger = logging.getLogger(__name__)

BACKUP_MAGIC = "fetcher_settings_v1"
BACKUP_FORMAT_VERSION = 2

# Non-Fetcher JSON top-level key (rejected on restore if present alongside a Fetcher-shaped file).
_FOREIGN_BACKUP_HEADER_KEY = "grabby_backup"

# Obsolete global Arr field names; not valid inside backup ``settings`` (per-app keys only).
_OBSOLETE_GLOBAL_ARR_JSON_KEYS = frozenset({"search_missing", "search_upgrades", "max_items_per_run"})

# Human-readable timestamps in JSON; datetime values may also use ISO-8601.
BACKUP_DATETIME_FMT = "%d-%m-%Y %H:%M:%S"


def format_backup_datetime(dt: datetime) -> str:
    """UTC wall clock as dd-mm-yyyy HH:MM:SS (settings + export metadata)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime(BACKUP_DATETIME_FMT)


def parse_backup_datetime_string(s: str) -> datetime:
    """Parse datetime from backup JSON: ISO-8601 or dd-mm-yyyy [HH:MM:SS]."""
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


def merge_legacy_interval_fields_into_canonical(data: dict[str, Any]) -> None:
    """Map Phase 3 deprecated keys in an imported ``settings`` dict to canonical keys (in-place).

    Allows restoring backups exported before canonical column names when the file is re-saved
    at the current schema version.
    """
    if "sonarr_search_interval_minutes" not in data and "sonarr_interval_minutes" in data:
        data["sonarr_search_interval_minutes"] = data["sonarr_interval_minutes"]
    if "radarr_search_interval_minutes" not in data and "radarr_interval_minutes" in data:
        data["radarr_search_interval_minutes"] = data["radarr_interval_minutes"]
    if "trimmer_interval_minutes" not in data and "emby_interval_minutes" in data:
        data["trimmer_interval_minutes"] = data["emby_interval_minutes"]
    if "movie_refiner_interval_seconds" not in data and "refiner_interval_seconds" in data:
        data["movie_refiner_interval_seconds"] = data["refiner_interval_seconds"]
    if "tv_refiner_interval_seconds" not in data and "sonarr_refiner_interval_seconds" in data:
        data["tv_refiner_interval_seconds"] = data["sonarr_refiner_interval_seconds"]
    shared = data.get("failed_import_cleanup_interval_minutes")
    if shared is not None:
        if "sonarr_failed_import_cleanup_interval_minutes" not in data:
            data["sonarr_failed_import_cleanup_interval_minutes"] = shared
        if "radarr_failed_import_cleanup_interval_minutes" not in data:
            data["radarr_failed_import_cleanup_interval_minutes"] = shared


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
    """Export the full ``app_settings`` row (all columns except ``id``) as JSON."""
    v = int(CURRENT_SCHEMA_VERSION)
    return {
        "fetcher_backup": BACKUP_MAGIC,
        "format_version": BACKUP_FORMAT_VERSION,
        # Explicit contract: restore is allowed only when this equals app.schema_version.CURRENT_SCHEMA_VERSION.
        "supported_schema_version": v,
        "exported_at": format_backup_datetime(datetime.now(timezone.utc)),
        "includes": {
            "fetcher": True,
            "trimmer": True,
            "refiner": True,
            "note": (
                "Full app_settings row: Sonarr, Radarr, Trimmer (media server), Refiners, web authentication, "
                "schedules, and schema_version. Excludes activity_log, job_run_log, app_snapshot, "
                "refiner_activity, and arr_action_log."
            ),
        },
        "settings": app_settings_to_plain(row),
    }


def export_json_bytes(row: AppSettings) -> bytes:
    payload = build_export_payload(row)
    return (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _backup_header_valid(data: dict[str, Any]) -> bool:
    return data.get("fetcher_backup") == BACKUP_MAGIC


def _reject_disallowed_backup_payload(data: dict[str, Any], settings: dict[str, Any]) -> None:
    if _FOREIGN_BACKUP_HEADER_KEY in data:
        raise ValueError(
            "This backup file contains an unsupported top-level key and cannot be restored. "
            "Export a new backup from this Fetcher build only."
        )
    bad = _OBSOLETE_GLOBAL_ARR_JSON_KEYS & settings.keys()
    if bad:
        raise ValueError(
            "Backup settings contain unsupported field name(s): "
            f"{', '.join(sorted(bad))}. Use current per-app keys only "
            "(for example sonarr_search_missing, not a single global search_missing)."
        )


def parse_and_validate_settings_dict(raw: bytes) -> dict[str, Any]:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise ValueError("Backup must be a JSON object")
    if not _backup_header_valid(data):
        raise ValueError(
            "This file is not a valid Fetcher settings backup (missing or invalid fetcher_backup)."
        )
    fv = data.get("format_version")
    if fv != BACKUP_FORMAT_VERSION:
        raise ValueError(
            f"Unsupported format_version: {fv!r} (expected {BACKUP_FORMAT_VERSION} only)."
        )
    settings = data.get("settings")
    if not isinstance(settings, dict):
        raise ValueError("Backup is missing a settings object")
    _enforce_backup_schema_version_matches_build(data, settings)
    _reject_disallowed_backup_payload(data, settings)
    return settings


def _backup_declared_schema_version(data: dict[str, Any], settings: dict[str, Any]) -> int:
    """Resolve schema version from payload (top-level preferred, then ``settings.schema_version``)."""
    raw_top = data.get("supported_schema_version")
    parsed_top: int | None = None
    if raw_top is not None:
        try:
            parsed_top = int(raw_top)
        except (TypeError, ValueError) as e:
            raise ValueError(
                "Backup field supported_schema_version is invalid (must be an integer)."
            ) from e
    raw_s = settings.get("schema_version")
    parsed_s: int | None = None
    if raw_s is not None:
        try:
            parsed_s = int(raw_s)
        except (TypeError, ValueError) as e:
            raise ValueError(
                "Backup settings.schema_version is invalid (must be an integer)."
            ) from e
    if parsed_top is not None and parsed_s is not None and parsed_top != parsed_s:
        raise ValueError(
            "Backup supported_schema_version and settings.schema_version disagree; file is inconsistent."
        )
    if parsed_top is not None:
        return parsed_top
    if parsed_s is not None:
        return parsed_s
    raise ValueError(
        "This backup does not include schema version metadata (supported_schema_version or "
        "settings.schema_version) and cannot be restored with this build."
    )


def _enforce_backup_schema_version_matches_build(data: dict[str, Any], settings: dict[str, Any]) -> None:
    found = _backup_declared_schema_version(data, settings)
    expected = int(CURRENT_SCHEMA_VERSION)
    if found != expected:
        logger.error(
            "Backup restore blocked: schema version mismatch (expected %s, found %s)",
            expected,
            found,
        )
        raise ValueError(
            f"This backup was created with schema version {found}, but this Fetcher build requires "
            f"schema version {expected}. Restore is not supported across schema versions."
        )


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
    merge_legacy_interval_fields_into_canonical(settings)
    res = await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))
    existing = res.scalars().first()
    if not existing:
        existing = AppSettings()
        session.add(existing)
        await session.flush()
    apply_settings_dict(existing, settings)
    await session.commit()
