from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import httpx

from app.db import SessionLocal
from app.models import AppSettings
from sqlalchemy import select


@dataclass(frozen=True)
class EndpointCall:
    name: str
    method: str
    path: str
    params: dict[str, str] | None = None


def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "object"
    return type(v).__name__


def _sample(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, list):
        if not v:
            return []
        return [_sample(v[0])]
    if isinstance(v, dict):
        out: dict[str, Any] = {}
        for i, (k, vv) in enumerate(v.items()):
            if i >= 8:
                out["..."] = "(truncated)"
                break
            out[k] = _sample(vv)
        return out
    return str(v)


def _collect_schema(
    value: Any,
    path: str,
    out: dict[str, set[str]],
    samples: dict[str, Any],
    *,
    max_depth: int = 7,
    depth: int = 0,
) -> None:
    out[path].add(_type_name(value))
    if path not in samples:
        samples[path] = _sample(value)
    if depth >= max_depth:
        return
    if isinstance(value, dict):
        for k, v in value.items():
            child = f"{path}.{k}" if path else k
            _collect_schema(v, child, out, samples, max_depth=max_depth, depth=depth + 1)
    elif isinstance(value, list):
        for i, v in enumerate(value[:3]):
            child = f"{path}[]" if path else "[]"
            _collect_schema(v, child, out, samples, max_depth=max_depth, depth=depth + 1)
            if i >= 2:
                break


async def _load_emby_settings() -> AppSettings | None:
    async with SessionLocal() as session:
        return (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()


async def main() -> None:
    settings = await _load_emby_settings()
    if not settings or not settings.emby_url or not settings.emby_api_key:
        raise SystemExit("Missing Emby settings in app DB (emby_url/api_key). Configure in app first.")

    base_url = settings.emby_url.rstrip("/")
    api_key = settings.emby_api_key.strip()
    if not api_key:
        raise SystemExit("Emby API key is empty.")

    headers = {
        "X-Emby-Token": api_key,
        "X-MediaBrowser-Token": api_key,
    }

    def _redact(text: str) -> str:
        return text.replace(api_key, "***REDACTED***")

    raw_results: dict[str, Any] = {}
    schema_types: dict[str, set[str]] = defaultdict(set)
    schema_samples: dict[str, Any] = {}
    call_results: list[dict[str, Any]] = []

    async with httpx.AsyncClient(
        base_url=base_url,
        headers=headers,
        params={"api_key": api_key},
        timeout=45.0,
    ) as client:
        seed_calls = [
            EndpointCall("system_info", "GET", "/System/Info"),
            EndpointCall("system_configuration", "GET", "/System/Configuration"),
            EndpointCall("users", "GET", "/Users"),
            EndpointCall("library_virtual_folders", "GET", "/Library/VirtualFolders"),
            EndpointCall("sessions", "GET", "/Sessions"),
            EndpointCall("scheduled_tasks", "GET", "/ScheduledTasks"),
        ]

        users_payload: list[dict[str, Any]] = []
        for call in seed_calls:
            rec: dict[str, Any] = {
                "name": call.name,
                "method": call.method,
                "path": call.path,
            }
            try:
                resp = await client.request(call.method, call.path, params=call.params)
                rec["status_code"] = resp.status_code
                resp.raise_for_status()
                data = resp.json()
                raw_results[call.name] = data
                _collect_schema(data, call.name, schema_types, schema_samples)
                if call.name == "users" and isinstance(data, list):
                    users_payload = [u for u in data if isinstance(u, dict)]
            except Exception as exc:  # noqa: BLE001
                rec["error"] = _redact(f"{type(exc).__name__}: {exc}")
            call_results.append(rec)

        user_ids: list[str] = []
        for u in users_payload:
            uid = str(u.get("Id", "")).strip()
            if uid:
                user_ids.append(uid)
        if settings.emby_user_id and settings.emby_user_id not in user_ids:
            user_ids.insert(0, settings.emby_user_id)
        user_ids = user_ids[:2]

        for uid in user_ids:
            calls = [
                EndpointCall(f"user_{uid}_views", "GET", f"/Users/{uid}/Views"),
                EndpointCall(
                    f"user_{uid}_items",
                    "GET",
                    f"/Users/{uid}/Items",
                    params={
                        "Recursive": "true",
                        "IncludeItemTypes": "Movie,Series,Episode",
                        "Fields": "Path,Overview,Genres,ProductionYear,CommunityRating,OfficialRating,UserData,DateCreated,DateLastMediaAdded,PremiereDate,ProviderIds,People",
                        "SortBy": "DateCreated",
                        "SortOrder": "Descending",
                        "Limit": "25",
                    },
                ),
                EndpointCall(
                    f"user_{uid}_resumable",
                    "GET",
                    f"/Users/{uid}/Items/Resume",
                    params={"Limit": "25"},
                ),
            ]
            for call in calls:
                rec: dict[str, Any] = {
                    "name": call.name,
                    "method": call.method,
                    "path": call.path,
                    "params": call.params or {},
                }
                try:
                    resp = await client.request(call.method, call.path, params=call.params)
                    rec["status_code"] = resp.status_code
                    resp.raise_for_status()
                    data = resp.json()
                    raw_results[call.name] = data
                    _collect_schema(data, call.name, schema_types, schema_samples)
                except Exception as exc:  # noqa: BLE001
                    rec["error"] = _redact(f"{type(exc).__name__}: {exc}")
                call_results.append(rec)

        # Deep detail on one item if available
        candidate_id = None
        for key, payload in raw_results.items():
            if "items" not in key.lower():
                continue
            if isinstance(payload, Mapping) and isinstance(payload.get("Items"), list) and payload["Items"]:
                first = payload["Items"][0]
                if isinstance(first, Mapping):
                    candidate_id = str(first.get("Id", "")).strip() or None
                    if candidate_id:
                        break
        if candidate_id:
            name = f"item_{candidate_id}_detail"
            rec = {"name": name, "method": "GET", "path": f"/Items/{candidate_id}"}
            try:
                resp = await client.get(
                    f"/Items/{candidate_id}",
                    params={
                        "Fields": "Path,Overview,Genres,Studios,Tags,ProviderIds,People,UserData,MediaSources,Chapters,DateCreated,DateLastMediaAdded,PremiereDate",
                    },
                )
                rec["status_code"] = resp.status_code
                resp.raise_for_status()
                data = resp.json()
                raw_results[name] = data
                _collect_schema(data, name, schema_types, schema_samples)
            except Exception as exc:  # noqa: BLE001
                rec["error"] = _redact(f"{type(exc).__name__}: {exc}")
            call_results.append(rec)

    timestamp = datetime.now(UTC).strftime("%d-%m-%Y_%H%M%S")
    out_dir = Path("tmp") / f"emby-inventory-{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    schema_serializable = {
        path: {
            "types": sorted(list(types)),
            "sample": schema_samples.get(path),
        }
        for path, types in sorted(schema_types.items())
    }

    metadata = {
        "generated_at_utc": datetime.now(UTC).strftime("%d-%m-%Y %H:%M:%S"),
        "emby_base_url": base_url,
        "calls": call_results,
        "output_dir": str(out_dir.resolve()),
    }

    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (out_dir / "schema.json").write_text(json.dumps(schema_serializable, indent=2), encoding="utf-8")
    (out_dir / "raw_payloads.json").write_text(json.dumps(raw_results, indent=2), encoding="utf-8")

    # Human-readable summary
    lines = [
        "# Emby API Inventory",
        "",
        f"- Generated: `{metadata['generated_at_utc']}`",
        f"- Base URL: `{base_url}`",
        f"- Successful payload groups: `{len(raw_results)}`",
        f"- Schema paths discovered: `{len(schema_serializable)}`",
        "",
        "## Calls",
    ]
    for c in call_results:
        status = c.get("status_code", "ERR")
        if "error" in c:
            lines.append(f"- `{c['name']}` -> `{status}` ({c['error']})")
        else:
            lines.append(f"- `{c['name']}` -> `{status}`")
    lines += [
        "",
        "## Files",
        f"- `metadata.json`",
        f"- `schema.json`",
        f"- `raw_payloads.json`",
        "",
        "Use `schema.json` to design settings fields and `raw_payloads.json` for concrete examples.",
    ]
    (out_dir / "README.md").write_text("\n".join(lines), encoding="utf-8")

    print(str(out_dir.resolve()))


if __name__ == "__main__":
    asyncio.run(main())
