"""Refiner folder pick: Windows via user-session HTTP companion; Linux via zenity subprocess."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from typing import Any

import httpx

from app.refiner_pick_sync import REFINER_PICK_FOLDER_FAIL_MESSAGE, run_picker_subprocess

logger = logging.getLogger(__name__)

# Re-export for routers / tests that import from this module.
__all__ = [
    "REFINER_PICK_FOLDER_FAIL_MESSAGE",
    "REFINER_COMPANION_UNAVAILABLE_MESSAGE",
    "refiner_pick_folder_subprocess",
    "run_picker_subprocess",
]

REFINER_COMPANION_URL_DEFAULT = "http://127.0.0.1:8767/pick-folder"
REFINER_COMPANION_UNAVAILABLE_MESSAGE = (
    "Folder picker companion is not running. Type or paste the path."
)


def _pick_folder_subprocess_argv() -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "--refiner-pick-folder-worker"]
    return [sys.executable, "-m", "app.refiner_pick_sync"]


def _companion_pick_url() -> str:
    return (os.environ.get("FETCHER_REFINER_COMPANION_URL") or REFINER_COMPANION_URL_DEFAULT).strip()


def _normalize_companion_response(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict) or "ok" not in data:
        return {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_COMPANION_UNAVAILABLE_MESSAGE,
        }
    if data.get("ok") is True and data.get("path"):
        return {"ok": True, "path": str(data["path"])}
    if data.get("ok") is False and data.get("reason") == "cancelled":
        return {"ok": False, "reason": "cancelled"}
    if data.get("ok") is False:
        return {
            "ok": False,
            "reason": "unavailable",
            "message": str(data.get("message") or REFINER_COMPANION_UNAVAILABLE_MESSAGE),
        }
    return {
        "ok": False,
        "reason": "unavailable",
        "message": REFINER_COMPANION_UNAVAILABLE_MESSAGE,
    }


async def _pick_folder_via_companion_http() -> dict[str, Any]:
    url = _companion_pick_url()
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(url, timeout=12.0)
    except httpx.TimeoutException:
        logger.warning("Refiner folder picker: companion request timed out (%s)", url)
        return {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_COMPANION_UNAVAILABLE_MESSAGE,
        }
    except httpx.RequestError as exc:
        logger.warning("Refiner folder picker: companion unreachable: %s", exc)
        return {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_COMPANION_UNAVAILABLE_MESSAGE,
        }
    try:
        data = r.json()
    except Exception:
        logger.warning("Refiner folder picker: companion returned non-JSON (status %s)", r.status_code)
        return {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_COMPANION_UNAVAILABLE_MESSAGE,
        }
    if r.status_code >= 400:
        return _normalize_companion_response(data if isinstance(data, dict) else {})
    return _normalize_companion_response(data)


async def _refiner_pick_folder_subprocess_linux() -> dict[str, Any]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *_pick_folder_subprocess_argv(),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            logger.warning("Refiner folder picker subprocess timed out")
            return {
                "ok": False,
                "reason": "unavailable",
                "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
            }

        if proc.returncode != 0:
            err_txt = (stderr or b"").decode(errors="replace").strip()
            logger.warning(
                "Refiner folder picker subprocess failed (rc=%s): %s",
                proc.returncode,
                err_txt,
            )
            return {
                "ok": False,
                "reason": "unavailable",
                "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
            }

        raw_out = stdout or b""
        decoded = raw_out.decode(errors="replace").strip()
        lines = [ln.strip() for ln in decoded.splitlines() if ln.strip()]
        if not lines:
            return {
                "ok": False,
                "reason": "unavailable",
                "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
            }
        try:
            row = json.loads(lines[-1])
        except Exception:
            logger.warning("Invalid subprocess output: %r", raw_out[:2048])
            return {
                "ok": False,
                "reason": "unavailable",
                "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
            }

        path = row.get("folder")
        outcome = row.get("outcome")
        if outcome == "ok" and path:
            return {"ok": True, "path": str(path)}
        if outcome == "cancelled":
            return {"ok": False, "reason": "cancelled"}
        return {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
        }
    except Exception:
        logger.exception("Refiner pick-folder: subprocess execution failed")
        return {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
        }


async def refiner_pick_folder_subprocess() -> dict[str, Any]:
    if sys.platform == "win32":
        return await _pick_folder_via_companion_http()
    return await _refiner_pick_folder_subprocess_linux()


if __name__ == "__main__":
    run_picker_subprocess()
