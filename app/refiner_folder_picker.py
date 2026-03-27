"""Refiner folder pick: Windows via user-session HTTP companion; Linux via zenity subprocess."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx

from app.refiner_pick_capability import get_refiner_pick_mode
from app.refiner_pick_capability import HEADLESS_FOLDER_BROWSE_MESSAGE
from app.refiner_pick_sync import REFINER_PICK_FOLDER_FAIL_MESSAGE, run_picker_subprocess
from app.windows_session_launch import resolve_companion_exe_path, start_companion_best_effort

logger = logging.getLogger(__name__)

# Re-export for routers / tests that import from this module.
__all__ = [
    "REFINER_PICK_FOLDER_FAIL_MESSAGE",
    "REFINER_COMPANION_UNAVAILABLE_MESSAGE",
    "companion_health_url",
    "refiner_companion_reachable",
    "ensure_windows_companion_running",
    "refiner_pick_folder_subprocess",
    "run_picker_subprocess",
]

REFINER_COMPANION_URL_DEFAULT = "http://127.0.0.1:8767/pick-folder"
REFINER_COMPANION_UNAVAILABLE_MESSAGE = (
    "Folder picker companion is not running. Type or paste the path."
)
_LAST_COMPANION_ENSURE_ATTEMPT_MONO = 0.0


def _pick_folder_subprocess_argv() -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "--refiner-pick-folder-worker"]
    return [sys.executable, "-m", "app.refiner_pick_sync"]


def _companion_pick_url() -> str:
    return (os.environ.get("FETCHER_REFINER_COMPANION_URL") or REFINER_COMPANION_URL_DEFAULT).strip()


def companion_health_url() -> str:
    override = (os.environ.get("FETCHER_REFINER_COMPANION_HEALTH_URL") or "").strip()
    if override:
        return override
    p = urlparse(_companion_pick_url())
    return urlunparse((p.scheme, p.netloc, "/health", "", "", ""))


async def refiner_companion_reachable() -> bool | None:
    """
    None when the companion HTTP helper is not used (non-Windows companion mode).
    On Windows companion mode, True if GET /health returns {\"ok\": true} within a short timeout.
    """
    if get_refiner_pick_mode() != "windows_companion":
        return None
    url = companion_health_url()
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, timeout=2.0)
        if r.status_code != 200:
            return False
        data = r.json()
        return isinstance(data, dict) and data.get("ok") is True
    except Exception:
        return False


async def ensure_windows_companion_running(timeout_seconds: float = 4.0) -> bool:
    """
    Service-side primary startup path:
    - if companion is already healthy, do nothing
    - otherwise attempt launch and probe /health briefly
    """
    global _LAST_COMPANION_ENSURE_ATTEMPT_MONO
    mode = get_refiner_pick_mode()
    logger.info("Refiner companion ensure: mode=%s", mode)
    if mode != "windows_companion":
        logger.info("Refiner companion ensure: final_result=skip_not_windows_companion")
        return False
    is_healthy = await refiner_companion_reachable()
    logger.info("Refiner companion ensure: prelaunch_health=%s", is_healthy)
    if is_healthy:
        logger.info("Refiner companion ensure: final_result=already_running")
        return True
    launch = None
    now = time.monotonic()
    if now - _LAST_COMPANION_ENSURE_ATTEMPT_MONO >= 1.0:
        _LAST_COMPANION_ENSURE_ATTEMPT_MONO = now
        companion_exe = resolve_companion_exe_path()
        launch = start_companion_best_effort(companion_exe)
        logger.info(
            "Refiner companion ensure: launch attempted=%s launched=%s reason=%s session_id=%s exe=%s cwd=%s env_block=%s pid=%s token_source=%s",
            launch.attempted,
            launch.launched,
            launch.reason,
            launch.session_id,
            launch.companion_exe,
            launch.working_dir,
            launch.environment_block_created,
            launch.process_id,
            launch.token_source,
        )
    else:
        logger.info("Refiner companion ensure: throttled repeated launch attempt.")

    deadline = time.monotonic() + max(0.5, timeout_seconds)
    post_launch_healthy = False
    while time.monotonic() < deadline:
        if await refiner_companion_reachable():
            post_launch_healthy = True
            break
        await asyncio.sleep(0.35)
    if post_launch_healthy:
        logger.info("Refiner companion ensure: final_result=launch_succeeded_health_ok")
        return True

    # Classify internal outcomes for production diagnostics.
    if launch is None:
        logger.info("Refiner companion ensure: final_result=launch_skipped_throttled")
    elif launch.reason.startswith("no_active_session"):
        logger.info("Refiner companion ensure: final_result=no_active_session")
    elif launch.reason in ("wts_token_failed", "fallback_token_not_found", "fallback_token_open_failed", "launch_failed"):
        logger.info("Refiner companion ensure: final_result=launch_failed detail=%s", launch.reason)
    elif launch.launched:
        logger.info("Refiner companion ensure: final_result=launch_succeeded_but_companion_not_healthy")
    else:
        logger.info("Refiner companion ensure: final_result=launch_failed detail=%s", launch.reason)
    return False


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
    healthy = await refiner_companion_reachable()
    if healthy is False:
        await ensure_windows_companion_running(timeout_seconds=4.0)
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
    mode = get_refiner_pick_mode()
    if mode == "headless_unavailable":
        return {
            "ok": False,
            "reason": "unavailable",
            "message": HEADLESS_FOLDER_BROWSE_MESSAGE,
        }
    if mode == "windows_companion":
        return await _pick_folder_via_companion_http()
    return await _refiner_pick_folder_subprocess_linux()


if __name__ == "__main__":
    run_picker_subprocess()
