"""GitHub Releases check + Windows silent in-place upgrade (Inno Setup)."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter
from packaging.version import InvalidVersion, Version
from starlette.responses import JSONResponse

from app.version_info import get_app_version

router = APIRouter(prefix="/api/updates", tags=["updates"])

DEFAULT_RELEASES_REPO = "jampat000/Grabby"
SETUP_ASSET_NAME = "GrabbySetup.exe"

GITHUB_API_VERSION = "2022-11-28"


def _github_headers(*, accept: str | None = None) -> dict[str, str]:
    """Headers for GitHub API and release-asset downloads.

    GitHub rejects many requests without a descriptive User-Agent (403). Optional
    ``GRABBY_GITHUB_TOKEN`` or ``GITHUB_TOKEN`` raises rate limits and allows
    private-repo release checks.
    """
    repo = _releases_repo()
    contact = f"https://github.com/{repo}"
    ver = get_app_version()
    h: dict[str, str] = {
        "User-Agent": f"Grabby/{ver} (+{contact})",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "Accept": accept or "application/vnd.github+json",
    }
    token = (os.environ.get("GRABBY_GITHUB_TOKEN") or os.environ.get("GITHUB_TOKEN") or "").strip()
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _github_error_message(response: httpx.Response) -> str:
    try:
        data = response.json()
        if isinstance(data, dict) and data.get("message"):
            return str(data["message"])
    except (ValueError, TypeError):
        pass
    return ""

_apply_lock = threading.Lock()

_NO_STORE_HEADERS = {"Cache-Control": "no-store, max-age=0, must-revalidate", "Pragma": "no-cache"}


def _no_store_json(body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(content=body, headers=_NO_STORE_HEADERS)


def _releases_repo() -> str:
    return (os.environ.get("GRABBY_UPDATES_REPO") or DEFAULT_RELEASES_REPO).strip().strip("/")


def _allow_apply_in_dev() -> bool:
    return os.environ.get("GRABBY_ALLOW_DEV_UPGRADE", "").strip().lower() in ("1", "true", "yes")


def _tag_to_version(tag: str) -> Version | None:
    t = (tag or "").strip()
    if t.startswith("v") or t.startswith("V"):
        t = t[1:]
    try:
        return Version(t)
    except InvalidVersion:
        return None


def _current_version_parsed() -> Version | None:
    return _tag_to_version(get_app_version())


def _platform_ok() -> bool:
    return sys.platform == "win32"


def _apply_eligible() -> bool:
    if not _platform_ok():
        return False
    if getattr(sys, "frozen", False):
        return True
    return _allow_apply_in_dev()


def _latest_api_url(repo: str) -> str:
    return f"https://api.github.com/repos/{repo}/releases/latest"


async def _fetch_latest_release_payload(repo: str) -> dict[str, Any]:
    url = _latest_api_url(repo)
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(
        headers=_github_headers(),
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.json()


def _pick_setup_asset(payload: dict[str, Any]) -> dict[str, Any] | None:
    for a in payload.get("assets") or []:
        if (a.get("name") or "") == SETUP_ASSET_NAME:
            return a
    return None


def _launch_installer_detached(exe_path: Path) -> None:
    """Start Inno installer in a new process tree so it survives service shutdown."""
    if sys.platform != "win32":
        raise OSError("Windows only")
    # Escape Windows job object (service) + detach from parent console.
    CREATE_BREAKAWAY_FROM_JOB = 0x01000000
    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    flags = CREATE_BREAKAWAY_FROM_JOB | DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
    cmd = [
        str(exe_path.resolve()),
        "/VERYSILENT",
        "/SUPPRESSMSGBOXES",
        "/NORESTART",
    ]
    subprocess.Popen(
        cmd,
        close_fds=True,
        creationflags=flags,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


async def _download_installer(url: str, dest: Path) -> None:
    timeout = httpx.Timeout(600.0, connect=30.0)
    async with httpx.AsyncClient(
        headers=_github_headers(accept="application/octet-stream"),
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            total = 0
            with open(dest, "wb") as f:
                async for chunk in resp.aiter_bytes(65536):
                    total += len(chunk)
                    f.write(chunk)
    if total < 512 * 1024:
        raise ValueError("Downloaded file is too small to be a valid installer")


async def _compute_updates_check_payload() -> dict[str, Any]:
    """JSON-serializable body for GET /check and for apply() logic."""
    repo = _releases_repo()
    current_raw = get_app_version()
    current_v = _current_version_parsed()
    base: dict[str, Any] = {
        "repo": repo,
        "current_version": current_raw,
        "platform_supported": _platform_ok(),
        "apply_supported": _apply_eligible(),
        "setup_asset_name": SETUP_ASSET_NAME,
    }
    if not _platform_ok():
        return {
            **base,
            "ok": True,
            "check_error": None,
            "latest_version": None,
            "update_available": False,
            "release_notes_url": f"https://github.com/{repo}/releases/latest",
            "message": "In-app upgrade is only available on Windows.",
        }
    try:
        payload = await _fetch_latest_release_payload(repo)
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        detail = _github_error_message(e.response)
        suffix = f" ({detail})" if detail else ""
        if code == 404:
            err = (
                "No GitHub releases found for this repository (404). "
                "If you use a fork, set GRABBY_UPDATES_REPO to owner/repo."
            )
        elif code == 403:
            err = (
                "GitHub denied access (403). This is often rate limiting or a missing/invalid token. "
                "Wait a few minutes and retry. If it persists, set GRABBY_GITHUB_TOKEN to a read-only "
                f"personal access token (see GitHub docs).{suffix}"
            )
        else:
            err = f"GitHub returned an error ({code}).{suffix}"
        return {
            **base,
            "ok": False,
            "check_error": err,
            "latest_version": None,
            "update_available": False,
            "release_notes_url": f"https://github.com/{repo}/releases/latest",
        }
    except (httpx.RequestError, ValueError, KeyError) as e:
        return {
            **base,
            "ok": False,
            "check_error": str(e) or type(e).__name__,
            "latest_version": None,
            "update_available": False,
            "release_notes_url": f"https://github.com/{repo}/releases/latest",
        }

    tag = (payload.get("tag_name") or "").strip()
    latest_v = _tag_to_version(tag)
    asset = _pick_setup_asset(payload)
    html_url = (payload.get("html_url") or f"https://github.com/{repo}/releases/latest").strip()

    update_available = False
    if current_v is not None and latest_v is not None:
        update_available = latest_v > current_v
    elif latest_v is not None and current_v is None:
        update_available = True

    return {
        **base,
        "ok": True,
        "check_error": None,
        "tag_name": tag,
        "latest_version": str(latest_v) if latest_v else tag,
        "latest_version_sortable": str(latest_v) if latest_v else None,
        "update_available": bool(update_available and asset),
        "download_url": (asset or {}).get("browser_download_url"),
        "release_notes_url": html_url,
        "asset_missing": asset is None,
    }


@router.get("/check")
async def api_updates_check() -> JSONResponse:
    return _no_store_json(await _compute_updates_check_payload())


@router.post("/apply")
async def api_updates_apply() -> dict[str, Any]:
    if not _platform_ok():
        return {"ok": False, "error": "In-app upgrade is only supported on Windows."}
    if not _apply_eligible():
        return {
            "ok": False,
            "error": "Automatic upgrade runs only in the installed (frozen) Windows build. "
            "Download GrabbySetup.exe from GitHub, or set GRABBY_ALLOW_DEV_UPGRADE=1 for testing.",
        }

    if not _apply_lock.acquire(blocking=False):
        return {"ok": False, "error": "An upgrade is already in progress."}

    tmp_path: Path | None = None
    try:
        check = await _compute_updates_check_payload()
        if not check.get("ok"):
            return {"ok": False, "error": check.get("check_error") or "Update check failed"}
        if not check.get("update_available"):
            return {"ok": False, "error": "No newer release available (or installer asset missing)."}
        url = check.get("download_url")
        if not url or not isinstance(url, str):
            return {"ok": False, "error": "Release has no GrabbySetup.exe asset."}

        fd, name = tempfile.mkstemp(suffix=".exe", prefix="GrabbyUpgrade-")
        os.close(fd)
        tmp_path = Path(name)

        await _download_installer(url, tmp_path)

        _launch_installer_detached(tmp_path)
    except Exception as e:
        if tmp_path and tmp_path.is_file():
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return {"ok": False, "error": str(e) or type(e).__name__}
    finally:
        _apply_lock.release()

    return {
        "ok": True,
        "message": "Installer started. The Grabby service will stop briefly during upgrade, then start again.",
    }
