"""GitHub Releases check + Windows silent in-place upgrade (Inno Setup)."""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx
from fastapi import APIRouter, Depends, Query

from app.auth import require_auth
from packaging.version import InvalidVersion, Version
from starlette.responses import JSONResponse

from app.version_info import get_app_version
from app.httpx_shared import get_shared_httpx_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/updates", tags=["updates"], dependencies=[Depends(require_auth)])

DEFAULT_RELEASES_REPO = "jampat000/Fetcher"
SETUP_ASSET_NAME = "FetcherSetup.exe"

GITHUB_API_VERSION = "2022-11-28"


def _github_headers(*, accept: str | None = None, include_token: bool = True) -> dict[str, str]:
    """Headers for GitHub API and (some) release-asset downloads.

    GitHub rejects many requests without a descriptive User-Agent (403). Optional
    ``FETCHER_GITHUB_TOKEN`` or ``GITHUB_TOKEN`` raises rate limits and allows
    private-repo release checks.

    A **bad or expired** token in the environment yields **401** on the API and can
    break **anonymous** ``/releases/download/...`` fetches if ``Authorization`` is sent,
    so callers can set ``include_token=False``.
    """
    repo = _releases_repo()
    contact = f"https://github.com/{repo}"
    ver = get_app_version()
    h: dict[str, str] = {
        "User-Agent": f"Fetcher/{ver} (+{contact})",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "Accept": accept or "application/vnd.github+json",
    }
    if include_token:
        token = (os.environ.get("FETCHER_GITHUB_TOKEN") or os.environ.get("GITHUB_TOKEN") or "").strip()
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

# Short cache so Settings / repeated checks do not burn GitHub REST API quota (60/hr per IP unauthenticated).
_RELEASE_CACHE_TTL_SEC = max(60, int(os.environ.get("FETCHER_UPDATES_CACHE_SECONDS", "900")))
_release_payload_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_release_cache_lock = threading.Lock()

_NO_STORE_HEADERS = {"Cache-Control": "no-store, max-age=0, must-revalidate", "Pragma": "no-cache"}

_ATOM_NS = "{http://www.w3.org/2005/Atom}"


def _no_store_json(body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(content=body, headers=_NO_STORE_HEADERS)


def _releases_repo() -> str:
    return (os.environ.get("FETCHER_UPDATES_REPO") or DEFAULT_RELEASES_REPO).strip().strip("/")


def _allow_apply_in_dev() -> bool:
    return os.environ.get("FETCHER_ALLOW_DEV_UPGRADE", "").strip().lower() in ("1", "true", "yes")


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


def _web_headers() -> dict[str, str]:
    """Headers for github.com pages / Atom (not api.github.com); avoids REST API rate limits."""
    repo = _releases_repo()
    ver = get_app_version()
    return {"User-Agent": f"Fetcher/{ver} (+https://github.com/{repo})"}


def _tag_from_releases_url(url: str) -> str | None:
    m = re.search(r"/releases/tag/([^/?#]+)", url)
    if not m:
        return None
    return unquote(m.group(1))


async def _payload_from_tag_and_repo(repo: str, tag: str) -> dict[str, Any]:
    """Build API-shaped payload using GitHub's conventional release asset URL."""
    html_url = f"https://github.com/{repo}/releases/tag/{tag}"
    dl = f"https://github.com/{repo}/releases/download/{tag}/{SETUP_ASSET_NAME}"
    return {
        "tag_name": tag,
        "html_url": html_url,
        "assets": [{"name": SETUP_ASSET_NAME, "browser_download_url": dl}],
    }


async def _fetch_latest_via_github_web(repo: str) -> dict[str, Any] | None:
    """Follow https://github.com/{repo}/releases/latest to discover tag (no REST API)."""
    timeout = httpx.Timeout(30.0, connect=10.0)
    client = get_shared_httpx_client()
    r = await client.get(
        f"https://github.com/{repo}/releases/latest",
        headers=_web_headers(),
        timeout=timeout,
    )
    if r.status_code >= 400:
        return None
    tag = _tag_from_releases_url(str(r.url))
    if not tag:
        return None
    return await _payload_from_tag_and_repo(repo, tag)


async def _fetch_latest_via_releases_atom(repo: str) -> dict[str, Any] | None:
    """Parse releases.atom first entry (fallback if /releases/latest did not expose a tag URL)."""
    timeout = httpx.Timeout(30.0, connect=10.0)
    client = get_shared_httpx_client()
    r = await client.get(
        f"https://github.com/{repo}/releases.atom",
        headers={**_web_headers(), "Accept": "application/atom+xml"},
        timeout=timeout,
    )
    if r.status_code >= 400:
        return None
    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return None
    entry = root.find(f"{_ATOM_NS}entry")
    if entry is None:
        return None
    tag: str | None = None
    for link in entry.findall(f"{_ATOM_NS}link"):
        href = link.get("href") or ""
        tag = _tag_from_releases_url(href)
        if tag:
            break
    if not tag:
        return None
    return await _payload_from_tag_and_repo(repo, tag)


async def _fetch_latest_without_api(repo: str) -> dict[str, Any] | None:
    """When REST API returns 403/429 (rate limit), use web/Atom instead."""
    w = await _fetch_latest_via_github_web(repo)
    if w:
        return w
    return await _fetch_latest_via_releases_atom(repo)


async def _fetch_latest_release_payload(repo: str, *, include_token: bool = True) -> dict[str, Any]:
    url = _latest_api_url(repo)
    timeout = httpx.Timeout(30.0, connect=10.0)
    client = get_shared_httpx_client()
    r = await client.get(url, headers=_github_headers(include_token=include_token), timeout=timeout)
    r.raise_for_status()
    return r.json()


async def _resolve_latest_release_payload(repo: str) -> dict[str, Any]:
    """GitHub REST API first; 401 (bad PAT) → retry without token; 403/429 → web/Atom fallback."""
    now = time.monotonic()
    with _release_cache_lock:
        hit = _release_payload_cache.get(repo)
        if hit and hit[0] > now:
            return hit[1].copy()

    try:
        payload = await _fetch_latest_release_payload(repo, include_token=True)
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        # Bad GITHUB_TOKEN / FETCHER_GITHUB_TOKEN in env: retry API anonymously for public repos.
        if code == 401:
            try:
                payload = await _fetch_latest_release_payload(repo, include_token=False)
            except httpx.HTTPStatusError:
                fb = await _fetch_latest_without_api(repo)
                if fb is None:
                    raise
                payload = fb
        elif code in (403, 429):
            fb = await _fetch_latest_without_api(repo)
            if fb is not None:
                payload = fb
            else:
                raise
        else:
            raise

    with _release_cache_lock:
        _release_payload_cache[repo] = (now + _RELEASE_CACHE_TTL_SEC, payload.copy())
    return payload.copy()


def _installer_download_headers(url: str) -> dict[str, str]:
    """Headers for downloading FetcherSetup.exe.

    ``https://github.com/.../releases/download/...`` must not send a bad ``Authorization``
    header (common when ``GITHUB_TOKEN`` is set globally for other tools).
    API-hosted release assets still use API headers (optional token).
    """
    base = url.split("?", 1)[0].lower()
    if "api.github.com" in base:
        return _github_headers(accept="application/octet-stream", include_token=True)
    return {**_web_headers(), "Accept": "application/octet-stream"}


def _pick_setup_asset(payload: dict[str, Any]) -> dict[str, Any] | None:
    want = SETUP_ASSET_NAME.casefold()
    for a in payload.get("assets") or []:
        name = (a.get("name") or "").strip()
        if name.casefold() == want:
            return a
    return None


def _synthetic_setup_asset(repo: str, tag: str) -> dict[str, Any] | None:
    """When the REST payload has no matching asset (transient API glitch, renamed file), use GitHub's conventional download URL."""
    t = (tag or "").strip()
    if not t:
        return None
    dl = f"https://github.com/{repo}/releases/download/{t}/{SETUP_ASSET_NAME}"
    return {"name": SETUP_ASSET_NAME, "browser_download_url": dl}


def _resolve_setup_asset(payload: dict[str, Any], repo: str, tag: str) -> dict[str, Any] | None:
    return _pick_setup_asset(payload) or _synthetic_setup_asset(repo, tag)


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
        "/CLOSEAPPLICATIONS",
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
    client = get_shared_httpx_client()
    async with client.stream("GET", url, headers=_installer_download_headers(url), timeout=timeout) as resp:
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
        payload = await _resolve_latest_release_payload(repo)
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        detail = _github_error_message(e.response)
        logger.warning("GitHub release check HTTP %s for %s: %s", code, repo, detail or e)
        suffix = f" ({detail})" if detail else ""
        if code == 404:
            err = (
                "No GitHub releases found for this repository (404). "
                "If you use a fork, set FETCHER_UPDATES_REPO to owner/repo."
            )
        elif code == 403:
            err = (
                "GitHub denied access (403). This is often rate limiting or a missing/invalid token. "
                "Wait a few minutes and retry. If it persists, set FETCHER_GITHUB_TOKEN to a read-only "
                f"personal access token (see GitHub docs).{suffix}"
            )
        elif code == 401:
            err = (
                "GitHub returned 401 (unauthorized). If GITHUB_TOKEN or FETCHER_GITHUB_TOKEN is set on "
                "this machine, it may be expired or wrong — remove it or fix it, then retry."
                f"{suffix}"
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
        logger.warning("GitHub release check failed for %s: %s", repo, e)
        return {
            **base,
            "ok": False,
            "check_error": str(e) or type(e).__name__,
            "latest_version": None,
            "update_available": False,
            "release_notes_url": f"https://github.com/{repo}/releases/latest",
        }
    except Exception as e:  # noqa: BLE001 - surface unexpected errors in UI + logs
        logger.exception("Unexpected error during GitHub release check for %s", repo)
        return {
            **base,
            "ok": False,
            "check_error": f"{type(e).__name__}: {e}"[:500],
            "latest_version": None,
            "update_available": False,
            "release_notes_url": f"https://github.com/{repo}/releases/latest",
        }

    tag = (payload.get("tag_name") or "").strip()
    latest_v = _tag_to_version(tag)
    asset = _resolve_setup_asset(payload, repo, tag)
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
async def api_updates_check(refresh: str | None = Query(None)) -> JSONResponse:
    """Optional ``?refresh=1`` skips the in-memory release cache (manual Check button)."""
    repo = _releases_repo()
    if (refresh or "").strip().lower() in ("1", "true", "yes"):
        with _release_cache_lock:
            _release_payload_cache.pop(repo, None)
    return _no_store_json(await _compute_updates_check_payload())


@router.post("/apply")
async def api_updates_apply() -> dict[str, Any]:
    if not _platform_ok():
        return {"ok": False, "error": "In-app upgrade is only supported on Windows."}
    if not _apply_eligible():
        return {
            "ok": False,
            "error": "Automatic upgrade runs only in the installed (frozen) Windows build. "
            "Download FetcherSetup.exe from GitHub, or set FETCHER_ALLOW_DEV_UPGRADE=1 for testing.",
        }

    if not _apply_lock.acquire(blocking=False):
        return {"ok": False, "error": "An upgrade is already in progress."}

    tmp_path: Path | None = None
    previous_version = get_app_version()
    try:
        check = await _compute_updates_check_payload()
        if not check.get("ok"):
            return {"ok": False, "error": check.get("check_error") or "Update check failed"}
        if not check.get("update_available"):
            return {"ok": False, "error": "No newer release available (or installer asset missing)."}
        url = check.get("download_url")
        if not url or not isinstance(url, str):
            return {"ok": False, "error": "Release has no FetcherSetup.exe asset."}

        target_version = check.get("latest_version")
        if target_version is not None:
            target_version = str(target_version).strip()

        fd, name = tempfile.mkstemp(suffix=".exe", prefix="FetcherUpgrade-")
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

    def _strip_version_prefix(raw: object) -> str:
        t = str(raw or "").strip()
        if len(t) > 1 and t[0].lower() == "v" and (t[1].isdigit() or t[1] == "."):
            return t[1:].strip()
        return t

    tv_norm = _strip_version_prefix(target_version) if target_version else ""
    return {
        "ok": True,
        "message": "Installer started. The Fetcher service will stop briefly during upgrade, then start again.",
        "previous_version": previous_version,
        "target_version": tv_norm or target_version,
    }
