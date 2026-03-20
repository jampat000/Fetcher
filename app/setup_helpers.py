"""Connection helpers for the setup wizard (no DB; avoids circular imports with main)."""

from __future__ import annotations

from urllib.parse import urlparse

import httpx

from app.arr_client import ArrClient, ArrConfig
from app.emby_client import EmbyClient, EmbyConfig


def normalize_setup_url(raw: str) -> str:
    """Same rules as main._normalize_base_url."""
    raw = (raw or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "http://" + raw
    p = urlparse(raw)
    if not p.scheme or not p.netloc:
        return raw
    if p.scheme == "https" and (p.port in (8989, 7878)) and (p.path in ("", "/")):
        return f"http://{p.netloc}".rstrip("/")
    base = f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")
    return base


def looks_like_url(raw: str) -> bool:
    v = (raw or "").strip().lower()
    return v.startswith("http://") or v.startswith("https://")


async def test_sonarr_connection(url: str, api_key: str) -> tuple[bool, str]:
    u = normalize_setup_url(url)
    k = (api_key or "").strip()
    if not u:
        return False, "Enter a Sonarr base URL (for example http://localhost:8989)."
    if not k:
        return False, "Enter your Sonarr API key."
    try:
        c = ArrClient(ArrConfig(u, k))
        try:
            await c.health()
        finally:
            await c.aclose()
        return True, "Sonarr responded OK."
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code}"
        if e.response.status_code in (401, 403):
            msg += " — check the API key in Sonarr (Settings → General)."
        return False, msg
    except httpx.HTTPError as e:
        return False, f"{type(e).__name__}: {e}"


async def test_radarr_connection(url: str, api_key: str) -> tuple[bool, str]:
    u = normalize_setup_url(url)
    k = (api_key or "").strip()
    if not u:
        return False, "Enter a Radarr base URL (for example http://localhost:7878)."
    if not k:
        return False, "Enter your Radarr API key."
    try:
        c = ArrClient(ArrConfig(u, k))
        try:
            await c.health()
        finally:
            await c.aclose()
        return True, "Radarr responded OK."
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code}"
        if e.response.status_code in (401, 403):
            msg += " — check the API key in Radarr (Settings → General)."
        return False, msg
    except httpx.HTTPError as e:
        return False, f"{type(e).__name__}: {e}"


async def test_emby_connection(url: str, api_key: str, user_id: str) -> tuple[bool, str]:
    u = normalize_setup_url(url)
    k = (api_key or "").strip()
    uid = (user_id or "").strip()
    if not u:
        return False, "Enter an Emby server URL (for example http://localhost:8096)."
    if not k:
        return False, "Enter your Emby API key."
    if looks_like_url(k):
        return False, "That value looks like a URL. Paste the API key from Emby → Dashboard → Advanced → API Keys."
    try:
        c = EmbyClient(EmbyConfig(u, k))
        try:
            await c.health()
            if uid:
                users = await c.users()
                if not any(str(x.get("Id", "")) == uid for x in users):
                    return False, "Emby User ID not found. Leave it blank unless you use per-user libraries."
        finally:
            await c.aclose()
        return True, "Emby responded OK."
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code}: {e}"
        if e.response.status_code in (401, 403):
            msg += " | Check the API key and URL."
        return False, msg
    except (httpx.HTTPError, ValueError) as e:
        return False, f"{type(e).__name__}: {e}"
