"""Connection helpers for the setup wizard (no DB; avoids circular imports with main)."""

from __future__ import annotations

import httpx

from app.connection_test_service import ConnectionTestService
from app.emby_client import EmbyClient, EmbyConfig
from app.form_helpers import _looks_like_url, _normalize_base_url


def normalize_setup_url(raw: str) -> str:
    """Same rules as ``_normalize_base_url`` (wizard + settings POST)."""
    return _normalize_base_url(raw)


def looks_like_url(raw: str) -> bool:
    return _looks_like_url(raw)


async def test_sonarr_connection(url: str, api_key: str) -> tuple[bool, str]:
    # Keep setup helper messaging stable; API routes pass these strings through directly.
    u = normalize_setup_url(url)
    k = (api_key or "").strip()
    if not u:
        return (
            False,
            "Sonarr URL is required — paste the same base address you use in the browser "
            "(for example http://localhost:8989 or your reverse-proxy URL).",
        )
    if not k:
        return (
            False,
            "Sonarr API key is required — copy it from Sonarr → Settings → General so Fetcher can talk to the API.",
        )
    result = await ConnectionTestService().check_arr_health(url=u, api_key=k)
    if result.ok:
        return True, "Sonarr responded OK."
    if result.error_kind == "http_status":
        return (
            False,
            ConnectionTestService.message_with_http_status_hint(
                result,
                auth_hint="Verify the API key in Sonarr → Settings → General matches what you pasted here.",
            ),
        )
    return False, (
        ConnectionTestService.message_with_exception_prefix(result)
        + " — Check the URL, HTTPS vs HTTP, firewall, and that this machine can reach Sonarr."
    )


async def test_radarr_connection(url: str, api_key: str) -> tuple[bool, str]:
    # Keep parity with existing setup UX; update regression tests before changing wording/branches.
    u = normalize_setup_url(url)
    k = (api_key or "").strip()
    if not u:
        return (
            False,
            "Radarr URL is required — paste the same base address you use in the browser "
            "(for example http://localhost:7878 or your reverse-proxy URL).",
        )
    if not k:
        return (
            False,
            "Radarr API key is required — copy it from Radarr → Settings → General so Fetcher can talk to the API.",
        )
    result = await ConnectionTestService().check_arr_health(url=u, api_key=k)
    if result.ok:
        return True, "Radarr responded OK."
    if result.error_kind == "http_status":
        return (
            False,
            ConnectionTestService.message_with_http_status_hint(
                result,
                auth_hint="Verify the API key in Radarr → Settings → General matches what you pasted here.",
            ),
        )
    return False, (
        ConnectionTestService.message_with_exception_prefix(result)
        + " — Check the URL, HTTPS vs HTTP, firewall, and that this machine can reach Radarr."
    )


async def test_emby_connection(url: str, api_key: str, user_id: str) -> tuple[bool, str]:
    u = normalize_setup_url(url)
    k = (api_key or "").strip()
    uid = (user_id or "").strip()
    if not u:
        return (
            False,
            "Emby server URL is required — use the same base URL as in your browser "
            "(for example http://localhost:8096 or your reverse-proxy URL).",
        )
    if not k:
        return (
            False,
            "Emby API key is required — create or copy a key from Emby → Dashboard → Advanced → API Keys.",
        )
    if looks_like_url(k):
        return (
            False,
            "That value looks like a URL, not an API key. In Emby go to Dashboard → Advanced → API Keys and paste the key string.",
        )
    try:
        c = EmbyClient(EmbyConfig(u, k))
        try:
            await c.health()
            if uid:
                users = await c.users()
                if not any(str(x.get("Id", "")) == uid for x in users):
                    return (
                        False,
                        "That Emby user ID was not found on this server. Leave the field blank unless your policy requires a specific user.",
                    )
        finally:
            await c.aclose()
        return True, "Emby responded OK."
    except httpx.HTTPStatusError as e:
        msg = f"Emby returned HTTP {e.response.status_code} — connection reached the server but the request was rejected."
        if e.response.status_code in (401, 403):
            msg += " Verify the API key and that the URL matches how you sign in to Emby."
        else:
            msg += " Confirm the URL and API key in Emby → Dashboard → Advanced → API Keys."
        return False, msg
    except (httpx.HTTPError, ValueError) as e:
        return (
            False,
            f"{type(e).__name__}: {e} — Check the URL, HTTPS vs HTTP, and that this machine can reach Emby.",
        )
