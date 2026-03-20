"""Short user-facing hints for common HTTP errors from Arr / Emby APIs."""

from __future__ import annotations

import httpx


def hint_for_http_status(status: int) -> str:
    """One-line hint to append to logs or errors (no secrets)."""
    hints: dict[int, str] = {
        400: "Bad request — check URL and API version (Sonarr/Radarr v3).",
        401: "Unauthorized — wrong or missing API key in Settings.",
        403: "Forbidden — API key may lack permission for this action.",
        404: "Not found — wrong base URL or API path (confirm Sonarr/Radarr v3 and port).",
        408: "Server reported timeout — try again; library or disk may be busy.",
        429: "Rate limited — Sonarr/Radarr/Emby asked to slow down.",
        500: "Server error on the Arr/Emby side — check their logs.",
        502: "Bad gateway — reverse proxy or upstream may be down.",
        503: "Service unavailable — Arr/Emby may be starting or overloaded.",
        504: "Gateway timeout — proxy or Arr did not respond in time.",
    }
    return hints.get(status, "")


def format_http_error_detail(exc: BaseException) -> str:
    """Format an exception for logs (e.g. Arr tag apply); include HTTP status + body when available."""
    if isinstance(exc, httpx.HTTPStatusError):
        r = exc.response
        code = r.status_code
        hint = hint_for_http_status(code)
        text = (r.text or "").replace("\r", " ").replace("\n", " ").strip()
        if len(text) > 200:
            text = text[:197] + "..."
        parts = [f"HTTP {code}"]
        if hint:
            parts.append(f"({hint})")
        if text:
            parts.append(f"— {text}")
        return " ".join(parts)
    msg = str(exc).strip()
    if msg:
        return f"{type(exc).__name__}: {msg}"
    return type(exc).__name__
