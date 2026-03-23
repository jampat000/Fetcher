"""Process-wide shared ``httpx.AsyncClient`` for connection reuse (started from FastAPI lifespan)."""

from __future__ import annotations

import httpx

# Default only applies when a call omits ``timeout=``; Arr/GitHub callers set explicit timeouts.
_DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=30.0)

_client: httpx.AsyncClient | None = None


async def init_shared_httpx_client() -> None:
    """Create the shared async client (idempotent). Call from app lifespan startup."""
    global _client
    if _client is not None:
        return
    _client = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, follow_redirects=True)


async def aclose_shared_httpx_client() -> None:
    """Close the shared client (idempotent). Call from app lifespan shutdown."""
    global _client
    if _client is None:
        return
    await _client.aclose()
    _client = None


def get_shared_httpx_client() -> httpx.AsyncClient:
    """Return the shared client; raises if lifespan has not initialized it."""
    if _client is None:
        raise RuntimeError(
            "Shared httpx.AsyncClient is not initialized (FastAPI lifespan did not run)."
        )
    return _client
