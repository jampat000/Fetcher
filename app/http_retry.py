"""Transient-safe httpx requests with backoff (no extra dependencies)."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

# Retry when the remote or infra may recover briefly.
_RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
_BACKOFF_S = (0.5, 1.5, 3.0)


async def httpx_request_with_retries(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_attempts: int = 4,
    **kwargs: Any,
) -> httpx.Response:
    """``client.request`` with retries on connection/timeout errors and 429/502/503/504."""
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        if attempt > 0:
            await asyncio.sleep(_BACKOFF_S[min(attempt - 1, len(_BACKOFF_S) - 1)])
        try:
            resp = await client.request(method, url, **kwargs)
            if resp.status_code in _RETRYABLE_STATUS and attempt < max_attempts - 1:
                continue
            return resp
        except (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
        ) as e:
            last_exc = e
            if attempt >= max_attempts - 1:
                raise
    if last_exc:
        raise last_exc
    raise RuntimeError("httpx_request_with_retries: exhausted without result")
