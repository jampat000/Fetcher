from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from app.http_retry import httpx_request_with_retries

# Emby Items API: fetch in pages so large libraries can exceed a single Limit cap.
_DEFAULT_ITEMS_PAGE_SIZE = 2000


@dataclass(frozen=True)
class EmbyConfig:
    base_url: str
    api_key: str


class EmbyClient:
    def __init__(self, cfg: EmbyConfig, *, timeout_s: float = 300.0) -> None:
        self._client = httpx.AsyncClient(
            base_url=cfg.base_url.rstrip("/"),
            # Emby installs vary in how they validate API credentials; send
            # both common token headers and api_key query param for compatibility.
            headers={
                "X-Emby-Token": cfg.api_key,
                "X-MediaBrowser-Token": cfg.api_key,
            },
            params={"api_key": cfg.api_key},
            timeout=timeout_s,
        )

    async def _req(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        return await httpx_request_with_retries(self._client, method, path, **kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def health(self) -> bool:
        # Simple health/probe endpoint.
        r = await self._req("GET", "/System/Info")
        r.raise_for_status()
        return True

    async def users(self) -> list[dict]:
        r = await self._req("GET", "/Users")
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

    async def items_for_user(self, *, user_id: str, limit: int) -> list[dict]:
        """Return Movies and Series for the user, newest DateCreated first.

        * ``limit`` > 0: return at most that many items (paged API calls).
        * ``limit`` <= 0: **entire library** — keep paging until Emby returns no more items.
        """
        unlimited = int(limit) <= 0
        max_items = max(1, int(limit)) if not unlimited else None
        chunk = _DEFAULT_ITEMS_PAGE_SIZE
        out: list[dict] = []
        start = 0
        while True:
            if not unlimited and max_items is not None and len(out) >= max_items:
                break
            take = chunk if unlimited else min(chunk, max_items - len(out))
            params = {
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Series",
                "Fields": "UserData,DateCreated,PremiereDate,DateLastMediaAdded,Genres,People",
                "SortBy": "DateCreated",
                "SortOrder": "Descending",
                "StartIndex": str(start),
                "Limit": str(take),
            }
            r = await self._req("GET", f"/Users/{user_id}/Items", params=params)
            r.raise_for_status()
            payload = r.json()
            items = payload.get("Items") if isinstance(payload, dict) else None
            batch = items if isinstance(items, list) else []
            if not batch:
                break
            out.extend(batch)
            if len(batch) < take:
                break
            start += len(batch)
        return out

    async def delete_item(self, item_id: str) -> None:
        # Emby delete endpoint.
        r = await self._req("DELETE", f"/Items/{item_id}")
        r.raise_for_status()
