from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from app.http_retry import httpx_request_with_retries
from app.httpx_shared import get_shared_httpx_client

# Emby Items API: fetch in pages so large libraries can exceed a single Limit cap.
_DEFAULT_ITEMS_PAGE_SIZE = 2000


@dataclass(frozen=True)
class EmbyConfig:
    base_url: str
    api_key: str


class EmbyClient:
    def __init__(
        self,
        cfg: EmbyConfig,
        *,
        timeout_s: float = 300.0,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._cfg = cfg
        self._client = http_client if http_client is not None else get_shared_httpx_client()
        self._timeout_s = timeout_s

    def _abs_url(self, path: str) -> str:
        base = self._cfg.base_url.rstrip("/")
        p = path if path.startswith("/") else f"/{path}"
        return f"{base}{p}"

    async def _req(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        opts: dict[str, Any] = dict(kwargs)
        opts.setdefault("timeout", self._timeout_s)
        # Emby installs vary in how they validate API credentials; send
        # both common token headers and api_key query param for compatibility.
        headers = dict(opts.pop("headers", {}))
        headers.setdefault("X-Emby-Token", self._cfg.api_key)
        headers.setdefault("X-MediaBrowser-Token", self._cfg.api_key)
        raw_params = opts.pop("params", None)
        if isinstance(raw_params, dict):
            params: dict[str, Any] = {**raw_params, "api_key": self._cfg.api_key}
        elif raw_params is None:
            params = {"api_key": self._cfg.api_key}
        else:
            params = raw_params
        return await httpx_request_with_retries(
            self._client, method, self._abs_url(path), headers=headers, params=params, **opts
        )

    async def aclose(self) -> None:
        # Shared ``httpx.AsyncClient`` is owned by FastAPI lifespan — never close it here.
        return None

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

    async def _items_page_for_user(self, user_id: str, start: int, take: int) -> list[dict]:
        """Single /Users/{id}/Items page — same params and parsing as ``items_for_user``."""
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
        return items if isinstance(items, list) else []

    async def items_for_user(self, *, user_id: str, limit: int) -> list[dict]:
        """Return Movies and Series for the user, newest DateCreated first.

        * ``limit`` > 0: return at most that many items (paged API calls).
        * ``limit`` <= 0: **entire library** — keep paging until Emby returns no more items.

        When another page may be needed, the next page request is started before ``out``
        is extended so at most two page fetches are in flight (current + one prefetch).
        """
        unlimited = int(limit) <= 0
        max_items = max(1, int(limit)) if not unlimited else None
        chunk = _DEFAULT_ITEMS_PAGE_SIZE
        out: list[dict] = []
        start = 0
        pending: asyncio.Task | None = None
        try:
            while True:
                if not unlimited and max_items is not None and len(out) >= max_items:
                    break
                take = chunk if unlimited else min(chunk, max_items - len(out))
                if pending is None:
                    pending = asyncio.create_task(self._items_page_for_user(user_id, start, take))
                batch = await pending
                pending = None

                if not batch:
                    break

                new_len = len(out) + len(batch)
                take_next = chunk if unlimited else min(chunk, max_items - new_len)
                should_prefetch = len(batch) == take and (unlimited or new_len < max_items)
                if should_prefetch:
                    pending = asyncio.create_task(
                        self._items_page_for_user(user_id, start + len(batch), take_next)
                    )

                out.extend(batch)

                if len(batch) < take:
                    break
                if not unlimited and max_items is not None and len(out) >= max_items:
                    break
                start += len(batch)
            return out
        finally:
            if pending is not None and not pending.done():
                pending.cancel()
                try:
                    await pending
                except asyncio.CancelledError:
                    pass

    async def delete_item(self, item_id: str) -> None:
        # Emby delete endpoint.
        r = await self._req("DELETE", f"/Items/{item_id}")
        r.raise_for_status()
