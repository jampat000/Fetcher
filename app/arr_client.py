from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from app.http_retry import httpx_request_with_retries
from app.httpx_shared import get_shared_httpx_client


@dataclass(frozen=True)
class ArrConfig:
    base_url: str
    api_key: str


class ArrClient:
    def __init__(
        self,
        cfg: ArrConfig,
        *,
        timeout_s: float = 30.0,
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
        headers = dict(opts.pop("headers", {}))
        headers.setdefault("X-Api-Key", self._cfg.api_key)
        return await httpx_request_with_retries(
            self._client, method, self._abs_url(path), headers=headers, **opts
        )

    async def aclose(self) -> None:
        # Shared ``httpx.AsyncClient`` is owned by FastAPI lifespan — never close it here.
        return None

    async def health(self) -> bool:
        # Both Sonarr and Radarr support /api/v3/system/status
        r = await self._req("GET", "/api/v3/system/status")
        r.raise_for_status()
        return True

    async def wanted_missing(self, *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        # Sonarr: /api/v3/wanted/missing
        # Radarr: /api/v3/wanted/missing
        r = await self._req("GET", "/api/v3/wanted/missing", params={"page": page, "pageSize": page_size})
        r.raise_for_status()
        return r.json()

    async def wanted_cutoff_unmet(self, *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        # Sonarr: /api/v3/wanted/cutoff
        # Radarr: /api/v3/wanted/cutoff
        r = await self._req("GET", "/api/v3/wanted/cutoff", params={"page": page, "pageSize": page_size})
        r.raise_for_status()
        return r.json()

    async def command(self, name: str, **kwargs: Any) -> dict[str, Any]:
        # POST /api/v3/command with { name: ... }
        payload = {"name": name}
        payload.update(kwargs)
        r = await self._req("POST", "/api/v3/command", json=payload)
        r.raise_for_status()
        return r.json()

    async def tags(self) -> list[dict[str, Any]]:
        r = await self._req("GET", "/api/v3/tag")
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

    async def movies(self) -> list[dict[str, Any]]:
        """Radarr movie catalog."""
        r = await self._req("GET", "/api/v3/movie")
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

    async def series(self) -> list[dict]:
        """Sonarr series catalog."""
        r = await self._req("GET", "/api/v3/series")
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

    async def update_series(self, series: dict[str, Any]) -> None:
        """Sonarr: update one series (used for surgical monitored-state adjustments)."""
        r = await self._req("PUT", "/api/v3/series", json=series)
        r.raise_for_status()

    async def episodes_for_series(self, *, series_id: int) -> list[dict[str, Any]]:
        r = await self._req("GET", "/api/v3/episode", params={"seriesId": int(series_id)})
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

    async def ensure_tag(self, label: str) -> int:
        wanted = (label or "").strip()
        if not wanted:
            raise ValueError("Tag label is required.")
        existing = await self.tags()
        for t in existing:
            if str(t.get("label", "")).strip().lower() == wanted.lower():
                tag_id = t.get("id")
                if isinstance(tag_id, int) and tag_id > 0:
                    return tag_id
        r = await self._req("POST", "/api/v3/tag", json={"label": wanted})
        r.raise_for_status()
        payload = r.json()
        created = payload if isinstance(payload, dict) else {}
        created_id = created.get("id")
        if isinstance(created_id, int) and created_id > 0:
            return created_id
        # Fallback lookup if API response shape differs.
        refreshed = await self.tags()
        for t in refreshed:
            if str(t.get("label", "")).strip().lower() == wanted.lower():
                tag_id = t.get("id")
                if isinstance(tag_id, int) and tag_id > 0:
                    return tag_id
        raise RuntimeError(f"Unable to resolve tag id for '{wanted}'.")

    async def add_tags_to_series(self, *, series_ids: list[int], tag_ids: list[int]) -> None:
        """Sonarr: tags are on series, not episodes (`PUT /api/v3/series/editor`)."""
        if not series_ids or not tag_ids:
            return
        payload = {
            "seriesIds": series_ids,
            "tags": tag_ids,
            "applyTags": "add",
        }
        r = await self._req("PUT", "/api/v3/series/editor", json=payload)
        r.raise_for_status()

    async def add_tags_to_movies(self, *, movie_ids: list[int], tag_ids: list[int]) -> None:
        if not movie_ids or not tag_ids:
            return
        payload = {
            "movieIds": movie_ids,
            "tags": tag_ids,
            "applyTags": "add",
        }
        r = await self._req("PUT", "/api/v3/movie/editor", json=payload)
        r.raise_for_status()

    async def unmonitor_movies(self, *, movie_ids: list[int]) -> None:
        if not movie_ids:
            return
        payload = {"movieIds": movie_ids, "monitored": False}
        r = await self._req("PUT", "/api/v3/movie/editor", json=payload)
        r.raise_for_status()

    async def unmonitor_series(self, *, series_ids: list[int]) -> None:
        if not series_ids:
            return
        payload = {"seriesIds": series_ids, "monitored": False}
        r = await self._req("PUT", "/api/v3/series/editor", json=payload)
        r.raise_for_status()

    async def set_episodes_monitored(self, *, episode_ids: list[int], monitored: bool) -> None:
        if not episode_ids:
            return
        payload = {"episodeIds": episode_ids, "monitored": monitored}
        r = await self._req("PUT", "/api/v3/episode/monitor", json=payload)
        r.raise_for_status()

    async def unmonitor_episodes(self, *, episode_ids: list[int]) -> None:
        await self.set_episodes_monitored(episode_ids=episode_ids, monitored=False)

    async def delete_episode_file(self, *, episode_file_id: int) -> None:
        r = await self._req("DELETE", f"/api/v3/episodeFile/{int(episode_file_id)}")
        r.raise_for_status()

    async def history_page(self, *, page: int = 1, page_size: int = 250) -> dict[str, Any]:
        """Sonarr/Radarr paginated history (``records``, ``totalRecords``)."""
        r = await self._req(
            "GET",
            "/api/v3/history",
            params={"page": int(page), "pageSize": int(page_size)},
        )
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else {}

    async def queue_page(self, *, page: int = 1, page_size: int = 200) -> dict[str, Any]:
        """Sonarr/Radarr download queue page (``records``, ``totalRecords``)."""
        r = await self._req(
            "GET",
            "/api/v3/queue",
            params={"page": int(page), "pageSize": int(page_size)},
        )
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else {}

    async def delete_queue_item(
        self,
        *,
        queue_id: int,
        remove_from_client: bool = False,
        blocklist: bool = False,
    ) -> None:
        """Remove one queue row by id (Radarr/Sonarr *arr queue API)."""
        r = await self._req(
            "DELETE",
            f"/api/v3/queue/{int(queue_id)}",
            params={
                "removeFromClient": "true" if remove_from_client else "false",
                "blocklist": "true" if blocklist else "false",
                "changeCategory": "false",
                "skipRedownload": "false",
            },
        )
        r.raise_for_status()


async def trigger_sonarr_missing_search(client: ArrClient, *, series_id: int | None = None, episode_ids: list[int] | None = None) -> None:
    if episode_ids:
        await client.command("EpisodeSearch", episodeIds=episode_ids)
        return
    if series_id is not None:
        await client.command("SeriesSearch", seriesId=series_id)
        return
    # global missing search
    await client.command("MissingEpisodeSearch")


async def trigger_radarr_missing_search(client: ArrClient, *, movie_ids: list[int] | None = None) -> None:
    if movie_ids:
        await client.command("MoviesSearch", movieIds=movie_ids)
        return
    await client.command("MissingMoviesSearch")


async def trigger_sonarr_cutoff_search(client: ArrClient, *, episode_ids: list[int] | None = None) -> None:
    if episode_ids:
        await client.command("EpisodeSearch", episodeIds=episode_ids)
        return
    await client.command("CutOffUnmetSearch")


async def trigger_radarr_cutoff_search(client: ArrClient, *, movie_ids: list[int] | None = None) -> None:
    if movie_ids:
        await client.command("MoviesSearch", movieIds=movie_ids)
        return
    await client.command("CutOffUnmetSearch")

