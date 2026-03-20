from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from app.http_retry import httpx_request_with_retries


@dataclass(frozen=True)
class ArrConfig:
    base_url: str
    api_key: str


class ArrClient:
    def __init__(self, cfg: ArrConfig, *, timeout_s: float = 30.0) -> None:
        self._cfg = cfg
        self._client = httpx.AsyncClient(
            base_url=cfg.base_url.rstrip("/"),
            headers={"X-Api-Key": cfg.api_key},
            timeout=timeout_s,
        )

    async def _req(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        return await httpx_request_with_retries(self._client, method, path, **kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def health(self) -> bool:
        # Both Sonarr and Radarr support /api/v3/system/status
        r = await self._req("GET", "/api/v3/system/status")
        r.raise_for_status()
        return True

    async def wanted_missing(self, *, page: int = 1, page_size: int = 50) -> dict:
        # Sonarr: /api/v3/wanted/missing
        # Radarr: /api/v3/wanted/missing
        r = await self._req("GET", "/api/v3/wanted/missing", params={"page": page, "pageSize": page_size})
        r.raise_for_status()
        return r.json()

    async def wanted_cutoff_unmet(self, *, page: int = 1, page_size: int = 50) -> dict:
        # Sonarr: /api/v3/wanted/cutoff
        # Radarr: /api/v3/wanted/cutoff
        r = await self._req("GET", "/api/v3/wanted/cutoff", params={"page": page, "pageSize": page_size})
        r.raise_for_status()
        return r.json()

    async def command(self, name: str, **kwargs) -> dict:
        # POST /api/v3/command with { name: ... }
        payload = {"name": name}
        payload.update(kwargs)
        r = await self._req("POST", "/api/v3/command", json=payload)
        r.raise_for_status()
        return r.json()

    async def tags(self) -> list[dict]:
        r = await self._req("GET", "/api/v3/tag")
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

