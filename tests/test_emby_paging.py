"""Emby Items API pagination (large scan limits)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.emby_client import EmbyClient, EmbyConfig


def test_items_for_user_stops_when_api_returns_short_page() -> None:
    async def _run() -> None:
        client = EmbyClient(EmbyConfig("http://localhost:8096", "key"))

        async def fake_request(_method: str, _url: str, **kwargs: object) -> MagicMock:
            p = kwargs.get("params") or {}
            assert isinstance(p, dict)
            idx = int(p.get("StartIndex", 0))
            lim = int(p.get("Limit", 0))
            if idx == 0:
                batch = [{"Id": str(i)} for i in range(lim)]
            else:
                batch = []
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"Items": batch}
            resp.raise_for_status = MagicMock()
            return resp

        client._client.request = AsyncMock(side_effect=fake_request)
        try:
            out = await client.items_for_user(user_id="u1", limit=50_000)
        finally:
            await client.aclose()

        assert len(out) == 2000  # one full page then empty

    asyncio.run(_run())


def test_items_for_user_respects_total_cap_across_pages() -> None:
    async def _run() -> None:
        client = EmbyClient(EmbyConfig("http://localhost:8096", "key"))

        async def fake_request(_method: str, _url: str, **kwargs: object) -> MagicMock:
            p = kwargs.get("params") or {}
            assert isinstance(p, dict)
            idx = int(p.get("StartIndex", 0))
            lim = int(p.get("Limit", 0))
            batch = [{"Id": f"{idx}-{i}"} for i in range(lim)]
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"Items": batch}
            resp.raise_for_status = MagicMock()
            return resp

        client._client.request = AsyncMock(side_effect=fake_request)
        try:
            out = await client.items_for_user(user_id="u1", limit=4500)
        finally:
            await client.aclose()

        assert len(out) == 4500
        assert client._client.request.call_count == 3  # 2000 + 2000 + 500

    asyncio.run(_run())


def test_items_for_user_limit_zero_scans_until_exhausted() -> None:
    """limit <= 0 means fetch every page until Emby returns a short/empty batch."""

    async def _run() -> None:
        client = EmbyClient(EmbyConfig("http://localhost:8096", "key"))

        async def fake_request(_method: str, _url: str, **kwargs: object) -> MagicMock:
            p = kwargs.get("params") or {}
            assert isinstance(p, dict)
            idx = int(p.get("StartIndex", 0))
            lim = int(p.get("Limit", 0))
            if idx == 0:
                batch = [{"Id": str(i)} for i in range(lim)]
            elif idx == 2000:
                batch = [{"Id": str(2000 + i)} for i in range(500)]
            else:
                batch = []
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"Items": batch}
            resp.raise_for_status = MagicMock()
            return resp

        client._client.request = AsyncMock(side_effect=fake_request)
        try:
            out = await client.items_for_user(user_id="u1", limit=0)
        finally:
            await client.aclose()

        assert len(out) == 2500
        assert client._client.request.call_count == 2

    asyncio.run(_run())
