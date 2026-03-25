"""Freeze EmbyClient.items_for_user paging, merge, and stop semantics + caller parity.

Regression guard before performance work (prefetch/overlap). Production code should
change only when behavior is intentionally updated.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.emby_client import EmbyClient, EmbyConfig
from app.models import AppSettings
from app.service_logic import RunContext, _execute_emby_block
from app.time_util import utc_now_naive
from app.trimmer_service import TrimmerReviewService

_REQUIRED_ITEM_PARAMS = {
    "Recursive": "true",
    "IncludeItemTypes": "Movie,Series",
    "Fields": "UserData,DateCreated,PremiereDate,DateLastMediaAdded,Genres,People",
    "SortBy": "DateCreated",
    "SortOrder": "Descending",
}


def _flatten_qs(url: str) -> dict[str, str]:
    raw = parse_qs(urlparse(url).query)
    return {k: v[0] for k, v in raw.items() if v}


def _make_slice_items_handler(
    *,
    all_ids: list[str],
    page_log: list[dict[str, str]],
    user_id: str = "u1",
) -> httpx.MockTransport:
    """Emby-faithful Items: return slice all_ids[StartIndex : StartIndex+Limit]."""

    items_path_suffix = f"/Users/{user_id}/Items"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        path = urlparse(url).path.rstrip("/")
        if path.endswith("/System/Info"):
            return httpx.Response(200, json={"Id": "system"})
        if path.endswith(items_path_suffix):
            q = _flatten_qs(url)
            page_log.append(dict(q))
            assert "api_key" in q and q["api_key"], q
            for key, expected in _REQUIRED_ITEM_PARAMS.items():
                assert q.get(key) == expected, (key, q.get(key), expected)
            si = int(q["StartIndex"])
            lim = int(q["Limit"])
            chunk = all_ids[si : si + lim]
            return httpx.Response(200, json={"Items": [{"Id": i, "Type": "Movie", "Name": i} for i in chunk]})
        if path.endswith("/Users"):
            return httpx.Response(200, json=[{"Id": user_id, "Name": "User1"}])
        return httpx.Response(404, json={"Message": f"unexpected path {path}"})

    return httpx.MockTransport(handler)


def _shared_emby_async_client(transport: httpx.MockTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=transport)


def _patch_emby_shared_client(monkeypatch: pytest.MonkeyPatch, client: httpx.AsyncClient) -> None:
    monkeypatch.setattr("app.emby_client.get_shared_httpx_client", lambda: client)


def _patch_trimmer_allow_all_candidates(monkeypatch: pytest.MonkeyPatch, capture: list[str]) -> None:
    def _eval(item: dict, **_: object) -> tuple:
        iid = str(item.get("Id", "")).strip()
        if iid:
            capture.append(iid)
        return (True, [], 0, 0, False)

    monkeypatch.setattr("app.trimmer_service.evaluate_candidate", _eval)
    monkeypatch.setattr("app.trimmer_service.movie_matches_selected_genres", lambda *a, **k: True)
    monkeypatch.setattr("app.trimmer_service.movie_matches_people", lambda *a, **k: True)
    monkeypatch.setattr("app.trimmer_service.tv_matches_selected_genres", lambda *a, **k: True)


def _patch_service_logic_allow_all_candidates(monkeypatch: pytest.MonkeyPatch, capture: list[str]) -> None:
    def _eval(item: dict, **_: object) -> tuple:
        iid = str(item.get("Id", "")).strip()
        if iid:
            capture.append(iid)
        return (True, [], 0, 0, False)

    monkeypatch.setattr("app.service_logic.evaluate_candidate", _eval)
    monkeypatch.setattr("app.service_logic.movie_matches_selected_genres", lambda *a, **k: True)
    monkeypatch.setattr("app.service_logic.movie_matches_people", lambda *a, **k: True)
    monkeypatch.setattr("app.service_logic.tv_matches_selected_genres", lambda *a, **k: True)


def _base_settings(**overrides: object) -> AppSettings:
    s = AppSettings()
    s.emby_url = "http://emby.test"
    s.emby_api_key = "secret"
    s.emby_user_id = "u1"
    s.emby_max_items_scan = 10_000
    s.emby_max_deletes_per_run = 500
    s.emby_dry_run = True
    s.emby_last_run_at = None
    s.emby_rule_movie_unwatched_days = 1
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def test_items_for_user_required_params_and_startindex_limit_progression(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        monkeypatch.setattr("app.emby_client._DEFAULT_ITEMS_PAGE_SIZE", 3)
        log: list[dict[str, str]] = []
        all_ids = [f"x{i}" for i in range(8)]
        transport = _make_slice_items_handler(all_ids=all_ids, page_log=log)
        client = EmbyClient(EmbyConfig("http://emby.test", "key"), http_client=_shared_emby_async_client(transport))

        out = await client.items_for_user(user_id="u1", limit=8)

        assert [it["Id"] for it in out] == all_ids
        assert [(p["StartIndex"], p["Limit"]) for p in log] == [("0", "3"), ("3", "3"), ("6", "2")]

    asyncio.run(_run())


def test_items_for_user_merge_order_no_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        monkeypatch.setattr("app.emby_client._DEFAULT_ITEMS_PAGE_SIZE", 2)
        log: list[dict[str, str]] = []
        all_ids = ["a", "b", "c", "d", "e"]
        transport = _make_slice_items_handler(all_ids=all_ids, page_log=log)
        client = EmbyClient(EmbyConfig("http://emby.test", "key"), http_client=_shared_emby_async_client(transport))

        out = await client.items_for_user(user_id="u1", limit=100)

        assert [x["Id"] for x in out] == all_ids
        starts = [int(p["StartIndex"]) for p in log]
        assert starts == [0, 2, 4]
        assert len(log) == 3

    asyncio.run(_run())


def test_items_for_user_stops_on_empty_items_first_page() -> None:
    async def _run() -> None:
        async def fake_request(_method: str, _url: str, **kwargs: object) -> MagicMock:
            p = kwargs.get("params") or {}
            assert isinstance(p, dict)
            for k, v in _REQUIRED_ITEM_PARAMS.items():
                assert p.get(k) == v
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"Items": []}
            resp.raise_for_status = MagicMock()
            return resp

        mock_http = MagicMock()
        mock_http.request = AsyncMock(side_effect=fake_request)
        client = EmbyClient(EmbyConfig("http://localhost:8096", "key"), http_client=mock_http)
        out = await client.items_for_user(user_id="u1", limit=50)
        assert out == []
        assert mock_http.request.call_count == 1

    asyncio.run(_run())


def test_items_for_user_stops_on_short_page(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        monkeypatch.setattr("app.emby_client._DEFAULT_ITEMS_PAGE_SIZE", 5)
        calls: list[tuple[int, int]] = []

        async def fake_request(_method: str, _url: str, **kwargs: object) -> MagicMock:
            p = kwargs.get("params") or {}
            assert isinstance(p, dict)
            si = int(p["StartIndex"])
            lim = int(p["Limit"])
            calls.append((si, lim))
            if si == 0:
                batch = [{"Id": str(i)} for i in range(5)]  # full page
            elif si == 5:
                batch = [{"Id": "last"}]  # short vs take=5 → stop
            else:
                batch = []
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"Items": batch}
            resp.raise_for_status = MagicMock()
            return resp

        mock_http = MagicMock()
        mock_http.request = AsyncMock(side_effect=fake_request)
        client = EmbyClient(EmbyConfig("http://localhost:8096", "key"), http_client=mock_http)
        out = await client.items_for_user(user_id="u1", limit=10_000)

        assert [x["Id"] for x in out] == ["0", "1", "2", "3", "4", "last"]
        assert calls == [(0, 5), (5, 5)]

    asyncio.run(_run())


def test_items_for_user_max_items_exactly_two_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        monkeypatch.setattr("app.emby_client._DEFAULT_ITEMS_PAGE_SIZE", 3)
        log: list[dict[str, str]] = []
        all_ids = [f"id{i}" for i in range(20)]
        transport = _make_slice_items_handler(all_ids=all_ids, page_log=log)
        client = EmbyClient(EmbyConfig("http://emby.test", "key"), http_client=_shared_emby_async_client(transport))

        out = await client.items_for_user(user_id="u1", limit=5)

        assert [x["Id"] for x in out] == ["id0", "id1", "id2", "id3", "id4"]
        assert [(p["StartIndex"], p["Limit"]) for p in log] == [("0", "3"), ("3", "2")]
        assert len(log) == 2

    asyncio.run(_run())


@pytest.mark.parametrize("unlimited_limit", [0, -1, -999])
def test_items_for_user_unlimited_until_exhausted(monkeypatch: pytest.MonkeyPatch, unlimited_limit: int) -> None:
    async def _run() -> None:
        monkeypatch.setattr("app.emby_client._DEFAULT_ITEMS_PAGE_SIZE", 4)
        log: list[dict[str, str]] = []
        all_ids = [f"z{i}" for i in range(9)]
        transport = _make_slice_items_handler(all_ids=all_ids, page_log=log)
        client = EmbyClient(EmbyConfig("http://emby.test", "key"), http_client=_shared_emby_async_client(transport))

        out = await client.items_for_user(user_id="u1", limit=unlimited_limit)

        assert [x["Id"] for x in out] == all_ids
        assert all(p["Limit"] == "4" for p in log)
        assert [(p["StartIndex"], p["Limit"]) for p in log] == [("0", "4"), ("4", "4"), ("8", "4")]

    asyncio.run(_run())


def test_items_for_user_non_dict_payload_yields_empty_batch() -> None:
    async def _run() -> None:
        async def fake_request(_method: str, _url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = []  # not a dict → no Items
            resp.raise_for_status = MagicMock()
            return resp

        mock_http = MagicMock()
        mock_http.request = AsyncMock(side_effect=fake_request)
        client = EmbyClient(EmbyConfig("http://localhost:8096", "key"), http_client=mock_http)
        out = await client.items_for_user(user_id="u1", limit=5)
        assert out == []

    asyncio.run(_run())


def test_items_for_user_items_json_null_yields_empty_batch() -> None:
    async def _run() -> None:
        async def fake_request(_method: str, _url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"Items": None}
            resp.raise_for_status = MagicMock()
            return resp

        mock_http = MagicMock()
        mock_http.request = AsyncMock(side_effect=fake_request)
        client = EmbyClient(EmbyConfig("http://localhost:8096", "key"), http_client=mock_http)
        out = await client.items_for_user(user_id="u1", limit=5)
        assert out == []
        assert mock_http.request.call_count == 1

    asyncio.run(_run())


def test_build_review_and_execute_emby_block_see_same_ids_order_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same HTTP Items sequence; candidate ID capture matches direct items_for_user."""

    async def _run() -> None:
        monkeypatch.setattr("app.emby_client._DEFAULT_ITEMS_PAGE_SIZE", 3)
        page_log: list[dict[str, str]] = []
        all_ids = [f"m{i}" for i in range(7)]
        transport = _make_slice_items_handler(all_ids=all_ids, page_log=page_log)
        shared = _shared_emby_async_client(transport)
        _patch_emby_shared_client(monkeypatch, shared)

        direct_client = EmbyClient(EmbyConfig("http://emby.test", "secret"))
        try:
            direct = await direct_client.items_for_user(user_id="u1", limit=100)
        finally:
            await direct_client.aclose()

        direct_ids = [str(x["Id"]) for x in direct]
        assert direct_ids == all_ids

        settings = _base_settings(emby_max_items_scan=100)

        cap_review: list[str] = []
        _patch_trimmer_allow_all_candidates(monkeypatch, cap_review)
        review = await TrimmerReviewService().build_review(settings, run_emby_scan=True)
        assert review.error == ""
        assert [r["id"] for r in review.rows] == direct_ids

        cap_exec: list[str] = []
        _patch_service_logic_allow_all_candidates(monkeypatch, cap_exec)
        session = MagicMock()
        session.add = MagicMock()
        log_row = SimpleNamespace(id=99)
        now = utc_now_naive()
        ctx = RunContext(
            settings=settings,
            arr_manual_scope=None,
            son_key="",
            rad_key="",
            em_key="secret",
            tz="UTC",
            sonarr_tick_m=60,
            radarr_tick_m=60,
            sonarr_retry_delay_minutes=60,
            radarr_retry_delay_minutes=60,
            emby_interval_m=5,
            now=now,
            do_sonarr_block=False,
            do_radarr_block=False,
        )
        actions: list[str] = []
        await _execute_emby_block(session, log=log_row, ctx=ctx, actions=actions)

        assert cap_exec == direct_ids
        assert any("dry-run matched 7 item(s)" in a for a in actions)
        assert cap_review == direct_ids

        await shared.aclose()

    asyncio.run(_run())
