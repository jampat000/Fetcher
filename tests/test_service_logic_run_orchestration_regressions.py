from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy import delete, desc, select

from app.db import SessionLocal, _get_or_create_settings
from app.models import ActivityLog, AppSnapshot, JobRunLog
from app.service_logic import ArrManualScope, run_once


async def _set_settings(**updates: Any) -> None:
    async with SessionLocal() as s:
        row = await _get_or_create_settings(s)
        for k, v in updates.items():
            setattr(row, k, v)
        await s.commit()


async def _clear_run_tables() -> None:
    async with SessionLocal() as s:
        await s.execute(delete(ActivityLog))
        await s.execute(delete(AppSnapshot))
        await s.execute(delete(JobRunLog))
        await s.commit()


async def _latest_run_log() -> JobRunLog:
    async with SessionLocal() as s:
        row = (
            (
                await s.execute(
                    select(JobRunLog).order_by(desc(JobRunLog.id)).limit(1)
                )
            )
            .scalars()
            .first()
        )
        assert row is not None
        return row


async def _latest_snapshot() -> AppSnapshot | None:
    async with SessionLocal() as s:
        return (
            (
                await s.execute(
                    select(AppSnapshot).order_by(desc(AppSnapshot.id)).limit(1)
                )
            )
            .scalars()
            .first()
        )


async def _latest_activity() -> ActivityLog | None:
    async with SessionLocal() as s:
        return (
            (
                await s.execute(
                    select(ActivityLog).order_by(desc(ActivityLog.id)).limit(1)
                )
            )
            .scalars()
            .first()
        )


async def _settings_row() -> Any:
    async with SessionLocal() as s:
        return await _get_or_create_settings(s)


async def _run_once(scope: ArrManualScope | None = None):
    async with SessionLocal() as s:
        return await run_once(s, arr_manual_scope=scope)


async def _run_once_scheduled(app_scope: str):
    async with SessionLocal() as s:
        return await run_once(s, scheduled_scope=app_scope)


def test_run_once_sonarr_schedule_skip_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=True,
            sonarr_url="http://localhost:8989",
            sonarr_search_missing=True,
            sonarr_search_upgrades=False,
            sonarr_last_run_at=None,
            radarr_enabled=False,
            emby_enabled=False,
        )
    )
    constructed = {"count": 0}

    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "k")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.in_window", lambda **_kw: False)

    class _ShouldNotConstruct:
        def __init__(self, _cfg):
            constructed["count"] += 1

    monkeypatch.setattr("app.service_logic.ArrClient", _ShouldNotConstruct)
    result = asyncio.run(_run_once())
    assert result.ok is True
    assert result.message == "Sonarr: skipped (outside schedule window)"
    assert constructed["count"] == 0
    log = asyncio.run(_latest_run_log())
    assert log.ok is True
    assert log.message == "Sonarr: skipped (outside schedule window)"
    assert asyncio.run(_latest_snapshot()) is None
    assert asyncio.run(_settings_row()).sonarr_last_run_at is None


def test_run_once_sonarr_no_internal_interval_skip_anymore(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 3, 23, 12, 0, 0)
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=True,
            sonarr_url="http://localhost:8989",
            sonarr_search_missing=True,
            sonarr_search_upgrades=False,
            sonarr_interval_minutes=60,
            sonarr_last_run_at=fixed_now - timedelta(minutes=5),
            radarr_enabled=False,
            emby_enabled=False,
        )
    )
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: fixed_now)
    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "k")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    seen = {"health": 0}

    class _FakeArrClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            seen["health"] += 1

        async def series(self):
            return []

        async def wanted_missing(self, **kwargs):
            return {"records": [], "totalRecords": 0}

        async def wanted_cutoff_unmet(self, **kwargs):
            return {"records": [], "totalRecords": 0}

        async def aclose(self):
            return None

    monkeypatch.setattr("app.service_logic.in_window", lambda **_kw: True)
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    async def _paginate(*args, **kwargs):
        return [], [], 0

    monkeypatch.setattr("app.service_logic._paginate_wanted_for_search", _paginate)
    result = asyncio.run(_run_once())
    assert result.ok is True
    assert "Sonarr: no missing episodes found" in result.message
    assert seen["health"] == 1
    assert asyncio.run(_settings_row()).sonarr_last_run_at == fixed_now


def test_scheduled_scoped_run_sonarr_only(monkeypatch: pytest.MonkeyPatch) -> None:
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=True,
            sonarr_url="http://localhost:8989",
            sonarr_search_missing=False,
            sonarr_search_upgrades=False,
            sonarr_last_run_at=None,
            radarr_enabled=True,
            radarr_url="http://localhost:7878",
            radarr_search_missing=False,
            radarr_search_upgrades=False,
            radarr_last_run_at=None,
            emby_enabled=True,
            emby_url="http://localhost:8096",
        )
    )
    seen = {"sonarr": 0, "radarr": 0, "emby": 0}

    class _FakeArrClient:
        def __init__(self, cfg):
            self._base = str(getattr(cfg, "base_url", ""))

        async def health(self):
            if ":8989" in self._base:
                seen["sonarr"] += 1
            elif ":7878" in self._base:
                seen["radarr"] += 1

        async def series(self):
            return []

        async def aclose(self):
            return None

    class _FakeEmbyClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            seen["emby"] += 1

        async def users(self):
            return [{"Id": "u1", "Name": "U"}]

        async def items_for_user(self, **kwargs):
            return []

        async def aclose(self):
            return None

    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "sk")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "rk")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "ek")
    monkeypatch.setattr("app.service_logic.in_window", lambda **_kw: True)
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.service_logic.EmbyClient", _FakeEmbyClient)
    async def _should_not_search(*args, **kwargs):
        raise AssertionError("should not search")

    monkeypatch.setattr("app.service_logic._paginate_wanted_for_search", _should_not_search)
    result = asyncio.run(_run_once_scheduled("sonarr"))
    assert result.ok is True
    assert seen["sonarr"] == 1
    assert seen["radarr"] == 0
    assert seen["emby"] == 0


def test_scheduled_sonarr_runs_when_schedule_window_disabled() -> None:
    asyncio.run(_run_scheduled_window_disabled_case("sonarr"))


def test_scheduled_radarr_runs_when_schedule_window_disabled() -> None:
    asyncio.run(_run_scheduled_window_disabled_case("radarr"))


def test_scheduled_trimmer_runs_when_schedule_window_disabled() -> None:
    asyncio.run(_run_scheduled_window_disabled_case("trimmer"))


async def _run_scheduled_window_disabled_case(scope: str) -> None:
    fixed_now = datetime(2026, 3, 24, 10, 0, 0)
    await _clear_run_tables()
    async with SessionLocal() as s:
        row = await _get_or_create_settings(s)
        row.sonarr_enabled = scope == "sonarr"
        row.sonarr_url = "http://localhost:8989"
        row.sonarr_search_missing = False
        row.sonarr_search_upgrades = False
        row.sonarr_schedule_enabled = False
        row.radarr_enabled = scope == "radarr"
        row.radarr_url = "http://localhost:7878"
        row.radarr_search_missing = False
        row.radarr_search_upgrades = False
        row.radarr_schedule_enabled = False
        row.emby_enabled = scope == "trimmer"
        row.emby_url = "http://localhost:8096"
        row.emby_schedule_enabled = False
        row.emby_dry_run = True
        row.emby_rule_movie_watched_rating_below = 0
        row.emby_rule_movie_unwatched_days = 0
        row.emby_rule_tv_delete_watched = False
        row.emby_rule_tv_unwatched_days = 0
        row.emby_last_run_at = None
        row.sonarr_last_run_at = None
        row.radarr_last_run_at = None
        await s.commit()

    seen = {"sonarr": 0, "radarr": 0, "emby": 0}

    class _FakeArrClient:
        def __init__(self, cfg):
            self._base = str(getattr(cfg, "base_url", ""))

        async def health(self):
            if ":8989" in self._base:
                seen["sonarr"] += 1
            if ":7878" in self._base:
                seen["radarr"] += 1

        async def series(self):
            return []

        async def aclose(self):
            return None

        async def wanted_missing(self, **kwargs):
            return {"records": [], "totalRecords": 0}

        async def wanted_cutoff_unmet(self, **kwargs):
            return {"records": [], "totalRecords": 0}

    class _FakeEmbyClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            seen["emby"] += 1

        async def users(self):
            return [{"Id": "u1", "Name": "U"}]

        async def items_for_user(self, **kwargs):
            return []

        async def aclose(self):
            return None

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: fixed_now)
    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "sk")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "rk")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "ek")
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.service_logic.EmbyClient", _FakeEmbyClient)
    try:
        result = await _run_once_scheduled(scope)
    finally:
        monkeypatch.undo()

    assert result.ok is True
    if scope == "sonarr":
        assert seen["sonarr"] == 1 and seen["radarr"] == 0 and seen["emby"] == 0
    elif scope == "radarr":
        assert seen["radarr"] == 1 and seen["sonarr"] == 0 and seen["emby"] == 0
    else:
        assert seen["emby"] == 1 and seen["sonarr"] == 0 and seen["radarr"] == 0


def test_sonarr_due_outside_window_skips_then_runs_when_window_opens(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 3, 24, 11, 0, 0)
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=True,
            sonarr_url="http://localhost:8989",
            sonarr_search_missing=True,
            sonarr_search_upgrades=False,
            sonarr_schedule_enabled=True,
            sonarr_last_run_at=None,
            radarr_enabled=False,
            emby_enabled=False,
        )
    )
    seen = {"health": 0}

    class _FakeArrClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            seen["health"] += 1

        async def series(self):
            return []

        async def wanted_missing(self, **kwargs):
            return {"records": [], "totalRecords": 0}

        async def wanted_cutoff_unmet(self, **kwargs):
            return {"records": [], "totalRecords": 0}

        async def aclose(self):
            return None

    gate = {"allow": False}

    def _in_window(**_kwargs):
        return gate["allow"]

    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: fixed_now)
    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "sk")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.service_logic.in_window", _in_window)

    res1 = asyncio.run(_run_once_scheduled("sonarr"))
    assert res1.ok is True
    assert "Sonarr: skipped (outside schedule window)" in res1.message
    assert seen["health"] == 0

    gate["allow"] = True
    res2 = asyncio.run(_run_once_scheduled("sonarr"))
    assert res2.ok is True
    assert "Sonarr: no missing episodes found" in res2.message
    assert seen["health"] == 1


def test_run_once_manual_scope_gating_sonarr_missing_skip_message(monkeypatch: pytest.MonkeyPatch) -> None:
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=False,
            sonarr_url="",
            radarr_enabled=True,
            radarr_url="http://localhost:7878",
            emby_enabled=False,
        )
    )
    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "rk")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.ArrClient", lambda _cfg: (_ for _ in ()).throw(AssertionError("no Arr client should be constructed for unrelated manual scope")))
    result = asyncio.run(_run_once("sonarr_missing"))
    assert result.ok is True
    assert result.message == (
        "Manual search: bypassing schedule windows and Sonarr/Radarr run-interval gates for this action only."
        " | Sonarr: skipped (enable Sonarr and set URL + API key in Settings)"
    )


def test_run_once_sonarr_suppressed_cooldown_snapshot_and_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 3, 23, 13, 0, 0)
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=True,
            sonarr_url="http://localhost:8989",
            sonarr_search_missing=True,
            sonarr_search_upgrades=False,
            sonarr_last_run_at=None,
            emby_enabled=False,
            radarr_enabled=False,
        )
    )
    seen = {"health": 0, "aclose": 0}

    class _FakeArrClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            seen["health"] += 1

        async def series(self):
            return []

        async def aclose(self):
            seen["aclose"] += 1

    async def _paginate(*args, **kwargs):
        return [], [], 3

    async def _wanted_total(*args, **kwargs):
        return 7

    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: fixed_now)
    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "k")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.in_window", lambda **_kw: True)
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.service_logic._paginate_wanted_for_search", _paginate)
    monkeypatch.setattr("app.service_logic._wanted_queue_total", _wanted_total)
    result = asyncio.run(_run_once())
    assert result.ok is True
    assert result.message == "Sonarr: missing search suppressed (cooldown)"
    assert seen["health"] == 1
    assert seen["aclose"] == 1
    snap = asyncio.run(_latest_snapshot())
    assert snap is not None
    assert snap.app == "sonarr"
    assert snap.ok is True
    assert snap.status_message == "OK"
    assert snap.missing_total == 3
    assert snap.cutoff_unmet_total == 7
    assert asyncio.run(_latest_activity()) is None
    assert asyncio.run(_settings_row()).sonarr_last_run_at == fixed_now


def test_run_once_radarr_manual_upgrade_success_activity_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 3, 23, 14, 0, 0)
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=False,
            radarr_enabled=True,
            radarr_url="http://localhost:7878",
            radarr_search_missing=False,
            radarr_search_upgrades=True,
            radarr_last_run_at=None,
            emby_enabled=False,
        )
    )
    seen = {"health": 0, "aclose": 0, "trigger_ids": []}

    class _FakeArrClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            seen["health"] += 1

        async def ensure_tag(self, _label: str):
            return 1

        async def add_tags_to_movies(self, **kwargs):
            return None

        async def aclose(self):
            seen["aclose"] += 1

    async def _paginate(client, session, *, kind, **kwargs):
        if kind == "cutoff":
            return [11, 12], [{"title": "M1", "year": 2020}, {"title": "M2", "year": 2021}], 9
        return [], [], 0

    async def _wanted_total(*args, **kwargs):
        return 4

    async def _trigger(_client, *, movie_ids):
        seen["trigger_ids"] = movie_ids

    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: fixed_now)
    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "rk")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.service_logic._paginate_wanted_for_search", _paginate)
    monkeypatch.setattr("app.service_logic._wanted_queue_total", _wanted_total)
    monkeypatch.setattr("app.service_logic.trigger_radarr_cutoff_search", _trigger)
    result = asyncio.run(_run_once("radarr_upgrade"))
    assert result.ok is True
    assert "Manual search: bypassing schedule windows and Sonarr/Radarr run-interval gates for this action only." in result.message
    assert "Radarr: cutoff-unmet search for 2 movie(s)" in result.message
    assert seen["health"] == 1
    assert seen["aclose"] == 1
    assert seen["trigger_ids"] == [11, 12]
    snap = asyncio.run(_latest_snapshot())
    assert snap is not None
    assert snap.app == "radarr"
    assert snap.ok is True
    assert snap.status_message == "OK"
    assert snap.missing_total == 4
    assert snap.cutoff_unmet_total == 9
    act = asyncio.run(_latest_activity())
    assert act is not None
    assert act.app == "radarr"
    assert act.kind == "upgrade"
    assert act.count == 2
    assert asyncio.run(_settings_row()).radarr_last_run_at == fixed_now


def test_run_once_tag_warning_is_nonfatal(monkeypatch: pytest.MonkeyPatch) -> None:
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=True,
            sonarr_url="http://localhost:8989",
            sonarr_search_missing=True,
            sonarr_search_upgrades=False,
            sonarr_last_run_at=None,
            radarr_enabled=False,
            emby_enabled=False,
        )
    )

    class _FakeArrClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            return None

        async def series(self):
            return []

        async def ensure_tag(self, _label: str):
            raise ValueError("tag fail")

        async def aclose(self):
            return None

    async def _paginate(*args, **kwargs):
        return [101], [{"seriesTitle": "X", "seasonNumber": 1, "episodeNumber": 2, "title": "Ep"}], 1

    async def _trigger(*args, **kwargs):
        return None

    async def _wanted_total(*args, **kwargs):
        return 0

    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "k")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.in_window", lambda **_kw: True)
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.service_logic._paginate_wanted_for_search", _paginate)
    monkeypatch.setattr("app.service_logic._wanted_queue_total", _wanted_total)
    monkeypatch.setattr("app.service_logic.trigger_sonarr_missing_search", _trigger)
    result = asyncio.run(_run_once())
    assert result.ok is True
    assert "Sonarr: tag apply warning (fetcher-missing): ValueError: tag fail" in result.message
    assert "Sonarr: missing search for 1 episode(s)" in result.message


def test_run_once_httpstatuserror_is_hard_failure_with_snapshot_and_activity(monkeypatch: pytest.MonkeyPatch) -> None:
    asyncio.run(_clear_run_tables())
    asyncio.run(
        _set_settings(
            sonarr_enabled=True,
            sonarr_url="http://localhost:8989",
            sonarr_search_missing=True,
            sonarr_search_upgrades=False,
            sonarr_last_run_at=None,
            radarr_enabled=False,
            emby_enabled=False,
        )
    )
    seen = {"health": 0, "aclose": 0}

    class _FakeArrClient:
        def __init__(self, _cfg):
            pass

        async def health(self):
            seen["health"] += 1
            req = httpx.Request("GET", "http://localhost:8989/api/v3/system/status")
            resp = httpx.Response(502, request=req, text="bad gateway body")
            raise httpx.HTTPStatusError("boom", request=req, response=resp)

        async def aclose(self):
            seen["aclose"] += 1

    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "k")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.in_window", lambda **_kw: True)
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    result = asyncio.run(_run_once())
    assert result.ok is False
    assert result.message.startswith("Run failed: HTTP 502 for GET http://localhost:8989")
    assert seen["health"] == 1
    assert seen["aclose"] == 1
    log = asyncio.run(_latest_run_log())
    assert log.ok is False
    snap = asyncio.run(_latest_snapshot())
    assert snap is not None
    assert snap.app == "sonarr"
    assert snap.ok is False
    assert snap.missing_total == 0
    assert snap.cutoff_unmet_total == 0
    act = asyncio.run(_latest_activity())
    assert act is not None
    assert act.app == "sonarr"
    assert act.kind == "error"
    assert act.status == "failed"
