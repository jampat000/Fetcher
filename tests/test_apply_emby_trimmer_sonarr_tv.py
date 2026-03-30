"""Contract tests for Sonarr TV episode-file deletes in ``apply_emby_trimmer_live_deletes`` (bounded concurrent deletes + partial failure)."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import pytest

from app.models import AppSettings
from app.service_logic import (
    _delete_sonarr_episode_files_bounded,
    _sonarr_episode_is_unaired_future,
    apply_emby_trimmer_live_deletes,
)


class _FakeEmby:
    def __init__(self) -> None:
        self.deleted_ids: list[str] = []

    async def delete_item(self, item_id: str) -> None:
        self.deleted_ids.append(item_id)


class _RecordingSonarrArrClient:
    """Drop-in for ``service_logic.ArrClient`` during TV-only apply tests."""

    def __init__(
        self,
        _cfg: Any,
        *,
        catalog: list[dict[str, Any]],
        episodes_by_series: dict[int, list[dict[str, Any]]],
        fail_episode_file_ids: frozenset[int] | None = None,
    ) -> None:
        self._catalog = catalog
        self._episodes_by_series = episodes_by_series
        self.fail_episode_file_ids = fail_episode_file_ids or frozenset()
        self.attempted_episode_file_ids: list[int] = []
        self.delete_episode_file_ids: list[int] = []
        self.set_episodes_monitored_calls: list[tuple[list[int], bool]] = []
        self.unmonitor_episodes_calls: list[list[int]] = []
        self.update_series_calls: list[dict[str, Any]] = []

    async def series(self) -> list[dict[str, Any]]:
        return self._catalog

    async def episodes_for_series(self, *, series_id: int) -> list[dict[str, Any]]:
        return list(self._episodes_by_series.get(int(series_id), []))

    async def delete_episode_file(self, *, episode_file_id: int) -> None:
        eid = int(episode_file_id)
        self.attempted_episode_file_ids.append(eid)
        if eid in self.fail_episode_file_ids:
            raise RuntimeError(f"simulated Sonarr delete_episode_file failure for {eid}")
        self.delete_episode_file_ids.append(eid)

    async def set_episodes_monitored(self, *, episode_ids: list[int], monitored: bool) -> None:
        self.set_episodes_monitored_calls.append((list(episode_ids), bool(monitored)))

    async def unmonitor_episodes(self, *, episode_ids: list[int]) -> None:
        self.unmonitor_episodes_calls.append(list(episode_ids))

    async def update_series(self, series: dict[str, Any]) -> None:
        self.update_series_calls.append(series)

    async def aclose(self) -> None:
        return None


def _settings_tv_only() -> AppSettings:
    s = AppSettings()
    s.sonarr_url = "http://sonarr.test"
    s.radarr_url = ""
    return s


def _series_catalog(*, status: str) -> list[dict[str, Any]]:
    return [
        {
            "id": 10,
            "title": "Show A",
            "year": 2020,
            "tvdbId": 12345,
            "status": status,
            "seasons": [
                {"seasonNumber": 1, "monitored": False},
            ],
        }
    ]


def _episodes_three_files() -> list[dict[str, Any]]:
    return [
        {"id": 201, "seasonNumber": 1, "episodeNumber": 1, "episodeFileId": 501},
        {"id": 202, "seasonNumber": 1, "episodeNumber": 2, "episodeFileId": 502},
        {"id": 203, "seasonNumber": 1, "episodeNumber": 3, "episodeFileId": 503},
    ]


def _episodes_no_files() -> list[dict[str, Any]]:
    return [
        {"id": 301, "seasonNumber": 1, "episodeNumber": 1},
        {"id": 302, "seasonNumber": 1, "episodeNumber": 2},
    ]


def _emby_series() -> dict[str, Any]:
    return {
        "Name": "Show A",
        "Type": "Series",
        "ProviderIds": {"Tvdb": "12345"},
        "ProductionYear": 2020,
    }


def _emby_episode_s01e01() -> dict[str, Any]:
    return {
        "Name": "Pilot",
        "Type": "Episode",
        "ProviderIds": {"Tvdb": "12345"},
        "ProductionYear": 2020,
        "ParentIndexNumber": 1,
        "IndexNumber": 1,
    }


async def _run_apply(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fake_sonarr: _RecordingSonarrArrClient,
    candidates: list[tuple[str, str, str, dict[str, Any]]],
    frozen_now: datetime | None = None,
) -> tuple[list[str], _FakeEmby]:
    emby = _FakeEmby()

    def _arr_factory(cfg: Any) -> _RecordingSonarrArrClient:
        return fake_sonarr

    monkeypatch.setattr("app.service_logic.ArrClient", _arr_factory)
    if frozen_now is not None:
        monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: frozen_now)
    actions = await apply_emby_trimmer_live_deletes(
        _settings_tv_only(),
        emby,  # type: ignore[arg-type]
        candidates,
        son_key="sonarr-key",
        rad_key=None,
    )
    return actions, emby


def _fixed_now() -> datetime:
    return datetime(2026, 6, 15, 12, 0, 0)


def test_sonarr_deletes_all_episode_file_ids_ended_series(monkeypatch: pytest.MonkeyPatch) -> None:
    eps = _episodes_three_files()
    fake = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=_series_catalog(status="ended"),
        episodes_by_series={10: eps},
    )
    actions, emby = asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=fake,
            candidates=[("e1", "Show A", "Series", _emby_series())],
        )
    )
    assert set(fake.attempted_episode_file_ids) == {501, 502, 503}
    assert len(fake.attempted_episode_file_ids) == 3
    assert set(fake.delete_episode_file_ids) == {501, 502, 503}
    assert len(fake.delete_episode_file_ids) == 3
    assert fake.set_episodes_monitored_calls == []
    assert fake.unmonitor_episodes_calls == [[201, 202, 203]]
    assert fake.update_series_calls == []
    assert any(
        a == "Sonarr: deleted 3 on-disk episode file(s) for 3 episode(s)" for a in actions
    )
    assert any(
        a == "Sonarr: unmonitored 3 episode(s) (ended series) after delete criteria met" for a in actions
    )
    assert emby.deleted_ids == ["e1"]


def test_sonarr_deletes_airing_then_monitor(monkeypatch: pytest.MonkeyPatch) -> None:
    """Continuing show: no air metadata → conservative unmonitor only; no season PUT."""
    eps = _episodes_three_files()
    fake = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=_series_catalog(status="continuing"),
        episodes_by_series={10: eps},
    )
    actions, _emby = asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=fake,
            candidates=[("e1", "Show A", "Series", _emby_series())],
            frozen_now=_fixed_now(),
        )
    )
    assert set(fake.attempted_episode_file_ids) == {501, 502, 503}
    assert set(fake.delete_episode_file_ids) == {501, 502, 503}
    assert fake.set_episodes_monitored_calls == [([201, 202, 203], False)]
    assert fake.unmonitor_episodes_calls == []
    assert fake.update_series_calls == []
    assert any(
        a == "Sonarr: deleted 3 on-disk episode file(s) for 3 episode(s)" for a in actions
    )
    assert any(
        "0 future episode(s) monitored" in a and "3 aired/unknown episode(s)" in a for a in actions
    )


def test_sonarr_continuing_mixed_air_dates_future_monitored_past_unmonitored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _fixed_now()
    eps = [
        {
            "id": 201,
            "seasonNumber": 1,
            "episodeNumber": 1,
            "episodeFileId": 501,
            "airDateUtc": "2026-01-01T12:00:00Z",
        },
        {
            "id": 202,
            "seasonNumber": 1,
            "episodeNumber": 2,
            "episodeFileId": 502,
            "airDateUtc": "2026-12-01T12:00:00Z",
        },
        {
            "id": 203,
            "seasonNumber": 1,
            "episodeNumber": 3,
            "episodeFileId": 503,
            "airDate": "2026-12-15",
        },
    ]
    fake = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=_series_catalog(status="continuing"),
        episodes_by_series={10: eps},
    )
    asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=fake,
            candidates=[("e1", "Show A", "Series", _emby_series())],
            frozen_now=now,
        )
    )
    assert fake.set_episodes_monitored_calls == [([202, 203], True), ([201], False)]
    assert fake.update_series_calls == []


def test_sonarr_continuing_series_level_monitored_when_catalog_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cat = _series_catalog(status="continuing")
    cat[0]["monitored"] = False
    eps = [
        {
            "id": 201,
            "seasonNumber": 1,
            "episodeNumber": 1,
            "episodeFileId": 501,
            "airDateUtc": "2027-01-01T12:00:00Z",
        },
    ]
    fake = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=cat,
        episodes_by_series={10: eps},
    )
    asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=fake,
            candidates=[("e1", "Show A", "Series", _emby_series())],
            frozen_now=_fixed_now(),
        )
    )
    assert fake.set_episodes_monitored_calls == [([201], True)]
    assert len(fake.update_series_calls) == 1
    assert fake.update_series_calls[0].get("monitored") is True


def test_sonarr_episode_unaired_future_classification() -> None:
    now = datetime(2026, 6, 15, 12, 0, 0)
    assert not _sonarr_episode_is_unaired_future(
        {"airDateUtc": "2026-06-10T12:00:00Z"}, now_utc_naive=now
    )
    assert _sonarr_episode_is_unaired_future(
        {"airDateUtc": "2026-06-20T12:00:00Z"}, now_utc_naive=now
    )
    assert _sonarr_episode_is_unaired_future({"airDate": "2026-06-15"}, now_utc_naive=now)
    assert not _sonarr_episode_is_unaired_future({"airDate": "2026-06-14"}, now_utc_naive=now)
    assert not _sonarr_episode_is_unaired_future({}, now_utc_naive=now)


def test_no_duplicate_episode_file_deletes_overlapping_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two Emby rows resolve to the same Sonarr episode — one delete attempt + one unmonitor slot."""
    eps = [
        {"id": 401, "seasonNumber": 1, "episodeNumber": 1, "episodeFileId": 601},
    ]
    fake = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=_series_catalog(status="ended"),
        episodes_by_series={10: eps},
    )
    ep = _emby_episode_s01e01()
    asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=fake,
            candidates=[
                ("a", "Pilot", "Episode", ep),
                ("b", "Pilot", "Episode", ep),
            ],
        )
    )
    assert fake.attempted_episode_file_ids == [601]
    assert fake.delete_episode_file_ids == [601]
    assert fake.unmonitor_episodes_calls == [[401]]


def test_deleted_files_count_zero_one_many(monkeypatch: pytest.MonkeyPatch) -> None:
    many = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=_series_catalog(status="continuing"),
        episodes_by_series={10: _episodes_three_files()},
    )
    actions_many, _ = asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=many,
            candidates=[("e1", "Show A", "Series", _emby_series())],
            frozen_now=_fixed_now(),
        )
    )
    assert "Sonarr: deleted 3 on-disk episode file(s) for 3 episode(s)" in actions_many

    one_ep = [{"id": 501, "seasonNumber": 1, "episodeNumber": 1, "episodeFileId": 9001}]
    one = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=_series_catalog(status="continuing"),
        episodes_by_series={10: one_ep},
    )
    actions_one, _ = asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=one,
            candidates=[("e1", "Pilot", "Episode", _emby_episode_s01e01())],
            frozen_now=_fixed_now(),
        )
    )
    assert "Sonarr: deleted 1 on-disk episode file(s) for 1 episode(s)" in actions_one

    zero = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=_series_catalog(status="continuing"),
        episodes_by_series={10: _episodes_no_files()},
    )
    actions_zero, _ = asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=zero,
            candidates=[("e1", "Show A", "Series", _emby_series())],
            frozen_now=_fixed_now(),
        )
    )
    assert "Sonarr: deleted 0 on-disk episode file(s) for 2 episode(s)" in actions_zero
    assert zero.delete_episode_file_ids == []


def test_no_episodes_linked_message_when_nothing_resolves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=[],
        episodes_by_series={},
    )
    actions, _ = asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=fake,
            candidates=[("e1", "Unknown", "Series", {"Name": "Nope", "Type": "Series", "ProviderIds": {}})],
        )
    )
    assert "Sonarr: no episodes linked for TV delete candidate(s)" in actions
    assert fake.attempted_episode_file_ids == []


def test_partial_delete_failure_continues_and_updates_action_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eps = _episodes_three_files()
    fake = _RecordingSonarrArrClient(
        _cfg=None,
        catalog=_series_catalog(status="ended"),
        episodes_by_series={10: eps},
        fail_episode_file_ids=frozenset({502}),
    )
    actions, emby = asyncio.run(
        _run_apply(
            monkeypatch,
            fake_sonarr=fake,
            candidates=[("e1", "Show A", "Series", _emby_series())],
        )
    )
    assert set(fake.attempted_episode_file_ids) == {501, 502, 503}
    assert set(fake.delete_episode_file_ids) == {501, 503}
    assert not any(a.startswith("Sonarr: sync warning after Emby deletes:") for a in actions)
    partial = [a for a in actions if a.startswith("Sonarr: deleted ") and "1 failed" in a]
    assert len(partial) == 1
    assert "for 3 episode(s); 1 failed" in partial[0]
    assert "502" in partial[0]
    assert emby.deleted_ids == ["e1"]


def test_delete_helper_dedupes_episode_file_ids() -> None:
    """Same episode file id listed twice → one HTTP attempt, two successes max 1."""

    class _SonarrStub:
        def __init__(self) -> None:
            self.calls: list[int] = []

        async def delete_episode_file(self, *, episode_file_id: int) -> None:
            self.calls.append(int(episode_file_id))

    async def _run() -> None:
        stub = _SonarrStub()
        r = await _delete_sonarr_episode_files_bounded(stub, [10, 10, 20])  # type: ignore[arg-type]
        assert r.success_count == 2
        assert r.failed_episode_file_ids == []
        assert stub.calls == [10, 20]

    asyncio.run(_run())
