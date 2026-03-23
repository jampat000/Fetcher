"""Contract tests for bounded concurrent Emby ``delete_item`` in ``apply_emby_trimmer_live_deletes``.

Uses settings with no Sonarr/Radarr URLs so Arr blocks are skipped; only the Emby phase runs.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.models import AppSettings
from app.service_logic import apply_emby_trimmer_live_deletes
from app.trimmer_service import TrimmerApplyService, TrimmerReviewResult


class _FakeEmby:
    """Records successful delete_item calls; optional per-id failure (every attempt with that id fails)."""

    def __init__(self, *, fail_item_ids: frozenset[str] | None = None) -> None:
        self.fail_item_ids = fail_item_ids or frozenset()
        self.deleted_ids: list[str] = []

    async def delete_item(self, item_id: str) -> None:
        if item_id in self.fail_item_ids:
            raise RuntimeError(f"simulated Emby delete failure for {item_id!r}")
        self.deleted_ids.append(item_id)


def _settings_arr_disabled() -> AppSettings:
    """No Radarr/Sonarr — only the Emby delete phase runs."""
    s = AppSettings()
    s.radarr_url = ""
    s.sonarr_url = ""
    return s


def _cand(item_id: str, *, name: str = "n", typ: str = "Movie", raw: dict[str, Any] | None = None) -> tuple[str, str, str, dict[str, Any]]:
    return (item_id, name, typ, raw or {})


def _emby_action(actions: list[str]) -> str:
    lines = [a for a in actions if a.startswith("Emby: ")]
    assert len(lines) == 1
    return lines[0]


def test_emby_all_success_exact_multiset_and_action() -> None:
    async def _run() -> None:
        emby = _FakeEmby()
        candidates = [
            _cand("alpha", name="A"),
            _cand("beta", name="B", typ="Series"),
            _cand("gamma", name="C", typ="Episode"),
        ]
        actions = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(),
            emby,  # type: ignore[arg-type]
            candidates,
            son_key=None,
            rad_key=None,
        )
        assert Counter(emby.deleted_ids) == Counter({"alpha", "beta", "gamma"})
        assert _emby_action(actions) == "Emby: deleted 3 item(s)"

    asyncio.run(_run())


def test_emby_overlapping_candidate_ids_two_attempts_no_dedupe() -> None:
    """Same Emby id in two rows → two delete_item attempts (both counted)."""

    async def _run() -> None:
        emby = _FakeEmby()
        candidates = [
            _cand("dup", name="First", typ="Movie"),
            _cand("dup", name="Second", typ="Movie"),
        ]
        actions = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(),
            emby,  # type: ignore[arg-type]
            candidates,
            son_key=None,
            rad_key=None,
        )
        assert len(emby.deleted_ids) == 2
        assert all(x == "dup" for x in emby.deleted_ids)
        assert _emby_action(actions) == "Emby: deleted 2 item(s)"

    asyncio.run(_run())


def test_emby_action_message_zero_one_many_full_success() -> None:
    async def _run() -> None:
        emby0 = _FakeEmby()
        a0 = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(), emby0, [], son_key=None, rad_key=None  # type: ignore[arg-type]
        )
        assert emby0.deleted_ids == []
        assert _emby_action(a0) == "Emby: deleted 0 item(s)"

        emby1 = _FakeEmby()
        a1 = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(),
            emby1,  # type: ignore[arg-type]
            [_cand("only")],
            son_key=None,
            rad_key=None,
        )
        assert emby1.deleted_ids == ["only"]
        assert _emby_action(a1) == "Emby: deleted 1 item(s)"

        emby_many = _FakeEmby()
        many = [_cand(str(i)) for i in range(5)]
        a_many = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(),
            emby_many,  # type: ignore[arg-type]
            many,
            son_key=None,
            rad_key=None,
        )
        assert Counter(emby_many.deleted_ids) == Counter({str(i): 1 for i in range(5)})
        assert _emby_action(a_many) == "Emby: deleted 5 item(s)"

    asyncio.run(_run())


def test_emby_partial_failure_continues_other_deletes(monkeypatch: pytest.MonkeyPatch) -> None:
    """One failing id does not stop other scheduled deletes; success count is successes only."""

    async def _run() -> None:
        monkeypatch.setattr("app.service_logic._EMBY_TRIMMER_ITEM_DELETE_CONCURRENCY", 5)
        emby = _FakeEmby(fail_item_ids=frozenset({"bad"}))
        actions = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(),
            emby,  # type: ignore[arg-type]
            [_cand("good"), _cand("bad"), _cand("ok")],
            son_key=None,
            rad_key=None,
        )
        assert Counter(emby.deleted_ids) == Counter({"good": 1, "ok": 1})
        line = _emby_action(actions)
        assert line.startswith("Emby: deleted 2 item(s); 1 failed —")
        assert "bad" in line

    asyncio.run(_run())


def test_emby_fail_on_first_still_attempts_remaining_ids() -> None:
    async def _run() -> None:
        emby = _FakeEmby(fail_item_ids=frozenset({"first"}))
        actions = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(),
            emby,  # type: ignore[arg-type]
            [_cand("first"), _cand("second")],
            son_key=None,
            rad_key=None,
        )
        assert Counter(emby.deleted_ids) == Counter({"second": 1})
        line = _emby_action(actions)
        assert "Emby: deleted 1 item(s); 1 failed —" in line
        assert "first" in line

    asyncio.run(_run())


def test_emby_duplicate_failing_id_lists_two_failures() -> None:
    async def _run() -> None:
        emby = _FakeEmby(fail_item_ids=frozenset({"dup"}))
        actions = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(),
            emby,  # type: ignore[arg-type]
            [_cand("dup"), _cand("dup")],
            son_key=None,
            rad_key=None,
        )
        assert emby.deleted_ids == []
        line = _emby_action(actions)
        assert line.startswith("Emby: deleted 0 item(s); 2 failed —")
        assert line.count("dup") >= 2

    asyncio.run(_run())


def test_emby_scheduling_order_preserved_when_concurrency_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """With concurrency 1, completion order matches candidate order (FIFO scheduling)."""

    async def _run() -> None:
        monkeypatch.setattr("app.service_logic._EMBY_TRIMMER_ITEM_DELETE_CONCURRENCY", 1)
        emby = _FakeEmby()
        actions = await apply_emby_trimmer_live_deletes(
            _settings_arr_disabled(),
            emby,  # type: ignore[arg-type]
            [_cand("a"), _cand("b"), _cand("c")],
            son_key=None,
            rad_key=None,
        )
        assert emby.deleted_ids == ["a", "b", "c"]
        assert _emby_action(actions) == "Emby: deleted 3 item(s)"

    asyncio.run(_run())


def test_trim_apply_dry_run_never_calls_apply_emby_trimmer_live_deletes(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        spy = AsyncMock(side_effect=AssertionError("apply_emby_trimmer_live_deletes must not run in dry-run"))
        monkeypatch.setattr("app.trimmer_service.apply_emby_trimmer_live_deletes", spy)

        settings = AppSettings()
        settings.emby_dry_run = True
        review = TrimmerReviewResult()
        review.candidates = [_cand("x", typ="Movie")]

        session = AsyncMock()
        await TrimmerApplyService().apply_live_delete_if_needed(settings, session, review)
        spy.assert_not_called()

    asyncio.run(_run())
