"""Refiner pre-promotion gate: terminal failed-import blocks + per-downloadId locks."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.import_item_lock import get_import_item_lock, hold_import_item_lock, import_item_lock_key
from app.refiner_promotion_gate import PromotionGateSyncResult, queue_row_contains_resolved_media_file
from app.refiner_promotion_gate import refiner_promotion_precheck


def test_queue_row_contains_resolved_media_file_under_output_path(tmp_path: Path) -> None:
    root = tmp_path / "q"
    root.mkdir()
    media = root / "file.mkv"
    media.write_bytes(b"x")
    q = {"outputPath": str(root)}
    assert queue_row_contains_resolved_media_file(media, q) is True


def test_promotion_gate_blocks_terminal_non_upgrade_radarr(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dl = tmp_path / "dl"
    dl.mkdir()
    media = dl / "m.mkv"
    media.write_bytes(b"x")
    recs = [
        {
            "id": 9,
            "downloadId": "did-1",
            "outputPath": str(dl),
            "errorMessage": "Not an upgrade for existing movie file. Existing quality: Bluray-1080p",
        }
    ]
    monkeypatch.setattr(
        "app.refiner_promotion_gate._fetch_queue_records",
        AsyncMock(return_value=recs),
    )
    fake_client = MagicMock()

    async def _run() -> None:
        r = await refiner_promotion_precheck(
            media_file=media,
            sonarr_client=None,
            radarr_client=fake_client,
        )
        assert r.allowed is False
        assert r.block_detail is not None
        assert r.block_detail.get("import_state") == "not an upgrade vs existing file"
        assert r.block_detail.get("non_upgrade") is True
        assert not r.held_locks

    asyncio.run(_run())


def test_promotion_gate_allows_pending_waiting_to_import(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dl = tmp_path / "dl"
    dl.mkdir()
    media = dl / "m.mkv"
    media.write_bytes(b"x")
    msg = "Downloaded - Waiting to Import - No files found are eligible for import in F:\\"
    recs = [{"id": 9, "downloadId": "did-w", "outputPath": str(dl), "errorMessage": msg}]
    monkeypatch.setattr(
        "app.refiner_promotion_gate._fetch_queue_records",
        AsyncMock(return_value=recs),
    )
    fake_client = MagicMock()

    async def _run() -> None:
        r = await refiner_promotion_precheck(
            media_file=media,
            sonarr_client=None,
            radarr_client=fake_client,
        )
        try:
            assert r.allowed is True
            assert r.block_detail is None
            assert len(r.held_locks) == 1
        finally:
            for lk in r.held_locks:
                try:
                    lk.release()
                except RuntimeError:
                    pass

    asyncio.run(_run())


def test_promotion_gate_allows_when_no_terminal_signal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dl = tmp_path / "dl"
    dl.mkdir()
    media = dl / "m.mkv"
    media.write_bytes(b"x")
    recs = [{"id": 9, "downloadId": "did-ok", "outputPath": str(dl), "errorMessage": ""}]
    monkeypatch.setattr(
        "app.refiner_promotion_gate._fetch_queue_records",
        AsyncMock(return_value=recs),
    )
    fake_client = MagicMock()

    async def _run() -> None:
        r = await refiner_promotion_precheck(
            media_file=media,
            sonarr_client=None,
            radarr_client=fake_client,
        )
        try:
            assert r.allowed is True
            assert r.block_detail is None
            assert len(r.held_locks) == 1
        finally:
            for lk in r.held_locks:
                try:
                    lk.release()
                except RuntimeError:
                    pass

    asyncio.run(_run())


def test_hold_import_item_lock_defers_second_acquirer() -> None:
    async def _run() -> None:
        key = import_item_lock_key("radarr", "z9")
        lk = get_import_item_lock(key)
        lk.acquire()
        entered = asyncio.Event()

        async def waiter() -> None:
            async with hold_import_item_lock("radarr", "z9"):
                entered.set()

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.03)
        assert not entered.is_set()
        lk.release()
        await asyncio.wait_for(task, timeout=3)
        assert entered.is_set()

    asyncio.run(_run())


def test_promotion_gate_sync_result_empty_locks_when_blocked() -> None:
    r = PromotionGateSyncResult(False, (), {"x": 1})
    assert r.allowed is False
    assert r.held_locks == ()
