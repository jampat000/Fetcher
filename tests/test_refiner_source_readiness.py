"""Unit tests for authority-first Refiner source readiness."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.models import AppSettings
from app.refiner_source_readiness import (
    RefinerQueueSnapshot,
    decide_refiner_readiness,
    fetch_refiner_queue_snapshot,
    queue_record_upstream_active,
    refiner_file_level_gate,
    upstream_blocks_path,
)


def test_queue_record_upstream_active_status_and_sizeleft() -> None:
    assert queue_record_upstream_active({"status": "downloading", "sizeleft": 0}) is True
    assert queue_record_upstream_active({"status": "completed", "sizeleft": 100}) is True
    assert queue_record_upstream_active({"status": "completed", "sizeleft": 0}) is False
    assert queue_record_upstream_active({"status": "failed", "sizeleft": 0}) is False


def test_file_gate_accepts_stable_nonempty_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = tmp_path / "ok.mkv"
    f.write_bytes(b"xyz" * 50)
    monkeypatch.setattr("app.refiner_source_readiness.time.sleep", lambda _s: None)
    ok, why = refiner_file_level_gate(f, strict=False)
    assert ok is True
    assert why == ""


def test_upstream_blocks_when_path_matches_active_radarr_row(tmp_path: Path) -> None:
    f = tmp_path / "Movie.mkv"
    f.write_bytes(b"x" * 100)
    key = str(f.resolve()).casefold()
    rec = {
        "status": "downloading",
        "sizeleft": 0,
        "outputPath": str(f.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _msg = upstream_blocks_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"


def test_decide_authority_blocks_before_file_gate(tmp_path: Path) -> None:
    f = tmp_path / "blocked.mkv"
    f.write_bytes(b"x" * 50)
    rec = {"status": "downloading", "sizeleft": 0, "outputPath": str(f.resolve())}
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    row = AppSettings()

    async def _run() -> None:
        d = await decide_refiner_readiness(f, row, snapshot=snap, gate_tag="initial")
        assert d.proceed is False
        assert d.reason_code == "radarr_queue_active_download"

    asyncio.run(_run())


def test_fetch_snapshot_parallel_handles_disabled_apps() -> None:
    row = AppSettings()
    row.radarr_enabled = False
    row.sonarr_enabled = False

    async def _run() -> None:
        snap = await fetch_refiner_queue_snapshot(row)
        assert snap.authority_configured is False

    asyncio.run(_run())


def test_fetch_snapshot_uses_queue_page(monkeypatch: pytest.MonkeyPatch) -> None:
    row = AppSettings()
    row.radarr_enabled = True
    row.radarr_url = "http://127.0.0.1:7878"
    row.radarr_api_key = "k"
    monkeypatch.setattr("app.refiner_source_readiness.resolve_radarr_api_key", lambda _s: "k")
    monkeypatch.setattr("app.refiner_source_readiness.resolve_sonarr_api_key", lambda _s: "")

    class DummyClient:
        async def queue_page(self, *, page: int, page_size: int) -> dict:
            return {
                "records": [{"id": 1, "status": "completed", "sizeleft": 0, "outputPath": "/x"}],
                "totalRecords": 1,
            }

    monkeypatch.setattr("app.refiner_source_readiness.ArrClient", lambda *a, **k: DummyClient())

    async def _run() -> None:
        snap = await fetch_refiner_queue_snapshot(row)
        assert snap.radarr_configured is True
        assert snap.radarr_fetch_succeeded is True
        assert len(snap.radarr_records) == 1

    asyncio.run(_run())
