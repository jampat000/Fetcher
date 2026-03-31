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
    iter_queue_path_strings,
    queue_record_upstream_active,
    refiner_file_level_gate,
    upstream_analyze_path,
    upstream_blocks_path,
)


def test_queue_record_upstream_active_status_and_sizeleft() -> None:
    assert queue_record_upstream_active({"status": "downloading", "sizeleft": 0}) is True
    assert queue_record_upstream_active({"status": "completed", "sizeleft": 100}) is True
    assert queue_record_upstream_active({"status": "completed", "sizeleft": 0}) is False
    assert queue_record_upstream_active({"status": "failed", "sizeleft": 0}) is False


def test_queue_record_upstream_active_honors_sizeLeft_camelcase() -> None:
    """*arr queue JSON uses ``sizeLeft`` (Radarr OpenAPI / Servarr)."""
    assert queue_record_upstream_active({"status": "completed", "sizeLeft": 100, "sizeleft": 0}) is True


def test_file_gate_accepts_stable_nonempty_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = tmp_path / "ok.mkv"
    f.write_bytes(b"xyz" * 50)
    monkeypatch.setattr("app.refiner_source_readiness.time.sleep", lambda _s: None)
    ok, why = refiner_file_level_gate(f, strict=False)
    assert ok is True
    assert why == ""


def test_upstream_analyze_path_skipped_when_authority_not_useful(tmp_path: Path) -> None:
    f = tmp_path / "solo.mkv"
    f.write_bytes(b"x" * 30)
    snap = RefinerQueueSnapshot(False, False, False, False, (), ())
    blocked, rc, msg, diag = upstream_analyze_path(f, snap)
    assert (blocked, rc, msg) == (False, "", "")
    assert diag["upstream_scan_skipped"] is True
    assert upstream_blocks_path(f, snap) == (False, "", "")


def test_iter_queue_path_strings_radarr_joins_movie_path_and_moviefile_relative(tmp_path: Path) -> None:
    film = tmp_path / "Film Title"
    film.mkdir()
    rec = {
        "movie": {"path": str(film.resolve()), "rootFolderPath": str(tmp_path.resolve())},
        "movieFile": {"relativePath": "Film.Title.2024.mkv"},
    }
    paths = iter_queue_path_strings(rec)
    assert paths
    assert any("Film.Title.2024.mkv" in p for p in paths)


def test_upstream_radarr_blocks_movie_folder_prefix_when_file_in_subpath(tmp_path: Path) -> None:
    """Radarr queue often omits ``outputPath`` but includes ``movie.path`` (folder) while the client writes inside it."""
    movie_dir = tmp_path / "Example Movie (2024)"
    movie_dir.mkdir()
    f = movie_dir / "release.1080p.mkv"
    f.write_bytes(b"x" * 120)
    rec = {
        "status": "downloading",
        "sizeLeft": 0,
        "movie": {"path": str(movie_dir.resolve())},
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _msg, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"
    assert diag["radarr_active_path_samples"]


def test_upstream_radarr_blocks_moviefile_path_field(tmp_path: Path) -> None:
    f = tmp_path / "standalone.mkv"
    f.write_bytes(b"x" * 60)
    rec = {
        "status": "downloading",
        "sizeLeft": 1,
        "movieFile": {
            "path": str(f.resolve()),
            "relativePath": "standalone.mkv",
        },
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    assert upstream_blocks_path(f, snap)[0] is True


def test_upstream_radarr_title_fallback_blocks_when_no_paths_live_shape(tmp_path: Path) -> None:
    """Live Radarr queue shape: active row with title + sizeleft/tracked state, but no usable filesystem paths."""
    f = tmp_path / "Atlas.2024.1080p.WEB-DL.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "paused",
        "trackedDownloadState": "downloading",
        "sizeleft": 5_000_000,
        "title": "Atlas.2024.1080p.WEB-DL",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["upstream_block_match_kind"] == "title"
    assert diag["radarr_active_path_samples"] == []
    assert diag["active_queue_title_samples_radarr"]
    assert diag["title_fallback_used_radarr"] is True


def test_upstream_radarr_title_fallback_no_match_does_not_block(tmp_path: Path) -> None:
    f = tmp_path / "Different.Movie.2021.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "paused",
        "trackedDownloadState": "downloading",
        "sizeleft": 9_000_000,
        "title": "Some.Other.Release.2024.1080p.WEB-DL",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is False
    assert rc == ""
    assert diag["radarr_active_path_samples"] == []
    assert diag["title_fallback_used_radarr"] is False
    assert diag["upstream_block_match_kind"] == ""


def test_upstream_radarr_does_not_use_title_fallback_when_paths_present(tmp_path: Path) -> None:
    """Title fallback applies only when queue row has no path candidates."""
    f = tmp_path / "movie-file.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "paused",
        "sizeleft": 10_000_000,
        "title": "movie file",
        "outputPath": str((tmp_path / "some-other-file.mkv").resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is False
    assert rc == ""
    assert diag["title_fallback_used_radarr"] is False


def test_upstream_sonarr_blocks_episode_file_path(tmp_path: Path) -> None:
    f = tmp_path / "show.episode.mkv"
    f.write_bytes(b"x" * 70)
    rec = {
        "status": "downloading",
        "sizeLeft": 0,
        "episode": {
            "series": {"path": str(tmp_path.resolve()), "title": "Show"},
            "episodeFile": {"path": str(f.resolve())},
        },
    }
    snap = RefinerQueueSnapshot(False, True, False, True, (), (rec,))
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "sonarr_queue_active_download"
    assert diag["sonarr_active_path_samples"]


def test_upstream_blocks_when_path_matches_active_radarr_row(tmp_path: Path) -> None:
    f = tmp_path / "Movie.mkv"
    f.write_bytes(b"x" * 100)
    rec = {
        "status": "downloading",
        "sizeleft": 0,
        "outputPath": str(f.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _msg = upstream_blocks_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"
    _b2, _r2, _m2, diag = upstream_analyze_path(f, snap)
    assert _b2 is True and _r2 == rc
    assert diag["upstream_blocked"] is True
    assert diag["radarr_upstream_active_rows"] >= 1
    assert isinstance(diag.get("candidate_resolved"), str) and len(diag["candidate_resolved"]) > 0


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
