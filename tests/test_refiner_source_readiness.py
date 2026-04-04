"""Unit tests for authority-first Refiner source readiness."""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

from app.models import AppSettings
from app.refiner_source_readiness import (
    RefinerQueueSnapshot,
    decide_refiner_readiness,
    derive_title_fallback_candidate,
    fetch_refiner_queue_snapshot,
    iter_queue_path_strings,
    queue_record_upstream_active,
    refiner_file_age_gate,
    refiner_file_level_gate,
    upstream_analyze_path,
    upstream_blocks_path,
)


def test_queue_record_upstream_active_tracked_state_and_sizeleft() -> None:
    assert queue_record_upstream_active({"trackedDownloadState": "downloading", "sizeleft": 0}) is True
    assert queue_record_upstream_active({"status": "completed", "sizeleft": 100}) is True
    assert queue_record_upstream_active({"status": "completed", "sizeleft": 0}) is False
    assert queue_record_upstream_active({"status": "failed", "sizeleft": 0}) is False


def test_queue_record_upstream_active_import_states_block() -> None:
    assert queue_record_upstream_active({"trackedDownloadState": "importPending", "sizeleft": 0}) is True
    assert queue_record_upstream_active({"trackedDownloadState": "importBlocked", "sizeleft": 0}) is True


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
        "trackedDownloadState": "downloading",
        "sizeLeft": 0,
        "movie": {"path": str(movie_dir.resolve())},
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _msg, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"
    assert diag["radarr_active_path_samples"]


def test_upstream_path_match_exact_file_path_equality_blocks(tmp_path: Path) -> None:
    f = tmp_path / "Exact.Match.2033.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "trackedDownloadState": "downloading",
        "sizeleft": 0,
        "outputPath": str(f.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"


def test_upstream_path_match_directory_vs_candidate_file_blocks(tmp_path: Path) -> None:
    d = tmp_path / "Movie.Name.2034.1080p"
    d.mkdir()
    f = d / "Movie.Name.2034.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "trackedDownloadState": "importPending",
        "sizeleft": 0,
        "outputPath": str(d.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"


def test_upstream_path_match_same_suffix_different_roots_blocks(tmp_path: Path) -> None:
    f = tmp_path / "root_a" / "Movies" / "Movie.Name.2035.2160p" / "Movie.Name.2035.2160p.mkv"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"x" * 80)
    rec = {
        "trackedDownloadState": "importBlocked",
        "sizeleft": 0,
        "outputPath": "Z:\\another_root\\Movies\\Movie.Name.2035.2160p",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"


def test_upstream_path_match_single_folder_suffix_different_roots_blocks(tmp_path: Path) -> None:
    f = tmp_path / "Completed-Movies" / "Sucker.Punch" / "Sucker.Punch.2011.1080p.mkv"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"x" * 80)
    rec = {
        "trackedDownloadState": "downloading",
        "sizeleft": 0,
        "outputPath": "F:\\Sucker.Punch\\",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"


def test_upstream_path_match_unrelated_paths_do_not_block(tmp_path: Path) -> None:
    f = tmp_path / "Downloads" / "Keep.This.2036.mkv"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"x" * 80)
    rec = {
        "trackedDownloadState": "downloading",
        "sizeleft": 0,
        "outputPath": "Z:\\unrelated\\different\\path",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is False
    assert rc == ""


def test_upstream_radarr_blocks_moviefile_path_field(tmp_path: Path) -> None:
    f = tmp_path / "standalone.mkv"
    f.write_bytes(b"x" * 60)
    rec = {
        "status": "downloading",
        "trackedDownloadState": "downloading",
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
    assert diag["title_fallback_candidate_source_radarr"] == "file_stem"


def test_upstream_radarr_title_fallback_prefix_case_blocks(tmp_path: Path) -> None:
    f = tmp_path / "Release.Name.2025.PROPER.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "paused",
        "sizeleft": 5_000_000,
        "title": "Release.Name.2025",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["title_fallback_match_norm_prefix_radarr"] is True


def test_upstream_radarr_title_fallback_blocks_when_match_not_first_active_row(tmp_path: Path) -> None:
    f = tmp_path / "Target.Movie.2026.1080p.WEB-DL.mkv"
    f.write_bytes(b"x" * 80)
    rows: list[dict] = [
        {"status": "paused", "sizeleft": 7_000_000, "title": "Other.Movie.2026.1080p.WEB-DL"},
        {"status": "paused", "sizeleft": 7_000_000, "title": "Target.Movie.2026.1080p.WEB-DL"},
        {"status": "paused", "sizeleft": 7_000_000, "title": "Unrelated.Title.2026.1080p"},
    ]
    snap = RefinerQueueSnapshot(True, False, True, False, tuple(rows), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["title_fallback_match_row_index_radarr"] == 1
    assert "Target.Movie.2026.1080p.WEB-DL" in (diag.get("title_fallback_match_title_radarr") or "")


def test_upstream_radarr_title_fallback_large_active_scan_keeps_correct_match(tmp_path: Path) -> None:
    f = tmp_path / "Big.Queue.Target.2027.2160p.HDR.mkv"
    f.write_bytes(b"x" * 80)
    rows: list[dict] = []
    for i in range(140):
        rows.append(
            {
                "status": "paused",
                "sizeleft": 1_000_000 + i,
                "title": f"Noise.Release.{i}.2027.1080p",
            }
        )
    # Match appears late and via movie.title fallback extraction.
    rows.append(
        {
            "status": "paused",
            "sizeleft": 2_000_000,
            "movie": {"title": "Big Queue Target 2027 2160p HDR"},
        }
    )
    rows.append({"status": "paused", "sizeleft": 2_000_001, "title": "Tail.Noise.2027.1080p"})
    snap = RefinerQueueSnapshot(True, False, True, False, tuple(rows), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["title_fallback_match_row_index_radarr"] == 140
    assert "Big Queue Target 2027 2160p HDR" in (diag.get("title_fallback_match_title_radarr") or "")
    assert len(diag.get("title_fallback_titles_considered_radarr") or []) > 0


def test_upstream_radarr_title_fallback_stops_after_first_match(tmp_path: Path) -> None:
    f = tmp_path / "Stop.Early.Target.2028.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rows: list[dict] = [
        {"status": "paused", "sizeleft": 3_000_000, "title": "Stop.Early.Target.2028.1080p"},
        {"status": "paused", "sizeleft": 3_100_000, "title": "Second.Possible.Match.2028.1080p"},
        {"status": "paused", "sizeleft": 3_200_000, "title": "Third.Possible.Match.2028.1080p"},
    ]
    snap = RefinerQueueSnapshot(True, False, True, False, tuple(rows), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    # If scanning continued after first match, row index would be > 0.
    assert diag["title_fallback_match_row_index_radarr"] == 0


def test_upstream_radarr_parent_folder_fallback_when_file_stem_not_release_like(tmp_path: Path) -> None:
    rel = tmp_path / "Movie.Name.2026.1080p.WEB-DL-GROUP"
    rel.mkdir()
    f = rel / "sample.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "paused",
        "sizeleft": 5_000_000,
        "title": "Movie.Name.2026.1080p.WEB-DL-GROUP",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["title_fallback_entered_radarr"] is True
    assert diag["title_fallback_candidate_source_radarr"] == "parent_folder"
    assert diag["upstream_block_match_kind"] == "title"


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


def test_upstream_radarr_non_matching_parent_folder_does_not_block(tmp_path: Path) -> None:
    rel = tmp_path / "Wrong.Release.2021"
    rel.mkdir()
    f = rel / "sample.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "paused",
        "sizeleft": 5_000_000,
        "title": "Movie.Name.2026.1080p.WEB-DL-GROUP",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is False
    assert rc == ""
    assert diag["title_fallback_entered_radarr"] is True
    assert diag["title_fallback_candidate_source_radarr"] == "parent_folder"
    assert diag["upstream_block_match_kind"] == ""


def test_upstream_radarr_junk_extracted_paths_still_trigger_title_fallback(tmp_path: Path) -> None:
    f = tmp_path / "Live.Release.2025.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "paused",
        "sizeleft": 7_000_000,
        "title": "Live.Release.2025.1080p",
        "movie": {"folderName": "Live.Release.2025.1080p"},  # non-path token (no slash)
        "movieFile": {"relativePath": "Live.Release.2025.1080p.mkv"},  # bare filename only
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["radarr_active_usable_path_count"] == 0
    assert diag["title_fallback_entered_radarr"] is True
    assert diag["title_fallback_used_radarr"] is True


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


def test_title_fallback_candidate_derivation_prefers_file_stem_when_release_like(tmp_path: Path) -> None:
    p = tmp_path / "Any Parent" / "Movie.Name.2027.2160p.HDR.mkv"
    p.parent.mkdir(parents=True)
    p.write_bytes(b"x")
    title, src = derive_title_fallback_candidate(p)
    assert src == "file_stem"
    assert title == "Movie.Name.2027.2160p.HDR"


def test_title_fallback_candidate_derivation_uses_parent_when_stem_not_release_like(tmp_path: Path) -> None:
    p = tmp_path / "Movie.Name.2027.2160p.HDR" / "sample.mkv"
    p.parent.mkdir(parents=True)
    p.write_bytes(b"x")
    title, src = derive_title_fallback_candidate(p)
    assert src == "parent_folder"
    assert title == "Movie.Name.2027.2160p.HDR"


def test_upstream_radarr_inactive_pathless_row_does_not_block(tmp_path: Path) -> None:
    f = tmp_path / "active.name.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "completed",
        "sizeleft": 0,
        "title": "active.name",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is False
    assert rc == ""
    assert diag["title_fallback_entered_radarr"] is False


def test_upstream_radarr_title_match_uses_candidate_stem_not_full_path(tmp_path: Path) -> None:
    deep = tmp_path / "Some Folder" / "Nested"
    deep.mkdir(parents=True)
    f = deep / "The.Movie.2026.2160p.HDR.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "paused",
        "sizeleft": 1_000_000,
        "title": "The.Movie.2026.2160p.HDR",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["title_fallback_candidate_title_radarr"] == "The.Movie.2026.2160p.HDR"
    assert diag["title_fallback_candidate_source_radarr"] == "file_stem"
    assert "Some Folder" not in (diag.get("candidate_stem_norm") or "")


def test_upstream_sonarr_blocks_episode_file_path(tmp_path: Path) -> None:
    f = tmp_path / "show.episode.mkv"
    f.write_bytes(b"x" * 70)
    rec = {
        "status": "downloading",
        "trackedDownloadState": "downloading",
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
        "trackedDownloadState": "downloading",
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


def test_upstream_path_match_keeps_path_reason_even_if_title_also_matches(tmp_path: Path) -> None:
    f = tmp_path / "Path.And.Title.2029.1080p.mkv"
    f.write_bytes(b"x" * 100)
    rec = {
        "status": "downloading",
        "trackedDownloadState": "downloading",
        "sizeleft": 10,
        "outputPath": str(f.resolve()),
        "title": "Path.And.Title.2029.1080p",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"
    assert diag["upstream_block_match_kind"] == "path"


def test_decide_authority_blocks_before_file_gate(tmp_path: Path) -> None:
    f = tmp_path / "blocked.mkv"
    f.write_bytes(b"x" * 50)
    rec = {
        "status": "downloading",
        "trackedDownloadState": "downloading",
        "sizeleft": 0,
        "outputPath": str(f.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    row = AppSettings()

    async def _run() -> None:
        d = await decide_refiner_readiness(f, row, snapshot=snap, gate_tag="initial")
        assert d.proceed is False
        assert d.reason_code == "radarr_queue_active_download"

    asyncio.run(_run())


def test_upstream_radarr_importpending_title_match_blocks(tmp_path: Path) -> None:
    f = tmp_path / "Import.Pending.Target.2030.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "completed",
        "trackedDownloadState": "importPending",
        "sizeleft": 0,
        "title": "Import.Pending.Target.2030.1080p",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["upstream_block_match_kind"] == "title"


def test_upstream_radarr_importpending_warning_no_eligible_does_not_block(tmp_path: Path) -> None:
    f = tmp_path / "Deadlock.Case.2040.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "completed",
        "trackedDownloadState": "importPending",
        "trackedDownloadStatus": "warning",
        "sizeleft": 0,
        "message": "No files found are eligible for import",
        "title": "Deadlock.Case.2040.1080p",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is False
    assert rc == ""


def test_upstream_radarr_importpending_completed_warning_path_match_does_not_block(tmp_path: Path) -> None:
    f = tmp_path / "Deadlock.Path.Match.2043.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "completed",
        "trackedDownloadState": "importPending",
        "trackedDownloadStatus": "warning",
        "sizeleft": 0,
        "outputPath": str(f.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is False
    assert rc == ""


def test_upstream_radarr_importpending_warning_non_completed_still_blocks(tmp_path: Path) -> None:
    f = tmp_path / "Import.Pending.Still.Active.2042.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "downloading",
        "trackedDownloadState": "importPending",
        "trackedDownloadStatus": "warning",
        "sizeleft": 0,
        "outputPath": str(f.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"


def test_upstream_radarr_importpending_completed_warning_sizeleft_nonzero_blocks(tmp_path: Path) -> None:
    f = tmp_path / "Import.Pending.Still.Downloading.2044.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "completed",
        "trackedDownloadState": "importPending",
        "trackedDownloadStatus": "warning",
        "sizeleft": 1024,
        "outputPath": str(f.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"


def test_upstream_radarr_importblocked_title_match_blocks(tmp_path: Path) -> None:
    f = tmp_path / "Import.Blocked.Target.2031.2160p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "completed",
        "trackedDownloadState": "importBlocked",
        "sizeleft": 0,
        "title": "Import.Blocked.Target.2031.2160p",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download_title"
    assert diag["upstream_block_match_kind"] == "title"


def test_upstream_radarr_completed_no_import_state_does_not_block(tmp_path: Path) -> None:
    f = tmp_path / "Completed.No.Import.State.2032.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "completed",
        "trackedDownloadState": "",
        "sizeleft": 0,
        "title": "Completed.No.Import.State.2032.1080p",
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is False
    assert rc == ""


def test_upstream_radarr_downloading_still_blocks(tmp_path: Path) -> None:
    f = tmp_path / "Still.Downloading.2041.1080p.mkv"
    f.write_bytes(b"x" * 80)
    rec = {
        "status": "downloading",
        "trackedDownloadState": "downloading",
        "trackedDownloadStatus": "ok",
        "sizeleft": 0,
        "outputPath": str(f.resolve()),
    }
    snap = RefinerQueueSnapshot(True, False, True, False, (rec,), ())
    blocked, rc, _m, _diag = upstream_analyze_path(f, snap)
    assert blocked is True
    assert rc == "radarr_queue_active_download"


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


def test_file_age_gate_rejects_recently_modified_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """File modified just now is not ready."""
    f = tmp_path / "new.mkv"
    f.write_bytes(b"x" * 1000)
    # mtime is right now — age is ~0s
    ok, why = refiner_file_age_gate(f, minimum_age_seconds=60)
    assert ok is False
    assert "minimum age" in why.lower() or "waiting" in why.lower()


def test_file_age_gate_accepts_old_stable_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """File last modified well before minimum age passes."""
    f = tmp_path / "old.mkv"
    f.write_bytes(b"x" * 1000)
    # Set mtime to 120 seconds ago
    old_time = time.time() - 120
    os.utime(f, (old_time, old_time))
    ok, why = refiner_file_age_gate(f, minimum_age_seconds=60)
    assert ok is True
    assert why == ""


def test_file_age_gate_rejects_missing_file(
    tmp_path: Path,
) -> None:
    """Missing file is not ready."""
    f = tmp_path / "missing.mkv"
    ok, why = refiner_file_age_gate(f, minimum_age_seconds=60)
    assert ok is False
    assert "missing" in why.lower() or "not a regular file" in why.lower()


def test_file_age_gate_rejects_empty_file(
    tmp_path: Path,
) -> None:
    """Empty file is not ready."""
    f = tmp_path / "empty.mkv"
    f.write_bytes(b"")
    old_time = time.time() - 120
    os.utime(f, (old_time, old_time))
    ok, why = refiner_file_age_gate(f, minimum_age_seconds=60)
    assert ok is False
    assert "empty" in why.lower()


def test_file_age_gate_rejects_growing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """File that grows between samples is not ready."""
    import pathlib

    f = (tmp_path / "growing.mkv").resolve()
    f.write_bytes(b"x" * 1000)
    old_time = time.time() - 120
    os.utime(f, (old_time, old_time))
    call_count = {"n": 0}
    real_path_stat = pathlib.Path.stat

    def patched_path_stat(self: Path, *, follow_symlinks: bool = True):
        r = real_path_stat(self, follow_symlinks=follow_symlinks)
        try:
            same = self.resolve() == f
        except OSError:
            same = False
        if not same:
            return r
        call_count["n"] += 1
        if call_count["n"] == 2:
            seq = list(r)
            seq[6] = int(r.st_size) + 1000
            return os.stat_result(seq)
        return r

    monkeypatch.setattr(pathlib.Path, "stat", patched_path_stat)
    ok, why = refiner_file_age_gate(f, minimum_age_seconds=60)
    assert ok is False
    assert "changing" in why.lower() or "still" in why.lower()
