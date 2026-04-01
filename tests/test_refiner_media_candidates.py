"""Refiner discovers only allowlisted video extensions; repair/download sidecars are ignored."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import func, select

from app.db import SessionLocal, get_or_create_settings
from app.models import RefinerActivity
from app.refiner_rules import collect_media_files_under_path, is_refiner_media_candidate
from app.refiner_service import (
    _cleanup_refiner_source_sidecar_artifacts_after_success,
    _gather_watched_files,
    run_refiner_pass,
)


def test_is_refiner_media_candidate_rejects_par2_and_common_sidecars(tmp_path: Path) -> None:
    (tmp_path / "a.par2").write_bytes(b"x")
    (tmp_path / "b.PAR2").write_bytes(b"x")
    (tmp_path / "c.sfv").write_text("chk", encoding="utf-8")
    (tmp_path / "d.nfo").write_text("info", encoding="utf-8")
    (tmp_path / "e.mkv").write_bytes(b"x")
    assert is_refiner_media_candidate(tmp_path / "a.par2") is False
    assert is_refiner_media_candidate(tmp_path / "b.PAR2") is False
    assert is_refiner_media_candidate(tmp_path / "c.sfv") is False
    assert is_refiner_media_candidate(tmp_path / "d.nfo") is False
    assert is_refiner_media_candidate(tmp_path / "e.mkv") is True


def test_sidecar_cleanup_allowlist_only_under_watched(tmp_path: Path) -> None:
    watched = tmp_path / "w"
    sub = watched / "job"
    watched.mkdir()
    sub.mkdir()
    (sub / "x.par2").write_bytes(b"p")
    (sub / "x.sfv").write_text("x", encoding="utf-8")
    (sub / "x.nzb").write_bytes(b"n")
    (sub / "x.nfo").write_text("nfo", encoding="utf-8")
    (sub / "keep.txt").write_text("hold", encoding="utf-8")
    n = _cleanup_refiner_source_sidecar_artifacts_after_success(
        media_parent=sub, watched_root=watched
    )
    assert n == 4
    assert not (sub / "x.par2").exists()
    assert (sub / "keep.txt").exists()


def test_sidecar_cleanup_runs_when_media_parent_is_watch_root(tmp_path: Path) -> None:
    watched = tmp_path / "w"
    watched.mkdir()
    (watched / "orphan.par2").write_bytes(b"p")
    assert (
        _cleanup_refiner_source_sidecar_artifacts_after_success(
            media_parent=watched, watched_root=watched
        )
        == 1
    )
    assert not (watched / "orphan.par2").exists()


def test_sidecar_cleanup_skips_path_outside_watch_root(tmp_path: Path) -> None:
    watched = tmp_path / "w"
    other = tmp_path / "evil"
    watched.mkdir()
    other.mkdir()
    (other / "x.par2").write_bytes(b"p")
    assert (
        _cleanup_refiner_source_sidecar_artifacts_after_success(
            media_parent=other, watched_root=watched
        )
        == 0
    )
    assert (other / "x.par2").exists()


def test_collect_media_files_under_path_ignores_par2(tmp_path: Path) -> None:
    w = tmp_path / "root"
    w.mkdir()
    (w / "a.mkv").write_bytes(b"m")
    (w / "a.par2").write_bytes(b"p")
    assert collect_media_files_under_path(str(w)) == [str((w / "a.mkv").resolve())]


def test_gather_watched_files_ignores_par2_keeps_mkv(tmp_path: Path) -> None:
    w = tmp_path / "watched"
    w.mkdir()
    (w / "movie.mkv").write_bytes(b"m" * 200)
    (w / "movie.par2").write_bytes(b"p" * 50)
    sub = w / "nested"
    sub.mkdir()
    (sub / "other.par2").write_bytes(b"q")
    found = _gather_watched_files(w)
    assert [p.name for p in found] == ["movie.mkv"]


def test_run_refiner_par2_only_no_activity_no_processing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    async def _track_fetch(_row):  # noqa: ANN001
        calls.append("fetch")
        from app.refiner_source_readiness import RefinerQueueSnapshot

        return RefinerQueueSnapshot(False, False, False, False, (), ())

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", _track_fetch)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: (_ for _ in ()).throw(AssertionError("no probe")))

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "orphan.par2").write_bytes(b"x" * 40)
        async with SessionLocal() as session:
            n_before = (
                await session.execute(select(func.count()).select_from(RefinerActivity))
            ).scalar_one()
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
            n_after = (
                await session.execute(select(func.count()).select_from(RefinerActivity))
            ).scalar_one()
        assert r.get("ran") is False
        assert r.get("reason") == "no_files"
        assert int(n_after or 0) == int(n_before or 0)

    asyncio.run(_go())
    assert calls == []


def test_run_refiner_mkv_with_sidecar_par2_only_media_gets_activity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(_row):  # noqa: ANN001
        from app.refiner_source_readiness import RefinerQueueSnapshot

        return RefinerQueueSnapshot(False, False, False, False, (), ())

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", fake_fetch)
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: {
            "streams": [
                {"index": 0, "codec_type": "video"},
                {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}},
            ]
        },
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "clip.mkv").write_bytes(b"x" * 400)
        (watched / "clip.par2").write_bytes(b"p" * 80)
        async with SessionLocal() as session:
            n0 = (
                await session.execute(select(func.count()).select_from(RefinerActivity))
            ).scalar_one()
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            n1 = (
                await session.execute(select(func.count()).select_from(RefinerActivity))
            ).scalar_one()
            clip_rows = (
                (
                    await session.execute(
                        select(RefinerActivity).where(RefinerActivity.file_name == "clip.mkv")
                    )
                )
                .scalars()
                .all()
            )
        assert int(n1 or 0) - int(n0 or 0) == 1
        assert len(clip_rows) == 1

    asyncio.run(_go())
