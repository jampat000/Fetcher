"""Refiner source readiness gate (stable, locked, too-fresh)."""

from __future__ import annotations

import asyncio
import os
import threading
import time
from pathlib import Path

import pytest

from app.db import SessionLocal, _get_or_create_settings
from collections import defaultdict

from app.refiner_source_readiness import (
    REFINER_MIN_SILENCE_AFTER_MTIME_SEC,
    REFINER_STABILITY_EXTRA_SAMPLES,
    REFINER_STABILITY_SAMPLE_INTERVAL_SEC,
    SourceReadinessResult,
    check_source_readiness,
    clear_readiness_log_throttle_for_tests,
    log_readiness_skip_throttled,
    path_looks_like_incomplete_download,
)
from app.refiner_service import run_refiner_pass


def _aged_file(path: Path, *, content: bytes = b"x" * 2000, age_sec: float = 120.0) -> None:
    path.write_bytes(content)
    t = time.time() - age_sec
    os.utime(path, (t, t))


def test_readiness_missing_file(tmp_path: Path) -> None:
    p = tmp_path / "nope.mkv"
    r = check_source_readiness(p)
    assert not r.ready
    assert r.code == "not_ready_missing"


def test_readiness_empty_file(tmp_path: Path) -> None:
    p = tmp_path / "empty.mkv"
    p.write_bytes(b"")
    old = time.time() - 100
    os.utime(p, (old, old))
    r = check_source_readiness(p)
    assert not r.ready
    assert r.code == "not_ready_missing"


def test_readiness_too_fresh_by_mtime(tmp_path: Path) -> None:
    p = tmp_path / "new.mkv"
    p.write_bytes(b"abc")
    # mtime = now → below silence threshold (no lock/stability path)
    r = check_source_readiness(p)
    assert not r.ready
    assert r.code == "not_ready_too_fresh"


def test_readiness_stable_old_file(tmp_path: Path) -> None:
    p = tmp_path / "ok.mkv"
    _aged_file(p)
    t0 = time.perf_counter()
    r = check_source_readiness(p)
    elapsed = time.perf_counter() - t0
    assert r.ready
    assert r.code == "ready"
    max_sleep = float(REFINER_STABILITY_SAMPLE_INTERVAL_SEC) * int(REFINER_STABILITY_EXTRA_SAMPLES)
    assert elapsed < max_sleep + 0.75


def test_readiness_unstable_when_size_changes(tmp_path: Path) -> None:
    p = tmp_path / "grow.mkv"
    _aged_file(p, content=b"start" * 200)
    barrier = threading.Barrier(2)

    def _grow() -> None:
        barrier.wait()
        time.sleep(REFINER_STABILITY_SAMPLE_INTERVAL_SEC * 0.35)
        with open(p, "ab", buffering=0) as f:
            f.write(b"more-bytes-added")

    th = threading.Thread(target=_grow, daemon=True)
    th.start()
    barrier.wait()
    r = check_source_readiness(p)
    th.join(timeout=2.0)
    assert not r.ready
    assert r.code == "not_ready_unstable"


@pytest.mark.skipif(os.name == "nt", reason="flock lock interaction differs on Windows")
def test_readiness_locked_posix(tmp_path: Path) -> None:
    import fcntl

    p = tmp_path / "lock.mkv"
    _aged_file(p)

    hold = threading.Event()
    done = threading.Event()

    def _hold() -> None:
        f = open(p, "rb")  # noqa: SIM115
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            hold.set()
            done.wait(timeout=3.0)
        finally:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            f.close()

    th = threading.Thread(target=_hold, daemon=True)
    th.start()
    assert hold.wait(timeout=2.0)
    try:
        r = check_source_readiness(p)
    finally:
        done.set()
        th.join(timeout=2.0)
    assert not r.ready
    assert r.code == "not_ready_locked"


def test_log_throttle_dedupes_rapid_calls(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    clear_readiness_log_throttle_for_tests()
    import logging

    caplog.set_level(logging.INFO)
    p = tmp_path / "x.mkv"
    p.write_bytes(b"a")
    res = SourceReadinessResult(False, "not_ready_too_fresh", "")
    log_readiness_skip_throttled(p, res)
    log_readiness_skip_throttled(p, res)
    hits = [r for r in caplog.records if "Refiner:" in r.getMessage() and "skipping" in r.getMessage()]
    assert len(hits) == 1


def test_run_refiner_pass_skips_not_ready_without_activity_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Not-ready sources never enqueue RefinerActivity rows (no failure spam)."""
    from sqlalchemy import delete, select

    from app.models import RefinerActivity

    clear_readiness_log_throttle_for_tests()

    watched = tmp_path / "in"
    out = tmp_path / "out"
    watched.mkdir()
    out.mkdir()
    busy = watched / "busy.mkv"
    busy.write_bytes(b"partial")

    def _fake(p: Path) -> SourceReadinessResult:
        try:
            if p.resolve() == busy.resolve():
                return SourceReadinessResult(False, "not_ready_too_fresh", "test")
        except OSError:
            pass
        return SourceReadinessResult(True, "ready", "")

    monkeypatch.setattr("app.refiner_service.check_source_readiness", _fake)

    async def _go() -> None:
        async with SessionLocal() as session:
            await session.execute(delete(RefinerActivity))
            await session.commit()
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(out)
            await session.commit()
            result = await run_refiner_pass(session, trigger="scheduled")
            assert result.get("reason") == "no_ready_sources"
            assert result.get("skipped_not_ready") == 1
            n = (
                await session.execute(select(RefinerActivity).where(RefinerActivity.file_name == "busy.mkv"))
            ).scalars().all()
            assert len(n) == 0

    asyncio.run(_go())


def test_path_looks_like_incomplete_download_suffix(tmp_path: Path) -> None:
    p = tmp_path / "x.mkv.part"
    p.write_bytes(b"a")
    assert path_looks_like_incomplete_download(p) is True


def test_path_looks_like_incomplete_qbittorrent_sidecar(tmp_path: Path) -> None:
    p = tmp_path / "movie.mkv"
    p.write_bytes(b"a")
    side = tmp_path / "movie.mkv.!qB"
    side.write_bytes(b"1")
    assert path_looks_like_incomplete_download(p) is True


def test_run_refiner_rechecks_readiness_immediately_before_processing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Second readiness call per file (pre-process) must run — catches stale FIFO queue state."""
    from sqlalchemy import delete, select

    from app.models import RefinerActivity

    clear_readiness_log_throttle_for_tests()

    watched = tmp_path / "in"
    out = tmp_path / "out"
    watched.mkdir()
    out.mkdir()
    a = watched / "a.mkv"
    b = watched / "b.mkv"
    a.write_bytes(b"a" * 3000)
    b.write_bytes(b"b" * 3000)
    old = time.time() - 120
    os.utime(a, (old, old))
    os.utime(b, (old, old))

    calls: defaultdict[str, int] = defaultdict(int)

    def _checker(p: Path) -> SourceReadinessResult:
        calls[p.name] += 1
        if p.name == "b.mkv" and calls[p.name] >= 2:
            return SourceReadinessResult(False, "not_ready_too_fresh", "deferred recheck")
        return SourceReadinessResult(True, "ready", "")

    monkeypatch.setattr("app.refiner_service.check_source_readiness", _checker)
    monkeypatch.setattr(
        "app.refiner_service.ffprobe_json",
        lambda _p: {
            "streams": [
                {"index": 0, "codec_type": "video"},
                {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}},
            ]
        },
    )
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        async with SessionLocal() as session:
            await session.execute(delete(RefinerActivity))
            await session.commit()
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(out)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            rows_b = (
                await session.execute(select(RefinerActivity).where(RefinerActivity.file_name == "b.mkv"))
            ).scalars().all()
            assert len(rows_b) == 1
            assert (rows_b[0].status or "").strip().lower() == "skipped"

    asyncio.run(_go())
    assert calls["b.mkv"] >= 2


def test_min_silence_constant_is_documented() -> None:
    assert REFINER_MIN_SILENCE_AFTER_MTIME_SEC >= 45.0
    assert REFINER_STABILITY_SAMPLE_INTERVAL_SEC >= 1.0
    assert REFINER_STABILITY_EXTRA_SAMPLES >= 1
