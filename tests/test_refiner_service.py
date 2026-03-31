from __future__ import annotations

import asyncio
import importlib
import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

from sqlalchemy import delete, func, select

from app.db import SessionLocal, _get_or_create_settings
from app.models import ActivityLog, JobRunLog, RefinerActivity
from app.time_util import utc_now_naive
from app.refiner_rules import RefinerRulesConfig
from app.refiner_service import (
    _finalize_output_file,
    _pick_primary_actionable_reason,
    _pipeline_from_settings,
    _process_one_refiner_file_sync,
    _reconcile_interrupted_refiner_processing_rows_before_pass,
    _refiner_run_parent_summary,
    _rules_config_from_settings,
    _set_refiner_pass_job_status,
    _try_remove_empty_watch_subfolder,
    reconcile_refiner_processing_rows_on_worker_boot,
    run_refiner_pass,
)
from app.schema_version import CURRENT_SCHEMA_VERSION


def _age_refiner_watch_source(path: Path, *, age_sec: float = 120.0) -> None:
    """Backdate mtime/atime so ``check_source_readiness`` treats the file as past the quiet window."""
    t = time.time() - age_sec
    os.utime(path, (t, t))


def _fake_probe_multi_audio() -> dict:
    return {
        "streams": [
            {"index": 0, "codec_type": "video"},
            {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}},
            {"index": 2, "codec_type": "audio", "tags": {"language": "spa"}, "disposition": {}},
        ]
    }


def _fake_probe_single_eng() -> dict:
    return {
        "format": {"tags": {"title": "ZZZ Junk Tag"}},
        "streams": [
            {"index": 0, "codec_type": "video"},
            {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}},
        ],
    }


def test_dry_run_no_file_changes(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls: list[int] = []
    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", lambda *_a, **_k: calls.append(1))
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"original")
        _age_refiner_watch_source(f)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert calls == []
        assert int(r.get("dry_run_items") or 0) >= 1
        assert f.exists()
        assert not (output / "m.mkv").exists()

    asyncio.run(_go())


def test_live_run_moves_to_output_and_deletes_source(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls: list[Path] = []

    def _fake_remux(*, src, work_dir, plan):  # noqa: ANN001
        calls.append(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / f"{src.stem}.tmp{src.suffix}"
        out.write_bytes(b"ok")
        return out

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "a" / "m.mkv"
        f.parent.mkdir(parents=True)
        f.write_bytes(b"original")
        _age_refiner_watch_source(f)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert len(calls) == 1
        assert r.get("remuxed") == 1
        assert not f.exists()
        assert (output / "a" / "m.mkv").exists()
        assert not (watched / "a").exists()

    asyncio.run(_go())


def test_live_no_remux_copies_to_output_removes_source_and_empty_folder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    calls: list[int] = []

    def _no_remux(*_a, **_k):
        calls.append(1)
        raise AssertionError("remux should not run when no stream changes are required")

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _no_remux)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        sub = watched / "sub"
        sub.mkdir(parents=True)
        f = sub / "clean.file.name.1080p.mkv"
        f.write_bytes(b"payload")
        _age_refiner_watch_source(f)
        output.mkdir()
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert calls == []
        assert r.get("remuxed") == 1
        assert not f.exists()
        assert (output / "sub" / "clean.file.name.1080p.mkv").read_bytes() == b"payload"
        assert not sub.exists()

    asyncio.run(_go())


def test_live_no_remux_leaves_folder_when_other_files_remain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        sub = watched / "sub"
        sub.mkdir(parents=True)
        f = sub / "one.mkv"
        f.write_bytes(b"x")
        _age_refiner_watch_source(f)
        # Dedicated release folder: subtitle sidecar must survive residue cleanup (.txt would be removed).
        (sub / "keep.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nhold\n", encoding="utf-8")
        output.mkdir()
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
        assert not f.exists()
        assert (output / "sub" / "one.mkv").exists()
        assert sub.is_dir()
        assert (sub / "keep.srt").exists()

    asyncio.run(_go())


def test_dry_run_no_remux_does_not_copy_or_delete(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "solo.mkv"
        f.write_bytes(b"orig")
        _age_refiner_watch_source(f)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert int(r.get("dry_run_items") or 0) >= 1
        assert f.exists()
        assert not (output / "solo.mkv").exists()

    asyncio.run(_go())


def test_try_remove_empty_skips_watch_root(tmp_path: Path) -> None:
    watched = tmp_path / "w"
    watched.mkdir()
    assert (
        _try_remove_empty_watch_subfolder(source_parent=watched, watched_root=watched)
        == "skipped_watch_root"
    )


def test_source_preserved_on_failure(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    def _boom(*_a, **_k):
        raise RuntimeError("ffmpeg failed")

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _boom)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"original")
        _age_refiner_watch_source(f)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert r.get("errors") == 1
        assert r.get("ok") is False
        assert f.read_bytes() == b"original"
        assert not (output / "m.mkv").exists()

    asyncio.run(_go())


def test_custom_work_folder_used(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    seen: list[Path] = []

    def _fake_remux(*, src, work_dir, plan):  # noqa: ANN001
        seen.append(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / f"{src.stem}.tmp{src.suffix}"
        out.write_bytes(b"ok")
        return out

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        work = tmp_path / "work-custom"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"orig")
        _age_refiner_watch_source(f)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_work_folder = str(work)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
        assert seen and seen[0] == work.resolve()

    asyncio.run(_go())


def test_missing_watched_or_output_folders_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(tmp_path / "watched")
            row.refiner_output_folder = ""
            await session.commit()
            result = await run_refiner_pass(session, trigger="scheduled")
        assert result.get("ok") is False
        assert result.get("error") == "folders_required"

    asyncio.run(_go())


def test_default_work_folder_usage(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    seen: list[Path] = []

    def _fake_remux(*, src, work_dir, plan):  # noqa: ANN001
        seen.append(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / f"{src.stem}.tmp{src.suffix}"
        out.write_bytes(b"ok")
        return out

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"orig")
        _age_refiner_watch_source(f)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_work_folder = ""
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
        assert seen
        assert seen[0].name == "refiner-work"

    asyncio.run(_go())


def test_pipeline_settings_parser(tmp_path) -> None:
    class _Row:
        refiner_watched_folder = str(tmp_path / "watched")
        refiner_output_folder = str(tmp_path / "out")
        refiner_work_folder = str(tmp_path / "work")

    watched, output, work = _pipeline_from_settings(_Row())  # type: ignore[arg-type]
    assert watched is not None and watched.name == "watched"
    assert output is not None and output.name == "out"
    assert work is not None and work.name == "work"


def test_pipeline_settings_accepts_posix_style_paths(tmp_path) -> None:
    """Docker/Linux-style absolute paths must parse (no drive-letter assumptions)."""
    root = tmp_path / "container"
    (root / "downloads").mkdir(parents=True)
    (root / "output").mkdir(parents=True)
    (root / "work").mkdir(parents=True)

    class _Row:
        refiner_watched_folder = str(root / "downloads")
        refiner_output_folder = str(root / "output")
        refiner_work_folder = str(root / "work")

    watched, output, work = _pipeline_from_settings(_Row())  # type: ignore[arg-type]
    assert watched is not None and watched.name == "downloads"
    assert output is not None and output.name == "output"
    assert work is not None and work.name == "work"


def test_rules_config_parses_dropdown_values() -> None:
    class _Row:
        refiner_enabled = True
        refiner_primary_audio_lang = "eng"
        refiner_secondary_audio_lang = "spa"
        refiner_tertiary_audio_lang = ""
        refiner_default_audio_slot = "secondary"
        refiner_remove_commentary = True
        refiner_subtitle_mode = "remove_all"
        refiner_subtitle_langs_csv = ""
        refiner_preserve_forced_subs = True
        refiner_preserve_default_subs = True
        refiner_audio_preference_mode = "best_available"

    cfg = _rules_config_from_settings(_Row())  # type: ignore[arg-type]
    assert cfg is not None
    assert cfg.primary_audio_lang == "eng"
    assert cfg.secondary_audio_lang == "spa"
    assert cfg.default_audio_slot == "secondary"
    assert cfg.audio_preference_mode == "preferred_langs_quality"


def test_refiner_schema_contract_v35_activity_context_media_title_and_trimmer_activity() -> None:
    assert CURRENT_SCHEMA_VERSION == 36
    from app.models import AppSettings, RefinerActivity

    assert "refiner_processing_pass_generation" not in AppSettings.__annotations__
    assert "processing_pass_generation" not in RefinerActivity.__annotations__
    assert "failure_hint" not in RefinerActivity.__annotations__
    assert "activity_context" in RefinerActivity.__annotations__
    assert "media_title" in RefinerActivity.__annotations__
    migrations_text = (Path(__file__).resolve().parents[1] / "app" / "migrations.py").read_text(
        encoding="utf-8"
    )
    assert "_migrate_033_refiner_activity_context" in migrations_text
    assert "_migrate_035_activity_log_trimmer_app_identity" in migrations_text
    assert "_migrate_036_refiner_activity_media_title" in migrations_text
    assert "_migrate_034_forward_app_settings_schema_version" in migrations_text
    assert "repair_refiner_app_settings_columns" in migrations_text
    assert "refiner_processing_pass_generation" not in migrations_text


def test_finalize_output_exclusive_write_cross_dir(tmp_path) -> None:
    """Stream-copy into a same-dir partial on the destination tree, then promote with os.replace."""
    src = tmp_path / "work" / "src.tmp.mkv"
    dst = tmp_path / "out" / "nested" / "final.mkv"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(b"remux-bytes")
    _finalize_output_file(src, dst)
    assert dst.read_bytes() == b"remux-bytes"
    assert not src.exists()
    assert not list(dst.parent.glob("*.refiner-*.tmp"))


def test_finalize_output_cross_root_stream_copy(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Work temp and output in different sub-trees still copy then promote on destination volume."""
    src = tmp_path / "volA" / "work" / "t.mkv"
    dst = tmp_path / "volB" / "out" / "f.mkv"
    src.parent.mkdir(parents=True, exist_ok=True)
    dst.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(b"xyz")
    replaced: list[tuple[str, str]] = []
    _real_replace = importlib.import_module("os").replace

    def _track_replace(a: str, b: str) -> None:
        replaced.append((a, b))
        _real_replace(a, b)

    monkeypatch.setattr("app.refiner_service.os.replace", _track_replace)

    _finalize_output_file(src, dst)
    assert dst.read_bytes() == b"xyz"
    assert not src.exists()
    assert len(replaced) == 1
    assert Path(replaced[0][1]) == dst


def test_finalize_output_rejects_pre_existing_destination(tmp_path) -> None:
    src = tmp_path / "t.mkv"
    dst = tmp_path / "out" / "final.mkv"
    src.write_bytes(b"x")
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(b"occupied")
    with pytest.raises(RuntimeError, match="already exists|appeared|another writer"):
        _finalize_output_file(src, dst)
    assert src.exists()


def test_finalize_output_copyfileobj_error_unlinks_partial_dst(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    src = tmp_path / "src.tmp"
    dst = tmp_path / "out" / "final.mkv"
    src.write_bytes(b"full")

    def _boom(*_a, **_k):
        raise OSError(28, "no space")

    monkeypatch.setattr("app.refiner_service.shutil.copyfileobj", _boom)
    with pytest.raises(OSError):
        _finalize_output_file(src, dst)
    assert src.exists()
    assert not dst.exists()


def test_reconcile_before_pass_closes_all_processing_rows() -> None:
    """Schema-free orphan handling: any processing row is closed before a new pass inserts rows."""

    async def _go() -> None:
        async with SessionLocal() as s:
            r1 = RefinerActivity(file_name="orphan-a.mkv", status="processing")
            r2 = RefinerActivity(file_name="orphan-b.mkv", status="processing")
            s.add_all([r1, r2])
            await s.commit()
            id1, id2 = r1.id, r2.id
        await _reconcile_interrupted_refiner_processing_rows_before_pass()
        async with SessionLocal() as s:
            for rid in (id1, id2):
                row = (await s.execute(select(RefinerActivity).where(RefinerActivity.id == rid))).scalars().first()
                assert row is not None
                assert row.status == "failed"
            await s.execute(delete(RefinerActivity).where(RefinerActivity.id.in_((id1, id2))))
            await s.commit()

    asyncio.run(_go())


def test_reconcile_before_pass_closes_finalizing_rows() -> None:
    """Interrupted ``finalizing`` rows are closed before a new pass (same as processing/queued)."""

    async def _go() -> None:
        async with SessionLocal() as s:
            r = RefinerActivity(file_name="stuck-final.mkv", status="finalizing")
            s.add(r)
            await s.commit()
            rid = r.id
        await _reconcile_interrupted_refiner_processing_rows_before_pass()
        async with SessionLocal() as s:
            row = (await s.execute(select(RefinerActivity).where(RefinerActivity.id == rid))).scalars().first()
            assert row is not None
            assert row.status == "failed"
            await s.execute(delete(RefinerActivity).where(RefinerActivity.id == rid))
            await s.commit()

    asyncio.run(_go())


def test_reconcile_before_pass_closes_queued_rows() -> None:
    """Queued rows from an interrupted pass are closed like processing rows."""

    async def _go() -> None:
        async with SessionLocal() as s:
            rq = RefinerActivity(file_name="q.mkv", status="queued")
            s.add(rq)
            await s.commit()
            rid = rq.id
        await _reconcile_interrupted_refiner_processing_rows_before_pass()
        async with SessionLocal() as s:
            row = (await s.execute(select(RefinerActivity).where(RefinerActivity.id == rid))).scalars().first()
            assert row is not None
            assert row.status == "failed"
            await s.execute(delete(RefinerActivity).where(RefinerActivity.id == rid))
            await s.commit()

    asyncio.run(_go())


def test_worker_boot_reconcile_closes_processing_rows() -> None:
    async def _go() -> None:
        async with SessionLocal() as s:
            r = RefinerActivity(file_name="boot-orphan.mkv", status="processing")
            s.add(r)
            await s.commit()
            rid = r.id
        await reconcile_refiner_processing_rows_on_worker_boot()
        async with SessionLocal() as s:
            row = (await s.execute(select(RefinerActivity).where(RefinerActivity.id == rid))).scalars().first()
            assert row is not None
            assert row.status == "failed"
            await s.execute(delete(RefinerActivity).where(RefinerActivity.id == rid))
            await s.commit()

    asyncio.run(_go())


def test_source_missing_skipped_at_readiness_gate(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        missing = watched / "ghost.mkv"
        monkeypatch.setattr(
            "app.refiner_service._gather_watched_files",
            lambda _w: [missing],
        )
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert r.get("reason") == "no_ready_sources"
        assert r.get("skipped_not_ready") == 1
        assert not missing.exists()

    asyncio.run(_go())


def test_refiner_pass_calls_insert_then_update_per_file(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls: list[tuple[str, Any]] = []

    async def _fake_insert(name: str, *, initial_status: str, source_path: str = "") -> int:
        calls.append(("insert", name, initial_status))
        return 901

    async def _fake_status(rid: int, status: str) -> None:
        calls.append(("status", rid, status))

    async def _fake_update(rid: int, meta: dict) -> None:
        calls.append(("update", rid, meta.get("status")))

    monkeypatch.setattr("app.refiner_service._insert_refiner_pass_job_row", _fake_insert)
    monkeypatch.setattr("app.refiner_service._set_refiner_pass_job_status", _fake_status)
    monkeypatch.setattr("app.refiner_service._update_refiner_activity_row", _fake_update)
    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "m.mkv").write_bytes(b"x")
        _age_refiner_watch_source(watched / "m.mkv")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")

    asyncio.run(_go())
    assert ("insert", "m.mkv", "queued") in calls
    assert ("status", 901, "processing") in calls
    assert ("update", 901, "skipped") in calls


def test_refiner_pass_processes_files_strict_fifo_order(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Second file is not processed until the first sync handler returns (serial pipeline)."""
    order: list[str] = []

    def _fake_sync(path, *_args, **_kwargs):
        order.append(path.name)
        return (
            "dry_run",
            {
                "file_name": path.name,
                "media_title": path.name,
                "status": "skipped",
                "size_before_bytes": 1,
                "size_after_bytes": 1,
                "audio_tracks_before": 1,
                "audio_tracks_after": 1,
                "subtitle_tracks_before": 0,
                "subtitle_tracks_after": 0,
                "processing_time_ms": 1,
                "activity_context": json.dumps({"v": 1, "dry_run": True}),
            },
        )

    monkeypatch.setattr("app.refiner_service._process_one_refiner_file_sync", _fake_sync)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "b.mkv").write_bytes(b"x")
        (watched / "a.mkv").write_bytes(b"y")
        _age_refiner_watch_source(watched / "b.mkv")
        _age_refiner_watch_source(watched / "a.mkv")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")

    asyncio.run(_go())
    assert order == ["a.mkv", "b.mkv"]


def test_enter_finalizing_runs_before_finalize_output_no_remux(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """``enter_finalizing`` runs once immediately before ``_finalize_output_file`` (no-remux path)."""
    order: list[str] = []

    def notify() -> None:
        order.append("finalizing")

    promote_calls: list[str] = []

    def wrap_finalize(src: Path, dst: Path) -> None:
        promote_calls.append("promote")
        return _finalize_output_file(src, dst)

    monkeypatch.setattr("app.refiner_service._finalize_output_file", wrap_finalize)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)
    watched = tmp_path / "watch"
    output = tmp_path / "out"
    watched.mkdir()
    output.mkdir()
    f = watched / "one.mkv"
    f.write_bytes(b"body")
    _age_refiner_watch_source(f)
    work = tmp_path / "work"
    cfg = RefinerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        tertiary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=False,
        preserve_default_subs=False,
        audio_preference_mode="preferred_langs_quality",
    )
    code, meta = _process_one_refiner_file_sync(
        f, cfg, False, watched, output, work, enter_finalizing=notify
    )
    assert code == "ok"
    assert meta.get("status") == "success"
    assert order == ["finalizing"]
    assert promote_calls == ["promote"]


def test_dry_run_does_not_invoke_enter_finalizing(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    hits: list[str] = []

    def notify() -> None:
        hits.append("x")

    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)
    watched = tmp_path / "watch"
    output = tmp_path / "out"
    watched.mkdir()
    output.mkdir()
    f = watched / "one.mkv"
    f.write_bytes(b"body")
    _age_refiner_watch_source(f)
    cfg = RefinerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        tertiary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=False,
        preserve_default_subs=False,
        audio_preference_mode="preferred_langs_quality",
    )
    code, _meta = _process_one_refiner_file_sync(
        f, cfg, True, watched, output, tmp_path / "w", enter_finalizing=notify
    )
    assert code == "dry_run"
    assert hits == []


def test_live_pass_calls_set_status_processing_then_finalizing(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    status_seq: list[str] = []

    async def wrap_set(rid: int | None, st: str) -> None:
        if rid is not None:
            status_seq.append(st)
        await _set_refiner_pass_job_status(rid, st)  # type: ignore[arg-type]

    monkeypatch.setattr("app.refiner_service._set_refiner_pass_job_status", wrap_set)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "solo.mkv"
        f.write_bytes(b"z")
        _age_refiner_watch_source(f)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")

    asyncio.run(_go())
    assert "processing" in status_seq
    assert "finalizing" in status_seq
    assert status_seq.index("processing") < status_seq.index("finalizing")


def test_finalize_failure_after_enter_finalizing_returns_failed(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    entered: list[str] = []

    def notify() -> None:
        entered.append("fin")

    def boom(_s: Path, _d: Path) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr("app.refiner_service._finalize_output_file", boom)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)
    watched = tmp_path / "watch"
    output = tmp_path / "out"
    watched.mkdir()
    output.mkdir()
    f = watched / "bad.mkv"
    f.write_bytes(b"body")
    _age_refiner_watch_source(f)
    cfg = RefinerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        tertiary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=False,
        preserve_default_subs=False,
        audio_preference_mode="preferred_langs_quality",
    )
    code, meta = _process_one_refiner_file_sync(
        f, cfg, False, watched, output, tmp_path / "work", enter_finalizing=notify
    )
    assert code == "error"
    assert meta.get("status") == "failed"
    assert entered == ["fin"]
    assert f.exists()


def test_refiner_pass_persists_job_run_log_with_failure_hints(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Logs page reads ``JobRunLog`` — Refiner scheduled passes must persist outcomes there."""

    def _boom(*_a, **_k):
        raise RuntimeError("ffmpeg failed")

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _boom)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"original")
        _age_refiner_watch_source(f)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            jl = (
                (await session.execute(select(JobRunLog).order_by(JobRunLog.id.desc()).limit(1)))
                .scalars()
                .first()
            )
            assert jl is not None
            assert jl.ok is False
            assert "item needs attention" in jl.message.lower()
            low = jl.message.lower()
            assert "stream_manager" not in low
            assert "streammgr" not in low
            assert "needs attention" in low
            assert "Per-file failures" in jl.message
            assert "m.mkv" in jl.message

    asyncio.run(_go())


def test_live_run_refuses_overwrite_existing_destination(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    def _fake_remux(*, src, work_dir, plan):  # noqa: ANN001
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / f"{src.stem}.tmp{src.suffix}"
        out.write_bytes(b"new")
        return out

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _fake_remux)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        src = watched / "m.mkv"
        src.write_bytes(b"original")
        _age_refiner_watch_source(src)
        existing = output / "m.mkv"
        existing.write_bytes(b"existing")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
            assert r.get("ok") is False
            assert r.get("errors") == 1
            assert src.exists()
            assert existing.read_bytes() == b"existing"
            act = (
                (
                    await session.execute(
                        select(RefinerActivity)
                        .where(RefinerActivity.file_name == "m.mkv")
                        .order_by(RefinerActivity.id.desc())
                    )
                )
                .scalars()
                .first()
            )
            assert act is not None
            assert act.status == "failed"

    asyncio.run(_go())


def test_remove_safe_release_residue_allow_list_only(tmp_path) -> None:
    from app.refiner_service import _remove_safe_release_residue

    watch = tmp_path / "watch"
    rel = watch / "release"
    rel.mkdir(parents=True)
    (rel / "junk.txt").write_text("x", encoding="utf-8")
    (rel / "link.url").write_text("url", encoding="utf-8")
    (rel / "archive.sfv").write_text("x", encoding="utf-8")
    (rel / "sidecar.nfo").write_text("n", encoding="utf-8")
    (rel / "part.r00").write_bytes(b"r")
    (rel / "keep.xyz").write_text("u", encoding="utf-8")
    (rel / "media.mkv").write_bytes(b"m")
    (rel / "extra.mp4").write_bytes(b"p")
    (rel / "subs.srt").write_text("sub", encoding="utf-8")
    (rel / "subs.idx").write_bytes(b"idx")
    (rel / "st.ass").write_text("[Events]", encoding="utf-8")
    (rel / "st.ssa").write_text("[Events]", encoding="utf-8")
    (rel / "st.sub").write_bytes(b"\x00")
    removed = _remove_safe_release_residue(release_dir=rel, watched_root=watch)
    assert "junk.txt" in removed
    assert "link.url" in removed
    assert "archive.sfv" in removed
    assert "sidecar.nfo" in removed
    assert "part.r00" in removed
    assert (rel / "keep.xyz").is_file()
    assert (rel / "media.mkv").is_file()
    assert (rel / "extra.mp4").is_file()
    assert (rel / "subs.srt").is_file()
    assert (rel / "subs.idx").is_file()
    assert (rel / "st.ass").is_file()
    assert (rel / "st.ssa").is_file()
    assert (rel / "st.sub").is_file()


def test_prune_after_residue_when_only_subtitles_remain(tmp_path) -> None:
    """Empty release dir after residue delete still prunes; preserved subtitles keep the folder."""
    from app.refiner_service import _prune_empty_ancestors_under_watch, _remove_safe_release_residue

    watch = tmp_path / "watch"
    rel = watch / "Rel" / "nested"
    rel.mkdir(parents=True)
    (rel / "trash.txt").write_text("x", encoding="utf-8")
    (rel / "en.srt").write_text("1\n", encoding="utf-8")
    _remove_safe_release_residue(release_dir=rel, watched_root=watch)
    assert not (rel / "trash.txt").exists()
    assert (rel / "en.srt").exists()
    tok = _prune_empty_ancestors_under_watch(rel, watch)
    assert tok == ""
    assert rel.is_dir()


def test_prune_after_residue_when_folder_fully_junk(tmp_path) -> None:
    from app.refiner_service import _prune_empty_ancestors_under_watch, _remove_safe_release_residue

    watch = tmp_path / "watch"
    rel = watch / "empty_after"
    rel.mkdir(parents=True)
    (rel / "readme.txt").write_text("x", encoding="utf-8")
    _remove_safe_release_residue(release_dir=rel, watched_root=watch)
    assert not any(rel.iterdir())
    tok = _prune_empty_ancestors_under_watch(rel, watch)
    assert tok == "removed_empty_ancestors"
    assert not rel.exists()


def test_prune_empty_ancestors_under_watch(tmp_path) -> None:
    from app.refiner_service import _prune_empty_ancestors_under_watch

    watch = tmp_path / "w"
    deep = watch / "a" / "b" / "c"
    deep.mkdir(parents=True)
    tok = _prune_empty_ancestors_under_watch(deep, watch)
    assert tok == "removed_empty_ancestors"
    assert not (watch / "a" / "b" / "c").exists()
    assert watch.is_dir()


def test_refiner_service_exports_promotion_precheck_symbol() -> None:
    import app.refiner_service as svc

    assert hasattr(svc, "refiner_promotion_precheck")


def test_sync_promotion_gate_runs_without_nameerror(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls = {"count": 0}

    async def _fake_precheck(*, media_file, sonarr_client, radarr_client):  # noqa: ANN001
        del media_file, sonarr_client, radarr_client
        calls["count"] += 1
        from app.refiner_promotion_gate import PromotionGateSyncResult

        return PromotionGateSyncResult(True, (), None)

    monkeypatch.setattr("app.refiner_service.refiner_promotion_precheck", _fake_precheck)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_service.is_remux_required", lambda *_a, **_k: False)
    class _DoneFuture:
        def __init__(self, value):
            self._value = value

        def result(self, timeout=None):  # noqa: ANN001
            del timeout
            return self._value

    def _run_threadsafe(coro, _loop):  # noqa: ANN001
        return _DoneFuture(asyncio.run(coro))

    monkeypatch.setattr("app.refiner_service.asyncio.run_coroutine_threadsafe", _run_threadsafe)

    watched = tmp_path / "watched"
    output = tmp_path / "out"
    work = tmp_path / "work"
    watched.mkdir()
    output.mkdir()
    work.mkdir()
    f = watched / "gate.mkv"
    f.write_bytes(b"payload")
    _age_refiner_watch_source(f)

    loop = asyncio.new_event_loop()
    bridge = importlib.import_module("app.refiner_service").RefinerPromotionBridge(
        loop=loop,
        sonarr=None,
        radarr=None,
    )
    code, _meta = _process_one_refiner_file_sync(
        f,
        RefinerRulesConfig(
            primary_audio_lang="eng",
            secondary_audio_lang="",
            tertiary_audio_lang="",
            default_audio_slot="primary",
            remove_commentary=False,
            subtitle_mode="remove_all",
            subtitle_langs=(),
            preserve_forced_subs=False,
            preserve_default_subs=False,
            audio_preference_mode="preferred_langs_quality",
        ),
        False,
        watched,
        output,
        work,
        promotion_bridge=bridge,
    )
    assert code == "ok"
    assert calls["count"] == 1
    loop.close()


def test_refiner_inflight_source_path_is_suppressed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A source path already in queued/processing/finalizing is ignored for this pass."""
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        src = watched / "same.mkv"
        src.write_bytes(b"x")
        _age_refiner_watch_source(src)

        # Existing in-flight row from another active pass.
        async with SessionLocal() as s:
            s.add(
                RefinerActivity(
                    file_name=src.name,
                    media_title="Processing file...",
                    status="processing",
                    activity_context=json.dumps({"v": 1, "source_path": str(src.resolve())}),
                )
            )
            await s.commit()
        # This test validates scan suppression behavior; skip interruption reconciliation.
        monkeypatch.setattr(
            "app.refiner_service._reconcile_interrupted_refiner_processing_rows_before_pass",
            lambda: asyncio.sleep(0),
        )

        def _should_not_run(*_args, **_kwargs):
            raise AssertionError("in-flight source should not be processed again")

        monkeypatch.setattr("app.refiner_service._process_one_refiner_file_sync", _should_not_run)

        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            result = await run_refiner_pass(session, trigger="scheduled")
            assert result.get("ran") is False

    asyncio.run(_go())


def test_refiner_duplicate_failure_suppressed_same_path_same_reason(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    def _boom(*_a, **_k):
        raise RuntimeError("same deterministic failure")

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _boom)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        src = watched / "dup.mkv"
        src.write_bytes(b"payload")
        _age_refiner_watch_source(src)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            first = await run_refiner_pass(session, trigger="scheduled")
            second = await run_refiner_pass(session, trigger="scheduled")
            assert first.get("errors") == 1
            # Duplicate unchanged failure is suppressed from new activity rows.
            assert int(second.get("errors") or 0) == 0
            rows = (
                await session.execute(
                    select(RefinerActivity).where(RefinerActivity.file_name == "dup.mkv")
                )
            ).scalars().all()
            failed_rows = [r for r in rows if (r.status or "").strip().lower() == "failed"]
            assert len(failed_rows) == 1

    asyncio.run(_go())


def test_refiner_failure_not_suppressed_when_reason_changes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    fail_mode = {"msg": "first failure"}

    def _boom(*_a, **_k):
        raise RuntimeError(fail_mode["msg"])

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _boom)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        src = watched / "reasons.mkv"
        src.write_bytes(b"payload")
        _age_refiner_watch_source(src)
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            fail_mode["msg"] = "different failure code path"
            await run_refiner_pass(session, trigger="scheduled")
            rows = (
                await session.execute(
                    select(RefinerActivity).where(RefinerActivity.file_name == "reasons.mkv")
                )
            ).scalars().all()
            failed_rows = [r for r in rows if (r.status or "").strip().lower() == "failed"]
            assert len(failed_rows) == 2

    asyncio.run(_go())


def test_refiner_parent_summary_uses_current_run_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        src = watched / "ok.mkv"
        src.write_bytes(b"payload")
        _age_refiner_watch_source(src)

        # Prior-run failure history must not bleed into current summary.
        async with SessionLocal() as s:
            s.add(
                RefinerActivity(
                    file_name="old.mkv",
                    media_title="Old",
                    status="failed",
                    activity_context=json.dumps(
                        {"v": 1, "source_path": str((watched / "old.mkv").resolve()), "reason_code": "failed_old"}
                    ),
                )
            )
            await s.commit()

        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            result = await run_refiner_pass(session, trigger="scheduled")
            assert result.get("ok") is True
            parent = (
                (
                    await session.execute(
                        select(ActivityLog)
                        .where(ActivityLog.app == "refiner", ActivityLog.kind == "refiner")
                        .order_by(ActivityLog.id.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
            assert parent is not None
            assert "processed" in (parent.detail or "").lower()
            assert "errors=" not in (parent.detail or "")

    asyncio.run(_go())


def test_refiner_parent_summary_success_only() -> None:
    assert (
        _refiner_run_parent_summary(processed=3, blocked=0, failed=0) == "3 files processed"
    )


def test_refiner_parent_summary_mixed_includes_primary_reason() -> None:
    msg = _refiner_run_parent_summary(
        processed=7,
        blocked=1,
        failed=0,
        primary_reason="Output file already exists in the destination folder.",
    )
    assert msg == "7 processed · 1 blocked — Output file already exists in the destination folder."


def test_refiner_parent_summary_reason_selection_prefers_common_then_recent() -> None:
    picked = _pick_primary_actionable_reason(
        [
            "Source file is missing or not a regular file.",
            "Output file already exists in the destination folder.",
            "Source file is missing or not a regular file.",
        ]
    )
    assert picked == "Source file is missing or not a regular file."


def test_refiner_parent_summary_never_exposes_raw_exception_blob() -> None:
    msg = _refiner_run_parent_summary(
        processed=1,
        blocked=0,
        failed=1,
        primary_reason="Traceback (most recent call last): RuntimeError('boom')",
    )
    assert msg == "1 processed · 1 blocked"
    assert "Traceback" not in msg


def test_refiner_pass_no_meaningful_work_skips_activity_log(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Duplicate-suppressed failures yield zero processed/blocked/failed counts — no parent ActivityLog row."""

    def _fake_process(path: Path, *args: Any, **kwargs: Any) -> tuple[str, dict[str, Any]]:
        sp = str(path.resolve())
        return (
            "error",
            {
                "file_name": path.name,
                "media_title": "D",
                "status": "failed",
                "size_before_bytes": 1,
                "size_after_bytes": 1,
                "audio_tracks_before": 1,
                "audio_tracks_after": 1,
                "subtitle_tracks_before": 0,
                "subtitle_tracks_after": 0,
                "processing_time_ms": 1,
                "failure_hint": "dup",
                "reason_code": "failed_dup_test",
                "source_path": sp,
                "activity_context": json.dumps(
                    {"v": 1, "source_path": sp, "reason_code": "failed_dup_test"}
                ),
            },
        )

    monkeypatch.setattr("app.refiner_service._process_one_refiner_file_sync", _fake_process)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        d = watched / "d.mkv"
        d.write_bytes(b"x")
        _age_refiner_watch_source(d)
        sp = str(d.resolve())
        async with SessionLocal() as s:
            await s.execute(delete(ActivityLog).where(ActivityLog.app == "refiner"))
            await s.execute(delete(RefinerActivity))
            s.add(
                RefinerActivity(
                    file_name="d.mkv",
                    media_title="D",
                    status="failed",
                    size_before_bytes=1,
                    size_after_bytes=1,
                    audio_tracks_before=1,
                    audio_tracks_after=1,
                    subtitle_tracks_before=0,
                    subtitle_tracks_after=0,
                    activity_context=json.dumps(
                        {"v": 1, "source_path": sp, "reason_code": "failed_dup_test"}
                    ),
                )
            )
            await s.commit()

        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")

        async with SessionLocal() as s:
            n_act = (
                await s.execute(select(func.count()).select_from(ActivityLog).where(ActivityLog.app == "refiner"))
            ).scalar_one()
            assert int(n_act) == 0
            jl = (
                (await s.execute(select(JobRunLog).order_by(JobRunLog.id.desc()).limit(1))).scalars().first()
            )
            assert jl is not None
            assert "no new actions" in (jl.message or "").lower()

    asyncio.run(_go())


def test_refiner_pass_blocked_import_still_writes_activity_log(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def _fake_blocked(path: Path, *args: Any, **kwargs: Any) -> tuple[str, dict[str, Any]]:
        sp = str(path.resolve())
        ctx = json.dumps(
            {
                "v": 1,
                "source_path": sp,
                "reason_code": "skipped_terminal_failed",
                "import_promotion_block": {
                    "subtitle": "Not promoted — item classified as a failed import",
                    "reason_code": "skipped_terminal_failed",
                },
            }
        )
        return (
            "blocked_import",
            {
                "file_name": path.name,
                "media_title": "B",
                "status": "skipped_terminal_failed",
                "size_before_bytes": 2,
                "size_after_bytes": 2,
                "audio_tracks_before": 1,
                "audio_tracks_after": 1,
                "subtitle_tracks_before": 0,
                "subtitle_tracks_after": 0,
                "processing_time_ms": 1,
                "activity_context": ctx,
            },
        )

    monkeypatch.setattr("app.refiner_service._process_one_refiner_file_sync", _fake_blocked)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "b.mkv").write_bytes(b"bb")
        _age_refiner_watch_source(watched / "b.mkv")
        async with SessionLocal() as s:
            await s.execute(delete(ActivityLog).where(ActivityLog.app == "refiner"))
            await s.commit()

        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")

        async with SessionLocal() as s:
            n_act = (
                await s.execute(select(func.count()).select_from(ActivityLog).where(ActivityLog.app == "refiner"))
            ).scalar_one()
            assert int(n_act) >= 1
            parent = (
                (
                    await s.execute(
                        select(ActivityLog)
                        .where(ActivityLog.app == "refiner", ActivityLog.kind == "refiner")
                        .order_by(ActivityLog.id.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
            assert parent is not None
            assert "needs attention" in (parent.detail or "").lower()

    asyncio.run(_go())


def test_refiner_pass_failure_still_writes_activity_log(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def _boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("ffmpeg failed")

    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", _boom)
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "m.mkv").write_bytes(b"original")
        _age_refiner_watch_source(watched / "m.mkv")
        async with SessionLocal() as s:
            await s.execute(delete(ActivityLog).where(ActivityLog.app == "refiner"))
            await s.commit()

        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")

        async with SessionLocal() as s:
            n_act = (
                await s.execute(select(func.count()).select_from(ActivityLog).where(ActivityLog.app == "refiner"))
            ).scalar_one()
            assert int(n_act) >= 1
            parent = (
                (
                    await s.execute(
                        select(ActivityLog)
                        .where(ActivityLog.app == "refiner", ActivityLog.kind == "refiner")
                        .order_by(ActivityLog.id.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
            assert parent is not None
            assert parent.status == "failed"
            assert "needs attention" in (parent.detail or "").lower()

    asyncio.run(_go())
