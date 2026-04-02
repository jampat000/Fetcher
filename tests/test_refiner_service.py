from __future__ import annotations

import asyncio
import importlib
import os
from pathlib import Path
from typing import Any

import pytest

from sqlalchemy import delete, select

from app.db import SessionLocal, get_or_create_settings
from app.models import ActivityLog, JobRunLog, RefinerActivity
from app.refiner_activity_context import parse_activity_context
from app.time_util import utc_now_naive
from app.refiner_service import (
    _finalize_output_file,
    _pipeline_from_settings,
    _reconcile_interrupted_refiner_processing_rows_before_pass,
    _rules_config_from_settings,
    _try_remove_empty_watch_subfolder,
    reconcile_refiner_processing_rows_on_worker_boot,
    run_refiner_pass,
)
from app.refiner_source_readiness import RefinerQueueSnapshot
from app.schema_version import CURRENT_SCHEMA_VERSION


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
    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", lambda *_a, **_k: calls.append(1))
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"original")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "a" / "m.mkv"
        f.parent.mkdir(parents=True)
        f.write_bytes(b"original")
        (f.parent / "m.par2").write_bytes(b"par")
        (f.parent / "readme.nfo").write_text("rel", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
        assert not (f.parent / "m.par2").exists()
        assert not (f.parent / "readme.nfo").exists()
        assert not (watched / "a").exists()

    asyncio.run(_go())


def test_live_no_remux_copies_to_output_removes_source_and_empty_folder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    calls: list[int] = []

    def _no_remux(*_a, **_k):
        calls.append(1)
        raise AssertionError("remux should not run when no stream changes are required")

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _no_remux)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        sub = watched / "sub"
        sub.mkdir(parents=True)
        f = sub / "clean.file.name.1080p.mkv"
        f.write_bytes(b"payload")
        (sub / "side.sfv").write_text("chk", encoding="utf-8")
        (sub / "grab.nzb").write_bytes(b"nzb")
        output.mkdir()
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
        assert not (sub / "side.sfv").exists()
        assert not (sub / "grab.nzb").exists()
        assert not sub.exists()

    asyncio.run(_go())


def test_live_no_remux_leaves_folder_when_other_files_remain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        sub = watched / "sub"
        sub.mkdir(parents=True)
        f = sub / "one.mkv"
        f.write_bytes(b"x")
        (sub / "keep.txt").write_text("hold", encoding="utf-8")
        output.mkdir()
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
        assert (sub / "keep.txt").exists()

    asyncio.run(_go())


def test_dry_run_no_remux_does_not_copy_or_delete(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "solo.mkv"
        f.write_bytes(b"orig")
        (watched / "solo.par2").write_bytes(b"p")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert int(r.get("dry_run_items") or 0) >= 1
        assert f.exists()
        assert (watched / "solo.par2").exists()
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

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _boom)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"original")
        (watched / "m.par2").write_bytes(b"parity")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
        assert (watched / "m.par2").read_bytes() == b"parity"
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

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        work = tmp_path / "work-custom"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"orig")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"orig")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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

    monkeypatch.setattr("app.refiner_pipeline.os.replace", _track_replace)

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

    monkeypatch.setattr("app.refiner_pipeline.shutil.copyfileobj", _boom)
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


def test_source_missing_skips_as_failed(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

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
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert r.get("errors") == 1
        assert not missing.exists()

    asyncio.run(_go())


def test_refiner_pass_calls_insert_then_update_per_file(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls: list[tuple[str, Any]] = []

    async def _fake_insert(name: str) -> int:
        calls.append(("insert", name))
        return 901

    async def _fake_update(rid: int, meta: dict) -> None:
        calls.append(("update", rid, meta.get("status")))

    monkeypatch.setattr("app.refiner_service._insert_refiner_processing_row", _fake_insert)
    monkeypatch.setattr("app.refiner_service._update_refiner_activity_row", _fake_update)
    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "m.mkv").write_bytes(b"x")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")

    asyncio.run(_go())
    assert ("insert", "m.mkv") in calls
    assert ("update", 901, "skipped") in calls


def test_refiner_pass_persists_job_run_log_with_failure_hints(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Logs page reads ``JobRunLog`` — Refiner scheduled passes must persist outcomes there."""

    def _boom(*_a, **_k):
        raise RuntimeError("ffmpeg failed")

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _boom)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"original")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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
            assert jl.message.startswith("Refiner (")
            low = jl.message.lower()
            assert "stream_manager" not in low
            assert "streammgr" not in low
            assert "errors=1" in jl.message
            assert "Per-file failures" in jl.message
            assert "m.mkv" in jl.message

    asyncio.run(_go())


def test_live_run_refuses_overwrite_existing_destination(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    def _fake_remux(*, src, work_dir, plan):  # noqa: ANN001
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / f"{src.stem}.tmp{src.suffix}"
        out.write_bytes(b"new")
        return out

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _fake_remux)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        src = watched / "m.mkv"
        src.write_bytes(b"original")
        existing = output / "m.mkv"
        existing.write_bytes(b"existing")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
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


def test_refiner_pass_final_gate_blocks_when_queue_turns_active(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe_calls: list[Path] = []

    def _probe(p: Path):
        probe_calls.append(p)
        return _fake_probe_multi_audio()

    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", _probe)
    phase = {"n": 0}

    async def phased_fetch(_row):  # noqa: ANN001
        watched = Path(_row.refiner_watched_folder or "")
        files = sorted(watched.rglob("*.mkv"))
        p = str(files[0].resolve()) if files else ""
        phase["n"] += 1
        if phase["n"] == 1:
            return RefinerQueueSnapshot(False, False, False, False, (), ())
        rec = {
            "status": "downloading",
            "trackedDownloadState": "downloading",
            "sizeleft": 0,
            "outputPath": p,
        }
        return RefinerQueueSnapshot(True, False, True, False, (rec,), ())

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", phased_fetch)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "late.mkv").write_bytes(b"x" * 400)
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            act = (
                (
                    await session.execute(
                        select(RefinerActivity).where(RefinerActivity.file_name == "late.mkv").order_by(RefinerActivity.id.desc())
                    )
                )
                .scalars()
                .first()
            )
            assert act is not None
            assert "radarr_queue_active_download" in (act.activity_context or "")

    asyncio.run(_go())
    assert probe_calls == []


def test_refiner_pass_skips_probe_when_radarr_queue_active(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    probe_calls: list[Path] = []

    def _probe(p: Path):
        probe_calls.append(p)
        return _fake_probe_multi_audio()

    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", _probe)

    async def fake_fetch(_row):  # noqa: ANN001
        w = Path(_row.refiner_watched_folder or "")
        files = sorted(w.rglob("*.mkv"))
        p = str(files[0].resolve()) if files else ""
        rec = {
            "status": "downloading",
            "trackedDownloadState": "downloading",
            "sizeleft": 0,
            "outputPath": p,
        }
        return RefinerQueueSnapshot(True, False, True, False, (rec,), ())

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", fake_fetch)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        src = watched / "hold.mkv"
        src.write_bytes(b"x" * 400)
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert probe_calls == []
        assert int(r.get("errors") or 0) == 0
        assert int(r.get("waiting") or 0) >= 1
        assert r.get("ok") is True

    asyncio.run(_go())


def test_refiner_pass_queue_snapshot_fetched_twice_before_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_calls = 0

    async def counting_fetch(_row):  # noqa: ANN001
        nonlocal fetch_calls
        fetch_calls += 1
        return RefinerQueueSnapshot(False, False, False, False, (), ())

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", counting_fetch)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "one.mkv").write_bytes(b"x" * 300)
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")

    asyncio.run(_go())
    assert fetch_calls == 2


def test_refiner_pass_ffprobe_fail_reclassified_when_upstream_active(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: (_ for _ in ()).throw(RuntimeError("probe failed")),
    )
    phase = {"n": 0}

    async def phased_fetch(_row):  # noqa: ANN001
        watched = Path(_row.refiner_watched_folder or "")
        files = sorted(watched.rglob("*.mkv"))
        p = str(files[0].resolve()) if files else ""
        phase["n"] += 1
        if phase["n"] <= 2:
            return RefinerQueueSnapshot(False, False, False, False, (), ())
        rec = {
            "status": "downloading",
            "trackedDownloadState": "downloading",
            "sizeleft": 0,
            "outputPath": p,
        }
        return RefinerQueueSnapshot(True, False, True, False, (rec,), ())

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", phased_fetch)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "bad.mkv").write_bytes(b"x" * 300)
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            act = (
                (
                    await session.execute(
                        select(RefinerActivity).where(RefinerActivity.file_name == "bad.mkv").order_by(RefinerActivity.id.desc())
                    )
                )
                .scalars()
                .first()
            )
            assert act is not None
            assert "radarr_queue_active_download" in (act.activity_context or "")

    asyncio.run(_go())


def test_refiner_waiting_upstream_repeat_dedupes_same_candidate_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(_row):  # noqa: ANN001
        return RefinerQueueSnapshot(
            True,
            False,
            True,
            False,
            (
                {
                    "status": "paused",
                    "sizeleft": 8_000_000,
                    "title": "Movie.Name.2026.1080p",
                },
            ),
            (),
        )

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", fake_fetch)
    probe_calls: list[Path] = []
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda p: (probe_calls.append(p), _fake_probe_multi_audio())[1],
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        src = watched / "Movie.Name.2026.1080p.mkv"
        src.write_bytes(b"x" * 500)
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            await run_refiner_pass(session, trigger="scheduled")
            rows = (
                (
                    await session.execute(
                        select(RefinerActivity)
                        .where(RefinerActivity.file_name == "Movie.Name.2026.1080p.mkv")
                        .order_by(RefinerActivity.id.desc())
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            ctx = parse_activity_context(rows[0].activity_context)
            assert ctx.get("reason_code") == "radarr_queue_active_download_title"
            assert int(ctx.get("wait_repeat_count") or 0) >= 2
            assert str(ctx.get("wait_last_seen_at") or "").strip()

    asyncio.run(_go())
    assert probe_calls == []


def test_refiner_scheduled_pass_waiting_only_no_failed_aggregate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(_row):  # noqa: ANN001
        return RefinerQueueSnapshot(
            True,
            False,
            True,
            False,
            (
                {
                    "status": "paused",
                    "sizeleft": 8_000_000,
                    "title": "Hold.2024.1080p",
                },
            ),
            (),
        )

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", fake_fetch)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "Hold.2024.1080p.mkv").write_bytes(b"x" * 500)
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            log = (
                (
                    await session.execute(
                        select(ActivityLog).where(ActivityLog.kind == "refiner").order_by(ActivityLog.id.desc())
                    )
                )
                .scalars()
                .first()
            )
            assert log is not None
            assert log.status == "ok"
            assert "waiting=1" in (log.detail or "")
            assert "errors=0" in (log.detail or "")

    asyncio.run(_go())


def test_refiner_mixed_refined_and_waiting_counts_waiting_separate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One file refines, one file blocked upstream — batch log must not count wait as errors."""

    async def fake_fetch(_row):  # noqa: ANN001
        return RefinerQueueSnapshot(
            True,
            False,
            True,
            False,
            (
                {
                    "status": "paused",
                    "sizeleft": 8_000_000,
                    "title": "Blocked.2024.1080p",
                },
            ),
            (),
        )

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", fake_fetch)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    def _proc_one(*args: Any, **kwargs: Any) -> tuple[str, dict[str, Any]]:
        path = args[0]
        if path.name == "ok.mkv":
            return (
                "ok",
                {
                    "file_name": "ok.mkv",
                    "media_title": "ok",
                    "status": "success",
                    "size_before_bytes": 100,
                    "size_after_bytes": 100,
                    "audio_tracks_before": 2,
                    "audio_tracks_after": 1,
                    "subtitle_tracks_before": 0,
                    "subtitle_tracks_after": 0,
                    "processing_time_ms": 1,
                    "activity_context": "{}",
                },
            )
        return ("error", {})

    monkeypatch.setattr("app.refiner_service._process_one_refiner_file_sync", _proc_one)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "ok.mkv").write_bytes(b"x" * 400)
        (watched / "Blocked.2024.1080p.mkv").write_bytes(b"x" * 500)
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            log = (
                (
                    await session.execute(
                        select(ActivityLog).where(ActivityLog.kind == "refiner").order_by(ActivityLog.id.desc())
                    )
                )
                .scalars()
                .first()
            )
            assert log is not None
            assert log.status == "ok"
            assert "processed=1" in (log.detail or "")
            assert "waiting=1" in (log.detail or "")
            assert "errors=0" in (log.detail or "")

    asyncio.run(_go())


def test_refiner_final_gate_wait_dedupes_across_passes_not_only_persist_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Initial gate sees empty queue (proceed); final gate blocks — next pass same; one RefinerActivity row."""

    phase = {"n": 0}

    async def fake_fetch(_row):  # noqa: ANN001
        phase["n"] += 1
        if phase["n"] % 2 == 1:
            return RefinerQueueSnapshot(True, False, False, False, (), ())
        return RefinerQueueSnapshot(
            True,
            False,
            True,
            False,
            (
                {
                    "status": "paused",
                    "sizeleft": 9_000_000,
                    "title": "GateHold.2024.1080p",
                },
            ),
            (),
        )

    monkeypatch.setattr("app.refiner_service.fetch_refiner_queue_snapshot", fake_fetch)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "GateHold.2024.1080p.mkv").write_bytes(b"x" * 500)
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
            await run_refiner_pass(session, trigger="scheduled")
            rows = (
                (
                    await session.execute(
                        select(RefinerActivity)
                        .where(RefinerActivity.file_name == "GateHold.2024.1080p.mkv")
                        .order_by(RefinerActivity.id.asc())
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            ctx = parse_activity_context(rows[0].activity_context)
            assert ctx.get("reason_code") == "radarr_queue_active_download_title"
            assert int(ctx.get("wait_repeat_count") or 0) >= 2

    asyncio.run(_go())
