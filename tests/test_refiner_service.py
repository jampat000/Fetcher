from __future__ import annotations

import asyncio
import importlib
import os
from pathlib import Path
from typing import Any

import pytest

from sqlalchemy import delete, func, select

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
    run_sonarr_refiner_pass,
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
            row.sonarr_enabled = True
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
        nested = sub / "nested"
        nested.mkdir()
        (sub / "keep.txt").write_text("hold", encoding="utf-8")
        (nested / "keep2.txt").write_text("hold2", encoding="utf-8")
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
        assert not sub.exists()

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


def test_live_no_remux_source_folder_tree_removed_with_nested_dirs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        sub = watched / "sub"
        nested = sub / "nested" / "deep"
        nested.mkdir(parents=True)
        f = sub / "movie.mkv"
        f.write_bytes(b"x")
        (nested / "junk.txt").write_text("junk", encoding="utf-8")
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
        assert r.get("errors") == 0
        assert (output / "sub" / "movie.mkv").exists()
        assert not sub.exists()
        assert watched.exists()

    asyncio.run(_go())


def test_live_remux_source_folder_tree_removed_with_nested_dirs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    def _fake_remux(*, src, work_dir, plan):  # noqa: ANN001
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / f"{src.stem}.tmp{src.suffix}"
        out.write_bytes(b"remuxed")
        return out

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        sub = watched / "sub"
        nested = sub / "child"
        nested.mkdir(parents=True)
        f = sub / "m.mkv"
        f.write_bytes(b"src")
        (nested / "x.jpg").write_bytes(b"img")
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
        assert r.get("remuxed") == 1
        assert (output / "sub" / "m.mkv").exists()
        assert not sub.exists()
        assert watched.exists()

    asyncio.run(_go())


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
    assert CURRENT_SCHEMA_VERSION == 41
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


def test_source_missing_skips_without_error_or_activity(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Age gate treats a missing path as not ready — no row, no error count."""
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
        assert r.get("errors") == 0
        assert not missing.exists()

    asyncio.run(_go())


def test_refiner_pass_skips_file_not_yet_aged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Files modified too recently are skipped silently —
    no RefinerActivity row, no error count."""
    from app.refiner_watch_config import clamp_refiner_minimum_age_seconds as _real_clamp

    monkeypatch.setattr(
        "app.refiner_service.clamp_refiner_minimum_age_seconds", _real_clamp
    )
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: _fake_probe_multi_audio(),
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "fresh.mkv"
        f.write_bytes(b"x" * 500)
        # File mtime is right now — age ~0s
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_minimum_age_seconds = 60
            row.radarr_enabled = False
            await session.commit()
            result = await run_refiner_pass(session, trigger="scheduled")
            assert result["errors"] == 0
            act = (
                (
                    await session.execute(
                        select(RefinerActivity)
                        .where(
                            RefinerActivity.file_name == "fresh.mkv"
                        )
                        .order_by(RefinerActivity.id.desc())
                    )
                )
                .scalars()
                .first()
            )
            assert act is None

    asyncio.run(_go())


def test_refiner_pass_processes_aged_stable_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """File older than minimum_age_seconds is processed."""
    import os as _os
    import time as _time

    from app.refiner_watch_config import clamp_refiner_minimum_age_seconds as _real_clamp

    monkeypatch.setattr(
        "app.refiner_service.clamp_refiner_minimum_age_seconds", _real_clamp
    )
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: _fake_probe_multi_audio(),
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "aged.mkv"
        f.write_bytes(b"x" * 500)
        # Set mtime to 120s ago so it passes the 60s gate
        old_t = _time.time() - 120
        _os.utime(f, (old_t, old_t))
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_minimum_age_seconds = 60
            row.radarr_enabled = False
            await session.commit()
            result = await run_refiner_pass(session, trigger="scheduled")
            assert result["dry_run_items"] == 1
            assert result["errors"] == 0

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
            assert "cleanup_needed=" in jl.message
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


def test_wrong_content_stop_leaves_watched_item_untouched_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())

    async def fake_wc_ctx(_fp, _row, _snap):  # noqa: ANN001
        return {
            "enabled": True,
            "movie_id": 123,
            "queue_id": 456,
            "target_title": "Target Movie",
            "target_year": 2024,
            "expected_runtime_minutes": 130.0,
        }

    class _WC:
        wrong_content = True
        triggered_reason = "runtime mismatch"
        score = 99
        hard_trigger = True
        probed_runtime_minutes = 20.0
        expected_runtime_minutes = 130.0
        runtime_ratio = 0.15
        token_overlap_summary = "tokens mismatch"

    monkeypatch.setattr("app.refiner_service._movie_wrong_content_ctx_for_candidate", fake_wc_ctx)
    monkeypatch.setattr("app.refiner_pipeline.evaluate_movie_wrong_content", lambda *_a, **_k: _WC())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        job = watched / "JobFolder"
        job.mkdir()
        media = job / "movie.mkv"
        media.write_bytes(b"x")
        radarr_rec = {
            "status": "completed",
            "trackedDownloadState": "",
            "sizeLeft": 0,
            "sizeleft": 0,
            "outputPath": str(media.resolve()),
            "title": "movie.1080p.bluray.x264",
            "movie": {"id": 123, "title": "Target Movie", "year": 2024},
            "id": 456,
        }

        async def fake_fetch(_row):  # noqa: ANN001
            return RefinerQueueSnapshot(
                True, False, True, False, (radarr_rec,), ()
            )

        monkeypatch.setattr(
            "app.refiner_service.fetch_refiner_queue_snapshot",
            fake_fetch,
        )
        leftover = job / "leftover.txt"
        leftover.write_text("keep", encoding="utf-8")
        sibling = watched / "SiblingFolder"
        sibling.mkdir()
        sibling_file = sibling / "sibling.txt"
        sibling_file.write_text("stay", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.radarr_enabled = True
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
            act = (
                (
                    await session.execute(
                        select(RefinerActivity)
                        .where(RefinerActivity.file_name == "movie.mkv")
                        .order_by(RefinerActivity.id.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
        assert r.get("ok") is False
        assert r.get("errors") == 1
        assert not (output / "JobFolder" / "movie.mkv").exists()
        assert media.exists()
        assert leftover.exists()
        assert job.exists()
        assert sibling_file.read_text(encoding="utf-8") == "stay"
        assert act is not None
        ctx = parse_activity_context(act.activity_context)
        assert ctx.get("reason_code") == "radarr_wrong_content"

    asyncio.run(_go())


def test_live_no_remux_keep_selected_preserves_external_srt_next_to_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        sub = watched / "sub"
        sub.mkdir(parents=True)
        f = sub / "clean.file.name.1080p.mkv"
        f.write_bytes(b"payload")
        (sub / "clean.file.name.1080p.en.srt").write_text("subtext", encoding="utf-8")
        (sub / "clean.file.name.1080p.forced.vtt").write_text("WEBVTT\n", encoding="utf-8")
        output.mkdir()
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "keep_selected"
            row.refiner_subtitle_langs_csv = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert r.get("errors") == 0
        assert (output / "sub" / "clean.file.name.1080p.mkv").read_bytes() == b"payload"
        assert (output / "sub" / "clean.file.name.1080p.en.srt").read_text(encoding="utf-8") == "subtext"
        assert (output / "sub" / "clean.file.name.1080p.forced.vtt").read_text(encoding="utf-8") == "WEBVTT\n"
        assert not (sub / "clean.file.name.1080p.en.srt").exists()
        assert not (sub / "clean.file.name.1080p.forced.vtt").exists()
        async with SessionLocal() as session:
            row = (
                (
                    await session.execute(
                        select(RefinerActivity)
                        .where(RefinerActivity.file_name == "clean.file.name.1080p.mkv")
                        .order_by(RefinerActivity.id.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
            assert row is not None
            ctx = parse_activity_context(row.activity_context)
            assert ctx.get("subtitle_sidecars_preserved") == [
                "clean.file.name.1080p.en.srt",
                "clean.file.name.1080p.forced.vtt",
            ]

    asyncio.run(_go())


def test_live_no_remux_subtitle_target_collision_fails_without_finalize(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "only.mkv"
        f.write_bytes(b"data")
        (watched / "only.en.srt").write_text("s", encoding="utf-8")
        (output / "only.en.srt").write_text("exists", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "keep_selected"
            row.refiner_subtitle_langs_csv = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert r.get("errors") == 1
        assert not (output / "only.mkv").exists()
        assert f.exists()

    asyncio.run(_go())


def test_live_no_remux_finalize_failure_rolls_back_preserved_sidecars(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    def _finalize_boom(_src: Path, _dst: Path) -> None:
        raise RuntimeError("finalize exploded")

    monkeypatch.setattr("app.refiner_pipeline._finalize_output_file", _finalize_boom)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "only.mkv"
        f.write_bytes(b"data")
        (watched / "only.en.srt").write_text("s", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "keep_selected"
            row.refiner_subtitle_langs_csv = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert r.get("errors") == 1
        assert not (output / "only.mkv").exists()
        assert not (output / "only.en.srt").exists()
        assert f.exists()

    asyncio.run(_go())


def test_live_no_remux_remove_all_does_not_copy_external_srt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        sub = watched / "sub"
        sub.mkdir(parents=True)
        f = sub / "a.mkv"
        f.write_bytes(b"x")
        (sub / "a.en.srt").write_text("s", encoding="utf-8")
        output.mkdir()
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "remove_all"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
        assert (output / "sub" / "a.mkv").exists()
        assert not (output / "sub" / "a.en.srt").exists()
        assert not (sub / "a.en.srt").exists()

    asyncio.run(_go())


def test_live_remux_keep_selected_preserves_external_srt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    def _fake_remux(*, src, work_dir, plan):  # noqa: ANN001
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / f"{src.stem}.tmp{src.suffix}"
        out.write_bytes(b"remuxed")
        return out

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"src")
        (watched / "m.en.srt").write_text("side", encoding="utf-8")
        sibling = tmp_path / "sibling"
        sibling.mkdir()
        (sibling / "unrelated.other.srt").write_text("leave", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "keep_selected"
            row.refiner_subtitle_langs_csv = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert r.get("remuxed") == 1
        assert (output / "m.mkv").read_bytes() == b"remuxed"
        assert (output / "m.en.srt").read_text(encoding="utf-8") == "side"
        assert not (output / "unrelated.other.srt").exists()
        assert (sibling / "unrelated.other.srt").read_text(encoding="utf-8") == "leave"

    asyncio.run(_go())


def test_live_remux_subtitle_collision_removes_output_and_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    def _fake_remux(*, src, work_dir, plan):  # noqa: ANN001
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / f"{src.stem}.tmp{src.suffix}"
        out.write_bytes(b"remuxed")
        return out

    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (output / "m.en.srt").write_text("block", encoding="utf-8")
        f = watched / "m.mkv"
        f.write_bytes(b"src")
        (watched / "m.en.srt").write_text("side", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "keep_selected"
            row.refiner_subtitle_langs_csv = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert r.get("errors") == 1
        assert not (output / "m.mkv").exists()

    asyncio.run(_go())


def test_dry_run_keep_selected_does_not_copy_external_srt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "solo.mkv"
        f.write_bytes(b"orig")
        (watched / "solo.en.srt").write_text("sub", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "keep_selected"
            row.refiner_subtitle_langs_csv = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
        assert not (output / "solo.mkv").exists()
        assert not (output / "solo.en.srt").exists()
        assert (watched / "solo.en.srt").exists()

    asyncio.run(_go())


def test_live_keep_selected_does_not_touch_sibling_folder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        other = watched / "Other.Release"
        other.mkdir(parents=True)
        (other / "ghost.en.srt").write_text("untouched", encoding="utf-8")
        job = watched / "Job.Folder"
        job.mkdir(parents=True)
        f = job / "Movie.2024.mkv"
        f.write_bytes(b"v")
        (job / "Movie.2024.en.srt").write_text("keepme", encoding="utf-8")
        output.mkdir()
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "keep_selected"
            row.refiner_subtitle_langs_csv = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
        assert (other / "ghost.en.srt").read_text(encoding="utf-8") == "untouched"
        assert not (output / "Other.Release").exists()
        assert (output / "Job.Folder" / "Movie.2024.en.srt").read_text(encoding="utf-8") == "keepme"

    asyncio.run(_go())


def test_refiner_failure_path_does_not_copy_subtitles(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.remux_to_temp_file", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("ffmpeg failed")))
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"x")
        (watched / "m.en.srt").write_text("s", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "keep_selected"
            row.refiner_subtitle_langs_csv = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            await run_refiner_pass(session, trigger="scheduled")
        assert not (output / "m.mkv").exists()
        assert not (output / "m.en.srt").exists()

    asyncio.run(_go())


def test_dry_run_keeps_stale_refiner_work_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())
    monkeypatch.setattr("app.refiner_service.REFINER_FFMPEG_TIMEOUT_S", 0)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        work = tmp_path / "work"
        watched.mkdir()
        output.mkdir()
        work.mkdir()
        stale = work / "movie.refiner.stale.tmp.mkv"
        stale.write_bytes(b"stale")
        (watched / "m.mkv").write_bytes(b"x")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_work_folder = str(work)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
        assert int(r.get("dry_run_items") or 0) >= 1
        assert stale.exists()

    asyncio.run(_go())


def test_live_run_deletes_stale_refiner_work_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_multi_audio())
    monkeypatch.setattr("app.refiner_service.REFINER_FFMPEG_TIMEOUT_S", 0)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        work = tmp_path / "work"
        watched.mkdir()
        output.mkdir()
        work.mkdir()
        stale = work / "movie.refiner.stale.tmp.mkv"
        stale.write_bytes(b"stale")
        (watched / "m.mkv").write_bytes(b"x")
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
        assert not stale.exists()

    asyncio.run(_go())


def test_live_no_remux_source_cleanup_failure_is_reported_and_siblings_untouched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    def _cleanup_boom(*_a, **_k) -> int:
        raise RuntimeError("Source folder cleanup failed after successful output finalize; could not delete: keep.txt")

    calls = {"folder": 0}

    def _folder_marker(*_a, **_k) -> str:
        calls["folder"] += 1
        return "removed_source_folder"

    monkeypatch.setattr("app.refiner_pipeline._cleanup_refiner_source_sidecar_artifacts_after_success", _cleanup_boom)
    monkeypatch.setattr("app.refiner_pipeline._try_remove_empty_watch_subfolder", _folder_marker)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"x")
        (watched / "keep.txt").write_text("hold", encoding="utf-8")
        sibling = tmp_path / "sibling"
        sibling.mkdir()
        (sibling / "stay.txt").write_text("stay", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            base_id = int(
                (
                    await session.execute(
                        select(func.max(ActivityLog.id)).where(ActivityLog.kind == "refiner")
                    )
                ).scalar()
                or 0
            )
            r = await run_refiner_pass(session, trigger="scheduled")
            recent = (
                (await session.execute(select(RefinerActivity).order_by(RefinerActivity.id.desc()).limit(1)))
                .scalars()
                .first()
            )
            batch = (
                (
                    await session.execute(
                        select(ActivityLog)
                        .where(ActivityLog.kind == "refiner")
                        .where(ActivityLog.id > base_id)
                        .order_by(ActivityLog.id.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
        assert r.get("errors") == 0
        assert r.get("cleanup_needed") == 1
        assert r.get("ok") is False
        assert (output / "m.mkv").exists()
        assert (sibling / "stay.txt").read_text(encoding="utf-8") == "stay"
        assert recent is not None
        ctx = parse_activity_context(recent.activity_context)
        assert ctx.get("reason_code") == "source_cleanup_failed"
        assert calls["folder"] == 0
        assert batch is None

    asyncio.run(_go())


def test_live_no_remux_watch_root_media_keeps_watched_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        media = watched / "rootitem.mkv"
        media.write_bytes(b"data")
        (watched / "rootitem.en.srt").write_text("s", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_subtitle_mode = "remove_all"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            r = await run_refiner_pass(session, trigger="scheduled")
            recent = (
                (await session.execute(select(RefinerActivity).order_by(RefinerActivity.id.desc()).limit(1)))
                .scalars()
                .first()
            )
        assert r.get("errors") == 0
        assert watched.exists() and watched.is_dir()
        assert (output / "rootitem.mkv").exists()
        assert recent is not None
        ctx = parse_activity_context(recent.activity_context)
        assert ctx.get("folder_cleanup") == "skipped_watch_root"

    asyncio.run(_go())


def test_live_no_remux_folder_removal_failure_is_reported(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr("app.refiner_pipeline.ffprobe_json", lambda _p: _fake_probe_single_eng())
    monkeypatch.setattr("app.refiner_pipeline.is_remux_required", lambda *_a, **_k: False)

    def _folder_fail(*_a, **_k) -> str:
        raise RuntimeError("Source folder removal failed after successful file cleanup; could not remove 'job'.")

    monkeypatch.setattr("app.refiner_pipeline._try_remove_empty_watch_subfolder", _folder_fail)

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        job = watched / "job"
        job.mkdir()
        media = job / "m.mkv"
        media.write_bytes(b"x")
        sibling = watched / "other"
        sibling.mkdir()
        (sibling / "keep.txt").write_text("stay", encoding="utf-8")
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            await session.commit()
            base_id = int(
                (
                    await session.execute(
                        select(func.max(ActivityLog.id)).where(ActivityLog.kind == "refiner")
                    )
                ).scalar()
                or 0
            )
            r = await run_refiner_pass(session, trigger="scheduled")
            recent = (
                (await session.execute(select(RefinerActivity).order_by(RefinerActivity.id.desc()).limit(1)))
                .scalars()
                .first()
            )
            batch = (
                (
                    await session.execute(
                        select(ActivityLog)
                        .where(ActivityLog.kind == "refiner")
                        .where(ActivityLog.id > base_id)
                        .order_by(ActivityLog.id.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
        assert r.get("errors") == 0
        assert r.get("cleanup_needed") == 1
        assert r.get("ok") is False
        assert (output / "job" / "m.mkv").exists()
        assert (sibling / "keep.txt").read_text(encoding="utf-8") == "stay"
        assert recent is not None
        ctx = parse_activity_context(recent.activity_context)
        assert ctx.get("reason_code") == "source_folder_removal_failed"
        assert batch is None

    asyncio.run(_go())


def test_sonarr_refiner_pass_disabled_returns_not_ran(
    tmp_path: Path,
) -> None:
    """Sonarr Refiner pass returns ran=False when disabled."""

    async def _go() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_refiner_enabled = False
            await session.commit()
            result = await run_sonarr_refiner_pass(
                session, trigger="scheduled"
            )
            assert result["ran"] is False
            assert result["reason"] == "disabled"

    asyncio.run(_go())


def test_sonarr_refiner_pass_missing_primary_lang_returns_error(
    tmp_path: Path,
) -> None:
    """Sonarr Refiner pass fails fast when primary lang unset."""

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_refiner_enabled = True
            row.sonarr_refiner_primary_audio_lang = ""
            row.sonarr_refiner_watched_folder = str(watched)
            row.sonarr_refiner_output_folder = str(output)
            await session.commit()
            result = await run_sonarr_refiner_pass(
                session, trigger="scheduled"
            )
            assert result["ok"] is False
            assert result["error"] == "primary_lang_required"

    asyncio.run(_go())


def test_sonarr_refiner_pass_no_files_returns_no_ran(
    tmp_path: Path,
) -> None:
    """Sonarr Refiner pass with empty watched folder."""

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_refiner_enabled = True
            row.sonarr_refiner_primary_audio_lang = "eng"
            row.sonarr_refiner_watched_folder = str(watched)
            row.sonarr_refiner_output_folder = str(output)
            await session.commit()
            result = await run_sonarr_refiner_pass(
                session, trigger="scheduled"
            )
            assert result["ran"] is False
            assert result["reason"] == "no_files"

    asyncio.run(_go())


def test_sonarr_refiner_pass_processes_aged_file_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sonarr Refiner dry-run pass processes an aged file."""
    import os as _os
    import time as _time

    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: _fake_probe_multi_audio(),
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "episode.mkv"
        f.write_bytes(b"x" * 500)
        old_t = _time.time() - 120
        _os.utime(f, (old_t, old_t))
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_refiner_enabled = True
            row.sonarr_refiner_dry_run = True
            row.sonarr_refiner_primary_audio_lang = "eng"
            row.sonarr_refiner_watched_folder = str(watched)
            row.sonarr_refiner_output_folder = str(output)
            row.sonarr_refiner_minimum_age_seconds = 60
            await session.commit()
            result = await run_sonarr_refiner_pass(
                session, trigger="scheduled"
            )
            assert result["dry_run_items"] == 1
            assert result["errors"] == 0

    asyncio.run(_go())


def test_sonarr_refiner_pass_independent_of_radarr_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sonarr and Radarr passes use separate locks — can both
    be called without deadlock."""

    async def _go() -> None:
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.sonarr_refiner_enabled = False
            row.refiner_enabled = False
            await session.commit()
            r1 = await run_sonarr_refiner_pass(
                session, trigger="scheduled"
            )
            r2 = await run_refiner_pass(
                session, trigger="scheduled"
            )
            assert r1["ran"] is False
            assert r2["ran"] is False

    asyncio.run(_go())


def test_refiner_pass_skips_radarr_disowned_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """File passes age gate but Radarr queue has rows and none
    match this file — skip with radarr_disowned, no activity
    row, no error count."""
    import os as _os, time as _time

    async def _fake_snap(row):
        from app.refiner_source_readiness import RefinerQueueSnapshot
        return RefinerQueueSnapshot(
            radarr_configured=True,
            sonarr_configured=False,
            radarr_fetch_succeeded=True,
            sonarr_fetch_succeeded=False,
            radarr_records=(
                {
                    "title": "Some.Other.Movie.2020.1080p.BluRay",
                    "trackedDownloadState": "downloading",
                    "status": "downloading",
                },
            ),
            sonarr_records=(),
        )

    monkeypatch.setattr(
        "app.refiner_service.fetch_refiner_queue_snapshot",
        _fake_snap,
    )
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: _fake_probe_multi_audio(),
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir(); output.mkdir()
        f = watched / "devil.wears.prada.2006.mkv"
        f.write_bytes(b"x" * 500)
        old_t = _time.time() - 120
        _os.utime(f, (old_t, old_t))
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = False
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_minimum_age_seconds = 60
            row.radarr_enabled = True
            await session.commit()
            result = await run_refiner_pass(
                session, trigger="scheduled"
            )
            assert result["errors"] == 0
            assert result["remuxed"] == 0
            act = (
                (await session.execute(
                    select(RefinerActivity)
                    .where(RefinerActivity.file_name == "devil.wears.prada.2006.mkv")
                    .order_by(RefinerActivity.id.desc())
                )).scalars().first()
            )
            assert act is None

    asyncio.run(_go())


def test_refiner_pass_disowned_check_skips_on_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Dry run skips when queue has records but none match the file."""
    import os as _os, time as _time

    async def _fake_snap(row):
        from app.refiner_source_readiness import RefinerQueueSnapshot
        return RefinerQueueSnapshot(
            radarr_configured=True,
            sonarr_configured=False,
            radarr_fetch_succeeded=True,
            sonarr_fetch_succeeded=False,
            radarr_records=(
                {
                    "title": "Some.Other.Movie.2020.1080p.BluRay",
                    "trackedDownloadState": "downloading",
                    "status": "downloading",
                },
            ),
            sonarr_records=(),
        )

    monkeypatch.setattr(
        "app.refiner_service.fetch_refiner_queue_snapshot",
        _fake_snap,
    )
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: _fake_probe_multi_audio(),
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir(); output.mkdir()
        f = watched / "orphan.mkv"
        f.write_bytes(b"x" * 500)
        old_t = _time.time() - 120
        _os.utime(f, (old_t, old_t))
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_minimum_age_seconds = 60
            row.radarr_enabled = True
            await session.commit()
            result = await run_refiner_pass(
                session, trigger="scheduled"
            )
            assert result["errors"] == 0
            assert result["dry_run_items"] == 0

    asyncio.run(_go())


def test_refiner_pass_proceeds_when_radarr_fetch_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Radarr authority fetch failure does not block —
    fail open."""
    import os as _os, time as _time

    async def _fake_snap_fail(row):
        from app.refiner_source_readiness import RefinerQueueSnapshot
        return RefinerQueueSnapshot(
            radarr_configured=True,
            sonarr_configured=False,
            radarr_fetch_succeeded=False,  # fetch failed
            sonarr_fetch_succeeded=False,
            radarr_records=(),
            sonarr_records=(),
        )

    monkeypatch.setattr(
        "app.refiner_service.fetch_refiner_queue_snapshot",
        _fake_snap_fail,
    )
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: _fake_probe_multi_audio(),
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir(); output.mkdir()
        f = watched / "movie.mkv"
        f.write_bytes(b"x" * 500)
        old_t = _time.time() - 120
        _os.utime(f, (old_t, old_t))
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_minimum_age_seconds = 60
            row.radarr_enabled = True
            await session.commit()
            result = await run_refiner_pass(
                session, trigger="scheduled"
            )
            # Fetch failed → fail open → file processed
            assert result["dry_run_items"] == 1
            assert result["errors"] == 0

    asyncio.run(_go())


def test_refiner_pass_proceeds_when_radarr_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No ownership check when radarr_enabled=False."""
    import os as _os, time as _time

    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: _fake_probe_multi_audio(),
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir(); output.mkdir()
        f = watched / "movie.mkv"
        f.write_bytes(b"x" * 500)
        old_t = _time.time() - 120
        _os.utime(f, (old_t, old_t))
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_minimum_age_seconds = 60
            row.radarr_enabled = False
            await session.commit()
            result = await run_refiner_pass(
                session, trigger="scheduled"
            )
            assert result["dry_run_items"] == 1
            assert result["errors"] == 0

    asyncio.run(_go())


def test_refiner_pass_proceeds_for_import_pending_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """importPending file with matching title proceeds —
    the core workflow: Radarr waits for Refiner to
    deliver the file to the output folder before it
    can import."""
    import os as _os
    import time as _time

    async def _fake_snap(row):
        from app.refiner_source_readiness import RefinerQueueSnapshot
        return RefinerQueueSnapshot(
            radarr_configured=True,
            sonarr_configured=False,
            radarr_fetch_succeeded=True,
            sonarr_fetch_succeeded=False,
            radarr_records=(
                {
                    "title": "orphan.2006.1080p.bluray",
                    "trackedDownloadState": "importPending",
                    "status": "completed",
                    "trackedDownloadStatus": "warning",
                },
            ),
            sonarr_records=(),
        )

    monkeypatch.setattr(
        "app.refiner_service.fetch_refiner_queue_snapshot",
        _fake_snap,
    )
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda _p: _fake_probe_multi_audio(),
    )

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "orphan.2006.1080p.bluray.mkv"
        f.write_bytes(b"x" * 500)
        old_t = _time.time() - 120
        _os.utime(f, (old_t, old_t))
        async with SessionLocal() as session:
            row = await get_or_create_settings(session)
            row.refiner_enabled = True
            row.refiner_dry_run = True
            row.refiner_primary_audio_lang = "eng"
            row.refiner_watched_folder = str(watched)
            row.refiner_output_folder = str(output)
            row.refiner_minimum_age_seconds = 60
            row.radarr_enabled = True
            await session.commit()
            result = await run_refiner_pass(
                session, trigger="scheduled"
            )
            assert result["dry_run_items"] == 1
            assert result["errors"] == 0

    asyncio.run(_go())
