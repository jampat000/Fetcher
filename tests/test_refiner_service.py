from __future__ import annotations

import asyncio
import importlib
import os
from pathlib import Path
from typing import Any

import pytest

from sqlalchemy import delete, select

from app.db import SessionLocal, _get_or_create_settings
from app.models import JobRunLog, RefinerActivity
from app.time_util import utc_now_naive
from app.refiner_service import (
    _finalize_output_file,
    _pipeline_from_settings,
    _reconcile_interrupted_refiner_processing_rows_before_pass,
    _rules_config_from_settings,
    reconcile_refiner_processing_rows_on_worker_boot,
    run_refiner_pass,
)
from app.schema_version import CURRENT_SCHEMA_VERSION


def _fake_probe_multi_audio() -> dict:
    return {
        "streams": [
            {"index": 0, "codec_type": "video"},
            {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}},
            {"index": 2, "codec_type": "audio", "tags": {"language": "spa"}, "disposition": {}},
        ]
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

    asyncio.run(_go())


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


def test_refiner_schema_contract_v34_activity_context_and_trimmer_activity() -> None:
    assert CURRENT_SCHEMA_VERSION == 34
    from app.models import AppSettings, RefinerActivity

    assert "refiner_processing_pass_generation" not in AppSettings.__annotations__
    assert "processing_pass_generation" not in RefinerActivity.__annotations__
    assert "failure_hint" not in RefinerActivity.__annotations__
    assert "activity_context" in RefinerActivity.__annotations__
    migrations_text = (Path(__file__).resolve().parents[1] / "app" / "migrations.py").read_text(
        encoding="utf-8"
    )
    assert "_migrate_033_refiner_activity_context" in migrations_text
    assert "_migrate_035_activity_log_trimmer_app_identity" in migrations_text
    assert "_migrate_034_forward_app_settings_schema_version" in migrations_text
    assert "_ensure_refiner_app_settings_columns" in migrations_text
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
    monkeypatch.setattr("app.refiner_service.remux_to_temp_file", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr("app.refiner_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        (watched / "m.mkv").write_bytes(b"x")
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
    assert ("insert", "m.mkv") in calls
    assert ("update", 901, "skipped") in calls


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
            assert jl.message.startswith("Refiner (")
            low = jl.message.lower()
            assert "stream_manager" not in low
            assert "streammgr" not in low
            assert "errors=1" in jl.message
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
