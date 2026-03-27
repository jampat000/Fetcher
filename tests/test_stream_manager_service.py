from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.db import SessionLocal, _get_or_create_settings
from app.stream_manager_service import _pipeline_from_settings, _rules_config_from_settings, run_stream_manager_pass


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
    monkeypatch.setattr("app.stream_manager_service.remux_to_temp_file", lambda *_a, **_k: calls.append(1))
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"original")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = True
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_watched_folder = str(watched)
            row.stream_manager_output_folder = str(output)
            await session.commit()
            r = await run_stream_manager_pass(session, trigger="scheduled")
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

    monkeypatch.setattr("app.stream_manager_service.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

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
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = False
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_watched_folder = str(watched)
            row.stream_manager_output_folder = str(output)
            await session.commit()
            r = await run_stream_manager_pass(session, trigger="scheduled")
        assert len(calls) == 1
        assert r.get("remuxed") == 1
        assert not f.exists()
        assert (output / "a" / "m.mkv").exists()

    asyncio.run(_go())


def test_source_preserved_on_failure(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    def _boom(*_a, **_k):
        raise RuntimeError("ffmpeg failed")

    monkeypatch.setattr("app.stream_manager_service.remux_to_temp_file", _boom)
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"original")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = False
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_watched_folder = str(watched)
            row.stream_manager_output_folder = str(output)
            await session.commit()
            r = await run_stream_manager_pass(session, trigger="scheduled")
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
    monkeypatch.setattr("app.stream_manager_service.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

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
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = False
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_watched_folder = str(watched)
            row.stream_manager_output_folder = str(output)
            row.stream_manager_work_folder = str(work)
            await session.commit()
            await run_stream_manager_pass(session, trigger="scheduled")
        assert seen and seen[0] == work.resolve()

    asyncio.run(_go())


def test_missing_watched_or_output_folders_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = False
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_watched_folder = str(tmp_path / "watched")
            row.stream_manager_output_folder = ""
            await session.commit()
            result = await run_stream_manager_pass(session, trigger="scheduled")
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

    monkeypatch.setattr("app.stream_manager_service.remux_to_temp_file", _fake_remux)
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        watched = tmp_path / "watched"
        output = tmp_path / "out"
        watched.mkdir()
        output.mkdir()
        f = watched / "m.mkv"
        f.write_bytes(b"orig")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = False
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_watched_folder = str(watched)
            row.stream_manager_output_folder = str(output)
            row.stream_manager_work_folder = ""
            await session.commit()
            await run_stream_manager_pass(session, trigger="scheduled")
        assert seen
        assert seen[0].name == "refiner-work"

    asyncio.run(_go())


def test_pipeline_settings_parser(tmp_path) -> None:
    class _Row:
        stream_manager_watched_folder = str(tmp_path / "watched")
        stream_manager_output_folder = str(tmp_path / "out")
        stream_manager_work_folder = str(tmp_path / "work")

    watched, output, work = _pipeline_from_settings(_Row())  # type: ignore[arg-type]
    assert watched is not None and watched.name == "watched"
    assert output is not None and output.name == "out"
    assert work is not None and work.name == "work"


def test_rules_config_parses_dropdown_values() -> None:
    class _Row:
        stream_manager_enabled = True
        stream_manager_primary_audio_lang = "eng"
        stream_manager_secondary_audio_lang = "spa"
        stream_manager_tertiary_audio_lang = ""
        stream_manager_default_audio_slot = "secondary"
        stream_manager_remove_commentary = True
        stream_manager_subtitle_mode = "remove_all"
        stream_manager_subtitle_langs_csv = ""
        stream_manager_preserve_forced_subs = True
        stream_manager_preserve_default_subs = True
        stream_manager_audio_preference_mode = "best_available"

    cfg = _rules_config_from_settings(_Row())  # type: ignore[arg-type]
    assert cfg is not None
    assert cfg.primary_audio_lang == "eng"
    assert cfg.secondary_audio_lang == "spa"
    assert cfg.default_audio_slot == "secondary"
    assert cfg.audio_preference_mode == "preferred_langs_quality"
