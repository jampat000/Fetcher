from __future__ import annotations

import asyncio

import pytest

from app.db import SessionLocal, _get_or_create_settings
from app.stream_manager_service import run_stream_manager_pass


def _fake_probe_multi_audio() -> dict:
    return {
        "streams": [
            {"index": 0, "codec_type": "video"},
            {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}},
            {"index": 2, "codec_type": "audio", "tags": {"language": "spa"}, "disposition": {}},
        ]
    }


def test_dry_run_does_not_call_remux(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls: list[int] = []
    monkeypatch.setattr(
        "app.stream_manager_service.remux_to_temp_then_replace",
        lambda *_a, **_k: calls.append(1),
    )
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        f = tmp_path / "m.mkv"
        f.write_bytes(b"")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = True
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_paths = str(f)
            await session.commit()
            r = await run_stream_manager_pass(session, trigger="manual")
        assert calls == []
        assert int(r.get("dry_run_items") or 0) >= 1

    asyncio.run(_go())


def test_live_run_calls_remux_once(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        "app.stream_manager_service.remux_to_temp_then_replace",
        lambda path, plan: calls.append((str(path), plan)),
    )
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        f = tmp_path / "m.mkv"
        f.write_bytes(b"")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = False
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_paths = str(f)
            await session.commit()
            r = await run_stream_manager_pass(session, trigger="manual")
        assert len(calls) == 1
        assert r.get("remuxed") == 1

    asyncio.run(_go())


def test_remux_failure_leaves_original_bytes(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    def _boom(*_a, **_k):
        raise RuntimeError("ffmpeg failed")

    monkeypatch.setattr("app.stream_manager_service.remux_to_temp_then_replace", _boom)
    monkeypatch.setattr("app.stream_manager_service.ffprobe_json", lambda _p: _fake_probe_multi_audio())

    async def _go() -> None:
        f = tmp_path / "m.mkv"
        f.write_bytes(b"original")
        async with SessionLocal() as session:
            row = await _get_or_create_settings(session)
            row.stream_manager_enabled = True
            row.stream_manager_dry_run = False
            row.stream_manager_primary_audio_lang = "eng"
            row.stream_manager_paths = str(f)
            await session.commit()
            r = await run_stream_manager_pass(session, trigger="manual")
        assert r.get("errors") == 1
        assert r.get("ok") is False
        assert f.read_bytes() == b"original"

    asyncio.run(_go())
