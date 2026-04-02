from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.refiner_movie_wrong_content import evaluate_movie_wrong_content, _SOFT_SCORE_THRESHOLD
from app.refiner_pipeline import _process_one_refiner_file_sync
from app.refiner_rules import RefinerRulesConfig


def _probe(*, duration_s: str, video: bool = True, audio: bool = True) -> dict:
    streams = []
    idx = 0
    if video:
        streams.append({"codec_type": "video", "index": idx, "codec_name": "h264"})
        idx += 1
    if audio:
        streams.append(
            {
                "codec_type": "audio",
                "index": idx,
                "codec_name": "aac",
                "channels": 2,
                "tags": {"language": "eng"},
            }
        )
    return {"streams": streams, "format": {"duration": duration_s}}


def test_wrong_content_runtime_far_below_expected(tmp_path: Path) -> None:
    p = tmp_path / "m.mkv"
    p.write_bytes(b"x")
    v = evaluate_movie_wrong_content(
        p,
        _probe(duration_s="2400"),
        [{"codec_type": "video", "index": 0}],
        target_title="Target Movie",
        target_year=2020,
        expected_runtime_minutes=100.0,
    )
    assert v.wrong_content
    assert v.hard_trigger
    assert "runtime_ratio_below" in v.triggered_reason


def test_wrong_content_runtime_far_above_expected(tmp_path: Path) -> None:
    p = tmp_path / "m.mkv"
    p.write_bytes(b"x")
    v = evaluate_movie_wrong_content(
        p,
        _probe(duration_s="36000"),
        [{"codec_type": "video", "index": 0}],
        target_title="Target Movie",
        target_year=2020,
        expected_runtime_minutes=100.0,
    )
    assert v.wrong_content
    assert v.hard_trigger
    assert "runtime_ratio_above" in v.triggered_reason


def test_wrong_content_no_video_stream(tmp_path: Path) -> None:
    p = tmp_path / "m.mkv"
    p.write_bytes(b"x")
    v = evaluate_movie_wrong_content(
        p,
        _probe(duration_s="7200", video=False),
        [],
        target_title="Target Movie",
        target_year=2020,
        expected_runtime_minutes=90.0,
    )
    assert v.wrong_content
    assert "no_video_stream" in v.triggered_reason


def test_wrong_content_normal_movie_no_trigger(tmp_path: Path) -> None:
    p = tmp_path / "Target.Movie.2020.1080p.mkv"
    p.write_bytes(b"x")
    v = evaluate_movie_wrong_content(
        p,
        _probe(duration_s="5400"),
        [{"codec_type": "video", "index": 0}],
        target_title="Target Movie",
        target_year=2020,
        expected_runtime_minutes=90.0,
    )
    assert not v.wrong_content


def test_wrong_content_borderline_soft_no_trigger(tmp_path: Path) -> None:
    p = tmp_path / "Dark.Shadow.2020.mkv"
    p.write_bytes(b"x")
    v = evaluate_movie_wrong_content(
        p,
        _probe(duration_s="5400"),
        [{"codec_type": "video", "index": 0}],
        target_title="Dark Knight Returns",
        target_year=2021,
        expected_runtime_minutes=90.0,
    )
    assert v.score < _SOFT_SCORE_THRESHOLD
    assert not v.wrong_content


def test_wrong_content_non_retryable_reason_in_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "x.mkv"
    p.write_bytes(b"x")
    monkeypatch.setattr(
        "app.refiner_pipeline.ffprobe_json",
        lambda *_a, **_k: _probe(duration_s="600"),
    )
    cfg = RefinerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        tertiary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="preferred_langs_quality",
    )
    wc = {
        "enabled": True,
        "movie_id": 9,
        "queue_id": 42,
        "target_title": "Epic Film",
        "target_year": 2020,
        "expected_runtime_minutes": 120.0,
    }
    status, meta = _process_one_refiner_file_sync(
        p,
        cfg,
        False,
        tmp_path,
        tmp_path / "out",
        tmp_path / "work",
        wc,
    )
    assert status == "error"
    assert meta.get("_refiner_reason_code") == "radarr_wrong_content"
    assert meta.get("_radarr_wrong_content_actions") is not None


def test_radarr_wrong_content_actions_called_high_confidence(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from app.refiner_radarr_wrong_content_actions import execute_radarr_wrong_content_actions

    calls: list[tuple[str, dict]] = []

    class _FakeClient:
        async def delete_queue_item(self, **kwargs):  # noqa: ANN003
            calls.append(("delete", kwargs))

        async def aclose(self) -> None:
            return None

    async def _fake_trigger(_client, **kwargs):  # noqa: ANN001
        calls.append(("search", kwargs))

    monkeypatch.setattr(
        "app.refiner_radarr_wrong_content_actions.ArrClient",
        lambda *a, **k: _FakeClient(),
    )
    monkeypatch.setattr(
        "app.refiner_radarr_wrong_content_actions.trigger_radarr_missing_search",
        _fake_trigger,
    )
    monkeypatch.setattr("app.refiner_radarr_wrong_content_actions.resolve_radarr_api_key", lambda _s: "k")

    row = SimpleNamespace(radarr_enabled=True, radarr_url="http://radarr.test")
    r = asyncio.run(execute_radarr_wrong_content_actions(row, queue_id=7, movie_id=99, dry_run=False))
    assert r.get("queue_delete_attempted") is True
    assert r.get("movies_search_ok") is True
    assert any(c[0] == "delete" for c in calls)
    assert any(c[0] == "search" for c in calls)


def test_radarr_wrong_content_skipped_when_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from app.refiner_radarr_wrong_content_actions import execute_radarr_wrong_content_actions

    monkeypatch.setattr("app.refiner_radarr_wrong_content_actions.ArrClient", lambda *a, **k: object())

    row = SimpleNamespace(radarr_enabled=True, radarr_url="http://x")
    r = asyncio.run(execute_radarr_wrong_content_actions(row, queue_id=1, movie_id=2, dry_run=True))
    assert r.get("queue_delete_attempted") is False
