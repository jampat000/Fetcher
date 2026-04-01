from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from app.refiner_mux import ffprobe_json


def test_ffprobe_json_valid_file_runs_with_exact_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    p = tmp_path / "x-valid.mkv"
    p.write_bytes(b"x")
    monkeypatch.setattr("app.refiner_mux.resolve_ffprobe_ffmpeg", lambda: ("ffprobe", "ffmpeg"))
    calls: list[list[str]] = []

    def _run(argv, **kwargs):  # noqa: ANN001
        calls.append(list(argv))
        return SimpleNamespace(returncode=0, stdout='{"streams":[],"format":{}}', stderr="")

    monkeypatch.setattr("app.refiner_mux.subprocess.run", _run)
    data = ffprobe_json(p)
    assert isinstance(data, dict)
    assert len(calls) == 1
    assert calls[0][-1] == str(p)
    assert "REFINER_FFPROBE_FILE_STATE:" in caplog.text
    assert "\"resolved_path\"" in caplog.text
    assert p.name in caplog.text


def test_ffprobe_json_missing_file_returns_controlled_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    p = tmp_path / "missing.mkv"
    monkeypatch.setattr("app.refiner_mux.resolve_ffprobe_ffmpeg", lambda: ("ffprobe", "ffmpeg"))
    called = {"n": 0}

    def _run(*a, **k):  # noqa: ANN001
        called["n"] += 1
        return SimpleNamespace(returncode=0, stdout='{"streams":[],"format":{}}', stderr="")

    monkeypatch.setattr("app.refiner_mux.subprocess.run", _run)
    with pytest.raises(RuntimeError, match="file missing or empty at probe time"):
        ffprobe_json(p)
    assert called["n"] == 0


def test_ffprobe_json_zero_size_file_returns_controlled_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    p = tmp_path / "x.mkv"
    p.write_bytes(b"")
    monkeypatch.setattr("app.refiner_mux.resolve_ffprobe_ffmpeg", lambda: ("ffprobe", "ffmpeg"))
    called = {"n": 0}

    def _run(*a, **k):  # noqa: ANN001
        called["n"] += 1
        return SimpleNamespace(returncode=0, stdout='{"streams":[],"format":{}}', stderr="")

    monkeypatch.setattr("app.refiner_mux.subprocess.run", _run)
    with pytest.raises(RuntimeError, match="file missing or empty at probe time"):
        ffprobe_json(p)
    assert called["n"] == 0


def test_ffprobe_json_directory_path_returns_controlled_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    p = tmp_path / "dir"
    p.mkdir()
    monkeypatch.setattr("app.refiner_mux.resolve_ffprobe_ffmpeg", lambda: ("ffprobe", "ffmpeg"))
    called = {"n": 0}

    def _run(*a, **k):  # noqa: ANN001
        called["n"] += 1
        return SimpleNamespace(returncode=0, stdout='{"streams":[],"format":{}}', stderr="")

    monkeypatch.setattr("app.refiner_mux.subprocess.run", _run)
    with pytest.raises(RuntimeError, match="file missing or empty at probe time"):
        ffprobe_json(p)
    assert called["n"] == 0


def test_ffprobe_json_none_stdout_returns_controlled_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    p = tmp_path / "x.mkv"
    p.write_bytes(b"x")
    monkeypatch.setattr("app.refiner_mux.resolve_ffprobe_ffmpeg", lambda: ("ffprobe", "ffmpeg"))
    monkeypatch.setattr(
        "app.refiner_mux.subprocess.run",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout=None, stderr=""),
    )
    with pytest.raises(RuntimeError, match="invalid or empty output"):
        ffprobe_json(p)


def test_ffprobe_json_invalid_payload_returns_controlled_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    p = tmp_path / "x2.mkv"
    p.write_bytes(b"x")
    monkeypatch.setattr("app.refiner_mux.resolve_ffprobe_ffmpeg", lambda: ("ffprobe", "ffmpeg"))
    monkeypatch.setattr(
        "app.refiner_mux.subprocess.run",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout="[1,2,3]", stderr=""),
    )
    with pytest.raises(RuntimeError, match="invalid or empty output"):
        ffprobe_json(p)
