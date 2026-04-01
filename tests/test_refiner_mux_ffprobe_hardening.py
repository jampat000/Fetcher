from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from app.refiner_mux import ffprobe_json


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
    with pytest.raises(RuntimeError, match="empty probe output"):
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
    with pytest.raises(RuntimeError, match="invalid probe payload"):
        ffprobe_json(p)
