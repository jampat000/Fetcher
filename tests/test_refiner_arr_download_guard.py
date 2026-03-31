"""*arr queue guard: active download rows block Refiner until complete."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.refiner_arr_download_guard import queue_row_active_download_in_progress


def test_queue_row_active_when_sizeleft_positive() -> None:
    assert queue_row_active_download_in_progress({"sizeleft": 100, "status": "completed"}) is True


def test_queue_row_inactive_when_sizeleft_zero_and_completed() -> None:
    assert queue_row_active_download_in_progress({"sizeleft": 0, "status": "completed"}) is False


def test_queue_row_active_on_downloading_status() -> None:
    assert queue_row_active_download_in_progress({"sizeleft": 0, "status": "downloading"}) is True


def test_refiner_path_blocked_when_queue_matches_path_and_active(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from app.refiner_arr_download_guard import refiner_path_blocked_by_arr_active_download

    media = tmp_path / "Movie" / "film.mkv"
    media.parent.mkdir(parents=True)
    media.write_bytes(b"x")

    class _FakeClient:
        pass

    async def _fake_fetch(_client: object) -> list[dict]:
        return [
            {
                "outputPath": str(media.parent.resolve()),
                "sizeleft": 999,
                "status": "downloading",
                "downloadId": "abc",
            }
        ]

    monkeypatch.setattr(
        "app.refiner_arr_download_guard._fetch_queue_records",
        _fake_fetch,
    )

    async def _go() -> tuple[bool, str]:
        return await refiner_path_blocked_by_arr_active_download(
            media,
            sonarr_client=_FakeClient(),
            radarr_client=None,
        )

    blocked, rc = asyncio.run(_go())
    assert blocked is True
    assert "queue_active_download" in rc


def test_refiner_path_not_blocked_when_queue_fetch_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from app.refiner_arr_download_guard import refiner_path_blocked_by_arr_active_download

    media = tmp_path / "film.mkv"
    media.write_bytes(b"x")

    class _FakeClient:
        pass

    async def _boom(_client: object) -> list[dict]:
        raise RuntimeError("network")

    monkeypatch.setattr(
        "app.refiner_arr_download_guard._fetch_queue_records",
        _boom,
    )

    async def _go() -> tuple[bool, str]:
        return await refiner_path_blocked_by_arr_active_download(
            media,
            sonarr_client=_FakeClient(),
            radarr_client=None,
        )

    blocked, _ = asyncio.run(_go())
    assert blocked is False
