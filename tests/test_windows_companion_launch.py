from __future__ import annotations

import asyncio

from app.refiner_folder_picker import ensure_windows_companion_running
from app.windows_session_launch import LaunchResult, resolve_companion_exe_path, start_companion_best_effort


def test_ensure_windows_companion_running_no_active_session(monkeypatch) -> None:
    async def _unhealthy() -> bool:
        return False

    monkeypatch.setattr("app.refiner_folder_picker.get_refiner_pick_mode", lambda: "windows_companion")
    monkeypatch.setattr("app.refiner_folder_picker._LAST_COMPANION_ENSURE_ATTEMPT_MONO", 0.0)
    monkeypatch.setattr("app.refiner_folder_picker.refiner_companion_reachable", _unhealthy)
    monkeypatch.setattr(
        "app.refiner_folder_picker.resolve_companion_exe_path",
        lambda: r"C:\Program Files\Fetcher\FetcherCompanion.exe",
    )
    monkeypatch.setattr(
        "app.refiner_folder_picker.start_companion_best_effort",
        lambda _p: LaunchResult(
            attempted=True,
            launched=False,
            reason="no_active_session",
            session_id=0,
            companion_exe=_p,
            working_dir=r"C:\Program Files\Fetcher",
        ),
    )

    ok = asyncio.run(ensure_windows_companion_running(timeout_seconds=0.6))
    assert ok is False


def test_ensure_windows_companion_running_already_healthy_no_launch(monkeypatch) -> None:
    async def _healthy() -> bool:
        return True

    monkeypatch.setattr("app.refiner_folder_picker.get_refiner_pick_mode", lambda: "windows_companion")
    monkeypatch.setattr("app.refiner_folder_picker._LAST_COMPANION_ENSURE_ATTEMPT_MONO", 0.0)
    monkeypatch.setattr("app.refiner_folder_picker.refiner_companion_reachable", _healthy)

    def _boom(_p: str) -> LaunchResult:
        raise AssertionError("launch should not be attempted when companion is already healthy")

    monkeypatch.setattr("app.refiner_folder_picker.start_companion_best_effort", _boom)
    ok = asyncio.run(ensure_windows_companion_running(timeout_seconds=0.6))
    assert ok is True


def test_start_companion_best_effort_invalid_path_fails_cleanly() -> None:
    out = start_companion_best_effort(r"C:\this\path\does\not\exist\FetcherCompanion.exe")
    assert out.launched is False
    assert out.reason.startswith("invalid_companion_path:")


def test_ensure_windows_companion_running_launch_branch_executes(monkeypatch) -> None:
    calls = {"health": 0, "launch": 0}

    async def _health() -> bool:
        calls["health"] += 1
        # unhealthy before launch, healthy after first loop iteration
        return calls["health"] >= 2

    def _launch(_p: str) -> LaunchResult:
        calls["launch"] += 1
        return LaunchResult(
            attempted=True,
            launched=True,
            reason="launch_succeeded",
            session_id=2,
            companion_exe=_p,
            working_dir=r"C:\Program Files\Fetcher",
            process_id=12345,
            environment_block_created=True,
        )

    monkeypatch.setattr("app.refiner_folder_picker.get_refiner_pick_mode", lambda: "windows_companion")
    monkeypatch.setattr("app.refiner_folder_picker._LAST_COMPANION_ENSURE_ATTEMPT_MONO", 0.0)
    monkeypatch.setattr("app.refiner_folder_picker.refiner_companion_reachable", _health)
    monkeypatch.setattr(
        "app.refiner_folder_picker.resolve_companion_exe_path",
        lambda: r"C:\Program Files\Fetcher\FetcherCompanion.exe",
    )
    monkeypatch.setattr("app.refiner_folder_picker.start_companion_best_effort", _launch)

    ok = asyncio.run(ensure_windows_companion_running(timeout_seconds=1.0))
    assert ok is True
    assert calls["launch"] == 1


def test_resolve_companion_exe_path_honors_override(monkeypatch) -> None:
    monkeypatch.setenv("FETCHER_COMPANION_EXE_PATH", r"D:\Custom\FetcherCompanion.exe")
    assert resolve_companion_exe_path() == r"D:\Custom\FetcherCompanion.exe"
