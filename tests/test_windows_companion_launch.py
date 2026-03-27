from __future__ import annotations

import asyncio
import os
import types

from app.refiner_folder_picker import ensure_windows_companion_running
import app.windows_session_launch as wsl
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
    if os.name == "nt":
        assert out.reason.startswith("invalid_companion_path:")
    else:
        assert out.reason == "non_windows"


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


def test_wts_query_user_token_failure_uses_fallback(monkeypatch) -> None:
    calls = {"fallback": 0}

    def _wts_fail(_sid, _token_ptr):
        return False

    def _fallback(_sid: int):
        calls["fallback"] += 1
        return wsl.wt.HANDLE(111), "ok"

    monkeypatch.setattr(wsl, "wtsapi32", types.SimpleNamespace(WTSQueryUserToken=_wts_fail))
    monkeypatch.setattr(wsl, "_get_primary_token_from_fallback_process_scan", _fallback)
    monkeypatch.setattr(wsl, "_close_handle", lambda _h: None)
    monkeypatch.setattr(wsl, "_last_winerr", lambda: 1008)

    primary, status, source = wsl._get_primary_token_for_session(3)
    assert primary is not None
    assert status == "ok"
    assert source == "fallback"
    assert calls["fallback"] == 1


def test_wts_query_user_token_failure_fallback_not_found(monkeypatch) -> None:
    def _wts_fail(_sid, _token_ptr):
        return False

    monkeypatch.setattr(wsl, "wtsapi32", types.SimpleNamespace(WTSQueryUserToken=_wts_fail))
    monkeypatch.setattr(
        wsl,
        "_get_primary_token_from_fallback_process_scan",
        lambda _sid: (None, "fallback_no_usable_process_token"),
    )
    monkeypatch.setattr(wsl, "_last_winerr", lambda: 1008)

    primary, status, source = wsl._get_primary_token_for_session(3)
    assert primary is None
    assert status == "fallback_no_usable_process_token"
    assert source == "fallback"


def test_fallback_process_scan_uses_non_explorer_when_needed(monkeypatch) -> None:
    monkeypatch.setattr(
        wsl,
        "_iter_session_process_candidates",
        lambda _sid: [(4321, "cmd.exe"), (8765, "powershell.exe")],
    )

    def _try(pid: int, name: str, _sid: int, _source: str):
        if pid == 4321:
            return wsl.wt.HANDLE(222), "ok"
        return None, "fallback_token_open_failed"

    monkeypatch.setattr(wsl, "_try_primary_token_from_process", _try)
    primary, status = wsl._get_primary_token_from_fallback_process_scan(3)
    assert primary is not None
    assert status == "ok"


def test_fallback_process_scan_no_usable_token(monkeypatch) -> None:
    monkeypatch.setattr(
        wsl,
        "_iter_session_process_candidates",
        lambda _sid: [(1111, "foo.exe"), (2222, "bar.exe")],
    )
    monkeypatch.setattr(
        wsl,
        "_try_primary_token_from_process",
        lambda _pid, _name, _sid, _source: (None, "fallback_token_open_failed"),
    )
    primary, status = wsl._get_primary_token_from_fallback_process_scan(7)
    assert primary is None
    assert status == "fallback_token_open_failed"


def test_fallback_process_scan_empty_candidates(monkeypatch) -> None:
    monkeypatch.setattr(wsl, "_iter_session_process_candidates", lambda _sid: [])
    primary, status = wsl._get_primary_token_from_fallback_process_scan(9)
    assert primary is None
    assert status == "fallback_no_usable_process_token"
