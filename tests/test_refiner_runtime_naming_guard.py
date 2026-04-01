"""Guard: Refiner runtime sources stay refiner-only (no legacy stream_manager / streammgr leakage)."""

from __future__ import annotations

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Python modules that define or schedule Refiner behavior, logging, and UI wiring.
_REFINER_CORE_PY = [
    _REPO_ROOT / "app" / "refiner_service.py",
    _REPO_ROOT / "app" / "refiner_pipeline.py",
    _REPO_ROOT / "app" / "refiner_mux.py",
    _REPO_ROOT / "app" / "refiner_rules.py",
    _REPO_ROOT / "app" / "refiner_errors.py",
    _REPO_ROOT / "app" / "refiner_readiness.py",
    _REPO_ROOT / "app" / "refiner_source_readiness.py",
    _REPO_ROOT / "app" / "refiner_watch_config.py",
    _REPO_ROOT / "app" / "routers" / "refiner.py",
    _REPO_ROOT / "app" / "scheduler.py",
]

_LEGACY_SUBSYSTEM_TOKENS = ("stream_manager", "streammgr", "stream_mgr")


@pytest.mark.parametrize("path", _REFINER_CORE_PY, ids=lambda p: str(p.relative_to(_REPO_ROOT)))
def test_refiner_core_py_has_no_legacy_subsystem_names(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    low = text.lower()
    for token in _LEGACY_SUBSYSTEM_TOKENS:
        assert token not in low, f"{path.relative_to(_REPO_ROOT)} must not contain {token!r}"


def test_scheduler_has_no_sm_seconds_abbreviation() -> None:
    sched = (_REPO_ROOT / "app" / "scheduler.py").read_text(encoding="utf-8")
    assert "sm_seconds" not in sched
    assert "fetcher_refiner" in sched


def test_refiner_temp_and_finalize_paths_use_refiner_markers() -> None:
    pipeline = (_REPO_ROOT / "app" / "refiner_pipeline.py").read_text(encoding="utf-8")
    svc = (_REPO_ROOT / "app" / "refiner_service.py").read_text(encoding="utf-8")
    mux = (_REPO_ROOT / "app" / "refiner_mux.py").read_text(encoding="utf-8")
    assert ".refiner-" in pipeline
    assert ".refiner." in svc
    assert 'f"{src.stem}.refiner."' in mux
