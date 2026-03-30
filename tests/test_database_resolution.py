"""Canonical DB path + Windows multi-database safety policy."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from app.database_resolution import (
    compute_canonical_db_path,
    enforce_database_location_policy,
    reset_database_resolution_for_tests,
)


def _write_db(path: Path, size: int = 5000) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)


@pytest.fixture(autouse=True)
def _clear_resolution_cache() -> None:
    reset_database_resolution_for_tests()
    yield
    reset_database_resolution_for_tests()


def test_compute_canonical_respects_fetcher_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)
    root = tmp_path / "d"
    monkeypatch.setenv("FETCHER_DATA_DIR", str(root))
    p, reason = compute_canonical_db_path()
    assert p == root.resolve() / "fetcher.db"
    assert "FETCHER_DATA_DIR" in reason


def test_compute_canonical_dev_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "dev.sqlite3"
    monkeypatch.setenv("FETCHER_DEV_DB_PATH", str(target))
    p, reason = compute_canonical_db_path()
    assert p == target.resolve()
    assert "FETCHER_DEV_DB_PATH" in reason


def test_policy_skipped_for_dev_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Two large DB files allowed when dev override is explicit (tests / dev server)."""
    monkeypatch.setenv("FETCHER_DEV_DB_PATH", str(tmp_path / "a.db"))
    p, reason = compute_canonical_db_path()
    res = enforce_database_location_policy(p, reason)
    assert res.skipped_policy is True


def test_windows_conflict_canonical_and_legacy_both_substantial(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)

    canonical_root = tmp_path / "data"
    canonical_root.mkdir()
    canonical = canonical_root / "fetcher.db"
    _write_db(canonical)

    local_root = tmp_path / "local"
    local_root.mkdir()
    monkeypatch.setenv("LOCALAPPDATA", str(local_root))
    legacy = local_root / "Fetcher" / "fetcher.db"
    _write_db(legacy)

    monkeypatch.setenv("FETCHER_DATA_DIR", str(canonical_root))
    p, reason = compute_canonical_db_path()
    with pytest.raises(RuntimeError, match="more than one substantial"):
        enforce_database_location_policy(p, reason)


def test_windows_canonical_missing_legacy_only_raises_actionable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)

    canonical_root = tmp_path / "empty_data"
    canonical_root.mkdir()

    local_root = tmp_path / "local"
    local_root.mkdir()
    monkeypatch.setenv("LOCALAPPDATA", str(local_root))
    legacy = local_root / "Fetcher" / "fetcher.db"
    _write_db(legacy)

    monkeypatch.setenv("FETCHER_DATA_DIR", str(canonical_root))
    p, reason = compute_canonical_db_path()
    with pytest.raises(RuntimeError) as excinfo:
        enforce_database_location_policy(p, reason)
    msg = str(excinfo.value)
    assert "canonical database file is missing or empty" in msg
    assert "does not copy" in msg


def test_windows_multiple_legacy_ambiguous(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)

    canonical_root = tmp_path / "empty_data"
    canonical_root.mkdir()

    local_root = tmp_path / "local"
    monkeypatch.setenv("LOCALAPPDATA", str(local_root))
    _write_db(local_root / "Fetcher" / "fetcher.db")

    pd_root = tmp_path / "ProgramData"
    monkeypatch.setenv("PROGRAMDATA", str(pd_root))
    _write_db(pd_root / "Fetcher" / "fetcher.db")

    monkeypatch.setenv("FETCHER_DATA_DIR", str(canonical_root))
    p, reason = compute_canonical_db_path()
    with pytest.raises(RuntimeError, match="ambiguous"):
        enforce_database_location_policy(p, reason)


def test_windows_unfrozen_skips_legacy_checks_even_if_localappdata_has_db(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Developer / pytest: empty FETCHER_DATA_DIR must not collide with a real profile DB."""
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)

    canonical_root = tmp_path / "data"
    monkeypatch.setenv("FETCHER_DATA_DIR", str(canonical_root))
    local_root = tmp_path / "local"
    monkeypatch.setenv("LOCALAPPDATA", str(local_root))
    _write_db(local_root / "Fetcher" / "fetcher.db")

    p, reason = compute_canonical_db_path()
    res = enforce_database_location_policy(p, reason)
    assert res.canonical_path == p
    assert not res.skipped_policy


def test_frozen_windows_ignores_fetcher_data_dir_under_local_system_profile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    r"""Service-style LOCALAPPDATA under systemprofile\...\Fetcher must not become canonical (frozen only)."""
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)

    windir = tmp_path / "Windows"
    bad_root = (
        windir
        / "System32"
        / "config"
        / "systemprofile"
        / "AppData"
        / "Local"
        / "Fetcher"
    )
    bad_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("WINDIR", str(windir))
    monkeypatch.setenv("FETCHER_DATA_DIR", str(bad_root))

    pd = tmp_path / "ProgramDataRoot"
    monkeypatch.setenv("PROGRAMDATA", str(pd))

    p, reason = compute_canonical_db_path()
    assert p == pd / "Fetcher" / "fetcher.db"
    assert "ignored" in reason.lower() or "LocalSystem" in reason


def test_frozen_windows_explicit_programdata_fetcher_data_dir_respected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)

    pd_fetcher = tmp_path / "ProgramData" / "Fetcher"
    pd_fetcher.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("FETCHER_DATA_DIR", str(pd_fetcher))

    p, reason = compute_canonical_db_path()
    assert p == pd_fetcher.resolve() / "fetcher.db"
    assert "FETCHER_DATA_DIR" in reason


def test_unfrozen_windows_does_not_ignore_systemprofile_fetcher_data_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Source/tests: odd FETCHER_DATA_DIR is honored (ignore rule is packaged frozen only)."""
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)

    windir = tmp_path / "Windows"
    bad_root = (
        windir
        / "System32"
        / "config"
        / "systemprofile"
        / "AppData"
        / "Local"
        / "Fetcher"
    )
    bad_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("WINDIR", str(windir))
    monkeypatch.setenv("FETCHER_DATA_DIR", str(bad_root))

    p, reason = compute_canonical_db_path()
    assert p == bad_root.resolve() / "fetcher.db"
    assert "FETCHER_DATA_DIR" in reason


def test_shipped_fetcher_service_xml_pins_programdata_fetcher_data_dir() -> None:
    """Regression: WinSW child must get literal ProgramData root (not %LOCALAPPDATA% in env value)."""
    xml_path = Path(__file__).resolve().parents[1] / "service" / "FetcherService.xml"
    text = xml_path.read_text(encoding="utf-8")
    env_lines = [
        ln
        for ln in text.splitlines()
        if "<env" in ln and 'name="FETCHER_DATA_DIR"' in ln
    ]
    assert len(env_lines) == 1
    assert 'value="C:\\ProgramData\\Fetcher"' in env_lines[0]
    assert "%LOCALAPPDATA%" not in env_lines[0]


def test_non_windows_no_alternates_no_raise(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    if sys.platform == "win32":
        pytest.skip("non-Windows behavior")
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)
    root = tmp_path / "d"
    monkeypatch.setenv("FETCHER_DATA_DIR", str(root))
    p, reason = compute_canonical_db_path()
    res = enforce_database_location_policy(p, reason)
    assert res.canonical_path == p
    assert res.skipped_policy is False
