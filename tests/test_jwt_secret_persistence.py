"""Packaged JWT secret file: load, create-once, env override (Windows service reliability)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

from app.security_utils import (
    persistent_jwt_secret_file_path,
    resolve_fetcher_jwt_secret_at_startup,
)


@pytest.fixture
def _packaged(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    root = tmp_path / "data"
    monkeypatch.setattr("app.database_resolution.default_data_dir", lambda: root)
    return root


def test_persistent_path_under_default_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    d = tmp_path / "pd" / "Fetcher"
    monkeypatch.setattr("app.database_resolution.default_data_dir", lambda: d)
    assert persistent_jwt_secret_file_path() == d / "machine-jwt-secret"


def test_env_override_wins_over_existing_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _packaged: Path,
) -> None:
    p = _packaged / "machine-jwt-secret"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("file-secret-" + "x" * 40 + "\n", encoding="utf-8")
    monkeypatch.setenv("FETCHER_JWT_SECRET", "env-override-" + "y" * 32)
    log = logging.getLogger("test_jwt")
    got = resolve_fetcher_jwt_secret_at_startup(logger=log)
    assert got.startswith("env-override-")


def test_creates_file_first_start_then_reuses(
    monkeypatch: pytest.MonkeyPatch, _packaged: Path,
) -> None:
    monkeypatch.delenv("FETCHER_JWT_SECRET", raising=False)
    log = logging.getLogger("test_jwt")
    s1 = resolve_fetcher_jwt_secret_at_startup(logger=log)
    assert len(s1) >= 64
    path = _packaged / "machine-jwt-secret"
    assert path.is_file()
    assert s1 in path.read_text(encoding="utf-8")
    monkeypatch.delenv("FETCHER_JWT_SECRET", raising=False)
    s2 = resolve_fetcher_jwt_secret_at_startup(logger=log)
    assert s1 == s2


def test_invalid_short_file_raises(monkeypatch: pytest.MonkeyPatch, _packaged: Path) -> None:
    monkeypatch.delenv("FETCHER_JWT_SECRET", raising=False)
    p = _packaged / "machine-jwt-secret"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("short\n", encoding="utf-8")
    log = logging.getLogger("test_jwt")
    with pytest.raises(RuntimeError, match="too short"):
        resolve_fetcher_jwt_secret_at_startup(logger=log)


def test_unfrozen_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.delenv("FETCHER_JWT_SECRET", raising=False)
    log = logging.getLogger("test_jwt")
    with pytest.raises(RuntimeError, match="FETCHER_JWT_SECRET"):
        resolve_fetcher_jwt_secret_at_startup(logger=log)
