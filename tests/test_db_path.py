"""Tests for SQLite path resolution (GRABBY_DEV_DB_PATH)."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.db import db_path, default_data_dir


def test_db_path_uses_grabby_dev_db_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "nested" / "grabby-dev.sqlite3"
    monkeypatch.setenv("GRABBY_DEV_DB_PATH", str(target))
    got = db_path()
    assert got == target.resolve()
    assert target.parent.is_dir()


def test_db_path_default_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRABBY_DEV_DB_PATH", raising=False)
    assert db_path() == default_data_dir() / "app.db"
