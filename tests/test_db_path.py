"""Tests for SQLite path resolution (FETCHER_DEV_DB_PATH)."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.db import _migrate_legacy_sqlite_if_needed, db_path, default_data_dir


def test_migrate_copies_when_canonical_missing(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"sqlite-data")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    assert canonical_db.read_bytes() == b"sqlite-data"
    assert legacy_db.read_bytes() == b"sqlite-data"


def test_migrate_copies_wal_shm_when_present(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "fetcher.db").write_bytes(b"db")
    (legacy_dir / "fetcher.db-wal").write_bytes(b"wal")
    (legacy_dir / "fetcher.db-shm").write_bytes(b"shm")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    _migrate_legacy_sqlite_if_needed(canonical_db, legacy_dir / "fetcher.db")
    assert (canonical_db.parent / "fetcher.db-wal").read_bytes() == b"wal"
    assert (canonical_db.parent / "fetcher.db-shm").read_bytes() == b"shm"


def test_migrate_skips_when_canonical_exists(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "fetcher.db").write_bytes(b"legacy")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    canonical_db.parent.mkdir(parents=True)
    canonical_db.write_bytes(b"canonical")
    _migrate_legacy_sqlite_if_needed(canonical_db, legacy_dir / "fetcher.db")
    assert canonical_db.read_bytes() == b"canonical"


def test_migrate_skips_when_no_legacy(tmp_path: Path) -> None:
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    _migrate_legacy_sqlite_if_needed(canonical_db, tmp_path / "missing" / "fetcher.db")
    assert not canonical_db.parent.exists()


def test_db_path_uses_fetcher_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = tmp_path / "data"
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)
    monkeypatch.setenv("FETCHER_DATA_DIR", str(root))
    got = db_path()
    assert got == root.resolve() / "fetcher.db"


def test_db_path_uses_fetcher_dev_db_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "nested" / "fetcher-dev.sqlite3"
    monkeypatch.setenv("FETCHER_DEV_DB_PATH", str(target))
    got = db_path()
    assert got == target.resolve()
    assert target.parent.is_dir()


def test_db_path_default_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)
    assert db_path() == default_data_dir() / "fetcher.db"
