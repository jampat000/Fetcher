"""Tests for SQLite path resolution (FETCHER_DEV_DB_PATH)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from app.db import (
    _LEGACY_ARCHIVE_SUFFIX,
    _LegacyMigrationProof,
    _archive_legacy_sqlite_after_programdata_migration,
    _canonical_matches_migration_proof,
    _legacy_main_still_matches_proof,
    _marker_dict_matches_proof,
    _migrate_legacy_sqlite_if_needed,
    _migration_marker_path,
    _read_migration_marker_dict,
    _unique_archive_path,
    db_path,
    default_data_dir,
)


def test_migrate_copies_when_canonical_missing(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"sqlite-data")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    assert proof is not None
    assert proof.source_size == len(b"sqlite-data")
    assert canonical_db.read_bytes() == b"sqlite-data"
    assert legacy_db.read_bytes() == b"sqlite-data"
    marker = _migration_marker_path(canonical_db)
    assert marker.is_file()
    data = json.loads(marker.read_text(encoding="utf-8"))
    assert data["legacy_path"] == proof.legacy_path
    assert data["source_size"] == proof.source_size
    assert data["source_mtime_ns"] == proof.source_mtime_ns
    assert "migrated_at_utc_iso" in data


def test_migrate_copies_wal_shm_when_present(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "fetcher.db").write_bytes(b"db")
    (legacy_dir / "fetcher.db-wal").write_bytes(b"wal")
    (legacy_dir / "fetcher.db-shm").write_bytes(b"shm")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_dir / "fetcher.db")
    assert proof is not None
    assert (canonical_db.parent / "fetcher.db-wal").read_bytes() == b"wal"
    assert (canonical_db.parent / "fetcher.db-shm").read_bytes() == b"shm"
    assert _migration_marker_path(canonical_db).is_file()


def test_migrate_skips_when_canonical_exists(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "fetcher.db").write_bytes(b"legacy")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    canonical_db.parent.mkdir(parents=True)
    canonical_db.write_bytes(b"canonical")
    assert _migrate_legacy_sqlite_if_needed(canonical_db, legacy_dir / "fetcher.db") is None
    assert canonical_db.read_bytes() == b"canonical"
    assert not _migration_marker_path(canonical_db).exists()


def test_migrate_skips_when_no_legacy(tmp_path: Path) -> None:
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    assert _migrate_legacy_sqlite_if_needed(canonical_db, tmp_path / "missing" / "fetcher.db") is None
    assert not canonical_db.parent.exists()


def test_migrate_skips_when_migration_marker_already_exists(tmp_path: Path) -> None:
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    canonical_db.parent.mkdir(parents=True)
    _migration_marker_path(canonical_db).write_text("{}", encoding="utf-8")
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "fetcher.db").write_bytes(b"orphan")
    assert _migrate_legacy_sqlite_if_needed(canonical_db, legacy_dir / "fetcher.db") is None
    assert not canonical_db.is_file()


def test_migrate_then_archive_renames_legacy_db(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"sqlite-data")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    assert proof is not None
    assert _migration_marker_path(canonical_db).is_file()
    _archive_legacy_sqlite_after_programdata_migration(canonical_db, proof)
    assert canonical_db.read_bytes() == b"sqlite-data"
    assert not legacy_db.is_file()
    archived = legacy_dir / f"fetcher.db{_LEGACY_ARCHIVE_SUFFIX}"
    assert archived.is_file()
    assert archived.read_bytes() == b"sqlite-data"
    assert _migration_marker_path(canonical_db).is_file()


def test_migrate_then_archive_renames_wal_shm(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "fetcher.db").write_bytes(b"db")
    (legacy_dir / "fetcher.db-wal").write_bytes(b"wal")
    (legacy_dir / "fetcher.db-shm").write_bytes(b"shm")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_dir / "fetcher.db")
    assert proof is not None
    _archive_legacy_sqlite_after_programdata_migration(canonical_db, proof)
    assert not (legacy_dir / "fetcher.db").is_file()
    assert (legacy_dir / f"fetcher.db-wal{_LEGACY_ARCHIVE_SUFFIX}").read_bytes() == b"wal"
    assert (legacy_dir / f"fetcher.db-shm{_LEGACY_ARCHIVE_SUFFIX}").read_bytes() == b"shm"
    assert (legacy_dir / f"fetcher.db{_LEGACY_ARCHIVE_SUFFIX}").read_bytes() == b"db"


def test_archive_skipped_when_marker_missing_after_migrate(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"x")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    assert proof is not None
    _migration_marker_path(canonical_db).unlink()
    _archive_legacy_sqlite_after_programdata_migration(canonical_db, proof)
    assert legacy_db.is_file()


def test_archive_skipped_when_marker_does_not_match_proof(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"sqlite-data")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    assert proof is not None
    wrong = _LegacyMigrationProof(proof.legacy_path + "__wrong", proof.source_size, proof.source_mtime_ns)
    _archive_legacy_sqlite_after_programdata_migration(canonical_db, wrong)
    assert legacy_db.is_file()


def test_archive_skipped_when_marker_corrupt(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"sqlite-data")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    assert proof is not None
    _migration_marker_path(canonical_db).write_text("not json", encoding="utf-8")
    _archive_legacy_sqlite_after_programdata_migration(canonical_db, proof)
    assert legacy_db.is_file()


def test_archive_skipped_when_canonical_missing(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"x")
    st = legacy_db.stat()
    proof = _LegacyMigrationProof(str(legacy_db.resolve()), st.st_size, st.st_mtime_ns)
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    canonical_db.parent.mkdir(parents=True)
    _migration_marker_path(canonical_db).write_text(
        json.dumps(
            {
                "legacy_path": proof.legacy_path,
                "source_size": proof.source_size,
                "source_mtime_ns": proof.source_mtime_ns,
                "migrated_at_utc_iso": "2099-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    _archive_legacy_sqlite_after_programdata_migration(canonical_db, proof)
    assert legacy_db.read_bytes() == b"x"


def test_archive_skipped_when_canonical_mtime_breaks_proof(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"sqlite-bytes-here")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    assert proof is not None
    os.utime(canonical_db, (1_000_000, 1_000_000))
    _archive_legacy_sqlite_after_programdata_migration(canonical_db, proof)
    assert legacy_db.is_file()


def test_archive_skipped_when_legacy_main_changed_after_migration(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "Fetcher"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "fetcher.db"
    legacy_db.write_bytes(b"original")
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    assert proof is not None
    legacy_db.write_bytes(b"tampered!!")
    _archive_legacy_sqlite_after_programdata_migration(canonical_db, proof)
    assert legacy_db.read_bytes() == b"tampered!!"


def test_marker_dict_matches_proof_helper() -> None:
    p = _LegacyMigrationProof("/legacy/fetcher.db", 10, 99)
    assert _marker_dict_matches_proof(
        {
            "legacy_path": "/legacy/fetcher.db",
            "source_size": 10,
            "source_mtime_ns": 99,
            "migrated_at_utc_iso": "x",
        },
        p,
    )
    assert not _marker_dict_matches_proof(
        {"legacy_path": "/other", "source_size": 10, "source_mtime_ns": 99, "migrated_at_utc_iso": "x"},
        p,
    )


def test_read_migration_marker_dict_requires_all_keys(tmp_path: Path) -> None:
    canonical_db = tmp_path / "ProgramData" / "Fetcher" / "fetcher.db"
    canonical_db.parent.mkdir(parents=True)
    mp = _migration_marker_path(canonical_db)
    mp.write_text(json.dumps({"legacy_path": "a", "source_size": 1}), encoding="utf-8")
    assert _read_migration_marker_dict(canonical_db) is None


def test_canonical_matches_migration_proof_size_and_mtime(tmp_path: Path) -> None:
    p = tmp_path / "c.db"
    p.write_bytes(b"abc")
    st = p.stat()
    proof = _LegacyMigrationProof(str(p.resolve()), st.st_size, st.st_mtime_ns)
    assert _canonical_matches_migration_proof(p, proof)
    assert not _canonical_matches_migration_proof(p, _LegacyMigrationProof(proof.legacy_path, 99, st.st_mtime_ns))
    assert not _canonical_matches_migration_proof(
        p, _LegacyMigrationProof(proof.legacy_path, st.st_size, st.st_mtime_ns + 1)
    )


def test_legacy_main_still_matches_proof(tmp_path: Path) -> None:
    p = tmp_path / "fetcher.db"
    p.write_bytes(b"x")
    st = p.stat()
    proof = _LegacyMigrationProof(str(p.resolve()), st.st_size, st.st_mtime_ns)
    assert _legacy_main_still_matches_proof(p, proof)
    p.write_bytes(b"yy")
    assert not _legacy_main_still_matches_proof(p, proof)


def test_ensure_windows_migration_skipped_when_fetcher_data_dir_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_ensure_windows_frozen_sqlite_migrated`` returns early — no migrate/archive (integration with env)."""
    import app.db as db_mod

    calls: list[str] = []

    def _fake_migrate(_c: Path, _l: Path):
        calls.append("migrate")
        return None

    monkeypatch.setattr(db_mod, "_migrate_legacy_sqlite_if_needed", _fake_migrate)
    monkeypatch.setattr(db_mod.sys, "platform", "win32")
    monkeypatch.setattr(db_mod.sys, "frozen", True, raising=False)
    monkeypatch.setenv("FETCHER_DATA_DIR", r"C:\Custom\Data")
    monkeypatch.delenv("FETCHER_DEV_DB_PATH", raising=False)
    db_mod._ensure_windows_frozen_sqlite_migrated()
    assert calls == []
    monkeypatch.delenv("FETCHER_DATA_DIR", raising=False)


def test_unique_archive_path_increments_on_collision(tmp_path: Path) -> None:
    p = tmp_path / "fetcher.db"
    p.write_text("a", encoding="utf-8")
    first = _unique_archive_path(p, ".suf")
    assert first.name == "fetcher.db.suf"
    first.write_text("b", encoding="utf-8")
    second = _unique_archive_path(p, ".suf")
    assert second.name == "fetcher.db.suf.1"


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
