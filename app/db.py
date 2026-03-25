from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple

from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.models import AppSettings, AppSnapshot

logger = logging.getLogger(__name__)

# aiosqlite: ``timeout`` is the SQLite busy-handler wait, in seconds (complements ``PRAGMA busy_timeout``).
_SQLITE_CONNECT_TIMEOUT_S = 10.0
# SQLite ``busy_timeout`` PRAGMA is in milliseconds.
_SQLITE_BUSY_TIMEOUT_MS = 10_000


def _windows_program_data_fetcher_dir() -> Path:
    return Path(os.environ.get("PROGRAMDATA", r"C:\ProgramData")) / "Fetcher"


def _legacy_windows_sqlite_path() -> Path:
    """Pre-canonical default: ``%USERPROFILE%\\AppData\\Local\\Fetcher\\fetcher.db`` (e.g. LocalSystem profile)."""
    return Path.home() / "AppData" / "Local" / "Fetcher" / "fetcher.db"


_LEGACY_ARCHIVE_SUFFIX = ".fetcher-programdata-migration-archive"
# Written next to ``fetcher.db`` after a verified copy; archive runs only if on-disk marker matches in-memory proof.
_MIGRATION_MARKER_FILENAME = "fetcher.db.migrated_from_legacy"
_MIGRATION_MARKER_KEYS = frozenset({"legacy_path", "source_size", "source_mtime_ns", "migrated_at_utc_iso"})


class _LegacyMigrationProof(NamedTuple):
    """Snapshot of legacy ``fetcher.db`` taken immediately before copy (path + metadata). Used with on-disk marker."""

    legacy_path: str
    source_size: int
    source_mtime_ns: int


def _migration_marker_path(canonical_db: Path) -> Path:
    return canonical_db.parent / _MIGRATION_MARKER_FILENAME


def _canonical_matches_migration_proof(canonical_db: Path, proof: _LegacyMigrationProof) -> bool:
    """True if ``canonical_db`` matches ``proof`` exactly (size + mtime_ns). ``copy2`` preserves mtime from source."""
    if not canonical_db.is_file():
        return False
    try:
        cst = canonical_db.stat()
    except OSError:
        return False
    return cst.st_size == proof.source_size and cst.st_mtime_ns == proof.source_mtime_ns


def _legacy_main_still_matches_proof(legacy_main: Path, proof: _LegacyMigrationProof) -> bool:
    """True if the legacy main DB still matches the pre-copy stat (exact size + mtime_ns)."""
    if not legacy_main.is_file():
        return False
    try:
        lst = legacy_main.stat()
    except OSError:
        return False
    return lst.st_size == proof.source_size and lst.st_mtime_ns == proof.source_mtime_ns


def _read_migration_marker_dict(canonical_db: Path) -> dict[str, Any] | None:
    path = _migration_marker_path(canonical_db)
    if not path.is_file():
        return None
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if not _MIGRATION_MARKER_KEYS.issubset(data.keys()):
        return None
    return data


def _marker_dict_matches_proof(data: dict[str, Any], proof: _LegacyMigrationProof) -> bool:
    try:
        lp = data.get("legacy_path")
        sz = data.get("source_size")
        mt = data.get("source_mtime_ns")
        if not isinstance(lp, str) or not isinstance(sz, int) or not isinstance(mt, int):
            return False
        return lp == proof.legacy_path and sz == proof.source_size and mt == proof.source_mtime_ns
    except TypeError:
        return False


def _write_migration_marker(canonical_db: Path, proof: _LegacyMigrationProof) -> bool:
    """Atomically write marker next to canonical DB. Never overwrites an existing marker file."""
    path = _migration_marker_path(canonical_db)
    if path.exists():
        logger.warning(
            "SQLite legacy migration skipped: refusing to overwrite existing migration marker at %s.",
            path,
        )
        return False
    payload = {
        "legacy_path": proof.legacy_path,
        "source_size": proof.source_size,
        "source_mtime_ns": proof.source_mtime_ns,
        "migrated_at_utc_iso": datetime.now(timezone.utc).isoformat(),
    }
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    except OSError as e:
        logger.warning(
            "SQLite legacy migration incomplete: could not write migration marker at %s (%s).",
            path,
            e,
        )
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    logger.info("Migration marker created: %s", path)
    return True


def _unique_archive_path(path: Path, suffix: str) -> Path:
    """Pick a non-colliding destination for renaming ``path`` with ``suffix`` appended."""
    candidate = Path(str(path) + suffix)
    if not candidate.exists():
        return candidate
    n = 1
    while True:
        c = Path(f"{path}{suffix}.{n}")
        if not c.exists():
            return c
        n += 1


def _migrate_legacy_sqlite_if_needed(canonical_db: Path, legacy_db: Path) -> _LegacyMigrationProof | None:
    """Copy legacy DB into the canonical folder once if canonical file is missing. Never overwrites canonical.

    Returns a :class:`_LegacyMigrationProof` when this call performed a verified copy (caller may archive legacy
    files). Returns ``None`` if migration was skipped or post-copy verification failed (no proof → no archive).
    """
    if canonical_db.is_file():
        logger.debug(
            "SQLite legacy migration skipped: canonical database already exists at %s (legacy is not copied or removed).",
            canonical_db,
        )
        return None
    if not legacy_db.is_file():
        logger.debug("SQLite legacy migration skipped: no legacy database at %s", legacy_db)
        return None
    try:
        legacy_resolved = str(legacy_db.resolve())
    except OSError:
        legacy_resolved = str(legacy_db)
    try:
        pre = legacy_db.stat()
    except OSError as e:
        logger.warning("SQLite legacy migration skipped: could not stat legacy database %s (%s).", legacy_db, e)
        return None
    proof = _LegacyMigrationProof(legacy_resolved, pre.st_size, pre.st_mtime_ns)
    canonical_db.parent.mkdir(parents=True, exist_ok=True)
    if _migration_marker_path(canonical_db).exists():
        logger.warning(
            "SQLite legacy migration skipped: %s exists while canonical database is missing (ambiguous state).",
            _migration_marker_path(canonical_db),
        )
        return None
    legacy_dir = legacy_db.parent
    try:
        for name in ("fetcher.db", "fetcher.db-wal", "fetcher.db-shm"):
            src = legacy_dir / name
            if src.is_file():
                shutil.copy2(src, canonical_db.parent / name)
    except OSError as e:
        logger.exception(
            "Could not migrate SQLite from %s to %s (%s). Stop the Fetcher service so files are not locked, then restart.",
            legacy_db,
            canonical_db,
            e,
        )
        raise RuntimeError(
            f"SQLite migration failed ({legacy_db} -> {canonical_db}). "
            "Stop the Fetcher service and retry."
        ) from e
    if not _canonical_matches_migration_proof(canonical_db, proof):
        try:
            cst = canonical_db.stat() if canonical_db.is_file() else None
        except OSError:
            cst = None
        logger.warning(
            "SQLite migration copy finished but verification failed (canonical at %s does not match pre-copy "
            "legacy metadata: expected size=%s mtime_ns=%s; got %s). Legacy files will not be archived; see HOWTO-RESTORE.md.",
            canonical_db,
            proof.source_size,
            proof.source_mtime_ns,
            f"size={getattr(cst, 'st_size', None)} mtime_ns={getattr(cst, 'st_mtime_ns', None)}"
            if cst is not None
            else "missing",
        )
        return None
    if not _write_migration_marker(canonical_db, proof):
        return None
    logger.info(
        "Migrated SQLite from legacy path %s to canonical path %s (one-time copy; runtime uses canonical only).",
        legacy_db,
        canonical_db,
    )
    return proof


def _archive_legacy_sqlite_after_programdata_migration(canonical_db: Path, proof: _LegacyMigrationProof) -> None:
    """Rename legacy ``fetcher.db`` (+ WAL/SHM) only when marker on disk matches ``proof`` and files match exactly.

    Call only with ``proof`` returned from :func:`_migrate_legacy_sqlite_if_needed` in the same process. Rename-only;
    marker file is left for audit. Does not delete data.
    """
    marker_data = _read_migration_marker_dict(canonical_db)
    if marker_data is None:
        logger.warning(
            "Archive skipped: migration marker missing or unreadable at %s.",
            _migration_marker_path(canonical_db),
        )
        return
    if not _marker_dict_matches_proof(marker_data, proof):
        logger.warning(
            "Archive skipped: migration marker contents do not match in-process proof (marker legacy_path=%r size=%r mtime_ns=%r; proof legacy_path=%r size=%s mtime_ns=%s).",
            marker_data.get("legacy_path"),
            marker_data.get("source_size"),
            marker_data.get("source_mtime_ns"),
            proof.legacy_path,
            proof.source_size,
            proof.source_mtime_ns,
        )
        return
    if not _canonical_matches_migration_proof(canonical_db, proof):
        try:
            cst = canonical_db.stat() if canonical_db.is_file() else None
        except OSError:
            cst = None
        logger.warning(
            "Archive skipped: canonical database at %s does not match migration proof "
            "(expected size=%s mtime_ns=%s; got %s).",
            canonical_db,
            proof.source_size,
            proof.source_mtime_ns,
            f"size={getattr(cst, 'st_size', None)} mtime_ns={getattr(cst, 'st_mtime_ns', None)}"
            if cst is not None
            else "missing",
        )
        return
    legacy_main = Path(proof.legacy_path)
    if not _legacy_main_still_matches_proof(legacy_main, proof):
        try:
            lst = legacy_main.stat() if legacy_main.is_file() else None
        except OSError:
            lst = None
        logger.warning(
            "Archive skipped: legacy main database at %s no longer matches pre-migration metadata "
            "(expected size=%s mtime_ns=%s; got %s).",
            legacy_main,
            proof.source_size,
            proof.source_mtime_ns,
            f"size={getattr(lst, 'st_size', None)} mtime_ns={getattr(lst, 'st_mtime_ns', None)}"
            if lst is not None
            else "missing",
        )
        return
    logger.info("Verified migrated DB — archiving legacy copy.")
    legacy_dir = legacy_main.parent
    # Sidecars first, then main DB; nothing should open the legacy path after migration.
    any_failed = False
    for name in ("fetcher.db-wal", "fetcher.db-shm", "fetcher.db"):
        src = legacy_dir / name
        if not src.is_file():
            continue
        dest = _unique_archive_path(src, _LEGACY_ARCHIVE_SUFFIX)
        try:
            src.rename(dest)
            logger.info("Archived legacy SQLite file %s -> %s", src, dest.name)
        except OSError as e:
            any_failed = True
            logger.warning(
                "Archive skipped: could not rename legacy SQLite file %s (%s). Authoritative DB is %s.",
                src,
                e,
                canonical_db,
            )
    if any_failed:
        logger.warning(
            "Archive skipped: one or more legacy renames failed under %s; authoritative database is %s.",
            legacy_dir,
            canonical_db,
        )
    else:
        logger.info(
            "Legacy SQLite under %s renamed with suffix %r; authoritative database is %s (migration marker retained).",
            legacy_dir,
            _LEGACY_ARCHIVE_SUFFIX,
            canonical_db,
        )


def _ensure_windows_frozen_sqlite_migrated() -> None:
    """One-time file copy before engine creation; skipped when dev path or FETCHER_DATA_DIR is set."""
    if sys.platform != "win32" or not getattr(sys, "frozen", False):
        return
    # So migration / archive lines respect FETCHER_LOG_LEVEL and redaction like the rest of startup.
    try:
        from app.log_sanitize import configure_fetcher_logging

        configure_fetcher_logging()
    except ImportError:
        pass
    if (os.environ.get("FETCHER_DEV_DB_PATH") or "").strip():
        return
    if (os.environ.get("FETCHER_DATA_DIR") or "").strip():
        logger.info(
            "Windows packaged SQLite: FETCHER_DATA_DIR is set; legacy profile migration and legacy file archive are disabled."
        )
        return
    canonical_db = _windows_program_data_fetcher_dir() / "fetcher.db"
    legacy_db = _legacy_windows_sqlite_path()
    proof = _migrate_legacy_sqlite_if_needed(canonical_db, legacy_db)
    if proof is not None:
        _archive_legacy_sqlite_after_programdata_migration(canonical_db, proof)


def default_data_dir() -> Path:
    if sys.platform == "win32" and getattr(sys, "frozen", False):
        base = _windows_program_data_fetcher_dir()
        base.mkdir(parents=True, exist_ok=True)
        return base
    base = Path.home() / "AppData" / "Local" / "Fetcher"
    base.mkdir(parents=True, exist_ok=True)
    return base


def db_path() -> Path:
    """Resolve SQLite file path.

    Precedence:

    1. ``FETCHER_DEV_DB_PATH`` — full path to the database file (dev/tests/Docker).
    2. ``FETCHER_DATA_DIR`` — directory containing ``fetcher.db`` (recommended for fixed production layout).
    3. ``default_data_dir()`` / ``fetcher.db`` — frozen Windows builds use ``%ProgramData%\\Fetcher``
       after a one-time migration from the legacy profile path if needed (see ``_ensure_windows_frozen_sqlite_migrated``).
       After a successful migration, a JSON marker ``fetcher.db.migrated_from_legacy`` is written beside the canonical DB,
       and legacy profile files may be **renamed** (not deleted) with a fixed suffix when all proof checks pass; see
       logs and ``HOWTO-RESTORE.md``.
    """
    override = (os.environ.get("FETCHER_DEV_DB_PATH") or "").strip()
    if override:
        p = Path(override).expanduser()
        try:
            p = p.resolve()
        except OSError:
            p = Path(override).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    data_dir = (os.environ.get("FETCHER_DATA_DIR") or "").strip()
    if data_dir:
        root = Path(data_dir).expanduser()
        try:
            root = root.resolve()
        except OSError:
            root = Path(data_dir).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        return root / "fetcher.db"
    return default_data_dir() / "fetcher.db"


_ensure_windows_frozen_sqlite_migrated()


def create_engine() -> AsyncEngine:
    eng = create_async_engine(
        f"sqlite+aiosqlite:///{db_path().as_posix()}",
        future=True,
        connect_args={"timeout": _SQLITE_CONNECT_TIMEOUT_S},
    )
    _register_sqlite_pragmas(eng)
    return eng


def _register_sqlite_pragmas(engine: AsyncEngine) -> None:
    """Apply WAL + sane sync + busy wait on every new SQLite connection (scheduler + HTTP concurrency)."""

    @event.listens_for(engine.sync_engine, "connect")
    def _on_sqlite_connect(dbapi_connection: object, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS};")
            cursor.execute("PRAGMA journal_mode;")
            jm = cursor.fetchone()
            if jm and str(jm[0]).upper() != "WAL":
                logger.warning(
                    "SQLite journal_mode is %r (WAL unavailable on this path/volume). "
                    "Concurrent access may see more 'database is locked' errors.",
                    jm[0],
                )
        finally:
            cursor.close()


engine = create_engine()
SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Yield one :class:`AsyncSession` per request; always closed in ``finally`` (success or exception)."""
    session = SessionLocal()
    try:
        yield session
    finally:
        await session.close()


async def _get_or_create_settings(session: AsyncSession) -> AppSettings:
    row = (await session.execute(select(AppSettings).order_by(AppSettings.id.asc()).limit(1))).scalars().first()
    if row:
        return row
    row = AppSettings()
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


async def fetch_latest_app_snapshots(session: AsyncSession) -> dict[str, AppSnapshot | None]:
    """Latest AppSnapshot row per app in one query (sonarr / radarr / emby)."""
    subq = (
        select(AppSnapshot.app, func.max(AppSnapshot.id).label("mx"))
        .where(AppSnapshot.app.in_(("sonarr", "radarr", "emby")))
        .group_by(AppSnapshot.app)
        .subquery()
    )
    rows = (
        (
            await session.execute(
                select(AppSnapshot).join(
                    subq,
                    (AppSnapshot.app == subq.c.app) & (AppSnapshot.id == subq.c.mx),
                )
            )
        )
        .scalars()
        .all()
    )
    out: dict[str, AppSnapshot | None] = {"sonarr": None, "radarr": None, "emby": None}
    for row in rows:
        if row.app in out:
            out[row.app] = row
    return out

