"""Single place for SQLite path choice + filesystem safety policy (upgrade / wrong-DB guard).

Production rules are explicit: we never silently pick a different database than the canonical
path implied by env + platform. Ambiguous multi-DB layouts stop startup with actionable text.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Ignore tiny/placeholder files so we do not treat an empty touch file as a second database.
_MIN_SUBSTANTIAL_DB_BYTES = 4096

_LAST_RESOLUTION: "DatabaseResolution | None" = None


@dataclass(frozen=True, slots=True)
class DatabaseResolution:
    """Snapshot of how the SQLite path was chosen (for startup logs and tests)."""

    canonical_path: Path
    reason: str
    """Human-readable: which rule selected ``canonical_path``."""
    legacy_candidates_checked: list[tuple[Path, str]] = field(default_factory=list)
    """Existing alternate paths inspected (path, label)."""
    skipped_policy: bool = False
    """True when FETCHER_DEV_DB_PATH is set (no legacy/conflict checks)."""


def _windows_program_data_fetcher_dir() -> Path:
    return Path(os.environ.get("PROGRAMDATA", r"C:\ProgramData")) / "Fetcher"


def default_data_dir() -> Path:
    """Writable root for SQLite when no ``FETCHER_DATA_DIR`` / ``FETCHER_DEV_DB_PATH`` is set.

    Packaged Windows (frozen): ``%ProgramData%\\Fetcher``. Otherwise: ``~/AppData/Local/Fetcher``.
    """
    if sys.platform == "win32" and getattr(sys, "frozen", False):
        base = _windows_program_data_fetcher_dir()
        base.mkdir(parents=True, exist_ok=True)
        return base
    base = Path.home() / "AppData" / "Local" / "Fetcher"
    base.mkdir(parents=True, exist_ok=True)
    return base


def compute_canonical_db_path() -> tuple[Path, str]:
    """Resolve the SQLite file path from environment (no I/O beyond mkdir for writable roots).

    Precedence matches historical ``db_path`` behavior:

    1. ``FETCHER_DEV_DB_PATH`` — full path to the database file (dev/tests/Docker).
    2. ``FETCHER_DATA_DIR`` — directory containing ``fetcher.db``.
    3. ``default_data_dir()`` / ``fetcher.db`` — packaged Windows uses ``%ProgramData%\\Fetcher``;
       dev/other uses ``%LOCALAPPDATA%\\Fetcher``-style layout.
    """
    override = (os.environ.get("FETCHER_DEV_DB_PATH") or "").strip()
    if override:
        p = Path(override).expanduser()
        try:
            p = p.resolve()
        except OSError:
            p = Path(override).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p, "FETCHER_DEV_DB_PATH (explicit database file)"

    data_dir = (os.environ.get("FETCHER_DATA_DIR") or "").strip()
    if data_dir:
        root = Path(data_dir).expanduser()
        try:
            root = root.resolve()
        except OSError:
            root = Path(data_dir).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        return root / "fetcher.db", "FETCHER_DATA_DIR (directory contains fetcher.db)"

    base = default_data_dir()
    return base / "fetcher.db", "default_data_dir() / fetcher.db (packaged Windows: ProgramData\\Fetcher)"


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size if path.is_file() else 0
    except OSError:
        return 0


def _resolve_existing(path: Path) -> Path:
    try:
        return path.resolve()
    except OSError:
        return path


def _paths_refer_to_same_file(a: Path, b: Path) -> bool:
    if not a.is_file() or not b.is_file():
        return False
    try:
        return os.path.samefile(a, b)
    except OSError:
        return _resolve_existing(a) == _resolve_existing(b)


def _windows_alternate_fetcher_dbs() -> list[tuple[Path, str]]:
    """Plausible second locations on Windows (legacy dev profile vs packaged ProgramData).

    Only **packaged** (frozen) builds run these checks. Unfrozen Windows (source/tests) may point
    ``FETCHER_DATA_DIR`` at an empty folder while a developer still has ``%LOCALAPPDATA%\\Fetcher``;
    that must not abort startup.
    """
    if sys.platform != "win32":
        return []
    if not getattr(sys, "frozen", False):
        return []
    out: list[tuple[Path, str]] = []
    la = (os.environ.get("LOCALAPPDATA") or "").strip()
    if la:
        out.append((Path(la) / "Fetcher" / "fetcher.db", "%LOCALAPPDATA%\\Fetcher\\fetcher.db"))
    pd = Path(os.environ.get("PROGRAMDATA", r"C:\ProgramData")) / "Fetcher" / "fetcher.db"
    out.append((pd, "%ProgramData%\\Fetcher\\fetcher.db"))
    return out


def enforce_database_location_policy(canonical: Path, reason: str) -> DatabaseResolution:
    """Raise ``RuntimeError`` if multiple substantial databases could confuse runtime auth/data.

    Skipped when ``FETCHER_DEV_DB_PATH`` is in effect (tests, dev, explicit file).
    """
    global _LAST_RESOLUTION

    if reason.startswith("FETCHER_DEV_DB_PATH"):
        res = DatabaseResolution(
            canonical_path=canonical,
            reason=reason,
            legacy_candidates_checked=[],
            skipped_policy=True,
        )
        _LAST_RESOLUTION = res
        return res

    checked: list[tuple[Path, str]] = []
    alternates: list[tuple[Path, str]] = []

    for alt, label in _windows_alternate_fetcher_dbs():
        checked.append((alt, label))
        if _paths_refer_to_same_file(canonical, alt):
            continue
        if _file_size(alt) >= _MIN_SUBSTANTIAL_DB_BYTES:
            alternates.append((alt, label))

    can_sz = _file_size(canonical)
    can_ok = can_sz >= _MIN_SUBSTANTIAL_DB_BYTES

    if can_ok and alternates:
        lines = [
            "Fetcher refused to start: more than one substantial SQLite database was found.",
            f"Canonical database in use would be: {canonical}",
            f"Reason: {reason}",
            "Other non-empty database file(s) on this machine:",
        ]
        for alt, label in alternates:
            lines.append(f"  - {alt} ({label})")
        lines.append(
            "Keep only the database you intend to use (stop the service, back up both files, "
            "remove or rename the extra copy, or set FETCHER_DATA_DIR to a single folder that "
            "contains the intended fetcher.db), then start again."
        )
        raise RuntimeError("\n".join(lines))

    if not can_ok and alternates:
        if len(alternates) > 1:
            lines = [
                "Fetcher refused to start: the canonical database file is missing or empty, "
                "but several older database files were found — which one to use is ambiguous.",
                f"Expected canonical path: {canonical}",
                f"Reason that path is canonical: {reason}",
                "Found:",
            ]
            for alt, label in alternates:
                lines.append(f"  - {alt} ({label})")
            lines.append(
                "Stop the Fetcher service, back up these files, keep a single fetcher.db in the "
                "folder you want (see FETCHER_DATA_DIR / ProgramData documentation), then start again."
            )
            raise RuntimeError("\n".join(lines))

        alt, label = alternates[0]
        lines = [
            "Fetcher refused to start: the canonical database file is missing or empty, but a "
            f"substantial database exists at {alt} ({label}).",
            f"Canonical path (empty or missing): {canonical}",
            f"Reason that path is canonical: {reason}",
            "This usually means data was left under an older profile path after the packaged "
            "service moved to %ProgramData%\\Fetcher (or FETCHER_DATA_DIR points at an empty folder).",
            "Fetcher does not copy databases automatically. With the service stopped, copy the "
            f"existing file to the canonical location (or set FETCHER_DATA_DIR to the folder that "
            "already contains fetcher.db), then start again.",
        ]
        raise RuntimeError("\n".join(lines))

    res = DatabaseResolution(
        canonical_path=canonical,
        reason=reason,
        legacy_candidates_checked=checked,
        skipped_policy=False,
    )
    _LAST_RESOLUTION = res
    return res


def get_last_database_resolution() -> DatabaseResolution | None:
    """Last successful resolution from :func:`enforce_database_location_policy` (or None)."""
    return _LAST_RESOLUTION


def log_database_resolution_startup(res: DatabaseResolution) -> None:
    """Structured startup lines for support (idempotent if called multiple times)."""
    logger.info("Database path (canonical): %s", res.canonical_path)
    logger.info("Database path reason: %s", res.reason)
    if res.skipped_policy:
        logger.info(
            "Database filesystem policy: skipped (FETCHER_DEV_DB_PATH — no legacy/conflict checks)"
        )
        return
    if sys.platform != "win32":
        logger.info("Database filesystem policy: non-Windows — no Windows legacy path checks")
        return
    if not getattr(sys, "frozen", False):
        logger.info(
            "Database filesystem policy: Windows unfrozen build — ProgramData vs LocalAppData "
            "multi-DB checks skipped (packaged service only)"
        )
        return
    if not res.legacy_candidates_checked:
        return
    found_legacy = [
        (p, lbl)
        for p, lbl in res.legacy_candidates_checked
        if _file_size(p) >= _MIN_SUBSTANTIAL_DB_BYTES and not _paths_refer_to_same_file(p, res.canonical_path)
    ]
    if found_legacy:
        logger.info(
            "Database filesystem policy: alternate path(s) present but not used (no conflict): %s",
            ", ".join(f"{p} ({lbl})" for p, lbl in found_legacy),
        )
    else:
        logger.info("Database filesystem policy: no conflicting alternate fetcher.db found")


def reset_database_resolution_for_tests() -> None:
    """Clear cached resolution (pytest only)."""
    global _LAST_RESOLUTION
    _LAST_RESOLUTION = None
