"""Filesystem locations for templates and static assets."""

from __future__ import annotations

import os
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
BASE_DIR = PACKAGE_DIR.parent
TEMPLATES_DIR = PACKAGE_DIR / "templates"
STATIC_DIR = PACKAGE_DIR / "static"
def resolved_logs_dir() -> Path:
    """Writable directory for ``fetcher.log`` (rotating file handler).

    - ``FETCHER_LOG_DIR`` — full path to a directory (created if missing).
    - Otherwise ``<SQLite database parent>/logs`` so installed Windows builds use
      ``%ProgramData%\\Fetcher\\logs`` next to ``fetcher.db`` (see ``app.db.db_path``).

    The dashboard **Logs** page lists files in this directory when serving ``/logs/file``.
    """
    override = (os.environ.get("FETCHER_LOG_DIR") or "").strip()
    if override:
        p = Path(override).expanduser()
        try:
            p = p.resolve()
        except OSError:
            p = Path(override).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        return p
    from app.db import db_path

    root = db_path().parent / "logs"
    root.mkdir(parents=True, exist_ok=True)
    return root


# Deprecated: use ``resolved_logs_dir()`` (``BASE_DIR / "logs"`` is wrong for packaged services).
LOGS_DIR = BASE_DIR / "logs"


def is_safe_path(target: Path, base: Path) -> bool:
    """True when ``target`` resolves to ``base`` or a path inside ``base`` (after ``.resolve()``)."""
    t = target.resolve()
    b = base.resolve()
    return t == b or b in t.parents
