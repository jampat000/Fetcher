"""Filesystem locations for templates and static assets."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
BASE_DIR = PACKAGE_DIR.parent
TEMPLATES_DIR = PACKAGE_DIR / "templates"
STATIC_DIR = PACKAGE_DIR / "static"
LOGS_DIR = BASE_DIR / "logs"


def is_safe_path(target: Path, base: Path) -> bool:
    """True when ``target`` resolves to ``base`` or a path inside ``base`` (after ``.resolve()``)."""
    t = target.resolve()
    b = base.resolve()
    return t == b or b in t.parents
