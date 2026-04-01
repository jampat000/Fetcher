"""Refiner watched-folder cleanup: empty subfolders and allowlisted sidecar files."""

from __future__ import annotations

import logging
from pathlib import Path

from app.refiner_rules import REFINER_SOURCE_SIDECAR_CLEANUP_SUFFIXES

logger = logging.getLogger(__name__)


def _try_remove_empty_watch_subfolder(*, source_parent: Path, watched_root: Path) -> str:
    """Remove the immediate parent of the source file only if it is empty and strictly inside watched_root.

    Does not walk up beyond one level. Returns a short token for activity context / support logs.
    """
    try:
        w = watched_root.resolve()
        parent = source_parent.resolve()
    except OSError as e:
        logger.info("Refiner folder cleanup: skipped (could not resolve paths: %s)", e)
        return "skipped_resolve"
    if parent == w:
        logger.info(
            "Refiner folder cleanup: skipped (source was directly under watch root: %s)",
            parent,
        )
        return "skipped_watch_root"
    try:
        parent.relative_to(w)
    except ValueError:
        logger.info(
            "Refiner folder cleanup: skipped (parent %s is not under watch root %s)",
            parent,
            w,
        )
        return "skipped_not_under_watch"
    if not parent.is_dir():
        logger.info("Refiner folder cleanup: skipped (not a directory: %s)", parent)
        return "skipped_not_dir"
    try:
        entries = list(parent.iterdir())
    except OSError as e:
        logger.warning("Refiner folder cleanup: skipped (could not list %s: %s)", parent, e)
        return "skipped_list_error"
    if entries:
        logger.info(
            "Refiner folder cleanup: skipped (folder not empty: %s has %s item(s))",
            parent,
            len(entries),
        )
        return "skipped_not_empty"
    try:
        parent.rmdir()
    except OSError as e:
        logger.warning("Refiner folder cleanup: failed to remove %s (%s)", parent, e)
        return "failed_rmdir"
    logger.info("Refiner folder cleanup: removed empty folder %s", parent)
    return "removed_empty_folder"


def _cleanup_refiner_source_sidecar_artifacts_after_success(
    *, media_parent: Path, watched_root: Path
) -> int:
    """Remove allowlisted download/repair sidecars from the watched source folder only.

    Runs only after successful completion of the media file that lived under
    ``media_parent``. Direct children only; suffix allowlist in
    ``REFINER_SOURCE_SIDECAR_CLEANUP_SUFFIXES``. Does not touch output or work trees.
    """
    try:
        w = watched_root.resolve()
        parent = media_parent.resolve()
    except OSError as e:
        logger.info("Refiner sidecar cleanup: skipped (could not resolve paths: %s)", e)
        return 0
    try:
        parent.relative_to(w)
    except ValueError:
        logger.info(
            "Refiner sidecar cleanup: skipped (parent %s is not under watch root %s)",
            parent,
            w,
        )
        return 0
    if not parent.is_dir():
        return 0
    removed = 0
    try:
        entries = list(parent.iterdir())
    except OSError as e:
        logger.warning("Refiner sidecar cleanup: skipped (could not list %s: %s)", parent, e)
        return 0
    for entry in entries:
        try:
            if not entry.is_file():
                continue
        except OSError:
            continue
        suf = entry.suffix.lower()
        if suf not in REFINER_SOURCE_SIDECAR_CLEANUP_SUFFIXES:
            continue
        try:
            entry.unlink()
            removed += 1
            logger.info("Refiner sidecar cleanup: removed %s", entry.name)
        except OSError as e:
            logger.warning(
                "Refiner sidecar cleanup: could not remove %s (%s)",
                entry,
                e,
            )
    return removed
