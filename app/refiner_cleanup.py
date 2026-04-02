"""Refiner watched-folder cleanup: post-success file pruning and source-folder removal."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def _try_remove_empty_watch_subfolder(*, source_parent: Path, watched_root: Path) -> str:
    """Remove the processed source folder tree for one completed item.

    Safety rules:
    - only the provided ``source_parent`` (and children) may be removed;
    - folder must resolve strictly under ``watched_root``;
    - ``watched_root`` itself is never removed.

    Returns a short token for activity context / support logs.
    Raises ``RuntimeError`` on removal failure so callers can mark the item as not-fully-cleaned.
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
    if not parent.exists():
        logger.info("Refiner folder cleanup: skipped (already removed: %s)", parent)
        return "skipped_not_dir"
    if not parent.is_dir():
        logger.info("Refiner folder cleanup: skipped (not a directory: %s)", parent)
        return "skipped_not_dir"
    try:
        shutil.rmtree(parent)
    except OSError as e:
        logger.warning("Refiner folder cleanup: failed to remove source folder tree %s (%s)", parent, e)
        raise RuntimeError(
            f"Source folder removal failed after successful file cleanup; could not remove {parent.name!r}."
        ) from e
    logger.info("Refiner folder cleanup: removed source folder tree %s", parent)
    return "removed_source_folder"


def _cleanup_refiner_source_sidecar_artifacts_after_success(
    *, media_parent: Path, watched_root: Path
) -> int:
    """Remove all direct-child files from the processed watched source folder only.

    Runs only after successful completion of the media file that lived under
    ``media_parent``. Scope is intentionally narrow (direct children only; no recursion),
    and never touches output/work trees.

    Returns the number of files removed. Raises ``RuntimeError`` when one or more files
    could not be removed so callers can mark the item as cleanup-failed.
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
    failures: list[str] = []
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
        try:
            entry.unlink()
            removed += 1
            logger.info("Refiner source cleanup: removed %s", entry.name)
        except OSError as e:
            logger.warning(
                "Refiner source cleanup: could not remove %s (%s)",
                entry,
                e,
            )
            failures.append(entry.name)
    if failures:
        raise RuntimeError(
            "Source folder cleanup failed after successful output finalize; could not delete: "
            + ", ".join(sorted(failures))
        )
    return removed
