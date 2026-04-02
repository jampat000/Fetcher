"""External subtitle sidecar discovery and preservation for Refiner (keep_selected mode only)."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from app.refiner_rules import RefinerRulesConfig

logger = logging.getLogger(__name__)

# Sidecar extensions only — not embedded/container streams.
EXTERNAL_SUBTITLE_SIDECAR_SUFFIXES: frozenset[str] = frozenset(
    {".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt"}
)


def should_preserve_external_subtitle_sidecars(cfg: RefinerRulesConfig) -> bool:
    """True when operator configuration keeps subtitles (embedded rules apply); external sidecars follow."""
    return cfg.subtitle_mode == "keep_selected"


def discover_matching_external_subtitle_paths(source_media_path: Path) -> list[Path]:
    """Return sorted paths to external subtitle sidecars in the same directory as the media file.

    Conservative association:
    - same parent directory as ``source_media_path``;
    - regular file with a recognized subtitle suffix;
    - filename equals ``{stem}{suffix}`` or starts with ``{stem}.`` (case-insensitive stem match).
    """
    parent = source_media_path.parent
    stem_l = source_media_path.stem.lower()
    stem_dot = stem_l + "."
    out: list[Path] = []
    try:
        src_res = source_media_path.resolve()
    except OSError:
        src_res = source_media_path
    try:
        entries = list(parent.iterdir())
    except OSError:
        return []
    for entry in entries:
        try:
            if not entry.is_file():
                continue
        except OSError:
            continue
        try:
            if entry.resolve() == src_res:
                continue
        except OSError:
            if entry.name == source_media_path.name and entry.parent == source_media_path.parent:
                continue
        suf = entry.suffix.lower()
        if suf not in EXTERNAL_SUBTITLE_SIDECAR_SUFFIXES:
            continue
        name_l = entry.name.lower()
        if name_l == stem_l + suf or name_l.startswith(stem_dot):
            out.append(entry)
    return sorted(out, key=lambda p: p.name.lower())


def preserve_external_subtitle_sidecars_if_configured(
    *,
    source_media_path: Path,
    destination_media_path: Path,
    cfg: RefinerRulesConfig,
) -> list[str]:
    """Copy matching external subtitle sidecars next to the finalized output media.

    Runs only when ``cfg.subtitle_mode == \"keep_selected\"``. Copies (never moves) from the
    watched source directory to ``destination_media_path.parent``.

    Returns basenames copied (possibly empty). Raises ``RuntimeError`` on collision with existing
    output files or copy failure (all-or-nothing; partial copies are rolled back).
    """
    if not should_preserve_external_subtitle_sidecars(cfg):
        return []
    sources = discover_matching_external_subtitle_paths(source_media_path)
    if not sources:
        return []
    dest_parent = destination_media_path.parent
    try:
        dest_parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise RuntimeError(
            f"External subtitle preservation failed — could not ensure output directory ({e})."
        ) from e

    source_stem = source_media_path.stem
    dest_stem = destination_media_path.stem
    pairs: list[tuple[Path, Path]] = []
    seen_dst_names: set[str] = set()
    for src in sources:
        src_name = src.name
        low_name = src_name.lower()
        low_stem = source_stem.lower()
        if low_name == low_stem + src.suffix.lower():
            tail = src_name[len(source_stem) :]
        elif low_name.startswith(low_stem + "."):
            tail = src_name[len(source_stem) :]
        else:
            # Should be unreachable because discovery already constrains matches.
            continue
        dst_name = f"{dest_stem}{tail}"
        if dst_name.lower() in seen_dst_names:
            raise RuntimeError(
                "External subtitle preservation blocked — multiple source subtitle files map to "
                f"the same output name {dst_name!r}. Rename source sidecars, then retry."
            )
        seen_dst_names.add(dst_name.lower())
        dst = dest_parent / dst_name
        try:
            exists = dst.exists()
        except OSError as e:
            raise RuntimeError(
                f"External subtitle preservation failed — could not check output for {dst_name!r} ({e})."
            ) from e
        if exists:
            raise RuntimeError(
                "External subtitle preservation blocked — output already has a file named "
                f"{dst_name!r}. Remove or rename it in the output folder, then retry."
            )
        pairs.append((src, dst))

    copied_dst: list[Path] = []
    copied_names: list[str] = []
    try:
        for src, dst in pairs:
            shutil.copy2(src, dst)
            copied_dst.append(dst)
            copied_names.append(src.name)
            logger.info("Refiner: preserved external subtitle sidecar to output: %s", dst.name)
    except OSError as e:
        for dst in copied_dst:
            try:
                if dst.exists():
                    dst.unlink()
            except OSError:
                logger.warning("Refiner: could not roll back partial subtitle copy %s", dst, exc_info=True)
        raise RuntimeError(
            f"External subtitle preservation failed while copying {src.name!r} ({e})."
        ) from e
    return copied_names
