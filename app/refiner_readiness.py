"""Refiner enable vs readiness: UI messaging and scheduler gating (not form-wide validation)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from app.models import AppSettings
from app.refiner_rules import normalize_lang

RefinerUiPhase = Literal["off", "not_ready", "ready"]


@dataclass(frozen=True, slots=True)
class RefinerState:
    """Single source of truth for Refiner banners and readiness-brief API."""

    phase: RefinerUiPhase
    enabled: bool
    issue_pairs: tuple[tuple[str, str], ...]


def get_refiner_state(config: AppSettings) -> RefinerState:
    """Derive off / not_ready / ready from settings (same logic for Jinja and JSON)."""
    enabled = bool(getattr(config, "refiner_enabled", False))
    if not enabled:
        return RefinerState(phase="off", enabled=False, issue_pairs=())
    pairs = refiner_readiness_issues(config)
    if pairs:
        return RefinerState(phase="not_ready", enabled=True, issue_pairs=tuple(pairs))
    return RefinerState(phase="ready", enabled=True, issue_pairs=())


def _resolve_folder_path(raw: str) -> Path | None:
    s = (raw or "").strip()
    if not s:
        return None
    p = Path(s).expanduser()
    try:
        return p.resolve()
    except OSError:
        return None


def refiner_validate_settings_save_section(
    section: str,
    *,
    enabled: bool,
    primary_lang: str,
    watched_folder: str,
    output_folder: str,
) -> tuple[str | None, str | None]:
    """Validate only the section being saved. Returns (reason_code, user_message) or (None, None)."""
    sec = (section or "").strip().lower()
    if sec == "processing":
        return None, None
    if sec == "folders":
        if enabled and (not watched_folder or not output_folder):
            return (
                "watched_output_required",
                "Watched folder and output folder are both required while Refiner is on. Set them in this section, or turn Refiner off under Processing first.",
            )
        return None, None
    if sec == "audio":
        if enabled and not normalize_lang(primary_lang):
            return (
                "primary_audio_required",
                "Primary audio language is required while Refiner is on. Choose one in this section.",
            )
        return None, None
    return None, None


def sonarr_refiner_validate_settings_save_section(
    section: str,
    *,
    enabled: bool,
    primary_lang: str,
    watched_folder: str,
    output_folder: str,
) -> tuple[str | None, str | None]:
    """Validate only the section being saved for the Sonarr
    Refiner pipeline. Returns (reason_code, user_message)
    or (None, None)."""
    sec = (section or "").strip().lower()
    if sec == "processing":
        return None, None
    if sec == "folders":
        if enabled and (not watched_folder or not output_folder):
            return (
                "watched_output_required",
                "Watched folder and output folder are both "
                "required while Sonarr Refiner is on. Set them "
                "in this section, or turn Sonarr Refiner off "
                "under Processing first.",
            )
        return None, None
    if sec == "audio":
        if enabled and not normalize_lang(primary_lang):
            return (
                "primary_audio_required",
                "Primary audio language is required while Sonarr "
                "Refiner is on. Choose one in this section.",
            )
        return None, None
    return None, None


def refiner_readiness_issues(row: AppSettings) -> list[tuple[str, str]]:
    """Human-readable issues when Refiner is enabled but not ready to process. (fragment id, message)."""
    if not getattr(row, "refiner_enabled", False):
        return []
    issues: list[tuple[str, str]] = []
    if not normalize_lang(getattr(row, "refiner_primary_audio_lang", "") or ""):
        issues.append(
            (
                "refiner-audio",
                "Primary audio language is not set. Refiner will not process files until you choose one under Audio.",
            )
        )

    w_raw = (getattr(row, "refiner_watched_folder", "") or "").strip()
    if not w_raw:
        issues.append(("refiner-folders", "Watched folder path is missing. Set it under Folders."))
    else:
        watched = _resolve_folder_path(w_raw)
        if watched is None:
            issues.append(
                (
                    "refiner-folders",
                    "Watched folder path is invalid or could not be resolved. Use an absolute path "
                    "(e.g. /downloads in Docker or a bind mount).",
                )
            )
        elif not watched.is_dir():
            issues.append(
                (
                    "refiner-folders",
                    f"Watched folder must be an existing directory on this machine or container: {watched}",
                )
            )

    o_raw = (getattr(row, "refiner_output_folder", "") or "").strip()
    if not o_raw:
        issues.append(("refiner-folders", "Output folder path is missing. Set it under Folders."))
    else:
        output = _resolve_folder_path(o_raw)
        if output is None:
            issues.append(
                (
                    "refiner-folders",
                    "Output folder path is invalid or could not be resolved. Use an absolute path "
                    "(e.g. /output) so it matches where finished files should go.",
                )
            )
        elif not output.is_dir():
            issues.append(
                (
                    "refiner-folders",
                    f"Output folder must be an existing directory (not a file): {output}",
                )
            )

    return issues


def refiner_scheduler_should_run(row: AppSettings) -> bool:
    """True when Refiner is on and minimum fields are set so the interval job may run (execution still validates paths)."""
    if not getattr(row, "refiner_enabled", False):
        return False
    if not normalize_lang(getattr(row, "refiner_primary_audio_lang", "") or ""):
        return False
    if not (getattr(row, "refiner_watched_folder", "") or "").strip():
        return False
    if not (getattr(row, "refiner_output_folder", "") or "").strip():
        return False
    return True


def sonarr_refiner_scheduler_should_run(
    row: AppSettings,
) -> bool:
    """True when Sonarr Refiner is on and minimum fields are
    set so the interval job may run (execution still validates
    paths)."""
    if not getattr(row, "sonarr_refiner_enabled", False):
        return False
    if not normalize_lang(
        getattr(row, "sonarr_refiner_primary_audio_lang", "") or ""
    ):
        return False
    if not (
        getattr(row, "sonarr_refiner_watched_folder", "") or ""
    ).strip():
        return False
    if not (
        getattr(row, "sonarr_refiner_output_folder", "") or ""
    ).strip():
        return False
    return True
