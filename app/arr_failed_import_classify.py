"""
Explicit failed-import disposition for Sonarr/Radarr queue cleanup.

Inspired by Cleanuparr’s separation of “stuck / failing imports” vs operational states,
but Fetcher stays conservative: only *clearly terminal* messages are cleanup candidates.
Transient or ambiguous *arr messages default to UNKNOWN (no remove, no blocklist).

See: https://github.com/cleanuparr/cleanuparr — strike/import-failure concepts informed
the three-way model here; phrase lists are Fetcher-owned and narrow.
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class FailedImportDisposition(str, Enum):
    """High-level classification for an importFailed history row or queue message blob."""

    PENDING_WAITING = "pending_waiting"  # Do nothing — release still settling / Refiner delay / etc.
    TERMINAL_CLEANUP = "terminal_cleanup"  # Safe cleanup candidate under Fetcher policy
    UNKNOWN = "unknown"  # Do nothing — not proven terminal


def flatten_import_failed_history_text(rec: dict[str, Any]) -> str:
    """Concatenate human-facing history fields (same sources as parse_*_reason)."""
    parts: list[str] = []
    for key in ("reason", "message", "downloadFailedMessage"):
        v = rec.get(key)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    data = rec.get("data")
    if isinstance(data, dict):
        for key in ("message", "reason", "errorMessage", "exceptionMessage"):
            v = data.get(key)
            if isinstance(v, str) and v.strip():
                parts.append(v.strip())
    return " ".join(parts)


def user_visible_text_is_pending_waiting_no_eligible(text: str) -> bool:
    """
    PENDING / WAITING — *arr still describes a download waiting to import with no eligible files.

    Includes the known good case:
    "Downloaded - Waiting to Import - No files found are eligible for import …"
    """
    low = (text or "").casefold()
    if "no files found are eligible for import" not in low:
        return False
    return "waiting to import" in low or ("downloaded" in low and "waiting" in low)


def import_failed_record_is_pending_waiting_no_eligible(rec: dict[str, Any]) -> bool:
    return user_visible_text_is_pending_waiting_no_eligible(flatten_import_failed_history_text(rec))


# "Unable to read file" alone is too generic (can appear in non-terminal contexts). Require a media hint.
_READ_FILE_FAILURE_PHRASES: tuple[str, ...] = (
    "unable to read file",
    "could not read file",
    "unable to read the file",
    "could not read the file",
    "unable to read the video file",
    "could not read the video file",
)
_MEDIA_EXTENSIONS_IN_MESSAGES: tuple[str, ...] = (
    ".mkv",
    ".mp4",
    ".m4v",
    ".avi",
    ".webm",
    ".ts",
    ".wmv",
)


def _read_file_failure_terminal_label(low: str) -> str | None:
    if not any(p in low for p in _READ_FILE_FAILURE_PHRASES):
        return None
    if any(ext in low for ext in _MEDIA_EXTENSIONS_IN_MESSAGES):
        return "unreadable source file"
    if "movie file" in low or "episode file" in low or "video file" in low:
        return "unreadable source file"
    return None


# (casefold substring, log label) — longest / most specific first.
_RADARR_TERMINAL_HISTORY: tuple[tuple[str, str], ...] = (
    ("not a preferred word upgrade for existing movie file", "not a preferred-word upgrade"),
    ("not an upgrade for existing movie file", "not an upgrade vs existing file"),
    ("movie file is corrupt", "corrupt or unreadable file"),
    ("movie file is corrupted", "corrupt or unreadable file"),
    ("one or more movies expected", "expected movie files missing"),
    ("hash mismatch", "hash mismatch"),
    ("checksum failed", "checksum failed"),
    ("checksum does not match", "checksum mismatch"),
    ("file is corrupt", "corrupt or unreadable file"),
    ("file may be corrupt", "possibly corrupt file"),
    ("corrupt", "corrupt or unreadable file"),
    ("damaged", "damaged file"),
)

_RADARR_TERMINAL_QUEUE: tuple[tuple[str, str], ...] = (
    ("not a preferred word upgrade for existing movie file", "not a preferred-word upgrade"),
    ("not an upgrade for existing movie file", "not an upgrade vs existing file"),
    ("movie file is corrupt", "corrupt or unreadable file"),
    ("movie file is corrupted", "corrupt or unreadable file"),
    ("one or more movies expected", "expected movie files missing"),
    ("hash mismatch", "hash mismatch"),
    ("checksum failed", "checksum failed"),
    ("checksum does not match", "checksum mismatch"),
    ("file is corrupt", "corrupt or unreadable file"),
    ("file may be corrupt", "possibly corrupt file"),
    ("corrupt", "corrupt or unreadable file"),
    ("damaged", "damaged file"),
)
# Queue: omit "unable to parse media info" — often transient / false positive (see tests).


_SONARR_TERMINAL_HISTORY: tuple[tuple[str, str], ...] = (
    ("not an upgrade for existing episode file", "not an upgrade vs existing file"),
    ("not a custom format upgrade", "not a custom-format upgrade"),
    ("episode file is corrupted", "corrupt or unreadable file"),
    ("episode file is corrupt", "corrupt or unreadable file"),
    ("one or more episodes expected", "expected episode files missing"),
    ("hash mismatch", "hash mismatch"),
    ("checksum failed", "checksum failed"),
    ("checksum does not match", "checksum mismatch"),
    ("file is corrupt", "corrupt or unreadable file"),
    ("file may be corrupt", "possibly corrupt file"),
    ("corrupt", "corrupt or unreadable file"),
    ("damaged", "damaged file"),
)

_SONARR_TERMINAL_QUEUE: tuple[tuple[str, str], ...] = (
    ("not an upgrade for existing episode file", "not an upgrade vs existing file"),
    ("not a custom format upgrade", "not a custom-format upgrade"),
    ("episode file is corrupted", "corrupt or unreadable file"),
    ("episode file is corrupt", "corrupt or unreadable file"),
    ("one or more episodes expected", "expected episode files missing"),
    ("hash mismatch", "hash mismatch"),
    ("checksum failed", "checksum failed"),
    ("checksum does not match", "checksum mismatch"),
    ("file is corrupt", "corrupt or unreadable file"),
    ("file may be corrupt", "possibly corrupt file"),
    ("corrupt", "corrupt or unreadable file"),
    ("damaged", "damaged file"),
)


def _first_terminal_label(low: str, table: tuple[tuple[str, str], ...]) -> str | None:
    for needle, label in table:
        if needle in low:
            return label
    return None


def radarr_import_failed_history_disposition(rec: dict[str, Any]) -> FailedImportDisposition:
    if import_failed_record_is_pending_waiting_no_eligible(rec):
        return FailedImportDisposition.PENDING_WAITING
    low = flatten_import_failed_history_text(rec).casefold()
    if _first_terminal_label(low, _RADARR_TERMINAL_HISTORY):
        return FailedImportDisposition.TERMINAL_CLEANUP
    if _read_file_failure_terminal_label(low):
        return FailedImportDisposition.TERMINAL_CLEANUP
    return FailedImportDisposition.UNKNOWN


def radarr_import_failed_history_terminal_label(rec: dict[str, Any]) -> str | None:
    """None when not TERMINAL_CLEANUP (caller already handled PENDING)."""
    if import_failed_record_is_pending_waiting_no_eligible(rec):
        return None
    low = flatten_import_failed_history_text(rec).casefold()
    lab = _first_terminal_label(low, _RADARR_TERMINAL_HISTORY)
    if lab:
        return lab
    return _read_file_failure_terminal_label(low)


def radarr_queue_terminal_cleanup_label(queue_blob: str) -> str | None:
    """``queue_blob``: flattened statusMessages + errorMessage."""
    low = (queue_blob or "").casefold()
    lab = _first_terminal_label(low, _RADARR_TERMINAL_QUEUE)
    if lab:
        return lab
    return _read_file_failure_terminal_label(low)


def sonarr_import_failed_history_disposition(rec: dict[str, Any]) -> FailedImportDisposition:
    if import_failed_record_is_pending_waiting_no_eligible(rec):
        return FailedImportDisposition.PENDING_WAITING
    low = flatten_import_failed_history_text(rec).casefold()
    if _first_terminal_label(low, _SONARR_TERMINAL_HISTORY):
        return FailedImportDisposition.TERMINAL_CLEANUP
    if _read_file_failure_terminal_label(low):
        return FailedImportDisposition.TERMINAL_CLEANUP
    return FailedImportDisposition.UNKNOWN


def sonarr_import_failed_history_terminal_label(rec: dict[str, Any]) -> str | None:
    if import_failed_record_is_pending_waiting_no_eligible(rec):
        return None
    low = flatten_import_failed_history_text(rec).casefold()
    lab = _first_terminal_label(low, _SONARR_TERMINAL_HISTORY)
    if lab:
        return lab
    return _read_file_failure_terminal_label(low)


def sonarr_queue_terminal_cleanup_label(queue_blob: str) -> str | None:
    low = (queue_blob or "").casefold()
    lab = _first_terminal_label(low, _SONARR_TERMINAL_QUEUE)
    if lab:
        return lab
    return _read_file_failure_terminal_label(low)
