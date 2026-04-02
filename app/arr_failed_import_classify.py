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
    CORRUPT = "corrupt"
    DOWNLOAD_FAILED = "download_failed"
    IMPORT_FAILED = "import_failed"  # Generic / unclassified import failure (opt-in toggles)
    UNMATCHED = "unmatched"
    QUALITY = "quality"
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


# Radarr history: corrupt/unreadable/damaged file signals
_RADARR_CORRUPT_HISTORY: tuple[tuple[str, str], ...] = (
    ("movie file is corrupt", "corrupt file"),
    ("movie file is corrupted", "corrupt file"),
    ("one or more movies expected", "expected movie files missing"),
    ("hash mismatch", "hash mismatch"),
    ("checksum failed", "checksum failed"),
    ("checksum does not match", "checksum mismatch"),
    ("file is corrupt", "corrupt file"),
    ("file may be corrupt", "possibly corrupt file"),
    ("corrupt", "corrupt file"),
    ("damaged", "damaged file"),
)

# Radarr history: quality/upgrade rejection signals
_RADARR_QUALITY_HISTORY: tuple[tuple[str, str], ...] = (
    ("not a preferred word upgrade for existing movie file", "not a preferred-word upgrade"),
    ("not an upgrade for existing movie file", "not an upgrade vs existing file"),
)

# Radarr queue: corrupt/unreadable signals
_RADARR_CORRUPT_QUEUE: tuple[tuple[str, str], ...] = (
    ("movie file is corrupt", "corrupt file"),
    ("movie file is corrupted", "corrupt file"),
    ("one or more movies expected", "expected movie files missing"),
    ("hash mismatch", "hash mismatch"),
    ("checksum failed", "checksum failed"),
    ("checksum does not match", "checksum mismatch"),
    ("file is corrupt", "corrupt file"),
    ("file may be corrupt", "possibly corrupt file"),
    ("corrupt", "corrupt file"),
    ("damaged", "damaged file"),
)

# Radarr queue: quality/upgrade rejection signals
_RADARR_QUALITY_QUEUE: tuple[tuple[str, str], ...] = (
    ("not a preferred word upgrade for existing movie file", "not a preferred-word upgrade"),
    ("not an upgrade for existing movie file", "not an upgrade vs existing file"),
)

# Radarr queue: unmatched / manual import required signals
_RADARR_UNMATCHED_QUEUE: tuple[tuple[str, str], ...] = (
    ("manual import required", "manual import required"),
    ("matched to movie by id", "matched by id — manual import required"),
    ("unable to import automatically", "unable to import automatically"),
    ("found matching movie via grab history", "grab history match — manual import required"),
)

# Sonarr history: corrupt/unreadable/damaged file signals
_SONARR_CORRUPT_HISTORY: tuple[tuple[str, str], ...] = (
    ("episode file is corrupted", "corrupt file"),
    ("episode file is corrupt", "corrupt file"),
    ("one or more episodes expected", "expected episode files missing"),
    ("hash mismatch", "hash mismatch"),
    ("checksum failed", "checksum failed"),
    ("checksum does not match", "checksum mismatch"),
    ("file is corrupt", "corrupt file"),
    ("file may be corrupt", "possibly corrupt file"),
    ("corrupt", "corrupt file"),
    ("damaged", "damaged file"),
)

# Sonarr history: quality/upgrade rejection signals
_SONARR_QUALITY_HISTORY: tuple[tuple[str, str], ...] = (
    ("not an upgrade for existing episode file", "not an upgrade vs existing file"),
    ("not a custom format upgrade", "not a custom-format upgrade"),
)

# Sonarr queue: corrupt/unreadable signals
_SONARR_CORRUPT_QUEUE: tuple[tuple[str, str], ...] = (
    ("episode file is corrupted", "corrupt file"),
    ("episode file is corrupt", "corrupt file"),
    ("one or more episodes expected", "expected episode files missing"),
    ("hash mismatch", "hash mismatch"),
    ("checksum failed", "checksum failed"),
    ("checksum does not match", "checksum mismatch"),
    ("file is corrupt", "corrupt file"),
    ("file may be corrupt", "possibly corrupt file"),
    ("corrupt", "corrupt file"),
    ("damaged", "damaged file"),
)

# Sonarr queue: quality/upgrade rejection signals
_SONARR_QUALITY_QUEUE: tuple[tuple[str, str], ...] = (
    ("not an upgrade for existing episode file", "not an upgrade vs existing file"),
    ("not a custom format upgrade", "not a custom-format upgrade"),
)

# Sonarr queue: unmatched / manual import required signals
_SONARR_UNMATCHED_QUEUE: tuple[tuple[str, str], ...] = (
    ("manual import required", "manual import required"),
    ("unable to import automatically", "unable to import automatically"),
    ("found matching episode via grab history", "grab history match — manual import required"),
)

# importFailed history uses the same phrase tables as queue for unmatched / manual-import reasons.
_RADARR_UNMATCHED_HISTORY: tuple[tuple[str, str], ...] = _RADARR_UNMATCHED_QUEUE
_SONARR_UNMATCHED_HISTORY: tuple[tuple[str, str], ...] = _SONARR_UNMATCHED_QUEUE

# Radarr queue: download-client / grab failure (parallels history eventType downloadFailed)
_RADARR_DOWNLOAD_FAILED_QUEUE: tuple[tuple[str, str], ...] = (
    ("wasn't grabbed by radarr", "release not grabbed by Radarr"),
    ("was not grabbed by radarr", "release not grabbed by Radarr"),
    ("download wasn't grabbed by radarr", "release not grabbed by Radarr"),
    ("the download failed", "download failed"),
    ("download failed", "download failed"),
    ("download client failed", "download client failed"),
    ("download client couldn't import", "download client import failed"),
    ("download client could not import", "download client import failed"),
)

# Sonarr queue: download-client / grab failure
_SONARR_DOWNLOAD_FAILED_QUEUE: tuple[tuple[str, str], ...] = (
    ("wasn't grabbed by sonarr", "release not grabbed by Sonarr"),
    ("was not grabbed by sonarr", "release not grabbed by Sonarr"),
    ("episode wasn't grabbed by sonarr", "episode not grabbed by Sonarr"),
    ("episode was not grabbed by sonarr", "episode not grabbed by Sonarr"),
    ("the download failed", "download failed"),
    ("download failed", "download failed"),
    ("download client failed", "download client failed"),
    ("download client couldn't import", "download client import failed"),
    ("download client could not import", "download client import failed"),
)

# Radarr queue: generic import-failed wording (after specific scenarios; opt-in toggles)
_RADARR_IMPORT_FAILED_QUEUE: tuple[tuple[str, str], ...] = (
    ("failed to import", "import failed"),
    ("import failed", "import failed"),
)

# Sonarr queue: generic import-failed wording
_SONARR_IMPORT_FAILED_QUEUE: tuple[tuple[str, str], ...] = (
    ("failed to import", "import failed"),
    ("import failed", "import failed"),
)


def tracked_queue_download_state_is_failed(rec: dict[str, Any]) -> bool:
    """
    Radarr/Sonarr ``GET /api/v3/queue`` — ``trackedDownloadState`` ``Failed`` when the client
    reported a terminal download failure (not importPending / downloading).
    """
    raw = rec.get("trackedDownloadState")
    if isinstance(raw, str):
        norm = "".join(raw.strip().casefold().split())
        return norm == "failed"
    return False


def _first_terminal_label(low: str, table: tuple[tuple[str, str], ...]) -> str | None:
    for needle, label in table:
        if needle in low:
            return label
    return None


def is_radarr_download_failed_record(rec: dict[str, Any]) -> bool:
    """History record with eventType == 'downloadFailed' (integer 4 or string)."""
    et = rec.get("eventType")
    if isinstance(et, str):
        return et.strip().casefold() == "downloadfailed"
    if isinstance(et, int):
        return et == 4
    return False


def is_sonarr_download_failed_record(rec: dict[str, Any]) -> bool:
    """History record with eventType == 'downloadFailed' (integer 4 or string)."""
    et = rec.get("eventType")
    if isinstance(et, str):
        return et.strip().casefold() == "downloadfailed"
    if isinstance(et, int):
        return et == 4
    return False


def radarr_import_failed_history_disposition(
    rec: dict[str, Any],
) -> FailedImportDisposition:
    if import_failed_record_is_pending_waiting_no_eligible(rec):
        return FailedImportDisposition.PENDING_WAITING
    low = flatten_import_failed_history_text(rec).casefold()
    if _first_terminal_label(low, _RADARR_QUALITY_HISTORY):
        return FailedImportDisposition.QUALITY
    if _first_terminal_label(low, _RADARR_UNMATCHED_HISTORY):
        return FailedImportDisposition.UNMATCHED
    if _first_terminal_label(low, _RADARR_CORRUPT_HISTORY):
        return FailedImportDisposition.CORRUPT
    if _read_file_failure_terminal_label(low):
        return FailedImportDisposition.CORRUPT
    return FailedImportDisposition.UNKNOWN


def radarr_import_failed_history_terminal_label(rec: dict[str, Any]) -> str | None:
    """None when not a terminal disposition (caller already handled PENDING)."""
    if import_failed_record_is_pending_waiting_no_eligible(rec):
        return None
    low = flatten_import_failed_history_text(rec).casefold()
    lab = _first_terminal_label(low, _RADARR_QUALITY_HISTORY)
    if lab:
        return lab
    lab = _first_terminal_label(low, _RADARR_UNMATCHED_HISTORY)
    if lab:
        return lab
    lab = _first_terminal_label(low, _RADARR_CORRUPT_HISTORY)
    if lab:
        return lab
    return _read_file_failure_terminal_label(low)


def radarr_queue_scenario_label(queue_blob: str) -> tuple[FailedImportDisposition, str] | None:
    """
    Return (scenario, label) for the first matching queue signal, or None.
    Checks quality first (most specific), then unmatched, then corrupt.
    """
    low = (queue_blob or "").casefold()
    lab = _first_terminal_label(low, _RADARR_QUALITY_QUEUE)
    if lab:
        return FailedImportDisposition.QUALITY, lab
    lab = _first_terminal_label(low, _RADARR_UNMATCHED_QUEUE)
    if lab:
        return FailedImportDisposition.UNMATCHED, lab
    lab = _first_terminal_label(low, _RADARR_CORRUPT_QUEUE)
    if lab:
        return FailedImportDisposition.CORRUPT, lab
    lab = _read_file_failure_terminal_label(low)
    if lab:
        return FailedImportDisposition.CORRUPT, lab
    lab = _first_terminal_label(low, _RADARR_DOWNLOAD_FAILED_QUEUE)
    if lab:
        return FailedImportDisposition.DOWNLOAD_FAILED, lab
    lab = _first_terminal_label(low, _RADARR_IMPORT_FAILED_QUEUE)
    if lab:
        return FailedImportDisposition.IMPORT_FAILED, lab
    return None


def radarr_queue_terminal_cleanup_label(queue_blob: str) -> str | None:
    result = radarr_queue_scenario_label(queue_blob)
    return result[1] if result else None


def sonarr_import_failed_history_disposition(
    rec: dict[str, Any],
) -> FailedImportDisposition:
    if import_failed_record_is_pending_waiting_no_eligible(rec):
        return FailedImportDisposition.PENDING_WAITING
    low = flatten_import_failed_history_text(rec).casefold()
    if _first_terminal_label(low, _SONARR_QUALITY_HISTORY):
        return FailedImportDisposition.QUALITY
    if _first_terminal_label(low, _SONARR_UNMATCHED_HISTORY):
        return FailedImportDisposition.UNMATCHED
    if _first_terminal_label(low, _SONARR_CORRUPT_HISTORY):
        return FailedImportDisposition.CORRUPT
    if _read_file_failure_terminal_label(low):
        return FailedImportDisposition.CORRUPT
    return FailedImportDisposition.UNKNOWN


def sonarr_import_failed_history_terminal_label(rec: dict[str, Any]) -> str | None:
    if import_failed_record_is_pending_waiting_no_eligible(rec):
        return None
    low = flatten_import_failed_history_text(rec).casefold()
    lab = _first_terminal_label(low, _SONARR_QUALITY_HISTORY)
    if lab:
        return lab
    lab = _first_terminal_label(low, _SONARR_UNMATCHED_HISTORY)
    if lab:
        return lab
    lab = _first_terminal_label(low, _SONARR_CORRUPT_HISTORY)
    if lab:
        return lab
    return _read_file_failure_terminal_label(low)


def sonarr_queue_scenario_label(queue_blob: str) -> tuple[FailedImportDisposition, str] | None:
    """
    Return (scenario, label) for the first matching queue signal, or None.
    """
    low = (queue_blob or "").casefold()
    lab = _first_terminal_label(low, _SONARR_QUALITY_QUEUE)
    if lab:
        return FailedImportDisposition.QUALITY, lab
    lab = _first_terminal_label(low, _SONARR_UNMATCHED_QUEUE)
    if lab:
        return FailedImportDisposition.UNMATCHED, lab
    lab = _first_terminal_label(low, _SONARR_CORRUPT_QUEUE)
    if lab:
        return FailedImportDisposition.CORRUPT, lab
    lab = _read_file_failure_terminal_label(low)
    if lab:
        return FailedImportDisposition.CORRUPT, lab
    lab = _first_terminal_label(low, _SONARR_DOWNLOAD_FAILED_QUEUE)
    if lab:
        return FailedImportDisposition.DOWNLOAD_FAILED, lab
    lab = _first_terminal_label(low, _SONARR_IMPORT_FAILED_QUEUE)
    if lab:
        return FailedImportDisposition.IMPORT_FAILED, lab
    return None


def sonarr_queue_terminal_cleanup_label(queue_blob: str) -> str | None:
    result = sonarr_queue_scenario_label(queue_blob)
    return result[1] if result else None
