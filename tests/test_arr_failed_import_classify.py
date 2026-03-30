from __future__ import annotations

from app.arr_failed_import_classify import (
    FailedImportDisposition,
    radarr_import_failed_history_disposition,
    sonarr_import_failed_history_disposition,
    terminal_cleanup_label_is_explicit_non_upgrade,
)


def test_radarr_disposition_terminal_not_upgrade() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "x",
        "reason": "Not an upgrade for existing movie file. Existing quality: WEBDL-1080p",
    }
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.TERMINAL_CLEANUP


def test_radarr_disposition_pending_waiting() -> None:
    msg = "Downloaded - Waiting to Import - No files found are eligible for import in /data"
    rec = {"eventType": "importFailed", "downloadId": "w", "reason": msg}
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.PENDING_WAITING


def test_radarr_disposition_unknown() -> None:
    rec = {"eventType": "importFailed", "downloadId": "u", "reason": "Unrecognized diagnostic XYZ"}
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.UNKNOWN


def test_radarr_disposition_ambiguous_unable_to_read_file_is_unknown() -> None:
    rec = {"eventType": "importFailed", "downloadId": "r", "reason": "Unable to read file"}
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.UNKNOWN


def test_radarr_disposition_unable_to_read_with_media_path_is_terminal() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "p",
        "reason": "Unable to read file /movies/foo.mkv",
    }
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.TERMINAL_CLEANUP


def test_sonarr_disposition_ambiguous_could_not_read_is_unknown() -> None:
    rec = {"eventType": "importFailed", "downloadId": "s", "reason": "Could not read file"}
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.UNKNOWN


def test_sonarr_disposition_read_failure_with_media_extension_is_terminal() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "v",
        "reason": "Could not read file /data/tv/Show.S01E01.mkv",
    }
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.TERMINAL_CLEANUP


def test_sonarr_disposition_read_failure_video_file_phrase_is_terminal() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "w",
        "reason": "Unable to read the video file",
    }
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.TERMINAL_CLEANUP


def test_sonarr_disposition_terminal_corrupt() -> None:
    rec = {"eventType": "importFailed", "downloadId": "c", "reason": "File is corrupt"}
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.TERMINAL_CLEANUP


def test_sonarr_disposition_episode_file_corrupted_phrase() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "e",
        "reason": "Episode file is corrupted",
    }
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.TERMINAL_CLEANUP


def test_radarr_disposition_movie_file_corrupt_phrase() -> None:
    rec = {"eventType": "importFailed", "downloadId": "m", "reason": "Movie file is corrupt"}
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.TERMINAL_CLEANUP


def test_sonarr_disposition_unknown() -> None:
    rec = {"eventType": "importFailed", "downloadId": "u", "reason": "Scheduled import retry pending"}
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.UNKNOWN


def test_terminal_cleanup_label_is_explicit_non_upgrade() -> None:
    assert terminal_cleanup_label_is_explicit_non_upgrade("not an upgrade vs existing file") is True
    assert terminal_cleanup_label_is_explicit_non_upgrade("not a preferred-word upgrade") is True
    assert terminal_cleanup_label_is_explicit_non_upgrade("not a custom-format upgrade") is True
    assert terminal_cleanup_label_is_explicit_non_upgrade("corrupt or unreadable file") is False
    assert terminal_cleanup_label_is_explicit_non_upgrade(None) is False
