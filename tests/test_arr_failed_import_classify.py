from __future__ import annotations

from app.arr_failed_import_classify import (
    FailedImportDisposition,
    radarr_import_failed_history_disposition,
    radarr_queue_scenario_label,
    sonarr_import_failed_history_disposition,
    sonarr_queue_scenario_label,
    tracked_queue_download_state_is_failed,
)


def test_radarr_disposition_terminal_not_upgrade() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "x",
        "reason": "Not an upgrade for existing movie file. Existing quality: WEBDL-1080p",
    }
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.QUALITY


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
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.CORRUPT


def test_sonarr_disposition_ambiguous_could_not_read_is_unknown() -> None:
    rec = {"eventType": "importFailed", "downloadId": "s", "reason": "Could not read file"}
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.UNKNOWN


def test_sonarr_disposition_read_failure_with_media_extension_is_terminal() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "v",
        "reason": "Could not read file /data/tv/Show.S01E01.mkv",
    }
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.CORRUPT


def test_sonarr_disposition_read_failure_video_file_phrase_is_terminal() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "w",
        "reason": "Unable to read the video file",
    }
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.CORRUPT


def test_sonarr_disposition_terminal_corrupt() -> None:
    rec = {"eventType": "importFailed", "downloadId": "c", "reason": "File is corrupt"}
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.CORRUPT


def test_sonarr_disposition_episode_file_corrupted_phrase() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "e",
        "reason": "Episode file is corrupted",
    }
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.CORRUPT


def test_radarr_disposition_movie_file_corrupt_phrase() -> None:
    rec = {"eventType": "importFailed", "downloadId": "m", "reason": "Movie file is corrupt"}
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.CORRUPT


def test_radarr_disposition_manual_import_required_history_is_unmatched() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "u2",
        "reason": "Manual import required for Movie.Title.2024",
    }
    assert radarr_import_failed_history_disposition(rec) == FailedImportDisposition.UNMATCHED


def test_sonarr_disposition_unable_to_import_automatically_history_is_unmatched() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "u3",
        "reason": "Unable to import automatically",
    }
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.UNMATCHED


def test_radarr_queue_scenario_download_failed_phrase() -> None:
    d, lab = radarr_queue_scenario_label("Download failed")
    assert d is FailedImportDisposition.DOWNLOAD_FAILED
    assert lab == "download failed"


def test_radarr_queue_scenario_corrupt_before_download_failed_text() -> None:
    """File/import corruption is more specific than a generic download-failed line."""
    d, _ = radarr_queue_scenario_label("Download failed: movie file is corrupt")
    assert d is FailedImportDisposition.CORRUPT


def test_radarr_queue_scenario_wasnt_grabbed_by_radarr() -> None:
    d, lab = radarr_queue_scenario_label("Release wasn't grabbed by Radarr.")
    assert d is FailedImportDisposition.DOWNLOAD_FAILED
    assert "radarr" in lab.casefold()


def test_sonarr_queue_scenario_download_failed_phrase() -> None:
    d, _ = sonarr_queue_scenario_label("The download failed.")
    assert d is FailedImportDisposition.DOWNLOAD_FAILED


def test_sonarr_queue_scenario_episode_not_grabbed() -> None:
    d, _ = sonarr_queue_scenario_label("Episode wasn't grabbed by Sonarr")
    assert d is FailedImportDisposition.DOWNLOAD_FAILED


def test_tracked_queue_download_state_is_failed() -> None:
    assert tracked_queue_download_state_is_failed({"trackedDownloadState": "failed"}) is True
    assert tracked_queue_download_state_is_failed({"trackedDownloadState": "Failed"}) is True
    assert tracked_queue_download_state_is_failed({"trackedDownloadState": "importPending"}) is False


def test_radarr_queue_scenario_generic_import_failed_phrase() -> None:
    d, lab = radarr_queue_scenario_label("Import failed")
    assert d is FailedImportDisposition.IMPORT_FAILED
    assert lab == "import failed"


def test_sonarr_queue_scenario_failed_to_import_phrase() -> None:
    d, _ = sonarr_queue_scenario_label("Failed to import episode")
    assert d is FailedImportDisposition.IMPORT_FAILED


def test_sonarr_disposition_unknown() -> None:
    rec = {"eventType": "importFailed", "downloadId": "u", "reason": "Scheduled import retry pending"}
    assert sonarr_import_failed_history_disposition(rec) == FailedImportDisposition.UNKNOWN
