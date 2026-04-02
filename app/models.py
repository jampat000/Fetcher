from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text

from app.schema_version import CURRENT_SCHEMA_VERSION
from app.time_util import utc_now_naive
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class AppSettings(Base):
    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Sonarr
    sonarr_url: Mapped[str] = mapped_column(String(512), default="")
    sonarr_api_key: Mapped[str] = mapped_column(String(256), default="")
    sonarr_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_search_missing: Mapped[bool] = mapped_column(Boolean, default=True)
    sonarr_search_upgrades: Mapped[bool] = mapped_column(Boolean, default=True)
    sonarr_max_items_per_run: Mapped[int] = mapped_column(Integer, default=50)
    sonarr_schedule_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_schedule_days: Mapped[str] = mapped_column(Text, default="")
    sonarr_schedule_start: Mapped[str] = mapped_column(String(5), default="00:00")  # HH:MM
    sonarr_schedule_end: Mapped[str] = mapped_column(String(5), default="23:59")  # HH:MM
    # Minutes between Sonarr runs when schedule allows (minimum 1; invalid/low values coerced to 60 on startup/save).
    sonarr_interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    sonarr_last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Sonarr-only: when enabled, remove queue rows that match import-failed history by exact downloadId.
    sonarr_remove_failed_imports: Mapped[bool] = mapped_column(Boolean, default=False)

    # Radarr
    radarr_url: Mapped[str] = mapped_column(String(512), default="")
    radarr_api_key: Mapped[str] = mapped_column(String(256), default="")
    radarr_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_search_missing: Mapped[bool] = mapped_column(Boolean, default=True)
    radarr_search_upgrades: Mapped[bool] = mapped_column(Boolean, default=True)
    radarr_max_items_per_run: Mapped[int] = mapped_column(Integer, default=50)
    radarr_schedule_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_schedule_days: Mapped[str] = mapped_column(Text, default="")
    radarr_schedule_start: Mapped[str] = mapped_column(String(5), default="00:00")  # HH:MM
    radarr_schedule_end: Mapped[str] = mapped_column(String(5), default="23:59")  # HH:MM
    # Minutes between Radarr runs when schedule allows (minimum 1; invalid/low values coerced to 60 on startup/save).
    radarr_interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    radarr_last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Radarr-only: when enabled, remove queue rows that match import-failed history by exact downloadId.
    radarr_remove_failed_imports: Mapped[bool] = mapped_column(Boolean, default=False)

    # Sonarr granular cleanup — each scenario has a remove toggle
    # and an independent blocklist toggle (all default False).
    sonarr_cleanup_corrupt: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_blocklist_corrupt: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_cleanup_download_failed: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_blocklist_download_failed: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_cleanup_unmatched: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_blocklist_unmatched: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_cleanup_quality: Mapped[bool] = mapped_column(Boolean, default=False)
    sonarr_blocklist_quality: Mapped[bool] = mapped_column(Boolean, default=False)

    # Radarr granular cleanup
    radarr_cleanup_corrupt: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_blocklist_corrupt: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_cleanup_download_failed: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_blocklist_download_failed: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_cleanup_unmatched: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_blocklist_unmatched: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_cleanup_quality: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_blocklist_quality: Mapped[bool] = mapped_column(Boolean, default=False)

    # How often Emby Trimmer may run (Trimmer Settings only; independent of Sonarr/Radarr).
    emby_interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    emby_last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Min minutes before Fetcher retries searching the same Sonarr episode.
    sonarr_retry_delay_minutes: Mapped[int] = mapped_column(Integer, default=1440)
    # Min minutes before Fetcher retries searching the same Radarr movie.
    radarr_retry_delay_minutes: Mapped[int] = mapped_column(Integer, default=1440)
    # Shared cadence for Sonarr/Radarr failed-import cleanup checks.
    failed_import_cleanup_interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    sonarr_failed_import_cleanup_last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    radarr_failed_import_cleanup_last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Activity, job_run_log, app_snapshot pruning window (days); clamped 7–3650 when pruning.
    log_retention_days: Mapped[int] = mapped_column(Integer, default=90)
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")  # IANA e.g. America/New_York

    # Emby Trimmer
    emby_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    emby_url: Mapped[str] = mapped_column(String(512), default="")
    emby_api_key: Mapped[str] = mapped_column(String(256), default="")
    emby_user_id: Mapped[str] = mapped_column(String(128), default="")
    emby_dry_run: Mapped[bool] = mapped_column(Boolean, default=True)
    emby_schedule_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    emby_schedule_days: Mapped[str] = mapped_column(Text, default="")
    emby_schedule_start: Mapped[str] = mapped_column(String(5), default="00:00")  # HH:MM
    emby_schedule_end: Mapped[str] = mapped_column(String(5), default="23:59")  # HH:MM
    emby_max_items_scan: Mapped[int] = mapped_column(Integer, default=2000)
    emby_max_deletes_per_run: Mapped[int] = mapped_column(Integer, default=25)
    emby_rule_watched_rating_below: Mapped[int] = mapped_column(Integer, default=0)  # 0 disables
    emby_rule_unwatched_days: Mapped[int] = mapped_column(Integer, default=0)  # 0 disables
    emby_rule_movie_watched_rating_below: Mapped[int] = mapped_column(Integer, default=0)  # 0 -> fallback/global or disabled
    emby_rule_movie_unwatched_days: Mapped[int] = mapped_column(Integer, default=0)  # 0 -> fallback/global or disabled
    emby_rule_movie_genres_csv: Mapped[str] = mapped_column(Text, default="")
    emby_rule_movie_people_csv: Mapped[str] = mapped_column(Text, default="")
    emby_rule_movie_people_credit_types_csv: Mapped[str] = mapped_column(Text, default="Actor")
    emby_rule_tv_delete_watched: Mapped[bool] = mapped_column(Boolean, default=False)
    emby_rule_tv_genres_csv: Mapped[str] = mapped_column(Text, default="")
    emby_rule_tv_people_csv: Mapped[str] = mapped_column(Text, default="")
    emby_rule_tv_people_credit_types_csv: Mapped[str] = mapped_column(Text, default="Actor")
    emby_rule_tv_watched_rating_below: Mapped[int] = mapped_column(Integer, default=0)  # 0 -> fallback/global or disabled
    emby_rule_tv_unwatched_days: Mapped[int] = mapped_column(Integer, default=0)  # 0 -> fallback/global or disabled

    # Refiner (remux-only audio/subtitle cleanup; isolated from Trimmer orchestration)
    refiner_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    refiner_dry_run: Mapped[bool] = mapped_column(Boolean, default=True)
    refiner_primary_audio_lang: Mapped[str] = mapped_column(String(16), default="")
    refiner_secondary_audio_lang: Mapped[str] = mapped_column(String(16), default="")
    refiner_tertiary_audio_lang: Mapped[str] = mapped_column(String(16), default="")
    # Which language slot gets the default audio disposition: primary | secondary
    refiner_default_audio_slot: Mapped[str] = mapped_column(String(16), default="primary")
    refiner_remove_commentary: Mapped[bool] = mapped_column(Boolean, default=False)
    refiner_subtitle_mode: Mapped[str] = mapped_column(String(24), default="remove_all")
    refiner_subtitle_langs_csv: Mapped[str] = mapped_column(Text, default="")
    refiner_preserve_forced_subs: Mapped[bool] = mapped_column(Boolean, default=True)
    refiner_preserve_default_subs: Mapped[bool] = mapped_column(Boolean, default=True)
    refiner_watched_folder: Mapped[str] = mapped_column(Text, default="")
    refiner_output_folder: Mapped[str] = mapped_column(Text, default="")
    # Optional advanced override. Empty uses managed internal work folder.
    refiner_work_folder: Mapped[str] = mapped_column(Text, default="")
    # Unused by current Refiner UI; column retained for existing SQLite rows.
    refiner_paths: Mapped[str] = mapped_column(Text, default="")
    # Small preset for choosing the best kept audio stream within allowed languages.
    refiner_audio_preference_mode: Mapped[str] = mapped_column(String(24), default="preferred_langs_quality")
    # Seconds between watched-folder scans when Refiner is configured (min 5; cap 7 days).
    refiner_interval_seconds: Mapped[int] = mapped_column(Integer, default=60)
    refiner_schedule_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    refiner_schedule_days: Mapped[str] = mapped_column(Text, default="")
    refiner_schedule_start: Mapped[str] = mapped_column(String(5), default="00:00")
    refiner_schedule_end: Mapped[str] = mapped_column(String(5), default="23:59")
    refiner_last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    refiner_current_pass_total: Mapped[int] = mapped_column(Integer, default=0)
    refiner_current_pass_done: Mapped[int] = mapped_column(Integer, default=0)

    # Web UI authentication (bcrypt password hash; TimestampSigner session secret)
    auth_username: Mapped[str] = mapped_column(Text, default="admin")
    auth_password_hash: Mapped[str] = mapped_column(Text, default="")
    auth_bypass_lan: Mapped[bool] = mapped_column(Boolean, default=False)
    # Newline-separated IPs and CIDR ranges; empty = no IP-based bypass.
    auth_ip_allowlist: Mapped[str] = mapped_column(Text, default="")
    auth_session_secret: Mapped[str] = mapped_column(Text, default="")
    auth_refresh_token_hash: Mapped[str] = mapped_column(Text, default="")
    auth_refresh_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Must equal app.schema_version.CURRENT_SCHEMA_VERSION for this build (enforced at startup).
    schema_version: Mapped[int] = mapped_column(Integer, default=CURRENT_SCHEMA_VERSION)

    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive)


class JobRunLog(Base):
    __tablename__ = "job_run_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ok: Mapped[bool] = mapped_column(Boolean, default=False)
    message: Mapped[str] = mapped_column(Text, default="")
    app: Mapped[str] = mapped_column(String(16), default="")


class AppSnapshot(Base):
    __tablename__ = "app_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive)

    app: Mapped[str] = mapped_column(String(16))  # "sonarr" | "radarr"
    ok: Mapped[bool] = mapped_column(Boolean, default=False)
    status_message: Mapped[str] = mapped_column(Text, default="")

    missing_total: Mapped[int] = mapped_column(Integer, default=0)
    cutoff_unmet_total: Mapped[int] = mapped_column(Integer, default=0)


class ActivityLog(Base):
    """What was grabbed per run: app + kind (missing/upgrade) + count. Displayed with tags."""
    __tablename__ = "activity_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)  # FK to job_run_log.id
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive)

    app: Mapped[str] = mapped_column(String(16))   # "sonarr" | "radarr" | "trimmer" | "refiner" | "service"
    kind: Mapped[str] = mapped_column(String(16))  # "missing" | "upgrade" | "cleanup" | ...
    status: Mapped[str] = mapped_column(String(16), default="ok")  # "ok" | "failed"
    count: Mapped[int] = mapped_column(Integer, default=0)
    detail: Mapped[str] = mapped_column(Text, default="")


class RefinerActivity(Base):
    """Per-file Refiner remux outcome for Activity feed (no JSON blobs)."""

    __tablename__ = "refiner_activity"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive)
    file_name: Mapped[str] = mapped_column(String(512), default="")
    # Canonical display label from container tags when known (ffprobe); may be empty for legacy rows.
    media_title: Mapped[str] = mapped_column(String(512), default="")
    # "processing" | "success" | "skipped" | "failed"
    status: Mapped[str] = mapped_column(String(16), default="failed")
    size_before_bytes: Mapped[int] = mapped_column(Integer, default=0)
    size_after_bytes: Mapped[int] = mapped_column(Integer, default=0)
    audio_tracks_before: Mapped[int] = mapped_column(Integer, default=0)
    audio_tracks_after: Mapped[int] = mapped_column(Integer, default=0)
    subtitle_tracks_before: Mapped[int] = mapped_column(Integer, default=0)
    subtitle_tracks_after: Mapped[int] = mapped_column(Integer, default=0)
    processing_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # JSON v1 snapshot for Refiner activity UI (audio · subtitle display lines, failure reason).
    activity_context: Mapped[str] = mapped_column(Text, default="")


class ArrActionLog(Base):
    """Cooldown/history to prevent repeated Arr searches/upgrades for the same item."""

    __tablename__ = "arr_action_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive)

    app: Mapped[str] = mapped_column(String(16))  # "sonarr" | "radarr"
    # Audit only — suppression uses (app, item_type, item_id), not action.
    action: Mapped[str] = mapped_column(String(16))  # "missing" | "upgrade"
    item_type: Mapped[str] = mapped_column(String(16))  # "episode" | "movie"
    item_id: Mapped[int] = mapped_column(Integer)  # episodeId/movieId


