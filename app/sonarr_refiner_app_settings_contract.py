"""Canonical SQLite repair DDL and expected default values for
``app_settings.sonarr_refiner_*``.

Aligned with :class:`app.models.AppSettings` (``mapped_column``
types and ``default=``).
Used by :func:`app.migrations.repair_sonarr_refiner_app_settings_columns`
and upgrade tests.
"""

from __future__ import annotations

SONARR_REFINER_APP_SETTINGS_SQLITE_SPECS: tuple[tuple[str, str], ...] = (
    ("sonarr_refiner_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("sonarr_refiner_dry_run", "INTEGER NOT NULL DEFAULT 1"),
    ("sonarr_refiner_primary_audio_lang", "TEXT NOT NULL DEFAULT ''"),
    ("sonarr_refiner_secondary_audio_lang", "TEXT NOT NULL DEFAULT ''"),
    ("sonarr_refiner_tertiary_audio_lang", "TEXT NOT NULL DEFAULT ''"),
    ("sonarr_refiner_default_audio_slot", "TEXT NOT NULL DEFAULT 'primary'"),
    ("sonarr_refiner_remove_commentary", "INTEGER NOT NULL DEFAULT 0"),
    ("sonarr_refiner_subtitle_mode", "TEXT NOT NULL DEFAULT 'remove_all'"),
    ("sonarr_refiner_subtitle_langs_csv", "TEXT NOT NULL DEFAULT ''"),
    ("sonarr_refiner_preserve_forced_subs", "INTEGER NOT NULL DEFAULT 1"),
    ("sonarr_refiner_preserve_default_subs", "INTEGER NOT NULL DEFAULT 1"),
    ("sonarr_refiner_watched_folder", "TEXT NOT NULL DEFAULT ''"),
    ("sonarr_refiner_output_folder", "TEXT NOT NULL DEFAULT ''"),
    ("sonarr_refiner_work_folder", "TEXT NOT NULL DEFAULT ''"),
    (
        "sonarr_refiner_audio_preference_mode",
        "TEXT NOT NULL DEFAULT 'preferred_langs_quality'",
    ),
    ("sonarr_refiner_minimum_age_seconds", "INTEGER NOT NULL DEFAULT 60"),
    ("sonarr_refiner_schedule_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("sonarr_refiner_schedule_days", "TEXT NOT NULL DEFAULT ''"),
    ("sonarr_refiner_schedule_start", "TEXT NOT NULL DEFAULT '00:00'"),
    ("sonarr_refiner_schedule_end", "TEXT NOT NULL DEFAULT '23:59'"),
    ("sonarr_refiner_last_run_at", "DATETIME"),
    ("sonarr_refiner_current_pass_total", "INTEGER NOT NULL DEFAULT 0"),
    ("sonarr_refiner_current_pass_done", "INTEGER NOT NULL DEFAULT 0"),
)

SONARR_REFINER_APP_SETTINGS_EXPECTED_SQLITE_VALUES: dict[str, int | str | None] = {
    "sonarr_refiner_enabled": 0,
    "sonarr_refiner_dry_run": 1,
    "sonarr_refiner_primary_audio_lang": "",
    "sonarr_refiner_secondary_audio_lang": "",
    "sonarr_refiner_tertiary_audio_lang": "",
    "sonarr_refiner_default_audio_slot": "primary",
    "sonarr_refiner_remove_commentary": 0,
    "sonarr_refiner_subtitle_mode": "remove_all",
    "sonarr_refiner_subtitle_langs_csv": "",
    "sonarr_refiner_preserve_forced_subs": 1,
    "sonarr_refiner_preserve_default_subs": 1,
    "sonarr_refiner_watched_folder": "",
    "sonarr_refiner_output_folder": "",
    "sonarr_refiner_work_folder": "",
    "sonarr_refiner_audio_preference_mode": "preferred_langs_quality",
    "sonarr_refiner_minimum_age_seconds": 60,
    "sonarr_refiner_schedule_enabled": 0,
    "sonarr_refiner_schedule_days": "",
    "sonarr_refiner_schedule_start": "00:00",
    "sonarr_refiner_schedule_end": "23:59",
    "sonarr_refiner_last_run_at": None,
    "sonarr_refiner_current_pass_total": 0,
    "sonarr_refiner_current_pass_done": 0,
}
