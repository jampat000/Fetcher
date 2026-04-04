"""Canonical SQLite repair DDL and expected default values for ``app_settings.refiner_*``.

Aligned with :class:`app.models.AppSettings` (``mapped_column`` types and ``default=``).
Used by :func:`app.migrations.repair_refiner_app_settings_columns` and upgrade tests.
"""

from __future__ import annotations

# ``ALTER TABLE app_settings ADD COLUMN <name> <fragment>`` (SQLite).
REFINER_APP_SETTINGS_SQLITE_SPECS: tuple[tuple[str, str], ...] = (
    ("refiner_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("refiner_dry_run", "INTEGER NOT NULL DEFAULT 1"),
    ("refiner_primary_audio_lang", "TEXT NOT NULL DEFAULT ''"),
    ("refiner_secondary_audio_lang", "TEXT NOT NULL DEFAULT ''"),
    ("refiner_tertiary_audio_lang", "TEXT NOT NULL DEFAULT ''"),
    ("refiner_default_audio_slot", "TEXT NOT NULL DEFAULT 'primary'"),
    ("refiner_remove_commentary", "INTEGER NOT NULL DEFAULT 0"),
    ("refiner_subtitle_mode", "TEXT NOT NULL DEFAULT 'remove_all'"),
    ("refiner_subtitle_langs_csv", "TEXT NOT NULL DEFAULT ''"),
    ("refiner_preserve_forced_subs", "INTEGER NOT NULL DEFAULT 1"),
    ("refiner_preserve_default_subs", "INTEGER NOT NULL DEFAULT 1"),
    ("refiner_watched_folder", "TEXT NOT NULL DEFAULT ''"),
    ("refiner_output_folder", "TEXT NOT NULL DEFAULT ''"),
    ("refiner_work_folder", "TEXT NOT NULL DEFAULT ''"),
    ("refiner_paths", "TEXT NOT NULL DEFAULT ''"),
    (
        "refiner_audio_preference_mode",
        "TEXT NOT NULL DEFAULT 'preferred_langs_quality'",
    ),
    ("refiner_interval_seconds", "INTEGER NOT NULL DEFAULT 60"),
    ("refiner_schedule_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("refiner_schedule_days", "TEXT NOT NULL DEFAULT ''"),
    ("refiner_schedule_start", "TEXT NOT NULL DEFAULT '00:00'"),
    ("refiner_schedule_end", "TEXT NOT NULL DEFAULT '23:59'"),
    ("refiner_last_run_at", "DATETIME"),
    ("refiner_current_pass_total", "INTEGER NOT NULL DEFAULT 0"),
    ("refiner_current_pass_done", "INTEGER NOT NULL DEFAULT 0"),
    ("refiner_minimum_age_seconds", "INTEGER NOT NULL DEFAULT 60"),
)

# Raw values SQLite stores after ``ADD COLUMN`` backfill (``sqlite3`` / aiosqlite row access).
REFINER_APP_SETTINGS_EXPECTED_SQLITE_VALUES: dict[str, int | str | None] = {
    "refiner_enabled": 0,
    "refiner_dry_run": 1,
    "refiner_primary_audio_lang": "",
    "refiner_secondary_audio_lang": "",
    "refiner_tertiary_audio_lang": "",
    "refiner_default_audio_slot": "primary",
    "refiner_remove_commentary": 0,
    "refiner_subtitle_mode": "remove_all",
    "refiner_subtitle_langs_csv": "",
    "refiner_preserve_forced_subs": 1,
    "refiner_preserve_default_subs": 1,
    "refiner_watched_folder": "",
    "refiner_output_folder": "",
    "refiner_work_folder": "",
    "refiner_paths": "",
    "refiner_audio_preference_mode": "preferred_langs_quality",
    "refiner_interval_seconds": 60,
    "refiner_schedule_enabled": 0,
    "refiner_schedule_days": "",
    "refiner_schedule_start": "00:00",
    "refiner_schedule_end": "23:59",
    "refiner_last_run_at": None,
    "refiner_current_pass_total": 0,
    "refiner_current_pass_done": 0,
    "refiner_minimum_age_seconds": 60,
}
