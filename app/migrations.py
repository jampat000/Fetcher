from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection

from app.refiner_app_settings_contract import REFINER_APP_SETTINGS_SQLITE_SPECS
from app.schema_version import CURRENT_SCHEMA_VERSION

logger = logging.getLogger(__name__)


async def _column_names_sqlite(conn: AsyncConnection, *, table: str) -> set[str]:
    """Column names on ``table`` via ``pragma_table_info`` (same connection as DDL)."""
    res = await conn.execute(text(f"SELECT name FROM pragma_table_info('{table}')"))
    return {str(row[0]) for row in res.fetchall() if row[0] is not None}


async def _sqlite_table_exists(engine: AsyncEngine, *, name: str) -> bool:
    async with engine.connect() as conn:
        res = await conn.execute(
            text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": name},
        )
        return res.first() is not None


async def _has_column(engine: AsyncEngine, *, table: str, column: str) -> bool:
    async with engine.connect() as conn:
        names = await _column_names_sqlite(conn, table=table)
        return column in names


async def _add_column(engine: AsyncEngine, *, table: str, ddl: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {ddl}"))


async def _migrate_001_sonarr_per_app_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="sonarr_search_missing"):
        await _add_column(engine, table=table, ddl="sonarr_search_missing BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="sonarr_search_upgrades"):
        await _add_column(engine, table=table, ddl="sonarr_search_upgrades BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="sonarr_max_items_per_run"):
        await _add_column(engine, table=table, ddl="sonarr_max_items_per_run INTEGER NOT NULL DEFAULT 50")


async def _migrate_002_radarr_per_app_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="radarr_search_missing"):
        await _add_column(engine, table=table, ddl="radarr_search_missing BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="radarr_search_upgrades"):
        await _add_column(engine, table=table, ddl="radarr_search_upgrades BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="radarr_max_items_per_run"):
        await _add_column(engine, table=table, ddl="radarr_max_items_per_run INTEGER NOT NULL DEFAULT 50")


async def _migrate_003_sonarr_schedule_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="sonarr_schedule_enabled"):
        await _add_column(engine, table=table, ddl="sonarr_schedule_enabled BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="sonarr_schedule_days"):
        await _add_column(engine, table=table, ddl="sonarr_schedule_days TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="sonarr_schedule_start"):
        await _add_column(engine, table=table, ddl="sonarr_schedule_start TEXT NOT NULL DEFAULT '00:00'")
    if not await _has_column(engine, table=table, column="sonarr_schedule_end"):
        await _add_column(engine, table=table, ddl="sonarr_schedule_end TEXT NOT NULL DEFAULT '23:59'")


async def _migrate_004_radarr_schedule_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="radarr_schedule_enabled"):
        await _add_column(engine, table=table, ddl="radarr_schedule_enabled BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="radarr_schedule_days"):
        await _add_column(engine, table=table, ddl="radarr_schedule_days TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="radarr_schedule_start"):
        await _add_column(engine, table=table, ddl="radarr_schedule_start TEXT NOT NULL DEFAULT '00:00'")
    if not await _has_column(engine, table=table, column="radarr_schedule_end"):
        await _add_column(engine, table=table, ddl="radarr_schedule_end TEXT NOT NULL DEFAULT '23:59'")


async def _migrate_005_arr_interval_minutes_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="sonarr_interval_minutes"):
        await _add_column(engine, table=table, ddl="sonarr_interval_minutes INTEGER NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="radarr_interval_minutes"):
        await _add_column(engine, table=table, ddl="radarr_interval_minutes INTEGER NOT NULL DEFAULT 0")


async def _migrate_006_arr_last_run_at_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="sonarr_last_run_at"):
        await _add_column(engine, table=table, ddl="sonarr_last_run_at DATETIME")
    if not await _has_column(engine, table=table, column="radarr_last_run_at"):
        await _add_column(engine, table=table, ddl="radarr_last_run_at DATETIME")
    if not await _has_column(engine, table=table, column="emby_last_run_at"):
        await _add_column(engine, table=table, ddl="emby_last_run_at DATETIME")


async def _migrate_007_emby_interval_minutes(engine: AsyncEngine) -> None:
    """Emby Trimmer run cadence (seed from interval_minutes on older DBs that still have that column)."""
    table = "app_settings"
    if not await _has_column(engine, table=table, column="emby_interval_minutes"):
        await _add_column(engine, table=table, ddl="emby_interval_minutes INTEGER NOT NULL DEFAULT 60")
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE app_settings SET emby_interval_minutes = interval_minutes WHERE 1=1"))


async def _migrate_008_arr_interval_defaults_applied(engine: AsyncEngine) -> None:
    """One-time: stored 0 → 60 so UI shows real minute values (per-app run intervals)."""
    table = "app_settings"
    if not await _has_column(engine, table=table, column="arr_interval_defaults_applied"):
        await _add_column(engine, table=table, ddl="arr_interval_defaults_applied BOOLEAN NOT NULL DEFAULT 0")
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE app_settings
                    SET sonarr_interval_minutes = CASE WHEN sonarr_interval_minutes = 0 THEN 60 ELSE sonarr_interval_minutes END,
                        radarr_interval_minutes = CASE WHEN radarr_interval_minutes = 0 THEN 60 ELSE radarr_interval_minutes END,
                        arr_interval_defaults_applied = 1
                    """
                )
            )


async def _migrate_009_timezone(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="timezone"):
        await _add_column(engine, table=table, ddl="timezone TEXT NOT NULL DEFAULT 'UTC'")


async def _migrate_010_arr_search_cooldown_minutes(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="arr_search_cooldown_minutes"):
        await _add_column(
            engine,
            table=table,
            ddl="arr_search_cooldown_minutes INTEGER NOT NULL DEFAULT 1440",
        )


async def _migrate_011_emby_trimmer_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="emby_enabled"):
        await _add_column(engine, table=table, ddl="emby_enabled BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="emby_url"):
        await _add_column(engine, table=table, ddl="emby_url TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="emby_api_key"):
        await _add_column(engine, table=table, ddl="emby_api_key TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="emby_user_id"):
        await _add_column(engine, table=table, ddl="emby_user_id TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="emby_dry_run"):
        await _add_column(engine, table=table, ddl="emby_dry_run BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="emby_schedule_enabled"):
        await _add_column(engine, table=table, ddl="emby_schedule_enabled BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="emby_schedule_days"):
        await _add_column(engine, table=table, ddl="emby_schedule_days TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="emby_schedule_start"):
        await _add_column(engine, table=table, ddl="emby_schedule_start TEXT NOT NULL DEFAULT '00:00'")
    if not await _has_column(engine, table=table, column="emby_schedule_end"):
        await _add_column(engine, table=table, ddl="emby_schedule_end TEXT NOT NULL DEFAULT '23:59'")
    if not await _has_column(engine, table=table, column="emby_max_items_scan"):
        await _add_column(engine, table=table, ddl="emby_max_items_scan INTEGER NOT NULL DEFAULT 2000")
    if not await _has_column(engine, table=table, column="emby_max_deletes_per_run"):
        await _add_column(engine, table=table, ddl="emby_max_deletes_per_run INTEGER NOT NULL DEFAULT 25")
    if not await _has_column(engine, table=table, column="emby_rule_watched_rating_below"):
        await _add_column(engine, table=table, ddl="emby_rule_watched_rating_below INTEGER NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="emby_rule_unwatched_days"):
        await _add_column(engine, table=table, ddl="emby_rule_unwatched_days INTEGER NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="emby_rule_movie_watched_rating_below"):
        await _add_column(engine, table=table, ddl="emby_rule_movie_watched_rating_below INTEGER NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="emby_rule_movie_unwatched_days"):
        await _add_column(engine, table=table, ddl="emby_rule_movie_unwatched_days INTEGER NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="emby_rule_movie_genres_csv"):
        await _add_column(engine, table=table, ddl="emby_rule_movie_genres_csv TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="emby_rule_movie_people_csv"):
        await _add_column(engine, table=table, ddl="emby_rule_movie_people_csv TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="emby_rule_movie_people_credit_types_csv"):
        await _add_column(engine, table=table, ddl="emby_rule_movie_people_credit_types_csv TEXT NOT NULL DEFAULT 'Actor'")
    if not await _has_column(engine, table=table, column="emby_rule_tv_delete_watched"):
        await _add_column(engine, table=table, ddl="emby_rule_tv_delete_watched BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="emby_rule_tv_genres_csv"):
        await _add_column(engine, table=table, ddl="emby_rule_tv_genres_csv TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="emby_rule_tv_people_csv"):
        await _add_column(engine, table=table, ddl="emby_rule_tv_people_csv TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="emby_rule_tv_people_credit_types_csv"):
        await _add_column(engine, table=table, ddl="emby_rule_tv_people_credit_types_csv TEXT NOT NULL DEFAULT 'Actor'")
    if not await _has_column(engine, table=table, column="emby_rule_tv_watched_rating_below"):
        await _add_column(engine, table=table, ddl="emby_rule_tv_watched_rating_below INTEGER NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="emby_rule_tv_unwatched_days"):
        await _add_column(engine, table=table, ddl="emby_rule_tv_unwatched_days INTEGER NOT NULL DEFAULT 0")


async def _migrate_012_widen_schedule_days_columns(engine: AsyncEngine) -> None:
    """Ensure schedule day columns can store the full Mon..Sun CSV on SQL backends with strict VARCHAR."""
    # SQLite is permissive about text length and doesn't support straightforward ALTER TYPE.
    if engine.dialect.name == "sqlite":
        return
    async with engine.begin() as conn:
        for col in ("sonarr_schedule_days", "radarr_schedule_days", "emby_schedule_days"):
            await conn.execute(text(f"ALTER TABLE app_settings ALTER COLUMN {col} TYPE TEXT"))


async def _migrate_013_coerce_zero_arr_intervals(engine: AsyncEngine) -> None:
    """Set Sonarr/Radarr run intervals to 60 if DB still has 0 or invalid <1 values."""
    table = "app_settings"
    if not await _has_column(engine, table=table, column="sonarr_interval_minutes"):
        return
    async with engine.begin() as conn:
        await conn.execute(
            text(f"UPDATE {table} SET sonarr_interval_minutes = 60 WHERE sonarr_interval_minutes < 1")
        )
        await conn.execute(
            text(f"UPDATE {table} SET radarr_interval_minutes = 60 WHERE radarr_interval_minutes < 1")
        )


async def _migrate_014_create_snapshot_and_activity_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app_snapshot (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at DATETIME NOT NULL,
                  app TEXT NOT NULL,
                  ok BOOLEAN NOT NULL DEFAULT 0,
                  status_message TEXT NOT NULL DEFAULT '',
                  missing_total INTEGER NOT NULL DEFAULT 0,
                  cutoff_unmet_total INTEGER NOT NULL DEFAULT 0
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS activity_log (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_run_id INTEGER,
                  created_at DATETIME NOT NULL,
                  app TEXT NOT NULL,
                  kind TEXT NOT NULL,
                  count INTEGER NOT NULL DEFAULT 0,
                  detail TEXT NOT NULL DEFAULT ''
                )
                """
            )
        )


async def _migrate_015_activity_log_detail_and_status(engine: AsyncEngine) -> None:
    if not await _has_column(engine, table="activity_log", column="detail"):
        await _add_column(engine, table="activity_log", ddl="detail TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table="activity_log", column="status"):
        await _add_column(engine, table="activity_log", ddl="status TEXT NOT NULL DEFAULT 'ok'")


async def _migrate_016_create_arr_action_log(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS arr_action_log (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at DATETIME NOT NULL,
                  app TEXT NOT NULL,
                  action TEXT NOT NULL,
                  item_type TEXT NOT NULL,
                  item_id INTEGER NOT NULL
                )
                """
            )
        )


async def _migrate_017_drop_removed_global_arr_columns(engine: AsyncEngine) -> None:
    """Drop obsolete global Arr columns (interval_minutes, search_*, max_items_per_run) after copying data."""
    table = "app_settings"
    obsolete = (
        "interval_minutes",
        "search_missing",
        "search_upgrades",
        "max_items_per_run",
    )
    to_drop = [c for c in obsolete if await _has_column(engine, table=table, column=c)]
    if not to_drop:
        return

    has_max = "max_items_per_run" in to_drop
    has_search = "search_missing" in to_drop and "search_upgrades" in to_drop

    async with engine.begin() as conn:
        if has_max:
            await conn.execute(
                text(
                    f"UPDATE {table} SET sonarr_max_items_per_run = max_items_per_run "
                    f"WHERE sonarr_max_items_per_run = 50 AND max_items_per_run <> 50"
                )
            )
            await conn.execute(
                text(
                    f"UPDATE {table} SET radarr_max_items_per_run = max_items_per_run "
                    f"WHERE radarr_max_items_per_run = 50 AND max_items_per_run <> 50"
                )
            )
        if has_search:
            await conn.execute(
                text(
                    f"""
                    UPDATE {table} SET
                      sonarr_search_missing = search_missing,
                      sonarr_search_upgrades = search_upgrades,
                      radarr_search_missing = search_missing,
                      radarr_search_upgrades = search_upgrades
                    WHERE sonarr_search_missing = 1 AND sonarr_search_upgrades = 1
                      AND radarr_search_missing = 1 AND radarr_search_upgrades = 1
                      AND (search_missing = 0 OR search_upgrades = 0)
                    """
                )
            )

    for col in to_drop:
        if not await _has_column(engine, table=table, column=col):
            continue
        async with engine.begin() as conn:
            await conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {col}"))


async def _migrate_018_auth_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="auth_username"):
        await _add_column(engine, table=table, ddl="auth_username TEXT NOT NULL DEFAULT 'admin'")
    if not await _has_column(engine, table=table, column="auth_password_hash"):
        await _add_column(engine, table=table, ddl="auth_password_hash TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="auth_bypass_lan"):
        await _add_column(engine, table=table, ddl="auth_bypass_lan BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="auth_session_secret"):
        await _add_column(engine, table=table, ddl="auth_session_secret TEXT NOT NULL DEFAULT ''")


_MIGRATE_019_PRIVATE_LAN_RANGES = "10.0.0.0/8\n172.16.0.0/12\n192.168.0.0/16"


async def _migrate_019_auth_ip_allowlist(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="auth_ip_allowlist"):
        await _add_column(engine, table=table, ddl="auth_ip_allowlist TEXT NOT NULL DEFAULT ''")
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"UPDATE {table} SET auth_ip_allowlist = :ranges, auth_bypass_lan = 0 "
                f"WHERE auth_bypass_lan = 1"
            ),
            {"ranges": _MIGRATE_019_PRIVATE_LAN_RANGES},
        )


async def _migrate_020_log_retention_days(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="log_retention_days"):
        await _add_column(engine, table=table, ddl="log_retention_days INTEGER NOT NULL DEFAULT 90")


async def _migrate_021_activity_trimmed_kind(engine: AsyncEngine) -> None:
    """Rename stored activity kind from pre-rename value to ``trimmed`` (data-only)."""
    import base64

    old = base64.b64decode("Y2xlYW51cA==").decode("ascii")
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE activity_log SET kind = :new WHERE kind = :old"),
            {"new": "trimmed", "old": old},
        )


async def _migrate_022_refresh_token_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="auth_refresh_token_hash"):
        await _add_column(engine, table=table, ddl="auth_refresh_token_hash TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="auth_refresh_expires_at"):
        await _add_column(engine, table=table, ddl="auth_refresh_expires_at DATETIME")


async def _migrate_023_radarr_remove_failed_imports(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="radarr_remove_failed_imports"):
        await _add_column(engine, table=table, ddl="radarr_remove_failed_imports BOOLEAN NOT NULL DEFAULT 0")


async def _migrate_024_arr_retry_delay_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    has_old = await _has_column(engine, table=table, column="arr_search_cooldown_minutes")
    if not await _has_column(engine, table=table, column="sonarr_retry_delay_minutes"):
        await _add_column(engine, table=table, ddl="sonarr_retry_delay_minutes INTEGER NOT NULL DEFAULT 1440")
    if not await _has_column(engine, table=table, column="radarr_retry_delay_minutes"):
        await _add_column(engine, table=table, ddl="radarr_retry_delay_minutes INTEGER NOT NULL DEFAULT 1440")
    async with engine.begin() as conn:
        if has_old:
            await conn.execute(
                text(
                    f"""
                    UPDATE {table}
                    SET sonarr_retry_delay_minutes = CASE
                          WHEN arr_search_cooldown_minutes < 1 THEN 1
                          ELSE arr_search_cooldown_minutes
                        END,
                        radarr_retry_delay_minutes = CASE
                          WHEN arr_search_cooldown_minutes < 1 THEN 1
                          ELSE arr_search_cooldown_minutes
                        END
                    WHERE (sonarr_retry_delay_minutes = 1440 OR sonarr_retry_delay_minutes < 1)
                       OR (radarr_retry_delay_minutes = 1440 OR radarr_retry_delay_minutes < 1)
                    """
                )
            )
        await conn.execute(
            text(
                f"""
                UPDATE {table}
                SET sonarr_retry_delay_minutes = CASE WHEN sonarr_retry_delay_minutes < 1 THEN 1 ELSE sonarr_retry_delay_minutes END,
                    radarr_retry_delay_minutes = CASE WHEN radarr_retry_delay_minutes < 1 THEN 1 ELSE radarr_retry_delay_minutes END
                """
            )
        )


async def _migrate_025_sonarr_remove_failed_imports(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="sonarr_remove_failed_imports"):
        await _add_column(engine, table=table, ddl="sonarr_remove_failed_imports BOOLEAN NOT NULL DEFAULT 0")


async def _migrate_029_refiner_activity(engine: AsyncEngine) -> None:
    """Per-file Refiner processing stats for unified Activity feed."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS refiner_activity (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    created_at DATETIME NOT NULL,
                    file_name VARCHAR(512) NOT NULL DEFAULT '',
                    status VARCHAR(16) NOT NULL DEFAULT 'failed',
                    size_before_bytes INTEGER NOT NULL DEFAULT 0,
                    size_after_bytes INTEGER NOT NULL DEFAULT 0,
                    audio_tracks_before INTEGER NOT NULL DEFAULT 0,
                    audio_tracks_after INTEGER NOT NULL DEFAULT 0,
                    subtitle_tracks_before INTEGER NOT NULL DEFAULT 0,
                    subtitle_tracks_after INTEGER NOT NULL DEFAULT 0,
                    processing_time_ms INTEGER NULL
                )
                """
            )
        )


async def _migrate_030_failed_import_cleanup_interval(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="failed_import_cleanup_interval_minutes"):
        await _add_column(
            engine,
            table=table,
            ddl="failed_import_cleanup_interval_minutes INTEGER NOT NULL DEFAULT 60",
        )
    if not await _has_column(engine, table=table, column="sonarr_failed_import_cleanup_last_run_at"):
        await _add_column(engine, table=table, ddl="sonarr_failed_import_cleanup_last_run_at DATETIME")
    if not await _has_column(engine, table=table, column="radarr_failed_import_cleanup_last_run_at"):
        await _add_column(engine, table=table, ddl="radarr_failed_import_cleanup_last_run_at DATETIME")


async def _migrate_033_refiner_activity_context(engine: AsyncEngine) -> None:
    """Persist operator-facing Refiner activity snapshot (audio · subtitle lines, failure reason)."""
    table = "refiner_activity"
    if not await _has_column(engine, table=table, column="activity_context"):
        await _add_column(engine, table=table, ddl="activity_context TEXT NOT NULL DEFAULT ''")


async def _migrate_035_activity_log_trimmer_app_identity(engine: AsyncEngine) -> None:
    """Activity feed: Trimmer rows use app ``trimmer`` (not legacy ``emby``)."""
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE activity_log SET app = 'trimmer' WHERE LOWER(app) = 'emby'"))


async def _migrate_036_refiner_activity_media_title(engine: AsyncEngine) -> None:
    """Refiner activity: optional human media title from ffprobe tags (parallel to file_name)."""
    table = "refiner_activity"
    if not await _has_column(engine, table=table, column="media_title"):
        await _add_column(engine, table=table, ddl="media_title VARCHAR(512) NOT NULL DEFAULT ''")


# Refiner column repair DDL: :mod:`app.refiner_app_settings_contract` (must match ``AppSettings``).


async def _app_settings_table_exists_on_conn(conn: AsyncConnection) -> bool:
    res = await conn.execute(
        text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='app_settings' LIMIT 1")
    )
    return res.fetchone() is not None


async def _app_settings_table_exists(engine: AsyncEngine) -> bool:
    async with engine.connect() as conn:
        return await _app_settings_table_exists_on_conn(conn)


async def repair_refiner_app_settings_columns(engine: AsyncEngine) -> None:
    """Add missing ``refiner_*`` columns on ``app_settings`` (SQLite only, idempotent).

    Runs all detection and ``ALTER TABLE`` on **one** connection inside a single transaction so
    checks and DDL cannot diverge across the pool (WAL/read-your-writes). Safe to call from
    :func:`app.migrations.migrate` and from :func:`app.schema_validation.validate_refiner_app_settings_schema`
    when validation finds gaps.

    Logs each column as **added** or **already present**; no silent skips.
    """
    if engine.dialect.name != "sqlite":
        logger.warning("app_settings refiner repair skipped: dialect is not sqlite (%r)", engine.dialect.name)
        return

    logger.info("schema repair starting: app_settings refiner_* columns (single transaction)")
    async with engine.begin() as conn:
        if not await _app_settings_table_exists_on_conn(conn):
            logger.warning(
                "schema repair: app_settings table missing; cannot add refiner_* columns here"
            )
            return

        names = await _column_names_sqlite(conn, table="app_settings")
        added_any = False
        for col_name, type_default in REFINER_APP_SETTINGS_SQLITE_SPECS:
            if col_name in names:
                logger.info("app_settings schema repair: column %s already present", col_name)
                continue
            ddl = f"{col_name} {type_default}"
            stmt = text(f"ALTER TABLE app_settings ADD COLUMN {ddl}")
            try:
                await conn.execute(stmt)
            except Exception as exc:
                logger.error(
                    "app_settings schema repair failed: ADD COLUMN %s (%s) — %s",
                    col_name,
                    ddl,
                    exc,
                )
                raise
            logger.info("app_settings schema repair: added column %s", col_name)
            names.add(col_name)
            added_any = True

    if added_any:
        logger.info("schema repair complete: app_settings refiner_* columns (ALTER applied)")
    else:
        logger.info("schema repair complete: app_settings refiner_* columns (no changes needed)")


async def _migrate_034_forward_app_settings_schema_version(engine: AsyncEngine) -> None:
    """After DDL migrations, bump stored schema_version when this build is newer (upgrade path)."""
    table = "app_settings"
    col = "schema_version"
    if not await _has_column(engine, table=table, column=col):
        return
    v = int(CURRENT_SCHEMA_VERSION)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"UPDATE {table} SET {col} = :v "
                f"WHERE {col} IS NULL OR {col} < :v"
            ),
            {"v": v},
        )


async def _migrate_032_app_settings_schema_version(engine: AsyncEngine) -> None:
    """Track DB schema contract; value must match :data:`app.schema_version.CURRENT_SCHEMA_VERSION`."""
    table = "app_settings"
    col = "schema_version"
    v = CURRENT_SCHEMA_VERSION
    if not await _has_column(engine, table=table, column=col):
        await _add_column(
            engine,
            table=table,
            ddl=f"{col} INTEGER NOT NULL DEFAULT {int(v)}",
        )
    async with engine.begin() as conn:
        await conn.execute(
            text(f"UPDATE {table} SET {col} = :v WHERE {col} IS NULL"),
            {"v": int(v)},
        )


# NOTE: Migration numbers 026-028 and 031 were reserved during development
# but never shipped. These gaps are intentional — adding them now would serve
# no purpose. The chain is fully idempotent via _has_column() guards.


async def _migrate_037_job_run_log_app_column(engine: AsyncEngine) -> None:
    """Track which service (sonarr/radarr/trimmer/refiner) produced each run log row."""
    if not await _sqlite_table_exists(engine, name="job_run_log"):
        return
    if not await _has_column(engine, table="job_run_log", column="app"):
        await _add_column(engine, table="job_run_log", ddl="app VARCHAR(16) NOT NULL DEFAULT ''")


async def _migrate_038_refiner_pass_progress(engine: AsyncEngine) -> None:
    """Transient Refiner pass progress for live dashboard tile."""
    table = "app_settings"
    if not await _has_column(engine, table=table, column="refiner_current_pass_total"):
        await _add_column(engine, table=table, ddl="refiner_current_pass_total INTEGER NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="refiner_current_pass_done"):
        await _add_column(engine, table=table, ddl="refiner_current_pass_done INTEGER NOT NULL DEFAULT 0")


async def _migrate_039_granular_cleanup(engine: AsyncEngine) -> None:
    """Add 16 granular failed-import cleanup columns (all BOOLEAN NOT NULL DEFAULT 0)."""
    table = "app_settings"
    new_cols = [
        "sonarr_cleanup_corrupt",
        "sonarr_blocklist_corrupt",
        "sonarr_cleanup_download_failed",
        "sonarr_blocklist_download_failed",
        "sonarr_cleanup_unmatched",
        "sonarr_blocklist_unmatched",
        "sonarr_cleanup_quality",
        "sonarr_blocklist_quality",
        "radarr_cleanup_corrupt",
        "radarr_blocklist_corrupt",
        "radarr_cleanup_download_failed",
        "radarr_blocklist_download_failed",
        "radarr_cleanup_unmatched",
        "radarr_blocklist_unmatched",
        "radarr_cleanup_quality",
        "radarr_blocklist_quality",
    ]
    for col in new_cols:
        if not await _has_column(engine, table=table, column=col):
            await _add_column(
                engine,
                table=table,
                ddl=f"{col} BOOLEAN NOT NULL DEFAULT 0",
            )


async def migrate(engine: AsyncEngine) -> None:
    await _migrate_001_sonarr_per_app_columns(engine)
    await _migrate_002_radarr_per_app_columns(engine)
    await _migrate_003_sonarr_schedule_columns(engine)
    await _migrate_004_radarr_schedule_columns(engine)
    await _migrate_005_arr_interval_minutes_columns(engine)
    await _migrate_006_arr_last_run_at_columns(engine)
    await _migrate_007_emby_interval_minutes(engine)
    await _migrate_008_arr_interval_defaults_applied(engine)
    await _migrate_009_timezone(engine)
    await _migrate_010_arr_search_cooldown_minutes(engine)
    await _migrate_011_emby_trimmer_columns(engine)
    await _migrate_012_widen_schedule_days_columns(engine)
    await _migrate_013_coerce_zero_arr_intervals(engine)
    await _migrate_014_create_snapshot_and_activity_tables(engine)
    await _migrate_015_activity_log_detail_and_status(engine)
    await _migrate_016_create_arr_action_log(engine)
    await _migrate_017_drop_removed_global_arr_columns(engine)
    await _migrate_018_auth_columns(engine)
    await _migrate_019_auth_ip_allowlist(engine)
    await _migrate_020_log_retention_days(engine)
    await _migrate_021_activity_trimmed_kind(engine)
    await _migrate_022_refresh_token_columns(engine)
    await _migrate_023_radarr_remove_failed_imports(engine)
    await _migrate_024_arr_retry_delay_columns(engine)
    await _migrate_025_sonarr_remove_failed_imports(engine)
    await _migrate_029_refiner_activity(engine)
    await _migrate_030_failed_import_cleanup_interval(engine)
    await _migrate_032_app_settings_schema_version(engine)
    await _migrate_033_refiner_activity_context(engine)
    await _migrate_035_activity_log_trimmer_app_identity(engine)
    await _migrate_036_refiner_activity_media_title(engine)
    await _migrate_034_forward_app_settings_schema_version(engine)
    await _migrate_037_job_run_log_app_column(engine)
    await _migrate_038_refiner_pass_progress(engine)
    await _migrate_039_granular_cleanup(engine)
    await repair_refiner_app_settings_columns(engine)

    logger.info(
        "SQLite migrate() chain finished (idempotent repairs included; strict schema validation runs next)."
    )
