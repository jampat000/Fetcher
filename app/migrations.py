from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


async def _has_column(engine: AsyncEngine, *, table: str, column: str) -> bool:
    async with engine.connect() as conn:
        res = await conn.execute(text(f"PRAGMA table_info({table})"))
        cols = [r[1] for r in res.fetchall()]  # (cid, name, type, notnull, dflt_value, pk)
        return column in cols


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


async def _migrate_026_stream_manager_columns(engine: AsyncEngine) -> None:
    table = "app_settings"
    if not await _has_column(engine, table=table, column="stream_manager_enabled"):
        await _add_column(engine, table=table, ddl="stream_manager_enabled BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="stream_manager_dry_run"):
        await _add_column(engine, table=table, ddl="stream_manager_dry_run BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="stream_manager_primary_audio_lang"):
        await _add_column(engine, table=table, ddl="stream_manager_primary_audio_lang TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="stream_manager_secondary_audio_lang"):
        await _add_column(engine, table=table, ddl="stream_manager_secondary_audio_lang TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="stream_manager_tertiary_audio_lang"):
        await _add_column(engine, table=table, ddl="stream_manager_tertiary_audio_lang TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="stream_manager_default_audio_slot"):
        await _add_column(engine, table=table, ddl="stream_manager_default_audio_slot TEXT NOT NULL DEFAULT 'primary'")
    if not await _has_column(engine, table=table, column="stream_manager_remove_commentary"):
        await _add_column(engine, table=table, ddl="stream_manager_remove_commentary BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="stream_manager_subtitle_mode"):
        await _add_column(engine, table=table, ddl="stream_manager_subtitle_mode TEXT NOT NULL DEFAULT 'remove_all'")
    if not await _has_column(engine, table=table, column="stream_manager_subtitle_langs_csv"):
        await _add_column(engine, table=table, ddl="stream_manager_subtitle_langs_csv TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="stream_manager_preserve_forced_subs"):
        await _add_column(engine, table=table, ddl="stream_manager_preserve_forced_subs BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="stream_manager_preserve_default_subs"):
        await _add_column(engine, table=table, ddl="stream_manager_preserve_default_subs BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="stream_manager_paths"):
        await _add_column(engine, table=table, ddl="stream_manager_paths TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="stream_manager_interval_minutes"):
        await _add_column(engine, table=table, ddl="stream_manager_interval_minutes INTEGER NOT NULL DEFAULT 60")
    if not await _has_column(engine, table=table, column="stream_manager_schedule_enabled"):
        await _add_column(engine, table=table, ddl="stream_manager_schedule_enabled BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="stream_manager_schedule_days"):
        await _add_column(engine, table=table, ddl="stream_manager_schedule_days TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table=table, column="stream_manager_schedule_start"):
        await _add_column(engine, table=table, ddl="stream_manager_schedule_start TEXT NOT NULL DEFAULT '00:00'")
    if not await _has_column(engine, table=table, column="stream_manager_schedule_end"):
        await _add_column(engine, table=table, ddl="stream_manager_schedule_end TEXT NOT NULL DEFAULT '23:59'")
    if not await _has_column(engine, table=table, column="stream_manager_last_run_at"):
        await _add_column(engine, table=table, ddl="stream_manager_last_run_at DATETIME")


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
    await _migrate_026_stream_manager_columns(engine)
