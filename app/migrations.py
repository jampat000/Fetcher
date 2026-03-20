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


async def _widen_schedule_days_columns(engine: AsyncEngine) -> None:
    """Ensure schedule day columns can store the full Mon..Sun CSV on SQL backends with strict VARCHAR."""
    # SQLite is permissive about text length and doesn't support straightforward ALTER TYPE.
    if engine.dialect.name == "sqlite":
        return
    async with engine.begin() as conn:
        for col in ("sonarr_schedule_days", "radarr_schedule_days", "emby_schedule_days"):
            await conn.execute(text(f"ALTER TABLE app_settings ALTER COLUMN {col} TYPE TEXT"))


async def migrate(engine: AsyncEngine) -> None:
    # Lightweight SQLite migration: add new columns if missing.
    table = "app_settings"

    # Sonarr per-app settings
    if not await _has_column(engine, table=table, column="sonarr_search_missing"):
        await _add_column(engine, table=table, ddl="sonarr_search_missing BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="sonarr_search_upgrades"):
        await _add_column(engine, table=table, ddl="sonarr_search_upgrades BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="sonarr_max_items_per_run"):
        await _add_column(engine, table=table, ddl="sonarr_max_items_per_run INTEGER NOT NULL DEFAULT 50")

    # Radarr per-app settings
    if not await _has_column(engine, table=table, column="radarr_search_missing"):
        await _add_column(engine, table=table, ddl="radarr_search_missing BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="radarr_search_upgrades"):
        await _add_column(engine, table=table, ddl="radarr_search_upgrades BOOLEAN NOT NULL DEFAULT 1")
    if not await _has_column(engine, table=table, column="radarr_max_items_per_run"):
        await _add_column(engine, table=table, ddl="radarr_max_items_per_run INTEGER NOT NULL DEFAULT 50")

    # Per-app schedules
    if not await _has_column(engine, table=table, column="sonarr_schedule_enabled"):
        await _add_column(engine, table=table, ddl="sonarr_schedule_enabled BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="sonarr_schedule_days"):
        await _add_column(engine, table=table, ddl="sonarr_schedule_days TEXT NOT NULL DEFAULT 'Mon,Tue,Wed,Thu,Fri,Sat,Sun'")
    if not await _has_column(engine, table=table, column="sonarr_schedule_start"):
        await _add_column(engine, table=table, ddl="sonarr_schedule_start TEXT NOT NULL DEFAULT '00:00'")
    if not await _has_column(engine, table=table, column="sonarr_schedule_end"):
        await _add_column(engine, table=table, ddl="sonarr_schedule_end TEXT NOT NULL DEFAULT '23:59'")

    if not await _has_column(engine, table=table, column="radarr_schedule_enabled"):
        await _add_column(engine, table=table, ddl="radarr_schedule_enabled BOOLEAN NOT NULL DEFAULT 0")
    if not await _has_column(engine, table=table, column="radarr_schedule_days"):
        await _add_column(engine, table=table, ddl="radarr_schedule_days TEXT NOT NULL DEFAULT 'Mon,Tue,Wed,Thu,Fri,Sat,Sun'")
    if not await _has_column(engine, table=table, column="radarr_schedule_start"):
        await _add_column(engine, table=table, ddl="radarr_schedule_start TEXT NOT NULL DEFAULT '00:00'")
    if not await _has_column(engine, table=table, column="radarr_schedule_end"):
        await _add_column(engine, table=table, ddl="radarr_schedule_end TEXT NOT NULL DEFAULT '23:59'")

    # Timezone
    if not await _has_column(engine, table=table, column="timezone"):
        await _add_column(engine, table=table, ddl="timezone TEXT NOT NULL DEFAULT 'UTC'")

    # Emby Cleaner
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
        await _add_column(engine, table=table, ddl="emby_schedule_days TEXT NOT NULL DEFAULT 'Mon,Tue,Wed,Thu,Fri,Sat,Sun'")
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

    # Older schemas used VARCHAR(16), which can be too short for full week CSV on strict DBs.
    await _widen_schedule_days_columns(engine)

    # Snapshots / activity tables (create if missing)
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

    # Detailed activity log text for existing DBs.
    if not await _has_column(engine, table="activity_log", column="detail"):
        await _add_column(engine, table="activity_log", ddl="detail TEXT NOT NULL DEFAULT ''")
    if not await _has_column(engine, table="activity_log", column="status"):
        await _add_column(engine, table="activity_log", ddl="status TEXT NOT NULL DEFAULT 'ok'")

