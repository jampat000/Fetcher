from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text

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
    sonarr_schedule_days: Mapped[str] = mapped_column(Text, default="Mon,Tue,Wed,Thu,Fri,Sat,Sun")
    sonarr_schedule_start: Mapped[str] = mapped_column(String(5), default="00:00")  # HH:MM
    sonarr_schedule_end: Mapped[str] = mapped_column(String(5), default="23:59")  # HH:MM

    # Radarr
    radarr_url: Mapped[str] = mapped_column(String(512), default="")
    radarr_api_key: Mapped[str] = mapped_column(String(256), default="")
    radarr_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_search_missing: Mapped[bool] = mapped_column(Boolean, default=True)
    radarr_search_upgrades: Mapped[bool] = mapped_column(Boolean, default=True)
    radarr_max_items_per_run: Mapped[int] = mapped_column(Integer, default=50)
    radarr_schedule_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    radarr_schedule_days: Mapped[str] = mapped_column(Text, default="Mon,Tue,Wed,Thu,Fri,Sat,Sun")
    radarr_schedule_start: Mapped[str] = mapped_column(String(5), default="00:00")  # HH:MM
    radarr_schedule_end: Mapped[str] = mapped_column(String(5), default="23:59")  # HH:MM

    # Scheduler & display
    interval_minutes: Mapped[int] = mapped_column(Integer, default=60)
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")  # IANA e.g. America/New_York

    # Emby Cleaner
    emby_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    emby_url: Mapped[str] = mapped_column(String(512), default="")
    emby_api_key: Mapped[str] = mapped_column(String(256), default="")
    emby_user_id: Mapped[str] = mapped_column(String(128), default="")
    emby_dry_run: Mapped[bool] = mapped_column(Boolean, default=True)
    emby_schedule_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    emby_schedule_days: Mapped[str] = mapped_column(Text, default="Mon,Tue,Wed,Thu,Fri,Sat,Sun")
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
    # Global defaults (kept for backward compatibility / existing DBs)
    search_missing: Mapped[bool] = mapped_column(Boolean, default=True)
    search_upgrades: Mapped[bool] = mapped_column(Boolean, default=True)
    max_items_per_run: Mapped[int] = mapped_column(Integer, default=50)

    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive)


class JobRunLog(Base):
    __tablename__ = "job_run_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ok: Mapped[bool] = mapped_column(Boolean, default=False)
    message: Mapped[str] = mapped_column(Text, default="")


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

    app: Mapped[str] = mapped_column(String(16))   # "sonarr" | "radarr"
    kind: Mapped[str] = mapped_column(String(16))  # "missing" | "upgrade"
    status: Mapped[str] = mapped_column(String(16), default="ok")  # "ok" | "failed"
    count: Mapped[int] = mapped_column(Integer, default=0)
    detail: Mapped[str] = mapped_column(Text, default="")


