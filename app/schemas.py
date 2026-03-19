from __future__ import annotations

from pydantic import BaseModel, Field, HttpUrl


class SettingsIn(BaseModel):
    sonarr_enabled: bool = False
    sonarr_url: str = Field(default="", description="Base URL, e.g. http://localhost:8989")
    sonarr_api_key: str = ""
    sonarr_search_missing: bool = True
    sonarr_search_upgrades: bool = True
    sonarr_max_items_per_run: int = Field(default=50, ge=1, le=1000)

    radarr_enabled: bool = False
    radarr_url: str = Field(default="", description="Base URL, e.g. http://localhost:7878")
    radarr_api_key: str = ""
    radarr_search_missing: bool = True
    radarr_search_upgrades: bool = True
    radarr_max_items_per_run: int = Field(default=50, ge=1, le=1000)

    interval_minutes: int = Field(default=60, ge=5, le=7 * 24 * 60)

    emby_enabled: bool = False
    emby_url: str = Field(default="", description="Base URL, e.g. http://localhost:8096")
    emby_api_key: str = ""
    emby_user_id: str = ""
    emby_dry_run: bool = True
    emby_max_items_scan: int = Field(default=2000, ge=0, le=100_000)
    emby_max_deletes_per_run: int = Field(default=25, ge=1, le=500)
    emby_rule_watched_rating_below: int = Field(default=0, ge=0, le=10)
    emby_rule_unwatched_days: int = Field(default=0, ge=0, le=36500)
    emby_rule_movie_watched_rating_below: int = Field(default=0, ge=0, le=10)
    emby_rule_movie_unwatched_days: int = Field(default=0, ge=0, le=36500)
    emby_rule_tv_delete_watched: bool = False
    emby_rule_tv_unwatched_days: int = Field(default=0, ge=0, le=36500)

    # Back-compat (older UI/DB); if used, service_logic will fall back to these
    search_missing: bool = True
    search_upgrades: bool = True
    max_items_per_run: int = Field(default=50, ge=1, le=1000)


class SettingsOut(SettingsIn):
    pass

