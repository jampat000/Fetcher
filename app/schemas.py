from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator


class SettingsIn(BaseModel):
    sonarr_enabled: bool = False
    sonarr_url: str = Field(default="", description="Base URL, e.g. http://localhost:8989")
    sonarr_api_key: str = ""
    sonarr_search_missing: bool = True
    sonarr_search_upgrades: bool = True
    sonarr_max_items_per_run: int = Field(default=50, ge=1, le=1000)
    sonarr_interval_minutes: int = Field(default=60, ge=1, le=7 * 24 * 60)

    radarr_enabled: bool = False
    radarr_url: str = Field(default="", description="Base URL, e.g. http://localhost:7878")
    radarr_api_key: str = ""
    radarr_search_missing: bool = True
    radarr_search_upgrades: bool = True
    radarr_max_items_per_run: int = Field(default=50, ge=1, le=1000)
    radarr_interval_minutes: int = Field(default=60, ge=1, le=7 * 24 * 60)

    interval_minutes: int = Field(
        default=60,
        ge=5,
        le=7 * 24 * 60,
        description="Grabby scheduler base interval (wake cadence). Sonarr/Radarr run intervals are set per app above (minimum 1 minute each).",
    )
    emby_interval_minutes: int = Field(
        default=60,
        ge=5,
        le=7 * 24 * 60,
        description="Emby Cleaner run cadence only (Cleaner Settings).",
    )
    arr_search_cooldown_minutes: int = Field(
        default=1440,
        ge=0,
        le=365 * 24 * 60,
        description="0 = same as run interval; else min minutes before re-searching the same Sonarr/Radarr item.",
    )

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

    @field_validator("sonarr_interval_minutes", "radarr_interval_minutes", mode="before")
    @classmethod
    def _coerce_arr_run_interval(cls, v: Any) -> int:
        """Legacy DB/UI used 0; treat as 60 so stored values match real cadence."""
        try:
            if v is None or v == "":
                return 60
            x = int(v)
        except (TypeError, ValueError):
            return 60
        if x < 1:
            return 60
        return x


class SettingsOut(SettingsIn):
    pass


class SetupConnTestIn(BaseModel):
    """JSON body for wizard connection tests (Sonarr/Radarr)."""

    url: str = ""
    api_key: str = ""


class SetupEmbyTestIn(BaseModel):
    url: str = ""
    api_key: str = ""
    user_id: str = ""

