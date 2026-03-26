from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl, field_validator


class SettingsIn(BaseModel):
    sonarr_enabled: bool = False
    sonarr_url: str = Field(default="", description="Base URL, e.g. http://localhost:8989")
    sonarr_api_key: str = ""
    sonarr_search_missing: bool = True
    sonarr_search_upgrades: bool = True
    sonarr_remove_failed_imports: bool = False
    sonarr_max_items_per_run: int = Field(default=50, ge=1, le=1000)
    sonarr_interval_minutes: int = Field(default=60, ge=1, le=7 * 24 * 60)

    radarr_enabled: bool = False
    radarr_url: str = Field(default="", description="Base URL, e.g. http://localhost:7878")
    radarr_api_key: str = ""
    radarr_search_missing: bool = True
    radarr_search_upgrades: bool = True
    radarr_remove_failed_imports: bool = False
    radarr_max_items_per_run: int = Field(default=50, ge=1, le=1000)
    radarr_interval_minutes: int = Field(default=60, ge=1, le=7 * 24 * 60)

    emby_interval_minutes: int = Field(
        default=60,
        ge=5,
        le=7 * 24 * 60,
        description="Trimmer run cadence only (Trimmer settings).",
    )
    sonarr_retry_delay_minutes: int = Field(
        default=1440,
        ge=1,
        le=365 * 24 * 60,
        description="Min minutes before retrying the same Sonarr item search.",
    )
    radarr_retry_delay_minutes: int = Field(
        default=1440,
        ge=1,
        le=365 * 24 * 60,
        description="Min minutes before retrying the same Radarr item search.",
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

    @field_validator("sonarr_interval_minutes", "radarr_interval_minutes", mode="before")
    @classmethod
    def _coerce_arr_run_interval(cls, v: Any) -> int:
        """Coerce invalid/low stored values to 60 minutes."""
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


class ArrSearchNowIn(BaseModel):
    """Dashboard one-shot Arr search (bypasses schedule + run-interval gates for that action)."""

    scope: Literal["sonarr_missing", "sonarr_upgrade", "radarr_missing", "radarr_upgrade"]


class SetupConnTestIn(BaseModel):
    """JSON body for wizard connection tests (Sonarr/Radarr)."""

    url: str = ""
    api_key: str = ""


class SetupEmbyTestIn(BaseModel):
    url: str = ""
    api_key: str = ""
    user_id: str = ""

