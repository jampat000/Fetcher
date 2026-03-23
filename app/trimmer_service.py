from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession

from app.emby_client import EmbyClient, EmbyConfig
from app.emby_rules import (
    evaluate_candidate,
    movie_matches_people,
    movie_matches_selected_genres,
    parse_genres_csv,
    parse_movie_people_credit_types_csv,
    parse_movie_people_phrases,
    tv_matches_selected_genres,
)
from app.models import AppSettings
from app.resolvers.api_keys import resolve_emby_api_key, resolve_radarr_api_key, resolve_sonarr_api_key
from app.service_logic import apply_emby_trimmer_live_deletes
from app.time_util import utc_now_naive
from app.web_common import effective_emby_rules

"""Trimmer orchestration services.

TrimmerReviewService is read-only and builds review rows/candidates for rendering.
TrimmerApplyService owns live-delete side effects (delete apply + last-run persistence).
If you change dry-run vs live-delete behavior, update regression tests first.
"""


@dataclass(slots=True)
class TrimmerReviewResult:
    error: str = ""
    rows: list[dict[str, Any]] = field(default_factory=list)
    candidates: list[tuple[str, str, str, dict[str, Any]]] = field(default_factory=list)
    used_user_id: str = ""
    used_user_name: str = ""
    movie_rating_below: int = 0
    movie_unwatched_days: int = 0
    tv_delete_watched: bool = False
    tv_unwatched_days: int = 0
    scan_limit: int = 2000
    max_deletes: int = 25
    selected_movie_genres: set[str] = field(default_factory=set)
    selected_tv_genres: set[str] = field(default_factory=set)
    selected_movie_people: list[str] = field(default_factory=list)
    selected_movie_credit_types: set[str] = field(default_factory=set)
    selected_tv_people: list[str] = field(default_factory=list)
    selected_tv_credit_types: set[str] = field(default_factory=set)
    scan_prompt: bool = False
    scan_loaded: bool = False


class TrimmerReviewService:
    """Builds the review result without mutating persistent state."""

    async def build_review(self, settings: AppSettings, *, run_emby_scan: bool) -> TrimmerReviewResult:
        result = TrimmerReviewResult()
        result.used_user_id = (settings.emby_user_id or "").strip()

        rules = effective_emby_rules(settings)
        result.movie_rating_below = rules["movie_rating_below"]
        result.movie_unwatched_days = rules["movie_unwatched_days"]
        result.tv_delete_watched = bool(rules["tv_delete_watched"])
        result.tv_unwatched_days = rules["tv_unwatched_days"]
        _v_scan = settings.emby_max_items_scan
        _raw_scan = int(_v_scan) if _v_scan is not None else 2000
        result.scan_limit = 0 if _raw_scan <= 0 else max(1, min(100_000, _raw_scan))
        result.max_deletes = max(1, int(settings.emby_max_deletes_per_run or 25))

        result.selected_movie_genres = parse_genres_csv(settings.emby_rule_movie_genres_csv)
        result.selected_tv_genres = parse_genres_csv(settings.emby_rule_tv_genres_csv)
        result.selected_movie_people = parse_movie_people_phrases(settings.emby_rule_movie_people_csv)
        result.selected_movie_credit_types = parse_movie_people_credit_types_csv(
            settings.emby_rule_movie_people_credit_types_csv
        )
        result.selected_tv_people = parse_movie_people_phrases(settings.emby_rule_tv_people_csv)
        result.selected_tv_credit_types = parse_movie_people_credit_types_csv(
            settings.emby_rule_tv_people_credit_types_csv
        )

        emby_key = resolve_emby_api_key(settings)
        if not settings.emby_url or not emby_key:
            result.error = "Emby URL and API key are required."
            return result

        if (
            result.movie_rating_below <= 0
            and result.movie_unwatched_days <= 0
            and (not result.tv_delete_watched)
            and result.tv_unwatched_days <= 0
        ):
            result.error = "No rules are enabled. Set at least one Emby Trimmer rule in Trimmer settings."
            return result

        if not run_emby_scan:
            # Fast path: sidebar / default navigation should not scan the whole library.
            result.scan_prompt = True
            return result

        client = EmbyClient(EmbyConfig(settings.emby_url, emby_key))
        try:
            await client.health()
            users = await client.users()
            users_by_id = {str(u.get("Id", "")).strip(): str(u.get("Name", "")).strip() for u in users}
            if not result.used_user_id and users:
                result.used_user_id = str(users[0].get("Id", "")).strip()
            result.used_user_name = users_by_id.get(result.used_user_id, "")
            if not result.used_user_id:
                result.error = "No Emby user available."
                return result
            if not result.used_user_name:
                result.error = "Configured Emby user ID was not found."
                return result

            result.scan_loaded = True
            items = await client.items_for_user(user_id=result.used_user_id, limit=result.scan_limit)
            for item in items:
                item_id = str(item.get("Id", "")).strip()
                if not item_id:
                    continue
                is_candidate, reasons, age_days, rating, played = evaluate_candidate(
                    item,
                    movie_watched_rating_below=result.movie_rating_below,
                    movie_unwatched_days=result.movie_unwatched_days,
                    tv_delete_watched=result.tv_delete_watched,
                    tv_unwatched_days=result.tv_unwatched_days,
                )
                item_type = str(item.get("Type", "")).strip()
                if item_type == "Movie" and not movie_matches_selected_genres(item, result.selected_movie_genres):
                    is_candidate = False
                if item_type == "Movie" and not movie_matches_people(
                    item, result.selected_movie_people, credit_types=result.selected_movie_credit_types
                ):
                    is_candidate = False
                if item_type in {"Series", "Season", "Episode"} and not tv_matches_selected_genres(
                    item, result.selected_tv_genres
                ):
                    is_candidate = False
                if item_type in {"Series", "Season", "Episode"} and not movie_matches_people(
                    item, result.selected_tv_people, credit_types=result.selected_tv_credit_types
                ):
                    is_candidate = False
                if not is_candidate:
                    continue
                name = str(item.get("Name", "") or item_id)
                item_type = str(item.get("Type", "") or "").strip()
                result.candidates.append((item_id, name, item_type, item))
                result.rows.append(
                    {
                        "id": item_id,
                        "name": name,
                        "type": item_type or "-",
                        "played": played,
                        "rating": rating,
                        "age_days": age_days,
                        "reasons": reasons,
                    }
                )
                if len(result.candidates) >= result.max_deletes:
                    break
            return result
        except Exception as e:  # noqa: BLE001 - user-facing review path
            result.error = f"Review failed: {type(e).__name__}: {e}"
            result.scan_loaded = False
            return result
        finally:
            await client.aclose()


class TrimmerApplyService:
    """Applies live-delete side effects only when review candidates exist and dry-run is off."""

    async def apply_live_delete_if_needed(
        self, settings: AppSettings, session: AsyncSession, review: TrimmerReviewResult
    ) -> None:
        if not review.candidates or settings.emby_dry_run:
            return
        sk = resolve_sonarr_api_key(settings)
        rk = resolve_radarr_api_key(settings)
        emby_key = resolve_emby_api_key(settings)
        client = EmbyClient(EmbyConfig(settings.emby_url, emby_key))
        try:
            await apply_emby_trimmer_live_deletes(
                settings, client, review.candidates, son_key=sk, rad_key=rk
            )
            settings.emby_last_run_at = utc_now_naive()
            await session.commit()
        finally:
            await client.aclose()
