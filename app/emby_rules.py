from __future__ import annotations

from datetime import UTC, datetime


def parse_iso_dt(raw: str | None) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def days_since(item: dict) -> int | None:
    for key in ("DateLastMediaAdded", "DateCreated", "PremiereDate"):
        dt = parse_iso_dt(item.get(key))
        if dt is not None:
            return max(0, int((datetime.now(UTC) - dt).total_seconds() // 86400))
    return None


def emby_rating(user_data: dict) -> float | None:
    rating = user_data.get("Rating")
    if isinstance(rating, (int, float)):
        return float(rating)
    return None


def emby_user_played(user_data: dict) -> bool:
    return bool(user_data.get("Played"))


def parse_genres_csv(raw: str | None) -> set[str]:
    if not raw:
        return set()
    out: set[str] = set()
    for chunk in raw.split(","):
        v = chunk.strip().lower()
        if v:
            out.add(v)
    return out


def movie_matches_selected_genres(item: dict, selected_genres: set[str]) -> bool:
    if not selected_genres:
        return True
    genres = item.get("Genres") if isinstance(item.get("Genres"), list) else []
    item_genres = {str(g).strip().lower() for g in genres if str(g).strip()}
    return bool(item_genres & selected_genres)


def tv_matches_selected_genres(item: dict, selected_genres: set[str]) -> bool:
    if not selected_genres:
        return True
    genres = item.get("Genres") if isinstance(item.get("Genres"), list) else []
    item_genres = {str(g).strip().lower() for g in genres if str(g).strip()}
    return bool(item_genres & selected_genres)


def parse_movie_people_phrases(raw: str | None) -> list[str]:
    """Lowercased name phrases from comma- or newline-separated input."""
    if not raw or not str(raw).strip():
        return []
    out: list[str] = []
    for line in str(raw).replace("\r", "").split("\n"):
        for chunk in line.split(","):
            v = chunk.strip().lower()
            if v:
                out.append(v)
    seen: set[str] = set()
    uniq: list[str] = []
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


KNOWN_MOVIE_PEOPLE_CREDIT_TYPES = frozenset({"actor", "director", "writer", "producer", "gueststar"})


def parse_movie_people_credit_types_csv(raw: str | None) -> frozenset[str]:
    """Emby People.Type values (lowercase). Default cast-only if unset or invalid."""
    if not raw or not str(raw).strip():
        return frozenset({"actor"})
    out: set[str] = set()
    for chunk in str(raw).split(","):
        v = chunk.strip().lower()
        if v in KNOWN_MOVIE_PEOPLE_CREDIT_TYPES:
            out.add(v)
    return frozenset(out) if out else frozenset({"actor"})


def movie_matches_people(
    item: dict,
    phrases: list[str],
    *,
    credit_types: frozenset[str] | None = None,
) -> bool:
    """Match name phrases against Emby People on a Movie or Series item (same field shape)."""
    if not phrases:
        return True
    ct = credit_types if credit_types is not None else frozenset({"actor"})
    people = item.get("People") if isinstance(item.get("People"), list) else []
    names_lower: list[str] = []
    for p in people:
        if not isinstance(p, dict):
            continue
        ptype = str(p.get("Type", "")).strip().lower()
        if ptype not in ct:
            continue
        n = str(p.get("Name", "")).strip().lower()
        if n:
            names_lower.append(n)
    for phrase in phrases:
        for n in names_lower:
            if phrase in n:
                return True
    return False


def evaluate_candidate(
    item: dict,
    *,
    movie_watched_rating_below: int,
    movie_unwatched_days: int,
    tv_delete_watched: bool,
    tv_unwatched_days: int,
) -> tuple[bool, list[str], int | None, float | None, bool]:
    item_type = str(item.get("Type", "") or "").strip()
    is_movie = item_type == "Movie"
    is_tv = item_type in {"Series", "Season", "Episode"}
    if not is_movie and not is_tv:
        return (False, [], None, None, False)

    user_data = item.get("UserData") if isinstance(item.get("UserData"), dict) else {}
    played = emby_user_played(user_data)
    rating = emby_rating(user_data)
    age_days = days_since(item)

    unwatched_days = movie_unwatched_days if is_movie else tv_unwatched_days
    media_label = "movie" if is_movie else "tv"

    reasons: list[str] = []
    if is_movie and movie_watched_rating_below > 0 and played and rating is not None and rating < float(movie_watched_rating_below):
        reasons.append(f"{media_label}: watched and rated {rating:g} < {movie_watched_rating_below}")
    if is_tv and tv_delete_watched and played:
        reasons.append(f"{media_label}: watched")
    if unwatched_days > 0 and (not played) and age_days is not None and age_days >= unwatched_days:
        reasons.append(f"{media_label}: unwatched and age {age_days}d >= {unwatched_days}d")

    return (len(reasons) > 0, reasons, age_days, rating, played)
