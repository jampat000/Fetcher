from app.service_logic import (
    _episode_ids_for_emby_tv_item,
    _match_radarr_movie_id,
    _match_sonarr_series_id,
    _sonarr_episode_file_id,
    _sonarr_series_is_ended,
)


def test_match_radarr_by_tmdb_then_imdb_then_title_year() -> None:
    catalog = [
        {"id": 1, "title": "Example Movie", "year": 2024, "tmdbId": 1234, "imdbId": "tt001"},
        {"id": 2, "title": "Other", "year": 2023, "tmdbId": 9999, "imdbId": "tt999"},
    ]
    by_tmdb = {"Name": "whatever", "ProviderIds": {"Tmdb": "1234"}}
    by_imdb = {"Name": "whatever", "ProviderIds": {"Imdb": "tt001"}}
    by_title = {"Name": "Example Movie", "ProductionYear": 2024}
    assert _match_radarr_movie_id(by_tmdb, catalog) == 1
    assert _match_radarr_movie_id(by_imdb, catalog) == 1
    assert _match_radarr_movie_id(by_title, catalog) == 1


def test_match_sonarr_by_tvdb_then_title_year() -> None:
    catalog = [
        {"id": 10, "title": "Example Show", "year": 2021, "tvdbId": 555},
        {"id": 11, "title": "Another Show", "year": 2022, "tvdbId": 777},
    ]
    by_tvdb = {"Name": "ignored", "ProviderIds": {"Tvdb": "555"}}
    by_title = {"Name": "Example Show", "ProductionYear": 2021}
    assert _match_sonarr_series_id(by_tvdb, catalog) == 10
    assert _match_sonarr_series_id(by_title, catalog) == 10


def test_episode_ids_for_emby_tv_item_episode_season_series() -> None:
    sonarr_eps = [
        {"id": 101, "seasonNumber": 1, "episodeNumber": 1},
        {"id": 102, "seasonNumber": 1, "episodeNumber": 2},
        {"id": 201, "seasonNumber": 2, "episodeNumber": 1},
    ]
    emby_episode = {"Type": "Episode", "ParentIndexNumber": 1, "IndexNumber": 2}
    emby_season = {"Type": "Season", "ParentIndexNumber": 1}
    emby_series = {"Type": "Series"}

    assert _episode_ids_for_emby_tv_item(emby_episode, sonarr_eps) == [102]
    assert _episode_ids_for_emby_tv_item(emby_season, sonarr_eps) == [101, 102]
    assert _episode_ids_for_emby_tv_item(emby_series, sonarr_eps) == [101, 102, 201]


def test_sonarr_series_is_ended_only_when_status_ended() -> None:
    assert _sonarr_series_is_ended({"status": "ended"}) is True
    assert _sonarr_series_is_ended({"status": "Continuing"}) is False
    assert _sonarr_series_is_ended({"status": "upcoming"}) is False
    assert _sonarr_series_is_ended({}) is False
    assert _sonarr_series_is_ended(None) is False


def test_sonarr_episode_file_id_from_field_or_nested() -> None:
    assert _sonarr_episode_file_id({"episodeFileId": 99}) == 99
    assert _sonarr_episode_file_id({"episodeFile": {"id": 88}}) == 88
    assert _sonarr_episode_file_id({"episodeFileId": 99, "episodeFile": {"id": 88}}) == 99
    assert _sonarr_episode_file_id({}) is None
