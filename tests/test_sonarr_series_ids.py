"""Sonarr tags series, not episodes — helper must match episode batch to series ids."""

from app.service_logic import _sonarr_series_ids_for_episode_batch, _take_int_ids


def test_series_ids_align_with_episode_batch_order() -> None:
    records = [
        {"id": 10, "seriesId": 100},
        {"id": 11, "seriesId": 100},
        {"id": 12, "seriesId": 200},
    ]
    epi = _take_int_ids(records, "episodeId", "id", limit=3)
    sids = _sonarr_series_ids_for_episode_batch(records, "episodeId", "id", limit=3)
    assert epi == [10, 11, 12]
    assert sids == [100, 200]


def test_series_ids_respect_episode_limit_not_series_limit() -> None:
    records = [
        {"id": 1, "seriesId": 1},
        {"id": 2, "seriesId": 2},
        {"id": 3, "seriesId": 3},
    ]
    sids = _sonarr_series_ids_for_episode_batch(records, "episodeId", "id", limit=2)
    assert sids == [1, 2]
