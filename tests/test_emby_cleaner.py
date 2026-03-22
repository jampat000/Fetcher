"""Unit tests for Emby Cleaner rule evaluation (plain API-shaped dicts)."""

from __future__ import annotations

import datetime as std_datetime

import pytest

from app.emby_rules import evaluate_candidate


@pytest.fixture
def pinned_now(monkeypatch: pytest.MonkeyPatch) -> std_datetime.datetime:
    """Fixed UTC instant so ``days_since`` is deterministic."""
    fixed = std_datetime.datetime(2025, 6, 15, 12, 0, 0, tzinfo=std_datetime.UTC)

    class _DateTimeProxy:
        @staticmethod
        def now(tz=None):
            return fixed

        def __getattr__(self, name: str):
            return getattr(std_datetime.datetime, name)

    monkeypatch.setattr("app.emby_rules.datetime", _DateTimeProxy())
    return fixed


def _iso_days_ago(pinned: std_datetime.datetime, days: int) -> str:
    dt = pinned - std_datetime.timedelta(days=days)
    return dt.replace(tzinfo=std_datetime.UTC).isoformat().replace("+00:00", "Z")


def test_movie_matches_watched_rating_rule(pinned_now: std_datetime.datetime) -> None:
    item = {
        "Type": "Movie",
        "Name": "Low Rated Flick",
        "CommunityRating": 6.0,
        "DateCreated": _iso_days_ago(pinned_now, 400),
        "UserData": {"Played": True, "Rating": 2.5},
    }
    ok, reasons, age_days, rating, played = evaluate_candidate(
        item,
        movie_watched_rating_below=5,
        movie_unwatched_days=0,
        tv_delete_watched=False,
        tv_unwatched_days=0,
    )
    assert ok is True
    assert played is True
    assert rating == 2.5
    assert age_days is not None
    assert any("watched and rated" in r and "2.5" in r for r in reasons)


def test_movie_matches_unwatched_days_rule(pinned_now: std_datetime.datetime) -> None:
    item = {
        "Type": "Movie",
        "Name": "Old Unwatched",
        "CommunityRating": 8.0,
        "DateCreated": _iso_days_ago(pinned_now, 40),
        "UserData": {"Played": False},
    }
    ok, reasons, age_days, rating, played = evaluate_candidate(
        item,
        movie_watched_rating_below=0,
        movie_unwatched_days=30,
        tv_delete_watched=False,
        tv_unwatched_days=0,
    )
    assert ok is True
    assert played is False
    assert age_days is not None
    assert age_days >= 30
    assert any("unwatched" in r and "30" in r for r in reasons)


def test_tv_episode_matches_tv_delete_watched(pinned_now: std_datetime.datetime) -> None:
    item = {
        "Type": "Episode",
        "Name": "S01E01",
        "SeriesName": "Test Show",
        "DateCreated": _iso_days_ago(pinned_now, 2),
        "UserData": {"Played": True, "Rating": 9.0},
    }
    ok, reasons, age_days, rating, played = evaluate_candidate(
        item,
        movie_watched_rating_below=0,
        movie_unwatched_days=0,
        tv_delete_watched=True,
        tv_unwatched_days=0,
    )
    assert ok is True
    assert played is True
    assert any("tv: watched" in r for r in reasons)


def test_tv_episode_matches_tv_unwatched_days(pinned_now: std_datetime.datetime) -> None:
    item = {
        "Type": "Episode",
        "Name": "S02E03",
        "DateCreated": _iso_days_ago(pinned_now, 10),
        "UserData": {"Played": False},
    }
    ok, reasons, age_days, rating, played = evaluate_candidate(
        item,
        movie_watched_rating_below=0,
        movie_unwatched_days=0,
        tv_delete_watched=False,
        tv_unwatched_days=5,
    )
    assert ok is True
    assert played is False
    assert age_days is not None
    assert age_days >= 5
    assert any("unwatched" in r and "5" in r for r in reasons)


def test_item_matches_no_rules_not_candidate(pinned_now: std_datetime.datetime) -> None:
    """Music library items are ignored; no rule flags → not a candidate."""
    item = {
        "Type": "MusicAlbum",
        "Name": "Greatest Hits",
        "DateCreated": _iso_days_ago(pinned_now, 999),
        "UserData": {"Played": True, "Rating": 1.0},
    }
    ok, reasons, age_days, rating, played = evaluate_candidate(
        item,
        movie_watched_rating_below=10,
        movie_unwatched_days=1,
        tv_delete_watched=True,
        tv_unwatched_days=1,
    )
    assert ok is False
    assert reasons == []
    assert age_days is None
    assert rating is None
    assert played is False
