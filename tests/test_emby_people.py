from app.emby_rules import (
    movie_matches_people,
    parse_movie_people_credit_types_csv,
    parse_movie_people_phrases,
)


def test_parse_movie_people_phrases_splits_and_dedupes() -> None:
    raw = "Tom Hanks,\nsteven spielberg\nTom Hanks"
    assert parse_movie_people_phrases(raw) == ["tom hanks", "steven spielberg"]


def test_movie_matches_people_substring() -> None:
    item = {
        "Type": "Movie",
        "People": [
            {"Name": "Tom Hanks", "Type": "Actor"},
            {"Name": "Someone Else", "Type": "Director"},
        ],
    }
    assert movie_matches_people(item, ["tom"]) is True
    assert movie_matches_people(item, ["hanks"]) is True
    assert movie_matches_people(item, ["spielberg"]) is False


def test_movie_matches_people_empty_rule_matches_all() -> None:
    item = {"Type": "Movie", "People": []}
    assert movie_matches_people(item, []) is True


def test_movie_matches_people_respects_credit_types() -> None:
    item = {
        "Type": "Movie",
        "People": [
            {"Name": "Tom Hanks", "Type": "Actor"},
            {"Name": "Steven Spielberg", "Type": "Director"},
        ],
    }
    directors = frozenset({"director"})
    actors = frozenset({"actor"})
    assert movie_matches_people(item, ["spielberg"], credit_types=actors) is False
    assert movie_matches_people(item, ["spielberg"], credit_types=directors) is True
    assert movie_matches_people(item, ["hanks"], credit_types=directors) is False


def test_parse_movie_people_credit_types_csv() -> None:
    assert parse_movie_people_credit_types_csv("") == frozenset({"actor"})
    assert parse_movie_people_credit_types_csv("bogus,,") == frozenset({"actor"})
    assert parse_movie_people_credit_types_csv("Director, Writer") == frozenset({"director", "writer"})


def test_movie_matches_people_series_item() -> None:
    series = {
        "Type": "Series",
        "People": [
            {"Name": "Bryan Cranston", "Type": "Actor"},
            {"Name": "Vince Gilligan", "Type": "Writer"},
        ],
    }
    assert movie_matches_people(series, ["cranston"]) is True
    assert movie_matches_people(series, ["gilligan"], credit_types=frozenset({"writer"})) is True
    assert movie_matches_people(series, ["gilligan"], credit_types=frozenset({"actor"})) is False
