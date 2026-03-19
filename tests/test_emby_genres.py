from app.emby_rules import movie_matches_selected_genres, parse_genres_csv, tv_matches_selected_genres


def test_parse_genres_csv_normalizes_values() -> None:
    values = parse_genres_csv("Action, drama, ,Thriller,action")
    assert values == {"action", "drama", "thriller"}


def test_movie_matches_selected_genres_true_on_any_overlap() -> None:
    item = {"Type": "Movie", "Genres": ["Comedy", "Thriller"]}
    assert movie_matches_selected_genres(item, {"thriller", "drama"}) is True


def test_movie_matches_selected_genres_false_when_no_overlap() -> None:
    item = {"Type": "Movie", "Genres": ["Comedy", "Family"]}
    assert movie_matches_selected_genres(item, {"thriller", "drama"}) is False


def test_tv_matches_selected_genres_true_on_any_overlap() -> None:
    item = {"Type": "Series", "Genres": ["Crime", "Drama"]}
    assert tv_matches_selected_genres(item, {"drama", "animation"}) is True


def test_tv_matches_selected_genres_false_when_no_overlap() -> None:
    item = {"Type": "Series", "Genres": ["Comedy", "Family"]}
    assert tv_matches_selected_genres(item, {"thriller", "drama"}) is False
