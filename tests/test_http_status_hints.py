from app.http_status_hints import hint_for_http_status


def test_hint_401_mentions_api_key() -> None:
    h = hint_for_http_status(401)
    assert "API key" in h or "key" in h.lower()


def test_hint_404_mentions_url() -> None:
    h = hint_for_http_status(404)
    assert "URL" in h or "found" in h.lower()


def test_hint_unknown_empty() -> None:
    assert hint_for_http_status(418) == ""
