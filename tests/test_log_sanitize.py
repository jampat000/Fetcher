from app.log_sanitize import redact_url_for_logging


def test_redacts_api_key_query() -> None:
    u = "http://localhost:8096/Items?api_key=SECRET123&Limit=1"
    out = redact_url_for_logging(u)
    assert "SECRET123" not in out
    # urlencode may emit literal *** or %2A%2A%2A depending on Python version
    assert "api_key=" in out and ("***" in out or "%2A%2A%2A" in out)


def test_redacts_userinfo() -> None:
    u = "http://user:pass@host:8096/path"
    out = redact_url_for_logging(u)
    assert "user" not in out
    assert "pass" not in out
    assert "host:8096" in out
