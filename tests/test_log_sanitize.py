from app.log_sanitize import redact_sensitive_text, redact_url_for_logging


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


def test_redact_sensitive_text_embedded_url() -> None:
    s = 'GET http://localhost:8989/api/v3/series?api_key=SECRET99 ok'
    out = redact_sensitive_text(s)
    assert "SECRET99" not in out
    assert "[REDACTED]" in out or "***" in out


def test_redact_sensitive_text_json_api_key() -> None:
    s = '{"api_key":"my-secret","ok":true}'
    out = redact_sensitive_text(s)
    assert "my-secret" not in out
    assert '"[REDACTED]"' in out


def test_redact_sensitive_text_sonarr_key_kv() -> None:
    s = "sonarr_key=abc123def"
    out = redact_sensitive_text(s)
    assert "abc123def" not in out
    assert "sonarr_key=[REDACTED]" in out


def test_redact_sensitive_text_bearer() -> None:
    s = "Authorization: Bearer abc.def.ghi"
    out = redact_sensitive_text(s)
    assert "abc.def" not in out
    # Line may become Bearer [REDACTED] then Authorization: [REDACTED]
    assert "[REDACTED]" in out
