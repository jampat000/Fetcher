import httpx

from app.http_status_hints import format_http_error_detail, hint_for_http_status


def test_hint_401_mentions_api_key() -> None:
    h = hint_for_http_status(401)
    assert "API key" in h or "key" in h.lower()


def test_hint_404_mentions_url() -> None:
    h = hint_for_http_status(404)
    assert "URL" in h or "found" in h.lower()


def test_hint_unknown_empty() -> None:
    assert hint_for_http_status(418) == ""


def test_format_http_error_detail_includes_status() -> None:
    req = httpx.Request("PUT", "http://sonarr.example/api/v3/series/editor")
    resp = httpx.Response(403, request=req, text="Forbidden")
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        detail = format_http_error_detail(e)
    assert "HTTP 403" in detail
    assert "Forbidden" in detail


def test_format_http_error_detail_redacts_json_body() -> None:
    req = httpx.Request("GET", "http://sonarr.example/api/v3/system/status")
    resp = httpx.Response(
        500,
        request=req,
        text='{"message":"bad","api_key":"LEAKED"}',
    )
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        detail = format_http_error_detail(e)
    assert "LEAKED" not in detail
    assert "[REDACTED]" in detail


def test_format_http_error_detail_generic_exception() -> None:
    assert "ValueError" in format_http_error_detail(ValueError("bad"))
