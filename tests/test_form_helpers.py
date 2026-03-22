"""Unit tests for shared form/url helpers."""

from app.form_helpers import _normalize_base_url


def test_normalize_base_url_rejects_autofill_username_tokens() -> None:
    assert _normalize_base_url("admin") == ""
    assert _normalize_base_url("  USER  ") == ""
    assert _normalize_base_url("password") == ""


def test_normalize_base_url_still_accepts_real_hosts() -> None:
    assert _normalize_base_url("http://admin.local:8989") != ""
    assert _normalize_base_url("localhost:8989") == "http://localhost:8989"
