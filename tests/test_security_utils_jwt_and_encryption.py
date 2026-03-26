"""Unit tests for JWT helpers and at-rest secret encryption (deterministic, no live services)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt as pyjwt
import pytest
from cryptography.fernet import Fernet

from app.security_utils import (
    decrypt_secret_from_storage,
    decode_token,
    encrypt_secret_for_storage,
    issue_access_token,
    issue_refresh_token,
)

_JWT_SECRET = "x" * 32


def test_issue_access_token_roundtrip_decode() -> None:
    tok, ttl = issue_access_token("admin", jwt_secret=_JWT_SECRET)
    assert ttl >= 60
    payload = decode_token(tok, "access", jwt_secret=_JWT_SECRET)
    assert payload["sub"] == "admin"
    assert payload["typ"] == "access"


def test_decode_refresh_as_access_raises_invalid_token_type() -> None:
    refresh_tok, _ = issue_refresh_token("admin", jwt_secret=_JWT_SECRET)
    with pytest.raises(pyjwt.InvalidTokenError, match="Invalid token type"):
        decode_token(refresh_tok, "access", jwt_secret=_JWT_SECRET)


def test_decode_expired_access_raises() -> None:
    now = datetime.now(UTC)
    payload = {
        "sub": "u",
        "typ": "access",
        "iat": int((now - timedelta(hours=2)).timestamp()),
        "exp": int((now - timedelta(hours=1)).timestamp()),
    }
    tok = pyjwt.encode(payload, _JWT_SECRET, algorithm="HS256")
    with pytest.raises(pyjwt.ExpiredSignatureError):
        decode_token(tok, "access", jwt_secret=_JWT_SECRET)


def test_encrypt_decrypt_roundtrip_with_fernet_key(monkeypatch: pytest.MonkeyPatch) -> None:
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("FETCHER_DATA_ENCRYPTION_KEY", key)
    plain = "my-api-key-value"
    stored = encrypt_secret_for_storage(plain)
    assert stored.startswith("enc:v1:")
    assert decrypt_secret_from_storage(stored) == plain


def test_encrypt_without_key_leaves_plaintext(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FETCHER_DATA_ENCRYPTION_KEY", raising=False)
    assert encrypt_secret_for_storage("plain-secret") == "plain-secret"


def test_decrypt_corrupt_ciphertext_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("FETCHER_DATA_ENCRYPTION_KEY", key)
    assert decrypt_secret_from_storage("enc:v1:!!!not-valid-fernet-payload!!!") == ""


def test_decrypt_enc_value_without_key_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FETCHER_DATA_ENCRYPTION_KEY", raising=False)
    assert decrypt_secret_from_storage("enc:v1:Zm9vYmFy") == ""


def test_encrypt_idempotent_when_already_prefixed(monkeypatch: pytest.MonkeyPatch) -> None:
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("FETCHER_DATA_ENCRYPTION_KEY", key)
    once = encrypt_secret_for_storage("abc")
    assert encrypt_secret_for_storage(once) == once
