"""Security primitives (security-sensitive).

Centralize JWT/password/hash/encryption helpers here; behavior changes require regression tests.
"""

from __future__ import annotations

import hmac
import os
import secrets
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import jwt
from cryptography.fernet import Fernet, InvalidToken
from passlib.context import CryptContext

_PASSWORD_CONTEXT = CryptContext(schemes=["argon2", "bcrypt"], deprecated="auto")

_ENC_PREFIX = "enc:v1:"


def hash_password(password: str) -> str:
    return _PASSWORD_CONTEXT.hash(password)


def verify_password(password: str, stored_hash: str) -> bool:
    if not (stored_hash or "").strip():
        return False
    try:
        return _PASSWORD_CONTEXT.verify(password, stored_hash)
    except Exception:
        return False


def needs_password_rehash(stored_hash: str) -> bool:
    if not (stored_hash or "").strip():
        return False
    try:
        return _PASSWORD_CONTEXT.needs_update(stored_hash)
    except Exception:
        return False


def get_jwt_secret_from_env() -> str:
    """Read ``FETCHER_JWT_SECRET`` for HS256 signing of access and refresh JWTs.

    **Required at process start:** the app lifespan in ``app.main`` refuses to boot if this is empty.
    Set it in the service environment, installer defaults, or shell to a stable, high-entropy secret
    (32+ random bytes encoded as hex or base64 is typical). Rotating it invalidates outstanding
    refresh tokens.
    """
    return (os.environ.get("FETCHER_JWT_SECRET") or "").strip()


def warn_if_data_encryption_key_missing(logger: logging.Logger) -> None:
    """Log once at startup when ``FETCHER_DATA_ENCRYPTION_KEY`` is unset (non-fatal)."""
    if _data_encryption_key():
        return
    logger.warning(
        "FETCHER_DATA_ENCRYPTION_KEY is not set: Sonarr, Radarr, and Emby API keys in the SQLite "
        "database are stored in plaintext on disk. To encrypt them at rest, set "
        "FETCHER_DATA_ENCRYPTION_KEY to a Fernet key (for example: "
        "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"). "
        "Use the same key on every run; losing or changing it prevents decrypting stored keys."
    )


def _jwt_access_minutes() -> int:
    try:
        return max(1, int((os.environ.get("FETCHER_JWT_ACCESS_MINUTES") or "15").strip()))
    except ValueError:
        return 15


def _jwt_refresh_days() -> int:
    try:
        return max(1, int((os.environ.get("FETCHER_JWT_REFRESH_DAYS") or "7").strip()))
    except ValueError:
        return 7


def issue_access_token(username: str, *, jwt_secret: str) -> tuple[str, int]:
    now = datetime.now(UTC)
    ttl = _jwt_access_minutes() * 60
    payload: dict[str, Any] = {
        "sub": username,
        "typ": "access",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=ttl)).timestamp()),
    }
    return jwt.encode(payload, jwt_secret, algorithm="HS256"), ttl


def issue_refresh_token(username: str, *, jwt_secret: str) -> tuple[str, datetime]:
    now = datetime.now(UTC)
    exp = now + timedelta(days=_jwt_refresh_days())
    payload: dict[str, Any] = {
        "sub": username,
        "typ": "refresh",
        "jti": secrets.token_urlsafe(24),
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, jwt_secret, algorithm="HS256"), exp


def decode_token(token: str, expected_type: str, *, jwt_secret: str) -> dict[str, Any]:
    payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
    if payload.get("typ") != expected_type:
        raise jwt.InvalidTokenError("Invalid token type")
    return payload


def hash_refresh_token(token: str, session_secret: str) -> str:
    return hmac.new(session_secret.encode("utf-8"), token.encode("utf-8"), sha256).hexdigest()


def _data_encryption_key() -> str:
    return (os.environ.get("FETCHER_DATA_ENCRYPTION_KEY") or "").strip()


def _fernet_or_none() -> Fernet | None:
    key = _data_encryption_key()
    if not key:
        return None
    try:
        return Fernet(key.encode("utf-8"))
    except Exception:
        return None


def encrypt_secret_for_storage(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith(_ENC_PREFIX):
        return raw
    f = _fernet_or_none()
    if f is None:
        return raw
    token = f.encrypt(raw.encode("utf-8")).decode("utf-8")
    return f"{_ENC_PREFIX}{token}"


def decrypt_secret_from_storage(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if not raw.startswith(_ENC_PREFIX):
        return raw
    f = _fernet_or_none()
    if f is None:
        return ""
    token = raw[len(_ENC_PREFIX) :]
    try:
        return f.decrypt(token.encode("utf-8")).decode("utf-8")
    except (InvalidToken, ValueError):
        return ""


def read_secret_env(*names: str) -> str:
    for name in names:
        val = (os.environ.get(name) or "").strip()
        if val:
            return val
    return ""
