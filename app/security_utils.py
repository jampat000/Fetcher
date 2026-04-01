"""Security primitives (security-sensitive).

Centralize JWT/password/hash/encryption helpers here; behavior changes require regression tests.
"""

from __future__ import annotations

import hmac
import logging
import os
import secrets
import sys
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
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

    **Packaged (frozen) builds** also persist a stable secret under the canonical data directory; see
    :func:`resolve_fetcher_jwt_secret_at_startup`, which may set this env var after reading that file.

    **Required at process start:** unfrozen/dev processes must have this set in the environment.
    Rotating the secret invalidates outstanding refresh tokens.
    """
    return (os.environ.get("FETCHER_JWT_SECRET") or "").strip()


_MACHINE_JWT_SECRET_FILENAME = "machine-jwt-secret"
_MIN_PERSISTED_JWT_SECRET_LEN = 32


def persistent_jwt_secret_file_path() -> Path:
    """Path to the stable JWT secret file for packaged installs (next to ``fetcher.db``)."""
    from app.database_resolution import default_data_dir

    return default_data_dir() / _MACHINE_JWT_SECRET_FILENAME


def _load_first_line_secret(path: Path) -> str:
    raw = path.read_text(encoding="utf-8")
    for ln in raw.splitlines():
        s = ln.split("#", 1)[0].strip()
        if s:
            return s
    return ""


def _atomic_write_utf8(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8", newline="\n")
        tmp.replace(path)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def resolve_fetcher_jwt_secret_at_startup(*, logger: logging.Logger) -> str:
    """Return JWT signing secret; packaged builds load or create a persisted file when env is unset.

    Precedence:

    1. Non-empty ``FETCHER_JWT_SECRET`` in the process environment (operator override).
    2. If ``sys.frozen``: read ``machine-jwt-secret`` under :func:`persistent_jwt_secret_file_path`
       parent (same layout as SQLite data). If missing, create with :func:`secrets.token_hex` (32 bytes)
       and write atomically. The value is copied into ``os.environ["FETCHER_JWT_SECRET"]`` so the rest
       of the process sees a single source.

    Unfrozen dev/test must set ``FETCHER_JWT_SECRET`` explicitly.
    """
    existing = get_jwt_secret_from_env()
    if existing:
        logger.info(
            "JWT configuration: using FETCHER_JWT_SECRET from the process environment (operator override)."
        )
        return existing

    if not getattr(sys, "frozen", False):
        raise RuntimeError(
            "Missing required JWT configuration: FETCHER_JWT_SECRET is not set or is empty. "
            "Set this environment variable to a stable, high-entropy secret before starting Fetcher "
            "(for example scripts/dev-start.ps1 sets a dev-only default). "
            "It signs API access and refresh tokens; changing it invalidates existing refresh tokens."
        )

    path = persistent_jwt_secret_file_path()
    if path.is_file():
        try:
            secret = _load_first_line_secret(path)
        except OSError as e:
            raise RuntimeError(
                f"Fetcher could not read persisted JWT secret file ({path}): {e}. "
                "Fix file permissions or set FETCHER_JWT_SECRET in the environment to override."
            ) from e
        if len(secret) < _MIN_PERSISTED_JWT_SECRET_LEN:
            raise RuntimeError(
                f"Persisted JWT secret in {path} is too short or empty "
                f"(need at least {_MIN_PERSISTED_JWT_SECRET_LEN} characters). "
                "Delete the file to regenerate on next start, or set FETCHER_JWT_SECRET."
            )
        logger.info(
            "JWT configuration: loaded stable secret from %s (set FETCHER_JWT_SECRET to override).",
            path,
        )
        os.environ["FETCHER_JWT_SECRET"] = secret
        return secret

    secret = secrets.token_hex(32)
    try:
        _atomic_write_utf8(path, secret + "\n")
    except OSError as e:
        raise RuntimeError(
            f"Fetcher could not create persisted JWT secret file ({path}): {e}. "
            "Ensure the service account can write under the Fetcher data directory "
            "(default %ProgramData%\\Fetcher), or set FETCHER_JWT_SECRET."
        ) from e
    try:
        path.chmod(0o600)
    except OSError:
        pass
    logger.info(
        "JWT configuration: created persisted secret file at %s (first packaged start). "
        "This file is kept across upgrades; set FETCHER_JWT_SECRET to use an explicit secret instead.",
        path,
    )
    os.environ["FETCHER_JWT_SECRET"] = secret
    return secret


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
