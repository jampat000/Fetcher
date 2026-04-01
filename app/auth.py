"""Fetcher authentication: passlib hashes, session cookies, and bearer-token API auth."""

from __future__ import annotations

import ipaddress
import hmac
import logging
from urllib.parse import quote
from datetime import UTC

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordBearer
from itsdangerous import BadSignature, SignatureExpired, TimestampSigner
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import RedirectResponse, Response

from app.db import SessionLocal, get_or_create_settings, get_session
from app.models import AppSettings
from app.auth_runtime import (
    get_client_ip,
)
from app.security_utils import (
    encrypt_secret_for_storage,
    hash_password,
    verify_password,
)

SESSION_COOKIE_NAME = "fetcher_session"
SESSION_MAX_AGE_SEC = 604800
SIGNER_SALT = "fetcher-session"
CSRF_SIGNER_SALT = "fetcher-csrf"
CSRF_MAX_AGE_SEC = 3600

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/token", auto_error=False)


class FetcherAuthRequired(Exception):
    """FastAPI ignores ``Response`` objects returned from ``dependencies=[Depends(...)]`` — raise this instead."""

    __slots__ = ("response",)

    def __init__(self, response: Response) -> None:
        self.response = response


def _wants_json(request: Request) -> bool:
    accept = (request.headers.get("accept") or "").lower()
    return "application/json" in accept


def request_prefers_json(request: Request) -> bool:
    """True when Accept includes application/json (e.g. API clients)."""
    return _wants_json(request)


def is_ip_allowed(ip_str: str, allowlist_text: str) -> bool:
    """True if ``ip_str`` matches a single IP or falls in a CIDR line in ``allowlist_text``."""
    if not (allowlist_text or "").strip():
        return False
    try:
        client = ipaddress.ip_address((ip_str or "").strip())
    except ValueError:
        return False
    for raw_line in allowlist_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            addr = ipaddress.ip_address(line)
        except ValueError:
            try:
                net = ipaddress.ip_network(line, strict=False)
            except ValueError:
                continue
            else:
                if client in net:
                    return True
        else:
            if client == addr or (client.is_loopback and addr.is_loopback):
                return True
    return False


def normalize_auth_ip_allowlist_input(raw: str) -> str:
    """Strip whole value and each line; validate each non-empty non-comment line; rejoin with ``\\n``."""
    entire = (raw or "").strip()
    out: list[str] = []
    for raw_line in entire.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            ipaddress.ip_address(line)
        except ValueError:
            try:
                ipaddress.ip_network(line, strict=False)
            except ValueError as e:
                raise ValueError(f"invalid allowlist entry: {line!r}") from e
        out.append(line)
    return "\n".join(out)


def _signer_for_secret(secret: str) -> TimestampSigner:
    return TimestampSigner(secret, salt=SIGNER_SALT)


def verify_session_cookie(*, secret: str, cookie_value: str, expected_username: str) -> bool:
    if not secret.strip() or not cookie_value.strip():
        return False
    try:
        signer = _signer_for_secret(secret)
        username_bytes = signer.unsign(cookie_value.encode("utf-8"), max_age=SESSION_MAX_AGE_SEC)
        username = username_bytes.decode("utf-8")
    except (BadSignature, SignatureExpired, UnicodeDecodeError):
        return False
    return hmac.compare_digest(username, (expected_username or "admin").strip())


def build_session_cookie_value(*, secret: str, username: str) -> str:
    signer = _signer_for_secret(secret)
    return signer.sign(username.encode("utf-8")).decode("utf-8")


def generate_csrf_token(session_secret: str, username: str) -> str:
    """Sign ``{username}:csrf`` for HTML forms (1h validity at verify time)."""
    signer = TimestampSigner(session_secret, salt=CSRF_SIGNER_SALT)
    return signer.sign(f"{username}:csrf".encode("utf-8")).decode("utf-8")


def verify_csrf_token(token: str, session_secret: str, username: str) -> bool:
    if not (token or "").strip() or not (session_secret or "").strip():
        return False
    try:
        signer = TimestampSigner(session_secret, salt=CSRF_SIGNER_SALT)
        raw = signer.unsign(token.encode("utf-8"), max_age=CSRF_MAX_AGE_SEC)
        return raw.decode("utf-8") == f"{username}:csrf"
    except Exception:
        return False


def get_session_username(request: Request, secret: str) -> str:
    """Decode username from session cookie; empty string if missing or invalid."""
    raw = (request.cookies.get(SESSION_COOKIE_NAME) or "").strip()
    if not raw or not (secret or "").strip():
        return ""
    try:
        signer = _signer_for_secret(secret)
        username_bytes = signer.unsign(raw.encode("utf-8"), max_age=SESSION_MAX_AGE_SEC)
        return username_bytes.decode("utf-8")
    except Exception:
        return ""


def effective_username_for_csrf(request: Request, settings: AppSettings) -> str:
    """Username bound to CSRF tokens: allowlisted IP uses configured account; else cookie session."""
    secret = (settings.auth_session_secret or "").strip()
    allow_txt = (settings.auth_ip_allowlist or "").strip()
    if allow_txt and is_ip_allowed(get_client_ip(request), settings.auth_ip_allowlist):
        return (settings.auth_username or "admin").strip() or "admin"
    return get_session_username(request, secret)


async def get_csrf_token_for_template(request: Request, session: AsyncSession) -> str:
    """Token for layout meta and hidden fields (empty if not signable)."""
    settings = await get_or_create_settings(session)
    secret = (settings.auth_session_secret or "").strip()
    if not secret:
        return ""
    u = effective_username_for_csrf(request, settings)
    if not u:
        return ""
    return generate_csrf_token(secret, u)


async def require_csrf(request: Request, session: AsyncSession = Depends(get_session)) -> None:
    """Validate form ``csrf_token`` for POST; no-op for GET. Skips ``POST /setup/0`` only."""
    if request.method != "POST":
        return
    step_raw = request.path_params.get("step")
    if step_raw is not None:
        try:
            if int(step_raw) == 0:
                return
        except (TypeError, ValueError):
            pass

    settings = await get_or_create_settings(session)
    secret = (settings.auth_session_secret or "").strip()
    username = effective_username_for_csrf(request, settings)
    if not username:
        raise HTTPException(
            status_code=403,
            detail="Invalid or expired CSRF token. Please reload the page and try again.",
        )

    form = await request.form()
    raw_tok = form.get("csrf_token")
    if isinstance(raw_tok, list):
        raw_tok = raw_tok[0] if raw_tok else ""
    token = (raw_tok or "").strip()
    if not verify_csrf_token(token, secret, username):
        header_tok = (request.headers.get("X-CSRF-Token") or "").strip()
        if not (header_tok and verify_csrf_token(header_tok, secret, username)):
            raise HTTPException(
                status_code=403,
                detail="Invalid or expired CSRF token. Please reload the page and try again.",
            )


def attach_session_cookie(
    response: Response, *, secret: str, username: str, request: Request | None = None
) -> None:
    value = build_session_cookie_value(secret=secret, username=username)
    is_secure = False
    if request is not None:
        is_secure = (
            request.url.scheme == "https"
            or (request.headers.get("x-forwarded-proto") or "").strip().lower() == "https"
        )
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=value,
        max_age=SESSION_MAX_AGE_SEC,
        httponly=True,
        samesite="lax",
        secure=is_secure,
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")


def sanitize_next_param(raw: str | None, *, max_len: int = 2048) -> str:
    """Allow only same-origin relative paths (with optional query) for post-login redirect."""
    s = (raw or "").strip()
    if not s:
        return "/"
    if len(s) > max_len:
        s = s[:max_len]
    if not s.startswith("/") or s.startswith("//"):
        return "/"
    if any(c in s for c in "\r\n\x00"):
        return "/"
    path_only = s.split("?", 1)[0]
    if "://" in path_only:
        return "/"
    return s


def login_url_with_next(*, request: Request) -> str:
    """``/login?next=…`` pointing at the current path+query (safe), for return after sign-in."""
    path = request.url.path
    query = request.url.query
    rel = path + (f"?{query}" if query else "")
    safe_rel = sanitize_next_param(rel)
    return f"/login?next={quote(safe_rel, safe='/?:=&')}"


async def bootstrap_auth_on_startup() -> None:
    """FETCHER_RESET_AUTH=1 clears credentials; ensure session signing secret exists."""
    import os
    import secrets

    from app.time_util import utc_now_naive

    log = logging.getLogger(__name__)
    async with SessionLocal() as session:
        settings = await get_or_create_settings(session)
        if os.environ.get("FETCHER_RESET_AUTH", "").strip() == "1":
            settings.auth_username = "admin"
            settings.auth_password_hash = ""
            settings.auth_refresh_token_hash = ""
            settings.auth_refresh_expires_at = None
            settings.updated_at = utc_now_naive()
            await session.commit()
            log.warning(
                "FETCHER_RESET_AUTH=1 — credentials cleared for this process. "
                "Visit /setup/0 to set a new password, then remove FETCHER_RESET_AUTH from the "
                "environment so it does not run on every start."
            )
            log.error(
                "SECURITY/RECOVERY: FETCHER_RESET_AUTH is set to 1. This is not a normal running state."
            )
        if not (settings.auth_session_secret or "").strip():
            settings.auth_session_secret = secrets.token_hex(32)
            settings.updated_at = utc_now_naive()
            await session.commit()

        for attr in ("sonarr_api_key", "radarr_api_key", "emby_api_key"):
            current = getattr(settings, attr, "") or ""
            encrypted = encrypt_secret_for_storage(current)
            if encrypted != current:
                setattr(settings, attr, encrypted)
                settings.updated_at = utc_now_naive()
        await session.commit()

        settings = await get_or_create_settings(session)
        has_pw = bool((settings.auth_password_hash or "").strip())
        log.info(
            "Auth startup diagnostic: password_hash_configured=%s next_ui=%s",
            has_pw,
            "login" if has_pw else "setup(/setup/0)",
        )

        if settings.auth_bypass_lan:
            log.warning(
                "auth_bypass_lan has been migrated to an explicit IP allowlist. The following ranges were added: "
                "10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16. Review your Access Control settings to confirm."
            )
            settings.auth_bypass_lan = False
            settings.updated_at = utc_now_naive()
            await session.commit()


async def require_api_auth(
    request: Request,
    bearer_token: str | None = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_session),
) -> None:
    """Allow either Bearer JWT (preferred) or the existing browser session cookie."""
    if bearer_token:
        try:
            jwt_secret = (request.app.state.jwt_secret or "").strip()
            payload = jwt.decode(
                bearer_token,
                jwt_secret,
                algorithms=["HS256"],
            )
        except Exception:
            raise HTTPException(
                status_code=401,
                detail={
                    "message": (
                        "Invalid or expired access token. Sign in with POST /api/auth/token or renew with "
                        "POST /api/auth/refresh, then send the returned access_token as Authorization: Bearer …."
                    )
                },
            ) from None
        if payload.get("typ") != "access" or not (payload.get("sub") or "").strip():
            raise HTTPException(
                status_code=401,
                detail={
                    "message": (
                        "This endpoint expects a Bearer access token (not a refresh token). "
                        "Use POST /api/auth/token after signing in, or POST /api/auth/refresh."
                    )
                },
            )
        return
    await require_auth(request, session)


async def require_auth(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> None:
    settings = await get_or_create_settings(session)

    # New installs and upgrades after adding auth: no password yet. Send everyone here
    # first (IP allowlist does not apply — avoids an open UI without choosing a password).
    if not (settings.auth_password_hash or "").strip():
        if _wants_json(request):
            raise HTTPException(
                status_code=401,
                detail={
                    "message": "Set a password in the setup wizard first.",
                    "setup_path": "/setup/0",
                },
            )
        raise FetcherAuthRequired(RedirectResponse(url="/setup/0", status_code=303))

    allow_txt = (settings.auth_ip_allowlist or "").strip()
    if allow_txt and is_ip_allowed(get_client_ip(request), settings.auth_ip_allowlist):
        return

    secret = (settings.auth_session_secret or "").strip()
    if not secret:
        if _wants_json(request):
            raise HTTPException(
                status_code=401,
                detail={
                    "message": (
                        "Not signed in. Use POST /api/auth/token with your credentials, then send the "
                        "access_token as Authorization: Bearer …, or open the web UI to establish a session."
                    )
                },
            )
        raise FetcherAuthRequired(RedirectResponse(url=login_url_with_next(request=request), status_code=303))

    raw = request.cookies.get(SESSION_COOKIE_NAME) or ""
    expected = (settings.auth_username or "admin").strip() or "admin"
    if not verify_session_cookie(secret=secret, cookie_value=raw, expected_username=expected):
        if _wants_json(request):
            raise HTTPException(
                status_code=401,
                detail={
                    "message": (
                        "Session missing or expired. Sign in via POST /api/auth/token or the /login page, "
                        "then retry with a valid session cookie or Bearer access token."
                    )
                },
            )
        raise FetcherAuthRequired(RedirectResponse(url=login_url_with_next(request=request), status_code=303))
