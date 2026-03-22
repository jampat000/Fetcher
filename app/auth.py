"""Fetcher web UI authentication: bcrypt passwords, TimestampSigner session cookie, optional IP allowlist."""

from __future__ import annotations

import ipaddress
import logging
import time
import bcrypt
from urllib.parse import quote
from fastapi import Depends, HTTPException, Request
from itsdangerous import BadSignature, SignatureExpired, TimestampSigner
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import RedirectResponse, Response

from app.db import SessionLocal, _get_or_create_settings, get_session
from app.models import AppSettings

SESSION_COOKIE_NAME = "fetcher_session"
SESSION_MAX_AGE_SEC = 604800
SIGNER_SALT = "fetcher-session"
CSRF_SIGNER_SALT = "fetcher-csrf"
CSRF_MAX_AGE_SEC = 3600

LOGIN_WINDOW_SEC = 600
LOGIN_MAX_FAILS = 5
_login_attempts: dict[str, list[float]] = {}

INVALID_LOGIN_MESSAGE = "Invalid username or password"
TOO_MANY_ATTEMPTS_MESSAGE = "Too many attempts, try again later."


class FetcherAuthRequired(Exception):
    """FastAPI ignores ``Response`` objects returned from ``dependencies=[Depends(...)]`` — raise this instead."""

    __slots__ = ("response",)

    def __init__(self, response: Response) -> None:
        self.response = response


def get_client_ip(request: Request) -> str:
    """Direct client host, or first X-Forwarded-For hop when the peer is private/loopback."""
    host = (request.client.host if request.client else "") or ""
    host = host.strip()
    direct: ipaddress.IPv4Address | ipaddress.IPv6Address | None = None
    try:
        direct = ipaddress.ip_address(host)
    except ValueError:
        direct = None

    if direct is not None and (direct.is_private or direct.is_loopback):
        xff = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
        if xff:
            return xff
    return host or "127.0.0.1"


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
    return username == (expected_username or "admin").strip()


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
    settings = await _get_or_create_settings(session)
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

    settings = await _get_or_create_settings(session)
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
        raise HTTPException(
            status_code=403,
            detail="Invalid or expired CSRF token. Please reload the page and try again.",
        )


def attach_session_cookie(response: Response, *, secret: str, username: str) -> None:
    value = build_session_cookie_value(secret=secret, username=username)
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=value,
        max_age=SESSION_MAX_AGE_SEC,
        httponly=True,
        samesite="lax",
        secure=False,
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")


def verify_password(*, password: str, stored_hash: str) -> bool:
    if not stored_hash.strip():
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
    except ValueError:
        return False


def hash_password(password: str) -> str:
    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12))
    return hashed.decode("utf-8")


def _cleanup_login_attempts(ip: str) -> list[float]:
    now = time.time()
    cutoff = now - LOGIN_WINDOW_SEC
    lst = [t for t in _login_attempts.get(ip, []) if t > cutoff]
    _login_attempts[ip] = lst
    return lst


def login_rate_limited(ip: str) -> bool:
    return len(_cleanup_login_attempts(ip)) >= LOGIN_MAX_FAILS


def record_login_failure(ip: str) -> None:
    now = time.time()
    lst = _cleanup_login_attempts(ip)
    lst.append(now)
    _login_attempts[ip] = lst


def clear_login_failures(ip: str) -> None:
    _login_attempts.pop(ip, None)


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
        settings = await _get_or_create_settings(session)
        if os.environ.get("FETCHER_RESET_AUTH", "").strip() == "1":
            settings.auth_username = "admin"
            settings.auth_password_hash = ""
            settings.updated_at = utc_now_naive()
            await session.commit()
            log.warning(
                "Auth credentials reset via FETCHER_RESET_AUTH. Visit /setup/0 to set a new password."
            )
        if not (settings.auth_session_secret or "").strip():
            settings.auth_session_secret = secrets.token_hex(32)
            settings.updated_at = utc_now_naive()
            await session.commit()

        settings = await _get_or_create_settings(session)
        if settings.auth_bypass_lan:
            log.warning(
                "auth_bypass_lan has been migrated to an explicit IP allowlist. The following ranges were added: "
                "10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16. Review your Access Control settings to confirm."
            )
            settings.auth_bypass_lan = False
            settings.updated_at = utc_now_naive()
            await session.commit()


async def require_auth(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> None:
    settings = await _get_or_create_settings(session)

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
            raise HTTPException(status_code=401, detail={"message": "Unauthorized"})
        raise FetcherAuthRequired(RedirectResponse(url=login_url_with_next(request=request), status_code=303))

    raw = request.cookies.get(SESSION_COOKIE_NAME) or ""
    expected = (settings.auth_username or "admin").strip() or "admin"
    if not verify_session_cookie(secret=secret, cookie_value=raw, expected_username=expected):
        if _wants_json(request):
            raise HTTPException(status_code=401, detail={"message": "Unauthorized"})
        raise FetcherAuthRequired(RedirectResponse(url=login_url_with_next(request=request), status_code=303))
