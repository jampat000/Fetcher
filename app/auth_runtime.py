"""Auth runtime helpers/constants (security-sensitive).

Keep login-throttling/IP behavior stable; changes require regression tests.
"""

from __future__ import annotations

import ipaddress
import time
from fastapi import Request

LOGIN_WINDOW_SEC = 600
LOGIN_MAX_FAILS = 5

INVALID_LOGIN_MESSAGE = "Invalid username or password"
TOO_MANY_ATTEMPTS_MESSAGE = "Too many attempts, try again later."

_login_attempts: dict[str, list[float]] = {}


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
