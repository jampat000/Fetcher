"""Auth orchestration layer (security-sensitive).

Keep route behavior stable; any auth-flow change requires regression tests.
JWT/hash primitives remain centralized in ``app.security_utils``.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from datetime import UTC, datetime

import jwt
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_runtime import (
    INVALID_LOGIN_MESSAGE,
    TOO_MANY_ATTEMPTS_MESSAGE,
    clear_login_failures,
    get_client_ip,
    login_rate_limited,
    record_login_failure,
)
from app.db import _get_or_create_settings
from app.models import AppSettings
from app.security_utils import (
    hash_password,
    hash_refresh_token,
    issue_access_token,
    issue_refresh_token,
    needs_password_rehash,
    verify_password,
)


@dataclass(frozen=True)
class AuthResult:
    ok: bool
    status_code: int
    message: str = ""


@dataclass(frozen=True)
class LoginFlowResult:
    ok: bool
    status_code: int
    message: str = ""
    cookie_username: str = ""
    cookie_secret: str = ""


class AuthService:
    async def get_settings(self, session: AsyncSession) -> AppSettings:
        return await _get_or_create_settings(session)

    @staticmethod
    def _upgrade_password_hash_if_needed(*, password: str, settings: AppSettings) -> None:
        current = (settings.auth_password_hash or "").strip()
        if current and needs_password_rehash(current):
            settings.auth_password_hash = hash_password(password)

    async def login(
        self,
        *,
        session: AsyncSession,
        request: Request,
        username: str,
        password: str,
    ) -> LoginFlowResult:
        settings = await _get_or_create_settings(session)
        ip = get_client_ip(request)
        if login_rate_limited(ip):
            return LoginFlowResult(ok=False, status_code=429, message=TOO_MANY_ATTEMPTS_MESSAGE)

        expected_user = (settings.auth_username or "admin").strip() or "admin"
        username_ok = hmac.compare_digest((username or "").strip(), expected_user)
        password_ok = verify_password(password=password or "", stored_hash=(settings.auth_password_hash or ""))
        if not (username_ok and password_ok):
            record_login_failure(ip)
            return LoginFlowResult(ok=False, status_code=401, message=INVALID_LOGIN_MESSAGE)
        self._upgrade_password_hash_if_needed(password=password or "", settings=settings)
        clear_login_failures(ip)
        await session.commit()
        return LoginFlowResult(
            ok=True,
            status_code=200,
            message="",
            cookie_username=expected_user,
            cookie_secret=(settings.auth_session_secret or "").strip(),
        )

    async def issue_api_token(
        self,
        *,
        session: AsyncSession,
        request: Request,
        username: str,
        password: str,
        jwt_secret: str,
    ) -> tuple[AuthResult, dict[str, str | int] | None]:
        settings = await _get_or_create_settings(session)
        login_result = await self.login(
            session=session,
            request=request,
            username=username,
            password=password,
        )
        if not login_result.ok:
            return AuthResult(ok=False, status_code=login_result.status_code, message=login_result.message), None
        expected_user = (settings.auth_username or "admin").strip() or "admin"
        session_secret = (settings.auth_session_secret or "").strip()
        access_token, access_ttl = issue_access_token(expected_user, jwt_secret=jwt_secret)
        refresh_token, refresh_expires = issue_refresh_token(expected_user, jwt_secret=jwt_secret)
        settings.auth_refresh_token_hash = hash_refresh_token(refresh_token, session_secret)
        settings.auth_refresh_expires_at = refresh_expires.replace(tzinfo=None)
        await session.commit()
        payload = {
            "token_type": "bearer",
            "access_token": access_token,
            "expires_in": access_ttl,
            "refresh_token": refresh_token,
            "refresh_expires_at": refresh_expires.isoformat(),
        }
        return AuthResult(ok=True, status_code=200, message=""), payload

    async def refresh_token(
        self,
        *,
        session: AsyncSession,
        refresh_token: str,
        jwt_secret: str,
    ) -> tuple[AuthResult, dict[str, str | int] | None]:
        settings = await _get_or_create_settings(session)
        try:
            payload = jwt.decode(refresh_token, jwt_secret, algorithms=["HS256"])
        except Exception:
            return AuthResult(ok=False, status_code=401, message="Invalid refresh token"), None
        if payload.get("typ") != "refresh":
            return AuthResult(ok=False, status_code=401, message="Invalid refresh token"), None
        username = (payload.get("sub") or "").strip()
        expected_user = (settings.auth_username or "admin").strip() or "admin"
        if not hmac.compare_digest(username, expected_user):
            return AuthResult(ok=False, status_code=401, message="Invalid refresh token"), None
        now = datetime.now(UTC)
        exp = datetime.fromtimestamp(int(payload.get("exp") or 0), tz=UTC)
        if exp <= now:
            return AuthResult(ok=False, status_code=401, message="Refresh token expired"), None
        expected_hash = hash_refresh_token(refresh_token, (settings.auth_session_secret or "").strip())
        if not hmac.compare_digest(expected_hash, (settings.auth_refresh_token_hash or "").strip()):
            return AuthResult(ok=False, status_code=401, message="Invalid refresh token"), None
        db_exp = settings.auth_refresh_expires_at
        if db_exp is None:
            return AuthResult(ok=False, status_code=401, message="Refresh token expired"), None
        db_exp_utc = db_exp.replace(tzinfo=UTC) if db_exp.tzinfo is None else db_exp.astimezone(UTC)
        if db_exp_utc <= now:
            return AuthResult(ok=False, status_code=401, message="Refresh token expired"), None
        session_secret = (settings.auth_session_secret or "").strip()
        access_token, access_ttl = issue_access_token(username, jwt_secret=jwt_secret)
        new_refresh_token, refresh_expires = issue_refresh_token(username, jwt_secret=jwt_secret)
        settings.auth_refresh_token_hash = hash_refresh_token(new_refresh_token, session_secret)
        settings.auth_refresh_expires_at = refresh_expires.replace(tzinfo=None)
        await session.commit()
        token_payload = {
            "token_type": "bearer",
            "access_token": access_token,
            "expires_in": access_ttl,
            "refresh_token": new_refresh_token,
            "refresh_expires_at": refresh_expires.isoformat(),
        }
        return AuthResult(ok=True, status_code=200, message=""), token_payload
