from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import httpx

from app.arr_client import ArrClient, ArrConfig


ErrorKind = Literal["none", "http_status", "http_error"]


@dataclass(frozen=True)
class ArrHealthCheckResult:
    ok: bool
    error_kind: ErrorKind
    status_code: int | None = None
    error_message: str = ""
    error_name: str = ""


class ConnectionTestService:
    """Shared ARR health-check plumbing only; no caller-specific side effects or response shaping."""

    async def check_arr_health(self, *, url: str, api_key: str) -> ArrHealthCheckResult:
        # Keep this focused on transport/client lifecycle. Callers own redirects/snapshots/JSON contracts.
        client = ArrClient(ArrConfig(url, api_key))
        try:
            await client.health()
            return ArrHealthCheckResult(ok=True, error_kind="none")
        except httpx.HTTPStatusError as e:
            return ArrHealthCheckResult(
                ok=False,
                error_kind="http_status",
                status_code=e.response.status_code,
                error_message=str(e),
                error_name=type(e).__name__,
            )
        except httpx.HTTPError as e:
            return ArrHealthCheckResult(
                ok=False,
                error_kind="http_error",
                error_message=str(e),
                error_name=type(e).__name__,
            )
        finally:
            await client.aclose()

    @staticmethod
    def message_with_exception_prefix(result: ArrHealthCheckResult) -> str:
        # Shared primitive only; endpoint-specific prefixes/wrapping stay with callers.
        return f"{result.error_name}: {result.error_message}"

    @staticmethod
    def message_with_http_status_hint(
        result: ArrHealthCheckResult,
        *,
        auth_hint: str,
    ) -> str:
        # If this format changes, update connection-testing regression snapshots/messages.
        msg = f"HTTP {result.status_code}"
        if result.status_code in (401, 403):
            msg += f" — {auth_hint}"
        return msg
