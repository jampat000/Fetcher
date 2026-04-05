"""Shared JWT secrets for tests (no env or DB side effects).

PyJWT emits InsecureKeyLengthWarning when HS256 secrets are shorter than 32 bytes (RFC 7518).
"""

from __future__ import annotations

# PyJWT warns when HS256 secrets are shorter than 32 bytes (RFC 7518). Keep test defaults compliant.
FETCHER_JWT_SECRET_TEST: str = "test-jwt-secret-for-pytest-only-ok"

# For refresh-token validation mismatch tests: two distinct secrets, each 32 bytes.
FETCHER_JWT_SECRET_TEST_MISMATCH_ENV: str = "jwt-refresh-mismatch-env-0123456"
FETCHER_JWT_SECRET_TEST_MISMATCH_APP: str = "jwt-refresh-mismatch-app-0123456"
