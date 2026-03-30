# Authentication Architecture (Developer Note)

This is a short maintenance map for auth-related code in Fetcher.

## Ownership by module

- **Auth orchestration (route flows):** `app/auth_service.py`
  - High-level methods: `login(...)`, `issue_api_token(...)`, `refresh_token(...)`
  - Handles settings lookup, client IP/rate-limit checks, credential verification, and refresh rotation.
  - Accepts plain values (session, ip, credentials); avoids tight coupling to FastAPI Request objects where possible.
- **Route layer:** `app/routers/auth.py`
  - Reads request/form/json input, calls `AuthService`, returns `JSONResponse` / `RedirectResponse`.
- **JWT/hash/encryption primitives:** `app/security_utils.py`
  - Password hashing/verify (`passlib`)
  - JWT encode/decode
  - Refresh-token hash helper
  - At-rest secret encryption/decryption helpers
- **Runtime helpers/constants:** `app/auth_runtime.py`
  - Login attempt window/limits, invalid-login messages, client IP extraction.
- **Session + CSRF + auth guards (and compatibility surface):** `app/auth.py`
  - Cookie/session verification, CSRF token verification
  - `require_auth`, `require_api_auth`
  - May re-export selected helpers for backward compatibility (do not expand this surface)

## Required environment variables

- **`FETCHER_JWT_SECRET`** (required for dev / unfrozen)
  - Dedicated JWT signing key. **Packaged:** if unset, a stable **`machine-jwt-secret`** file next to `fetcher.db` is used or created; this env var overrides the file.
- **Optional auth/security env vars**
  - `FETCHER_JWT_ACCESS_MINUTES`
  - `FETCHER_JWT_REFRESH_DAYS`
  - `FETCHER_DATA_ENCRYPTION_KEY`
  - App API key envs (when used): `FETCHER_SONARR_API_KEY`, `FETCHER_RADARR_API_KEY`, `FETCHER_EMBY_API_KEY`

Do not commit secrets. Do not hardcode secret values.

## Refresh token rotation

- On successful token issuance, server stores a hash of the current refresh token.
- On refresh, server validates JWT + stored hash + expiry.
- On success, a **new** access token and **new** refresh token are issued, and stored refresh hash/expiry are replaced.
- Reusing an old refresh token after rotation must fail.

## Maintenance rule

Any auth/security behavior change must include regression tests.

Minimum expected test coverage for behavior changes:
- startup JWT-secret requirement
- invalid login message consistency
- refresh rotation / old-token rejection
- wrong-secret token validation failure
- password hash upgrade only after successful verification
