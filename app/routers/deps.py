"""Common FastAPI dependencies for HTML + JSON routes."""

from __future__ import annotations

from fastapi import Depends

from app.auth import require_auth, require_csrf

AUTH_DEPS = [Depends(require_auth)]
AUTH_FORM_DEPS = [Depends(require_auth), Depends(require_csrf)]
