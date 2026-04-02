"""Single integer stored on ``app_settings.schema_version`` — migrations bump it; startup requires >= :data:`CURRENT_SCHEMA_VERSION`."""

from __future__ import annotations

# Bump when the persisted schema contract changes (with a matching migration).
CURRENT_SCHEMA_VERSION: int = 37
