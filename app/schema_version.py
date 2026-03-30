"""Single integer stored on ``app_settings.schema_version`` — must match this build exactly."""

from __future__ import annotations

# Bump when the persisted schema contract changes (with a matching migration).
CURRENT_SCHEMA_VERSION: int = 36
