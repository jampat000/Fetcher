from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address

# Shared limiter for route decorators and middleware.
limiter = Limiter(key_func=get_remote_address)
