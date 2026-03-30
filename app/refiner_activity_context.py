"""JSON snapshot persisted on ``RefinerActivity`` for operator-facing activity rows."""

from __future__ import annotations

import json
from typing import Any


def dumps_activity_context(payload: dict[str, Any]) -> str:
    body = {"v": 1, **payload}
    return json.dumps(body, separators=(",", ":"), ensure_ascii=True)[:120_000]


def parse_activity_context(raw: str | None) -> dict[str, Any]:
    t = (raw or "").strip()
    if not t:
        return {}
    try:
        d = json.loads(t)
        return d if isinstance(d, dict) else {}
    except json.JSONDecodeError:
        return {}
