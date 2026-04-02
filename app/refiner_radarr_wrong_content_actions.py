"""Radarr automation for Refiner wrong-content movie outcomes (queue fail, blocklist, re-search)."""

from __future__ import annotations

import json
import logging
from typing import Any

from app.arr_client import ArrClient, ArrConfig, trigger_radarr_missing_search
from app.models import AppSettings
from app.resolvers.api_keys import resolve_radarr_api_key

logger = logging.getLogger(__name__)

_LOG_MAX = 2500


def _clip_obj(d: dict[str, Any]) -> str:
    raw = json.dumps(d, ensure_ascii=True)
    return raw if len(raw) <= _LOG_MAX else raw[: _LOG_MAX - 24] + "…(truncated)"


async def execute_radarr_wrong_content_actions(
    settings: AppSettings,
    *,
    queue_id: int | None,
    movie_id: int,
    dry_run: bool,
) -> dict[str, Any]:
    """
    Fail/remove queue row with blocklist when queue_id known; always request MoviesSearch for movie_id
    when not dry-run. Matches failed-import cleanup: try blocklist=true first on delete.
    """
    result: dict[str, Any] = {
        "dry_run": bool(dry_run),
        "queue_delete_attempted": False,
        "queue_delete_ok": False,
        "queue_blocklist_requested": False,
        "movies_search_ok": False,
        "errors": [],
    }
    if dry_run:
        logger.info("Refiner wrong-content: dry-run — skipping Radarr fail/block/search.")
        return result

    url = (settings.radarr_url or "").strip()
    key = resolve_radarr_api_key(settings)
    if not (settings.radarr_enabled and url and key):
        result["errors"].append("radarr_not_configured")
        logger.warning(
            "REFINER_WRONG_CONTENT_SCORE: %s",
            _clip_obj({"radarr_automation": result, "note": "Radarr disabled or missing URL/key"}),
        )
        return result

    client = ArrClient(ArrConfig(url, key), timeout_s=60.0)
    if queue_id is not None and int(queue_id) > 0:
        result["queue_delete_attempted"] = True
        result["queue_blocklist_requested"] = True
        try:
            await client.delete_queue_item(queue_id=int(queue_id), blocklist=True, remove_from_client=False)
            result["queue_delete_ok"] = True
        except Exception as e1:
            result["errors"].append(f"queue_delete_blocklist:{e1!s}"[:300])
            try:
                await client.delete_queue_item(queue_id=int(queue_id), blocklist=False, remove_from_client=False)
                result["queue_delete_ok"] = True
                result["queue_blocklist_requested"] = False
            except Exception as e2:
                result["errors"].append(f"queue_delete_no_blocklist:{e2!s}"[:300])

    try:
        await trigger_radarr_missing_search(client, movie_ids=[int(movie_id)])
        result["movies_search_ok"] = True
    except Exception as e:
        result["errors"].append(f"movies_search:{e!s}"[:300])

    logger.warning(
        "REFINER_WRONG_CONTENT_SCORE: %s",
        _clip_obj({"radarr_automation": result, "movie_id": movie_id, "queue_id": queue_id}),
    )
    return result
