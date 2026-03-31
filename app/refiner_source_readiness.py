"""Authority-first Refiner source readiness: *arr queue before file heuristics.

Order per gate:
1. Upstream: Sonarr/Radarr download queue — block when a matching row is still actively downloading.
2. File-level: exists, regular file, non-empty, stable size/mtime (stricter when upstream is unavailable).

When *arr is configured but queue fetch fails, we do not assume readiness — stricter file gate; still
uncertain after that → skip with ``skipped_readiness``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.arr_client import ArrClient, ArrConfig
from app.models import AppSettings
from app.radarr_failed_import_cleanup import _paginate_records
from app.resolvers.api_keys import resolve_radarr_api_key, resolve_sonarr_api_key

logger = logging.getLogger(__name__)

_REFINER_QUEUE_PAGE_SIZE = 200
_REFINER_QUEUE_HTTP_TIMEOUT_S = 12.0

# QueueStatus (Servarr): block while the client is still fetching or not yet settled.
_BLOCKING_QUEUE_STATUSES = frozenset(
    {
        "unknown",
        "queued",
        "paused",
        "downloading",
        "warning",
        "delay",
        "downloadclientunavailable",
        "fallback",
    }
)


@dataclass(frozen=True)
class RefinerQueueSnapshot:
    """Result of fetching *arr queue pages (may be empty on success)."""

    radarr_configured: bool
    sonarr_configured: bool
    radarr_fetch_succeeded: bool
    sonarr_fetch_succeeded: bool
    radarr_records: tuple[dict[str, Any], ...]
    sonarr_records: tuple[dict[str, Any], ...]

    @property
    def authority_configured(self) -> bool:
        return self.radarr_configured or self.sonarr_configured

    @property
    def authority_useful(self) -> bool:
        """At least one configured client returned HTTP success (records may be empty)."""
        ok = False
        if self.radarr_configured:
            ok = ok or self.radarr_fetch_succeeded
        if self.sonarr_configured:
            ok = ok or self.sonarr_fetch_succeeded
        return ok


@dataclass(frozen=True)
class RefinerReadinessDecision:
    proceed: bool
    reason_code: str
    operator_message: str
    strict_file_fallback: bool


def _norm_status(raw: object) -> str:
    s = str(raw or "").strip().casefold().replace(" ", "")
    return s


def _size_left_bytes(rec: dict[str, Any]) -> int:
    raw = rec.get("sizeleft")
    if isinstance(raw, (int, float)):
        return max(0, int(raw))
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return 0


def queue_record_upstream_active(rec: dict[str, Any]) -> bool:
    """True when *arr still treats this queue row as an in-flight / unsettled download."""
    if _size_left_bytes(rec) > 0:
        return True
    st = _norm_status(rec.get("status"))
    return st in _BLOCKING_QUEUE_STATUSES


def iter_queue_path_strings(rec: dict[str, Any]) -> list[str]:
    """Collect filesystem paths from a queue record (shape varies by *arr version)."""
    paths: list[str] = []
    for key in ("outputPath", "sourcePath", "path"):
        v = rec.get(key)
        if isinstance(v, str) and v.strip():
            paths.append(v.strip())
    movie = rec.get("movie")
    if isinstance(movie, dict):
        mp = movie.get("path")
        if isinstance(mp, str) and mp.strip():
            paths.append(mp.strip())
    movie_file = rec.get("movieFile")
    if isinstance(movie_file, dict):
        for key in ("path", "relativePath"):
            v = movie_file.get(key)
            if isinstance(v, str) and v.strip():
                paths.append(v.strip())
    episode = rec.get("episode")
    if isinstance(episode, dict):
        ef = episode.get("episodeFile")
        if isinstance(ef, dict):
            for key in ("path", "relativePath"):
                v = ef.get(key)
                if isinstance(v, str) and v.strip():
                    paths.append(v.strip())
    return paths


def _resolved_key(p: Path) -> str:
    try:
        return str(p.resolve()).casefold()
    except OSError:
        return str(p).casefold()


def path_matches_queue_record(file_key: str, rec: dict[str, Any]) -> bool:
    for s in iter_queue_path_strings(rec):
        try:
            q_key = _resolved_key(Path(s))
        except OSError:
            q_key = str(s).casefold()
        if file_key == q_key:
            return True
    return False


def upstream_blocks_path(path: Path, snap: RefinerQueueSnapshot) -> tuple[bool, str, str]:
    """If upstream authority says this path is an active download, return (True, reason_code, message)."""
    if not snap.authority_useful:
        return False, "", ""
    file_key = _resolved_key(path)
    for rec in snap.radarr_records:
        if not isinstance(rec, dict):
            continue
        if not queue_record_upstream_active(rec):
            continue
        if path_matches_queue_record(file_key, rec):
            return (
                True,
                "radarr_queue_active_download",
                "Radarr still reports this path in the active download queue — waiting until the download finishes.",
            )
    for rec in snap.sonarr_records:
        if not isinstance(rec, dict):
            continue
        if not queue_record_upstream_active(rec):
            continue
        if path_matches_queue_record(file_key, rec):
            return (
                True,
                "sonarr_queue_active_download",
                "Sonarr still reports this path in the active download queue — waiting until the download finishes.",
            )
    return False, "", ""


def refiner_file_level_gate(path: Path, *, strict: bool) -> tuple[bool, str]:
    """Conservative local checks: non-empty file with stable size and mtime across short windows."""
    if not path.is_file():
        return False, "Source is missing or not a regular file."
    delays = (0.12, 0.12) if strict else (0.12,)
    samples: list[tuple[int, int]] = []
    try:
        st0 = path.stat()
    except OSError as e:
        return False, f"Could not read file metadata ({e})."
    samples.append((st0.st_size, int(getattr(st0, "st_mtime_ns", int(st0.st_mtime * 1e9)))))
    if st0.st_size <= 0:
        return False, "Source file is empty (still writing or incomplete)."
    for d in delays:
        time.sleep(d)
        try:
            stn = path.stat()
        except OSError as e:
            return False, f"File became unreadable during readiness check ({e})."
        samples.append((stn.st_size, int(getattr(stn, "st_mtime_ns", int(stn.st_mtime * 1e9)))))
        if stn.st_size <= 0:
            return False, "Source file is empty (still writing or incomplete)."
    sizes = [s[0] for s in samples]
    if len(set(sizes)) != 1:
        return False, "File size is still changing — not ready yet."
    mtimes = [s[1] for s in samples]
    if len(set(mtimes)) != 1:
        return False, "File is still being modified — not ready yet."
    return True, ""


async def fetch_refiner_queue_snapshot(settings: AppSettings) -> RefinerQueueSnapshot:
    """Best-effort parallel fetch of Radarr and Sonarr queue pages."""

    async def _rad() -> tuple[bool, bool, list[dict[str, Any]]]:
        url = (settings.radarr_url or "").strip()
        key = resolve_radarr_api_key(settings)
        if not (settings.radarr_enabled and url and key):
            return False, False, []
        client = ArrClient(ArrConfig(url, key), timeout_s=_REFINER_QUEUE_HTTP_TIMEOUT_S)
        try:
            recs = await _paginate_records(
                client.queue_page,
                page_size=_REFINER_QUEUE_PAGE_SIZE,
                label="refiner radarr queue",
                max_pages=50,
            )
            return True, True, recs
        except Exception:
            logger.info("Refiner: Radarr queue fetch failed — treating upstream authority as unavailable.", exc_info=True)
            return True, False, []

    async def _son() -> tuple[bool, bool, list[dict[str, Any]]]:
        url = (settings.sonarr_url or "").strip()
        key = resolve_sonarr_api_key(settings)
        if not (settings.sonarr_enabled and url and key):
            return False, False, []
        client = ArrClient(ArrConfig(url, key), timeout_s=_REFINER_QUEUE_HTTP_TIMEOUT_S)
        try:
            recs = await _paginate_records(
                client.queue_page,
                page_size=_REFINER_QUEUE_PAGE_SIZE,
                label="refiner sonarr queue",
                max_pages=50,
            )
            return True, True, recs
        except Exception:
            logger.info("Refiner: Sonarr queue fetch failed — treating upstream authority as unavailable.", exc_info=True)
            return True, False, []

    (r_cfg, r_ok, r_recs), (s_cfg, s_ok, s_recs) = await asyncio.gather(_rad(), _son())
    return RefinerQueueSnapshot(
        radarr_configured=r_cfg,
        sonarr_configured=s_cfg,
        radarr_fetch_succeeded=r_ok,
        sonarr_fetch_succeeded=s_ok,
        radarr_records=tuple(r_recs),
        sonarr_records=tuple(s_recs),
    )


async def decide_refiner_readiness(
    path: Path,
    settings: AppSettings,
    *,
    snapshot: RefinerQueueSnapshot,
    gate_tag: str,
) -> RefinerReadinessDecision:
    """Authority-first then file-level gate. ``gate_tag`` is for logs only."""
    strict = False
    if snapshot.authority_configured and not snapshot.authority_useful:
        strict = True
    blocked, rc, msg = upstream_blocks_path(path, snapshot)
    if blocked:
        logger.info("Refiner readiness [%s]: upstream block for %s (%s)", gate_tag, path.name, rc)
        return RefinerReadinessDecision(False, rc, msg, strict_file_fallback=strict)
    ok, why = refiner_file_level_gate(path, strict=strict)
    if not ok:
        logger.info("Refiner readiness [%s]: file gate failed for %s — %s", gate_tag, path.name, why)
        rc2 = "skipped_final_readiness_gate" if gate_tag == "final" else "skipped_readiness"
        return RefinerReadinessDecision(False, rc2, why, strict_file_fallback=strict)
    return RefinerReadinessDecision(True, "", "", strict_file_fallback=strict)


def ffprobe_failure_hint_is_read_analyze(hint: str) -> bool:
    t = (hint or "").casefold()
    return "could not read or analyze the file" in t

