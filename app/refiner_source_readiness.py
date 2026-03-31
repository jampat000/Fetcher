"""Authority-first Refiner source readiness: *arr queue before file heuristics.

Order per gate:
1. Upstream: Sonarr/Radarr download queue — block when a matching row is still actively downloading.
2. File-level: exists, regular file, non-empty, stable size/mtime (stricter when upstream is unavailable).

When *arr is configured but queue fetch fails, we do not assume readiness — stricter file gate; still
uncertain after that → skip with ``skipped_readiness``.
"""

from __future__ import annotations

import asyncio
import json
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


def upstream_analyze_path(path: Path, snap: RefinerQueueSnapshot) -> tuple[bool, str, str, dict[str, Any]]:
    """Same blocking rules as historical ``upstream_blocks_path``, plus a diagnostic dict for live tracing."""
    file_key = _resolved_key(path)
    diag: dict[str, Any] = {
        "gate_context": "upstream_analyze",
        "candidate_resolved": file_key,
        "radarr_configured": snap.radarr_configured,
        "sonarr_configured": snap.sonarr_configured,
        "radarr_fetch_succeeded": snap.radarr_fetch_succeeded,
        "sonarr_fetch_succeeded": snap.sonarr_fetch_succeeded,
        "authority_useful": snap.authority_useful,
        "radarr_queue_rows": len(snap.radarr_records),
        "sonarr_queue_rows": len(snap.sonarr_records),
        "radarr_upstream_active_rows": 0,
        "sonarr_upstream_active_rows": 0,
        "radarr_active_path_samples": [],
        "sonarr_active_path_samples": [],
        "inactive_path_match_radarr": False,
        "inactive_path_match_sonarr": False,
        "upstream_blocked": False,
        "upstream_block_reason_code": "",
        "upstream_scan_skipped": False,
    }

    if not snap.authority_useful:
        diag["upstream_scan_skipped"] = True
        return False, "", "", diag

    def _collect_for_app(
        records: tuple[dict[str, Any], ...],
        *,
        app: str,
    ) -> tuple[bool, str, str] | None:
        active_samples: list[str] = []
        inactive_match = False
        active_count = 0
        for rec in records:
            if not isinstance(rec, dict):
                continue
            matched = path_matches_queue_record(file_key, rec)
            active = queue_record_upstream_active(rec)
            if active:
                active_count += 1
                for ps in iter_queue_path_strings(rec):
                    if len(active_samples) < 4:
                        active_samples.append(ps[:160] + ("…" if len(ps) > 160 else ""))
            elif matched:
                inactive_match = True
            if active and matched:
                if app == "radarr":
                    diag["radarr_upstream_active_rows"] = active_count
                    diag["radarr_active_path_samples"] = active_samples
                    diag["inactive_path_match_radarr"] = inactive_match
                else:
                    diag["sonarr_upstream_active_rows"] = active_count
                    diag["sonarr_active_path_samples"] = active_samples
                    diag["inactive_path_match_sonarr"] = inactive_match
                msg = (
                    "Radarr still reports this path in the active download queue — waiting until the download finishes."
                    if app == "radarr"
                    else "Sonarr still reports this path in the active download queue — waiting until the download finishes."
                )
                rc = "radarr_queue_active_download" if app == "radarr" else "sonarr_queue_active_download"
                return True, rc, msg
        if app == "radarr":
            diag["radarr_upstream_active_rows"] = active_count
            diag["radarr_active_path_samples"] = active_samples
            diag["inactive_path_match_radarr"] = inactive_match
        else:
            diag["sonarr_upstream_active_rows"] = active_count
            diag["sonarr_active_path_samples"] = active_samples
            diag["inactive_path_match_sonarr"] = inactive_match
        return None

    hit = _collect_for_app(snap.radarr_records, app="radarr")
    if hit is not None:
        blocked, rc, msg = hit
        diag["upstream_blocked"] = True
        diag["upstream_block_reason_code"] = rc
        return blocked, rc, msg, diag

    hit = _collect_for_app(snap.sonarr_records, app="sonarr")
    if hit is not None:
        blocked, rc, msg = hit
        diag["upstream_blocked"] = True
        diag["upstream_block_reason_code"] = rc
        return blocked, rc, msg, diag

    return False, "", "", diag


def upstream_blocks_path(path: Path, snap: RefinerQueueSnapshot) -> tuple[bool, str, str]:
    """If upstream authority says this path is an active download, return (True, reason_code, message)."""
    blocked, rc, msg, _diag = upstream_analyze_path(path, snap)
    return blocked, rc, msg


def log_refiner_readiness_diagnostic(
    *,
    gate_tag: str,
    path: Path,
    snap: RefinerQueueSnapshot,
    up_diag: dict[str, Any],
    strict_file_fallback: bool,
    decision_proceed: bool,
    decision_reason_code: str,
    file_gate_ok: bool | None,
    file_gate_detail: str,
    extra: dict[str, Any] | None = None,
) -> None:
    """Temporary structured trace for live validation (grep ``Refiner readiness diagnostic``)."""
    payload: dict[str, Any] = {
        "kind": "refiner_readiness_diagnostic",
        "gate": gate_tag,
        "candidate": str(path),
        "candidate_resolved_key": up_diag.get("candidate_resolved"),
        "radarr_fetch_succeeded": snap.radarr_fetch_succeeded,
        "sonarr_fetch_succeeded": snap.sonarr_fetch_succeeded,
        "radarr_configured": snap.radarr_configured,
        "sonarr_configured": snap.sonarr_configured,
        "authority_useful": snap.authority_useful,
        "strict_file_only_fallback": strict_file_fallback,
        "upstream_scan_skipped": up_diag.get("upstream_scan_skipped"),
        "upstream_blocked": up_diag.get("upstream_blocked"),
        "upstream_block_reason_code": up_diag.get("upstream_block_reason_code") or "",
        "queue_rows_radarr": up_diag.get("radarr_queue_rows"),
        "queue_rows_sonarr": up_diag.get("sonarr_queue_rows"),
        "upstream_active_rows_radarr": up_diag.get("radarr_upstream_active_rows"),
        "upstream_active_rows_sonarr": up_diag.get("sonarr_upstream_active_rows"),
        "active_queue_path_samples_radarr": up_diag.get("radarr_active_path_samples"),
        "active_queue_path_samples_sonarr": up_diag.get("sonarr_active_path_samples"),
        "inactive_path_match_radarr": up_diag.get("inactive_path_match_radarr"),
        "inactive_path_match_sonarr": up_diag.get("inactive_path_match_sonarr"),
        "file_gate_ok": file_gate_ok,
        "file_gate_detail": (file_gate_detail or "")[:300],
        "decision_proceed": decision_proceed,
        "decision_reason_code": decision_reason_code or "",
    }
    if extra:
        payload["extra"] = extra
    # WARNING: default FETCHER_LOG_LEVEL is WARNING (see app.log_sanitize.configure_fetcher_logging);
    # INFO would not appear in fetcher.log unless FETCHER_LOG_LEVEL=INFO.
    logger.warning("Refiner readiness diagnostic %s", json.dumps(payload, ensure_ascii=True))


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
    _ = settings
    strict = False
    if snapshot.authority_configured and not snapshot.authority_useful:
        strict = True
    blocked, rc, msg, up_diag = upstream_analyze_path(path, snapshot)
    if blocked:
        logger.warning("Refiner readiness [%s]: upstream block for %s (%s)", gate_tag, path.name, rc)
        log_refiner_readiness_diagnostic(
            gate_tag=gate_tag,
            path=path,
            snap=snapshot,
            up_diag=up_diag,
            strict_file_fallback=strict,
            decision_proceed=False,
            decision_reason_code=rc,
            file_gate_ok=None,
            file_gate_detail="",
        )
        return RefinerReadinessDecision(False, rc, msg, strict_file_fallback=strict)
    ok, why = refiner_file_level_gate(path, strict=strict)
    if not ok:
        logger.warning("Refiner readiness [%s]: file gate failed for %s — %s", gate_tag, path.name, why)
        rc2 = "skipped_final_readiness_gate" if gate_tag == "final" else "skipped_readiness"
        log_refiner_readiness_diagnostic(
            gate_tag=gate_tag,
            path=path,
            snap=snapshot,
            up_diag=up_diag,
            strict_file_fallback=strict,
            decision_proceed=False,
            decision_reason_code=rc2,
            file_gate_ok=False,
            file_gate_detail=why,
        )
        return RefinerReadinessDecision(False, rc2, why, strict_file_fallback=strict)
    log_refiner_readiness_diagnostic(
        gate_tag=gate_tag,
        path=path,
        snap=snapshot,
        up_diag=up_diag,
        strict_file_fallback=strict,
        decision_proceed=True,
        decision_reason_code="",
        file_gate_ok=True,
        file_gate_detail="",
    )
    return RefinerReadinessDecision(True, "", "", strict_file_fallback=strict)


def ffprobe_failure_hint_is_read_analyze(hint: str) -> bool:
    t = (hint or "").casefold()
    return "could not read or analyze the file" in t

