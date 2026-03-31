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
import re
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
    # Radarr/Sonarr JSON uses camelCase ``sizeLeft``; older payloads may use ``sizeleft``.
    raw = rec.get("sizeLeft")
    if raw is None:
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


def _nonempty_str(val: object) -> str | None:
    if isinstance(val, str):
        t = val.strip()
        return t if t else None
    return None


def _dedupe_append(paths: list[str], s: str | None) -> None:
    if s and s not in paths:
        paths.append(s)


def iter_queue_path_strings(rec: dict[str, Any]) -> list[str]:
    """Filesystem path candidates from *arr ``GET /api/v3/queue`` records (Radarr QueueResource, Sonarr)."""
    paths: list[str] = []
    if not isinstance(rec, dict):
        return paths

    movie = rec.get("movie") if isinstance(rec.get("movie"), dict) else None
    episode = rec.get("episode") if isinstance(rec.get("episode"), dict) else None

    for key in (
        "outputPath",
        "sourcePath",
        "path",
        "downloadClientOutputPath",
        "targetPath",
    ):
        _dedupe_append(paths, _nonempty_str(rec.get(key)))

    if movie:
        for key in ("path", "folderName", "folder", "rootFolderPath"):
            _dedupe_append(paths, _nonempty_str(movie.get(key)))

    mf = rec.get("movieFile")
    if isinstance(mf, dict):
        _dedupe_append(paths, _nonempty_str(mf.get("path")))
        _dedupe_append(paths, _nonempty_str(mf.get("originalFilePath")))
        rel = _nonempty_str(mf.get("relativePath"))
        if rel and movie:
            for root_key in ("path", "rootFolderPath"):
                root = _nonempty_str(movie.get(root_key))
                if root:
                    try:
                        combined = str((Path(root) / rel).resolve())
                    except (OSError, ValueError):
                        combined = str(Path(root) / rel)
                    _dedupe_append(paths, combined)
        elif rel:
            _dedupe_append(paths, rel)

    if episode:
        series = episode.get("series")
        if isinstance(series, dict):
            for key in ("path", "rootFolderPath"):
                _dedupe_append(paths, _nonempty_str(series.get(key)))
        ef = episode.get("episodeFile")
        if isinstance(ef, dict):
            for key in ("path", "relativePath"):
                _dedupe_append(paths, _nonempty_str(ef.get(key)))
            rel2 = _nonempty_str(ef.get("relativePath"))
            if rel2 and isinstance(series, dict):
                for root_key in ("path", "rootFolderPath"):
                    root = _nonempty_str(series.get(root_key))
                    if root:
                        try:
                            combined = str((Path(root) / rel2).resolve())
                        except (OSError, ValueError):
                            combined = str(Path(root) / rel2)
                        _dedupe_append(paths, combined)

    return paths


def _resolved_key(p: Path) -> str:
    try:
        return str(p.resolve()).casefold()
    except OSError:
        return str(p).casefold()


def _path_key_matches_candidate(file_key: str, queue_path: str) -> bool:
    """Resolved equality, or ``file_key`` is a file under ``queue_path`` (folder from API)."""
    try:
        q_key = _resolved_key(Path(queue_path))
    except OSError:
        q_key = str(Path(queue_path)).casefold()
    if not q_key:
        return False
    if file_key == q_key:
        return True
    return file_key.startswith(q_key + "\\") or file_key.startswith(q_key + "/")


_TITLE_EXT_RE = re.compile(r"\.(mkv|mp4|m4v|avi|mov|wmv|ts|m2ts|webm)$", re.IGNORECASE)
_TITLE_SEP_RE = re.compile(r"[^a-z0-9]+")
_DRIVE_RE = re.compile(r"^[a-zA-Z]:")


def _normalize_releaseish_title(raw: object) -> str:
    t = str(raw or "").strip().casefold()
    if not t:
        return ""
    t = _TITLE_EXT_RE.sub("", t)
    t = _TITLE_SEP_RE.sub(" ", t)
    t = " ".join(t.split())
    return t


def _looks_release_like_title(raw: object) -> bool:
    n = _normalize_releaseish_title(raw)
    if len(n) < 6:
        return False
    toks = n.split()
    if len(toks) < 2:
        return False
    # Very short/generic stems are poor release identity; require at least one digit
    # (year/season/resolution/group token) to reduce accidental matches.
    return any(ch.isdigit() for ch in n)


def derive_title_fallback_candidate(path: Path) -> tuple[str, str]:
    """
    Candidate title identity for title fallback matching.
    Priority: file stem, then parent folder when stem is not release-like.
    Returns (raw_candidate_title, source).
    """
    stem = (path.stem or "").strip()
    if _looks_release_like_title(stem):
        return stem, "file_stem"
    parent = (path.parent.name or "").strip()
    if _looks_release_like_title(parent):
        return parent, "parent_folder"
    return stem, "file_stem"


def _queue_row_title_candidates(rec: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in ("title", "sourceTitle", "releaseTitle"):
        v = rec.get(key)
        if isinstance(v, str) and v.strip() and v.strip() not in out:
            out.append(v.strip())
    return out


def _is_usable_queue_path_candidate(raw: object) -> bool:
    t = str(raw or "").strip()
    if not t:
        return False
    low = t.casefold()
    if low in ("none", "null", "n/a", "unknown", "-", "—"):
        return False
    # Path-like only; release-title-ish tokens are not usable for path matching.
    return ("\\" in t) or ("/" in t) or bool(_DRIVE_RE.match(t))


def _usable_queue_path_strings(rec: dict[str, Any]) -> list[str]:
    return [p for p in iter_queue_path_strings(rec) if _is_usable_queue_path_candidate(p)]


def _title_fallback_match(candidate_stem_norm: str, rec: dict[str, Any]) -> tuple[bool, str]:
    if not candidate_stem_norm:
        return False, ""
    for raw in _queue_row_title_candidates(rec):
        t_norm = _normalize_releaseish_title(raw)
        if not t_norm:
            continue
        # Conservative release-name matching only.
        if t_norm == candidate_stem_norm:
            return True, raw
        if candidate_stem_norm.startswith(t_norm):
            return True, raw
        if t_norm.startswith(candidate_stem_norm):
            return True, raw
    return False, ""


def path_matches_queue_record(file_key: str, rec: dict[str, Any]) -> bool:
    for s in iter_queue_path_strings(rec):
        if _path_key_matches_candidate(file_key, s):
            return True
    return False


def upstream_analyze_path(path: Path, snap: RefinerQueueSnapshot) -> tuple[bool, str, str, dict[str, Any]]:
    """Same blocking rules as historical ``upstream_blocks_path``, plus a diagnostic dict for live tracing."""
    file_key = _resolved_key(path)
    candidate_title_raw, candidate_title_source = derive_title_fallback_candidate(path)
    candidate_stem_norm = _normalize_releaseish_title(candidate_title_raw)
    diag: dict[str, Any] = {
        "gate_context": "upstream_analyze",
        "candidate_resolved": file_key,
        "candidate_stem": path.stem,
        "candidate_stem_norm": _normalize_releaseish_title(path.stem),
        "title_fallback_candidate_title": candidate_title_raw,
        "title_fallback_candidate_title_norm": candidate_stem_norm,
        "title_fallback_candidate_source": candidate_title_source,
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
        "active_queue_title_samples_radarr": [],
        "active_queue_title_samples_sonarr": [],
        "radarr_active_usable_path_count": 0,
        "sonarr_active_usable_path_count": 0,
        "inactive_path_match_radarr": False,
        "inactive_path_match_sonarr": False,
        "title_fallback_used_radarr": False,
        "title_fallback_used_sonarr": False,
        "title_fallback_entered_radarr": False,
        "title_fallback_entered_sonarr": False,
        "title_fallback_queue_title_radarr": "",
        "title_fallback_queue_title_sonarr": "",
        "title_fallback_queue_title_norm_radarr": "",
        "title_fallback_queue_title_norm_sonarr": "",
        "title_fallback_candidate_title_radarr": "",
        "title_fallback_candidate_title_sonarr": "",
        "title_fallback_candidate_title_norm_radarr": "",
        "title_fallback_candidate_title_norm_sonarr": "",
        "title_fallback_candidate_source_radarr": "",
        "title_fallback_candidate_source_sonarr": "",
        "title_fallback_match_norm_equal_radarr": False,
        "title_fallback_match_norm_prefix_radarr": False,
        "title_fallback_match_norm_equal_sonarr": False,
        "title_fallback_match_norm_prefix_sonarr": False,
        "upstream_block_match_kind": "",
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
        active_title_samples: list[str] = []
        inactive_match = False
        title_fallback_used = False
        title_fallback_entered = False
        title_fallback_queue_title = ""
        title_fallback_queue_title_norm = ""
        title_fallback_norm_equal = False
        title_fallback_norm_prefix = False
        active_usable_path_count = 0
        active_count = 0
        for rec in records:
            if not isinstance(rec, dict):
                continue
            active = queue_record_upstream_active(rec)
            q_paths = _usable_queue_path_strings(rec)
            path_matched = any(_path_key_matches_candidate(file_key, qp) for qp in q_paths)
            title_matched = False
            title_raw = ""
            if active and not q_paths:
                title_fallback_entered = True
                title_matched, title_raw = _title_fallback_match(candidate_stem_norm, rec)
                qt = _queue_row_title_candidates(rec)
                if qt:
                    title_fallback_queue_title = qt[0]
                    title_fallback_queue_title_norm = _normalize_releaseish_title(qt[0])
                    title_fallback_norm_equal = title_fallback_queue_title_norm == candidate_stem_norm
                    title_fallback_norm_prefix = (
                        candidate_stem_norm.startswith(title_fallback_queue_title_norm)
                        or title_fallback_queue_title_norm.startswith(candidate_stem_norm)
                    )
                if title_matched:
                    title_fallback_used = True
            if active:
                active_count += 1
                active_usable_path_count += len(q_paths)
                for ps in q_paths:
                    if len(active_samples) < 4:
                        active_samples.append(ps[:160] + ("…" if len(ps) > 160 else ""))
                if title_raw and len(active_title_samples) < 4:
                    active_title_samples.append(title_raw[:160] + ("…" if len(title_raw) > 160 else ""))
            elif path_matched:
                inactive_match = True
            if active and (path_matched or title_matched):
                if app == "radarr":
                    diag["radarr_upstream_active_rows"] = active_count
                    diag["radarr_active_path_samples"] = active_samples
                    diag["active_queue_title_samples_radarr"] = active_title_samples
                    diag["radarr_active_usable_path_count"] = active_usable_path_count
                    diag["inactive_path_match_radarr"] = inactive_match
                    diag["title_fallback_used_radarr"] = title_fallback_used
                    diag["title_fallback_entered_radarr"] = title_fallback_entered
                    diag["title_fallback_queue_title_radarr"] = title_fallback_queue_title
                    diag["title_fallback_queue_title_norm_radarr"] = title_fallback_queue_title_norm
                    diag["title_fallback_candidate_title_radarr"] = candidate_title_raw
                    diag["title_fallback_candidate_title_norm_radarr"] = candidate_stem_norm
                    diag["title_fallback_candidate_source_radarr"] = candidate_title_source
                    diag["title_fallback_match_norm_equal_radarr"] = title_fallback_norm_equal
                    diag["title_fallback_match_norm_prefix_radarr"] = title_fallback_norm_prefix
                else:
                    diag["sonarr_upstream_active_rows"] = active_count
                    diag["sonarr_active_path_samples"] = active_samples
                    diag["active_queue_title_samples_sonarr"] = active_title_samples
                    diag["sonarr_active_usable_path_count"] = active_usable_path_count
                    diag["inactive_path_match_sonarr"] = inactive_match
                    diag["title_fallback_used_sonarr"] = title_fallback_used
                    diag["title_fallback_entered_sonarr"] = title_fallback_entered
                    diag["title_fallback_queue_title_sonarr"] = title_fallback_queue_title
                    diag["title_fallback_queue_title_norm_sonarr"] = title_fallback_queue_title_norm
                    diag["title_fallback_candidate_title_sonarr"] = candidate_title_raw
                    diag["title_fallback_candidate_title_norm_sonarr"] = candidate_stem_norm
                    diag["title_fallback_candidate_source_sonarr"] = candidate_title_source
                    diag["title_fallback_match_norm_equal_sonarr"] = title_fallback_norm_equal
                    diag["title_fallback_match_norm_prefix_sonarr"] = title_fallback_norm_prefix
                msg = (
                    "Radarr still reports this path in the active download queue — waiting until the download finishes."
                    if app == "radarr"
                    else "Sonarr still reports this path in the active download queue — waiting until the download finishes."
                )
                if app == "radarr":
                    rc = "radarr_queue_active_download_title" if title_matched and not path_matched else "radarr_queue_active_download"
                else:
                    rc = "sonarr_queue_active_download_title" if title_matched and not path_matched else "sonarr_queue_active_download"
                diag["upstream_block_match_kind"] = "title" if title_matched and not path_matched else "path"
                return True, rc, msg
        if app == "radarr":
            diag["radarr_upstream_active_rows"] = active_count
            diag["radarr_active_path_samples"] = active_samples
            diag["active_queue_title_samples_radarr"] = active_title_samples
            diag["radarr_active_usable_path_count"] = active_usable_path_count
            diag["inactive_path_match_radarr"] = inactive_match
            diag["title_fallback_used_radarr"] = title_fallback_used
            diag["title_fallback_entered_radarr"] = title_fallback_entered
            diag["title_fallback_queue_title_radarr"] = title_fallback_queue_title
            diag["title_fallback_queue_title_norm_radarr"] = title_fallback_queue_title_norm
            diag["title_fallback_candidate_title_radarr"] = candidate_title_raw
            diag["title_fallback_candidate_title_norm_radarr"] = candidate_stem_norm
            diag["title_fallback_candidate_source_radarr"] = candidate_title_source
            diag["title_fallback_match_norm_equal_radarr"] = title_fallback_norm_equal
            diag["title_fallback_match_norm_prefix_radarr"] = title_fallback_norm_prefix
        else:
            diag["sonarr_upstream_active_rows"] = active_count
            diag["sonarr_active_path_samples"] = active_samples
            diag["active_queue_title_samples_sonarr"] = active_title_samples
            diag["sonarr_active_usable_path_count"] = active_usable_path_count
            diag["inactive_path_match_sonarr"] = inactive_match
            diag["title_fallback_used_sonarr"] = title_fallback_used
            diag["title_fallback_entered_sonarr"] = title_fallback_entered
            diag["title_fallback_queue_title_sonarr"] = title_fallback_queue_title
            diag["title_fallback_queue_title_norm_sonarr"] = title_fallback_queue_title_norm
            diag["title_fallback_candidate_title_sonarr"] = candidate_title_raw
            diag["title_fallback_candidate_title_norm_sonarr"] = candidate_stem_norm
            diag["title_fallback_candidate_source_sonarr"] = candidate_title_source
            diag["title_fallback_match_norm_equal_sonarr"] = title_fallback_norm_equal
            diag["title_fallback_match_norm_prefix_sonarr"] = title_fallback_norm_prefix
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
        "active_queue_title_samples_radarr": up_diag.get("active_queue_title_samples_radarr"),
        "active_queue_title_samples_sonarr": up_diag.get("active_queue_title_samples_sonarr"),
        "active_usable_path_count_radarr": up_diag.get("radarr_active_usable_path_count"),
        "active_usable_path_count_sonarr": up_diag.get("sonarr_active_usable_path_count"),
        "inactive_path_match_radarr": up_diag.get("inactive_path_match_radarr"),
        "inactive_path_match_sonarr": up_diag.get("inactive_path_match_sonarr"),
        "title_fallback_used_radarr": up_diag.get("title_fallback_used_radarr"),
        "title_fallback_used_sonarr": up_diag.get("title_fallback_used_sonarr"),
        "title_fallback_entered_radarr": up_diag.get("title_fallback_entered_radarr"),
        "title_fallback_entered_sonarr": up_diag.get("title_fallback_entered_sonarr"),
        "title_fallback_queue_title_radarr": up_diag.get("title_fallback_queue_title_radarr"),
        "title_fallback_queue_title_sonarr": up_diag.get("title_fallback_queue_title_sonarr"),
        "title_fallback_queue_title_norm_radarr": up_diag.get("title_fallback_queue_title_norm_radarr"),
        "title_fallback_queue_title_norm_sonarr": up_diag.get("title_fallback_queue_title_norm_sonarr"),
        "title_fallback_candidate_title_radarr": up_diag.get("title_fallback_candidate_title_radarr"),
        "title_fallback_candidate_title_sonarr": up_diag.get("title_fallback_candidate_title_sonarr"),
        "title_fallback_candidate_title_norm_radarr": up_diag.get("title_fallback_candidate_title_norm_radarr"),
        "title_fallback_candidate_title_norm_sonarr": up_diag.get("title_fallback_candidate_title_norm_sonarr"),
        "title_fallback_candidate_source_radarr": up_diag.get("title_fallback_candidate_source_radarr"),
        "title_fallback_candidate_source_sonarr": up_diag.get("title_fallback_candidate_source_sonarr"),
        "title_fallback_match_norm_equal_radarr": up_diag.get("title_fallback_match_norm_equal_radarr"),
        "title_fallback_match_norm_prefix_radarr": up_diag.get("title_fallback_match_norm_prefix_radarr"),
        "title_fallback_match_norm_equal_sonarr": up_diag.get("title_fallback_match_norm_equal_sonarr"),
        "title_fallback_match_norm_prefix_sonarr": up_diag.get("title_fallback_match_norm_prefix_sonarr"),
        "upstream_block_match_kind": up_diag.get("upstream_block_match_kind") or "",
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

