"""Shared helpers for server-rendered pages and settings persistence."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Sequence
from urllib.parse import quote

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.display_helpers import (
    _as_utc_naive,
    fmt_local,
    _relative_phrase_past,
    _truncate_display,
    activity_relative_time,
)
from app.failed_import_activity import parse_failed_import_cleanup_activity_detail
from app.models import ActivityLog, AppSettings, JobRunLog, RefinerActivity
from app.schedule import DAY_NAMES, normalize_schedule_days_csv
from app.scheduler import scheduler
from app.time_util import utc_now_naive

logger = logging.getLogger(__name__)

# Activity list shows 5 title lines + “+N more”; full list is stored in ``ActivityLog.detail``.
ACTIVITY_DETAIL_PREVIEW_LINES = 5

# Tab scope for Activity page filtering (canonical keys, aligned with ``data-pill-filter``).
ACTIVITY_TAB_ALL_ONLY = "all_only"
ACTIVITY_TAB_SONARR = "sonarr"
ACTIVITY_TAB_RADARR = "radarr"
ACTIVITY_TAB_TRIMMER = "trimmer"
ACTIVITY_TAB_REFINER = "refiner"


def activity_log_tab_scope(e: ActivityLog) -> str:
    """Classify an ``ActivityLog`` row for Activity tab filtering (server + ``data-activity-tab-scope``)."""
    app = (getattr(e, "app", "") or "").strip().lower()
    kind = (getattr(e, "kind", "") or "").strip().lower()
    if app == "sonarr":
        return ACTIVITY_TAB_SONARR
    if app == "radarr":
        return ACTIVITY_TAB_RADARR
    if app == "trimmer":
        return ACTIVITY_TAB_TRIMMER
    if app == "refiner" and kind == "refiner":
        return ACTIVITY_TAB_REFINER
    return ACTIVITY_TAB_ALL_ONLY


def normalize_activity_tab_query(raw: str | None) -> str:
    """Normalize URL/query ``app`` to a pill key: ``all`` | ``sonarr`` | ``radarr`` | ``trimmer`` | ``refiner``."""
    s = (raw or "").strip().lower()
    if s in ("", "all"):
        return "all"
    if s in ("sonarr", "tv"):
        return "sonarr"
    if s in ("radarr", "movies", "movie"):
        return "radarr"
    if s == "trimmer":
        return "trimmer"
    if s == "refiner":
        return "refiner"
    return "all"


def filter_activity_display_for_tab(rows: list[dict[str, Any]], tab: str) -> list[dict[str, Any]]:
    """Keep rows for the selected Activity tab (``all`` shows every row, including ``all_only``)."""
    t = (tab or "").strip().lower()
    if t in ("", "all"):
        return list(rows)
    out: list[dict[str, Any]] = []
    for r in rows:
        scope = (r.get("activity_tab_scope") or "").strip().lower()
        if scope == t:
            out.append(r)
    return out


def normalize_activity_search_query(q: str | None) -> str:
    return (q or "").strip().lower()


def _activity_row_search_blob(row: dict[str, Any]) -> str:
    """Flat text for substring search over operator-visible activity fields (no regex / indexing)."""
    parts: list[str] = []

    def add(v: object) -> None:
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
        elif isinstance(v, list):
            for x in v:
                add(x)
        elif isinstance(v, dict):
            for x in v.values():
                add(x)

    add(row.get("primary_label"))
    add(row.get("detail_lines"))
    add(row.get("app"))
    add(row.get("kind"))
    add(row.get("refiner_media_title"))
    add(row.get("refiner_file_title"))
    add(row.get("refiner_source_file_line"))
    add(row.get("refiner_outcome_label"))
    add(row.get("refiner_outcome_sub"))
    add(row.get("refiner_primary_line"))
    add(row.get("refiner_summary_line"))
    add(row.get("refiner_summary_bullets"))
    add(row.get("refiner_technical_notes"))
    add(row.get("refiner_compare_rows"))
    add(row.get("refiner_detail_blocks"))
    return " ".join(parts)


def activity_row_matches_search(row: dict[str, Any], needle: str) -> bool:
    if not needle:
        return True
    hay = _activity_row_search_blob(row).lower()
    return needle in hay


def filter_activity_display_for_search(rows: list[dict[str, Any]], q: str | None) -> list[dict[str, Any]]:
    needle = normalize_activity_search_query(q)
    if not needle:
        return list(rows)
    return [r for r in rows if activity_row_matches_search(r, needle)]
_ACTIVITY_LOG_LEGACY_MORE = re.compile(r"^\+\d+ more$")
# ``waiting=`` was added in v3.5.1; older rows omit it (treated as 0).
_REFINER_BATCH_LOG = re.compile(
    r"Refiner\s*\(([^)]*)\):\s*processed=(\d+)\s+unchanged=(\d+)\s+dry_run_items=(\d+)\s+(?:waiting=(\d+)\s+)?errors=(\d+)",
    re.I | re.DOTALL,
)


def parse_refiner_batch_activity_detail(detail: str) -> tuple[str, int, int, int, int, int] | None:
    """Return (trigger, processed, unchanged, dry_run, waiting, errors) or None if not a batch summary line."""
    m = _REFINER_BATCH_LOG.search((detail or "").replace("\n", " ").strip())
    if not m:
        return None
    trigger = (m.group(1) or "scheduled").strip()
    proc, noop, dry = (int(m.group(i)) for i in range(2, 5))
    wait = int(m.group(5) or 0)
    err = int(m.group(6))
    return trigger, proc, noop, dry, wait, err


def activity_log_title_lines(detail: str) -> list[str]:
    """Split stored detail into display lines; drop obsolete synthetic ``+N more`` rows."""
    lines: list[str] = []
    for raw in (detail or "").splitlines():
        s = raw.strip()
        if not s or _ACTIVITY_LOG_LEGACY_MORE.match(s):
            continue
        lines.append(s)
    return lines


def user_visible_job_run_message(
    *,
    message: str | None,
    ok: bool,
    finished_at: Any,
) -> str:
    """Never return blank strings for JobRunLog presentation."""
    t = _truncate_display((message or "").strip())
    if t:
        return t
    if finished_at is None:
        return "Run summary not available yet."
    return (
        "Run completed with no summary text."
        if ok
        else "Operation failed with no additional details."
    )


_RUN_SUMMARY_PENDING_DISPLAY = "Run summary not available yet."
# Orphan early-commit row vs final row may differ by more than one wall second in practice.
_JOB_RUN_LOG_PLACEHOLDER_NEARBY_SECONDS = 5.0


def job_run_log_started_at_bucket_key(started_at: datetime | None) -> tuple[datetime | None, ...]:
    """Group rows that share the same wall-clock start second (tests / diagnostics)."""
    if started_at is None:
        return (None,)
    return (started_at.replace(microsecond=0),)


def _job_run_log_started_at_delta_seconds(a: datetime | None, b: datetime | None) -> float | None:
    if a is None or b is None:
        return None
    return abs((a - b).total_seconds())


def job_run_log_row_is_terminal_for_dedupe(r: JobRunLog) -> bool:
    """True when the row is finalized or already carries a non-empty message (real summary)."""
    if r.finished_at is not None:
        return True
    if (r.message or "").strip():
        return True
    return False


def job_run_log_row_is_suppressible_placeholder(r: JobRunLog) -> bool:
    """Early-commit orphan: no finish time and no message → pending placeholder display only."""
    if job_run_log_row_is_terminal_for_dedupe(r):
        return False
    disp = user_visible_job_run_message(
        message=r.message, ok=bool(r.ok), finished_at=r.finished_at
    )
    return disp == _RUN_SUMMARY_PENDING_DISPLAY


def dedupe_job_run_logs_for_display(rows: Sequence[JobRunLog]) -> list[JobRunLog]:
    """Drop stale ``Run summary not available yet.`` rows when a terminal row exists nearby in time.

    Uses a short ``started_at`` proximity window so orphan early commits still match a final row
    when real-world timestamps differ by a few seconds. Read/render-time only.
    """
    seq = list(rows)

    def placeholder_has_nearby_terminal(p: JobRunLog) -> bool:
        if not job_run_log_row_is_suppressible_placeholder(p):
            return False
        for t in seq:
            if t is p:
                continue
            if not job_run_log_row_is_terminal_for_dedupe(t):
                continue
            delta = _job_run_log_started_at_delta_seconds(p.started_at, t.started_at)
            if delta is not None and delta <= _JOB_RUN_LOG_PLACEHOLDER_NEARBY_SECONDS:
                return True
        return False

    return [r for r in seq if not placeholder_has_nearby_terminal(r)]


def _activity_log_domain_and_icon(e: ActivityLog) -> tuple[str, str]:
    """Return (``activity_domain`` CSS token, Lucide icon name) for ActivityLog rows."""
    app = (getattr(e, "app", "") or "").strip().lower()
    kind = (getattr(e, "kind", "") or "").strip().lower()
    if app == "sonarr":
        return ("sonarr", "tv")
    if app == "radarr":
        return ("radarr", "clapperboard")
    if app == "trimmer" and kind == "trimmed":
        return ("trimmer", "scissors")
    if app == "trimmer":
        return ("trimmer", "server")
    if app == "service":
        return ("service", "server")
    if app == "refiner" and kind == "refiner":
        return ("refiner", "sliders-horizontal")
    return ("service", "server")


def _activity_log_outcome_class(e: ActivityLog) -> str:
    """Map log row to outcome styling: success | processing | skipped | failed | waiting."""
    app = (getattr(e, "app", "") or "").strip().lower()
    kind = (getattr(e, "kind", "") or "").strip().lower()
    status = (getattr(e, "status", "") or "ok").strip().lower()
    count = int(getattr(e, "count", 0) or 0)
    if kind == "refiner" and app == "refiner":
        parsed = parse_refiner_batch_activity_detail(getattr(e, "detail", "") or "")
        if parsed is not None:
            _trigger, proc, noop, dry, wait, err = parsed
            if err > 0:
                return "failed"
            if wait > 0 and proc + noop + dry == 0:
                return "waiting"
            if proc > 0 or dry > 0:
                return "success"
            return "skipped"
        return "failed" if status == "failed" else ("success" if count > 0 else "skipped")
    if kind == "error" or status == "failed":
        return "failed"
    if kind == "trimmed":
        return "success" if count > 0 else "skipped"
    if kind in ("missing", "upgrade"):
        return "success" if count > 0 else "skipped"
    if kind == "cleanup":
        return "success" if count > 0 else "skipped"
    if status == "failed":
        return "failed"
    return "success"


def _activity_primary_label(e: ActivityLog) -> str:
    app = (getattr(e, "app", "") or "").strip().lower()
    kind = (getattr(e, "kind", "") or "").strip().lower()
    status = (getattr(e, "status", "") or "ok").strip().lower()
    count = int(getattr(e, "count", 0) or 0)
    if kind == "error":
        return "Run error"
    if kind == "refiner" and app == "refiner":
        parsed = parse_refiner_batch_activity_detail(getattr(e, "detail", "") or "")
        if parsed is not None:
            _trigger, proc, noop, dry, wait, err = parsed
            if err > 0:
                return "Refiner failed"
            if wait > 0 and proc + noop + dry == 0:
                return "Refiner waiting"
            if proc > 0 or dry > 0:
                return "Refiner completed"
            return "Refiner skipped"
        if status == "failed":
            return "Refiner failed"
        if count > 0:
            return "Refiner completed"
        return "Refiner skipped"
    if status == "failed":
        return "Run failed"
    if kind == "cleanup":
        app_l = (app or "").strip().lower()
        if app_l in ("sonarr", "radarr"):
            parsed = parse_failed_import_cleanup_activity_detail(getattr(e, "detail", "") or "")
            if parsed:
                return parsed[0]
        return f"Queue cleanup · {count} item{'s' if count != 1 else ''}"
    if kind == "trimmed":
        return f"Trimmer · {count} item{'s' if count != 1 else ''} matched rules"
    if kind == "missing":
        unit = "episode" if app == "sonarr" else "movie"
        return f"Missing search · {count} {unit}{'s' if count != 1 else ''}"
    if kind == "upgrade":
        unit = "episode" if app == "sonarr" else "movie"
        return f"Upgrade search · {count} {unit}{'s' if count != 1 else ''}"
    return f"Activity ({kind or 'event'})"


def _activity_detail_fallback_line(e: ActivityLog) -> str:
    if (getattr(e, "status", "") or "").strip().lower() == "failed":
        return "Didn’t finish — check the service log or try again."
    if int(getattr(e, "count", 0) or 0) > 0:
        return "No extra detail was stored for this entry."
    return "Nothing was queued for this run."


def _humanize_refiner_batch_log_detail(detail: str) -> list[str] | None:
    """Turn canonical Refiner batch log text into one summary line (display-only)."""
    parsed = parse_refiner_batch_activity_detail(detail or "")
    if parsed is None:
        return None
    trigger, proc, noop, dry, wait, err = parsed
    bits: list[str] = []
    if proc:
        bits.append(f"{proc} refined")
    if noop:
        bits.append(f"{noop} unchanged")
    if dry:
        bits.append(f"{dry} dry run")
    if wait:
        bits.append(f"{wait} waiting on upstream")
    if err:
        bits.append(f"{err} failed")
    head = " · ".join(bits) if bits else "No file actions"
    lines = [f"{head} · {trigger}"]
    if err > 0:
        lines.append("Open per-file rows for retry vs manual-action guidance.")
    return lines


def _activity_timestamp_fields(created_at: datetime, tz: str, now: datetime) -> dict[str, str]:
    t_utc = _as_utc_naive(created_at).replace(microsecond=0)
    return {
        "time_local": fmt_local(created_at, tz),
        "time_relative": activity_relative_time(created_at, now),
        "activity_time_iso": f"{t_utc.isoformat()}Z",
    }


def activity_display_row(e: ActivityLog, tz: str, *, now: datetime | None = None) -> dict[str, Any]:
    raw_detail = (getattr(e, "detail", "") or "").strip()
    app = (getattr(e, "app", "") or "").strip().lower()
    kind = (getattr(e, "kind", "") or "").strip().lower()
    human = None
    if app == "refiner" and kind == "refiner":
        human = _humanize_refiner_batch_log_detail(raw_detail)
    if human is None and kind == "cleanup" and app in ("sonarr", "radarr"):
        fi = parse_failed_import_cleanup_activity_detail(raw_detail)
        if fi is not None:
            _head, summary, remainder = fi
            human = [summary]
            extra = activity_log_title_lines(remainder)
            if extra:
                human.extend(extra)
    if human is not None:
        detail_lines = human
    else:
        detail_lines = activity_log_title_lines(raw_detail)
        if not detail_lines:
            detail_lines = [_activity_detail_fallback_line(e)]
    tnow = now if now is not None else utc_now_naive()
    domain, lucide_name = _activity_log_domain_and_icon(e)
    row: dict[str, Any] = {
        "activity_type": "log",
        "type": "log",
        "id": e.id,
        "app": e.app,
        "kind": e.kind,
        "status": (getattr(e, "status", "") or "ok").strip().lower(),
        "count": e.count,
        "primary_label": _activity_primary_label(e),
        "detail_lines": detail_lines,
        "detail_preview": 2,
        "activity_domain": domain,
        "activity_lucide": lucide_name,
        "activity_outcome": _activity_log_outcome_class(e),
        "activity_tab_scope": activity_log_tab_scope(e),
    }
    row.update(_activity_timestamp_fields(e.created_at, tz, tnow))
    return row


def refiner_activity_display_row(r: RefinerActivity, tz: str, now: datetime) -> dict[str, Any]:
    from app.refiner_activity_row import build_refiner_activity_row_dict

    row = build_refiner_activity_row_dict(r, tz, now)
    row["activity_tab_scope"] = ACTIVITY_TAB_REFINER
    row.update(_activity_timestamp_fields(r.created_at, tz, now))
    return row


def merge_activity_feed(
    logs: list[ActivityLog],
    refiners: list[RefinerActivity],
    tz: str,
    now: datetime,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Interleave Refiner file rows with ``activity_log`` by time (newest first)."""
    pairs: list[tuple[datetime, str, int, dict[str, Any]]] = []
    for e in logs:
        pairs.append((e.created_at, "L", e.id, activity_display_row(e, tz, now=now)))
    for r in refiners:
        pairs.append((r.created_at, "R", r.id, refiner_activity_display_row(r, tz, now)))
    pairs.sort(key=lambda p: (p[0], p[2], p[1]), reverse=True)
    return [p[3] for p in pairs[:limit]]


def trimmer_settings_fragment(trimmer_section: str | None) -> str:
    key = (trimmer_section or "").strip().lower()
    ids = {
        "connection": "trimmer-connection",
        "schedule": "trimmer-schedule",
        "rules": "trimmer-rules",
        "people": "trimmer-people",
    }
    fid = ids.get(key)
    return f"#{fid}" if fid else ""


def trimmer_settings_saved_query_value(section: str | None, save_scope: str | None) -> str | None:
    """Stable ``trimmer_saved=`` query token for banner/toast copy (independent save scopes)."""
    panel = (section or "").strip().lower()
    sc = (save_scope or "").strip().lower()
    if panel == "connection":
        return "connection"
    if panel == "schedule" and sc == "schedule":
        return "schedule"
    if panel == "rules" and sc == "tv":
        return "tv_rules"
    if panel == "rules" and sc == "movies":
        return "movie_rules"
    if panel == "people" and sc == "tv":
        return "tv_people"
    if panel == "people" and sc == "movies":
        return "movie_people"
    return None


def trimmer_settings_redirect_url(
    *,
    saved: bool,
    reason: str | None = None,
    section: str | None = None,
    save_scope: str | None = None,
) -> str:
    frag = trimmer_settings_fragment(section)
    if saved:
        parts = ["saved=1"]
        qv = trimmer_settings_saved_query_value(section, save_scope)
        if qv:
            parts.append(f"trimmer_saved={quote(qv, safe='')}")
        return f"/trimmer/settings?{'&'.join(parts)}{frag}"
    err = quote(str(reason or "error").replace("\n", " ").strip()[:240], safe="")
    return f"/trimmer/settings?save=fail&reason={err}{frag}"


def trimmer_settings_test_redirect_url(*, ok: bool) -> str:
    """After Emby test, stay on Trimmer settings on the connection section."""
    return f"/trimmer/settings?test={'emby_ok' if ok else 'emby_fail'}{trimmer_settings_fragment('connection')}"


def refiner_settings_fragment(refiner_section: str | None) -> str:
    key = (refiner_section or "").strip().lower()
    ids = {
        "processing": "refiner-processing",
        "folders": "refiner-folders",
        "audio": "refiner-audio",
        "subtitles": "refiner-subtitles",
        "schedule": "refiner-schedule",
    }
    fid = ids.get(key)
    return f"#{fid}" if fid else ""


def refiner_settings_saved_query_value(section: str | None) -> str | None:
    key = (section or "").strip().lower()
    if key in ("processing", "folders", "audio", "subtitles", "schedule"):
        return key
    return None


def refiner_settings_redirect_url(
    *, saved: bool, reason: str | None = None, section: str | None = None
) -> str:
    frag = refiner_settings_fragment(section)
    sec_token = refiner_settings_saved_query_value(section)
    sec_q_fail = f"&refiner_section={quote(sec_token, safe='')}" if sec_token else ""
    if saved:
        parts = ["saved=1"]
        if sec_token:
            parts.append(f"refiner_saved={quote(sec_token, safe='')}")
        return f"/refiner/settings?{'&'.join(parts)}{frag}"
    err = quote(str(reason or "error").replace("\n", " ").strip()[:240], safe="")
    return f"/refiner/settings?save=fail&reason={err}{sec_q_fail}{frag}"


def settings_looks_like_existing_fetcher_install(settings: AppSettings) -> bool:
    """True when Sonarr/Radarr/Emby were already configured — tailors setup step 0 for upgrades."""
    return bool(
        (settings.sonarr_url or "").strip()
        or (settings.radarr_url or "").strip()
        or (settings.emby_url or "").strip()
        or settings.sonarr_enabled
        or settings.radarr_enabled
        or settings.emby_enabled
    )


async def try_commit_and_reschedule(
    session: AsyncSession,
    *,
    targets: set[str] | None = None,
) -> bool:
    """Persist settings and refresh scheduler tick. False if SQLite could not commit (e.g. DB locked)."""
    try:
        await session.commit()
    except SQLAlchemyError:
        try:
            await session.rollback()
        except Exception:
            logger.exception("rollback after failed settings commit")
        return False
    try:
        await scheduler.reschedule(targets=targets)
    except Exception:
        logger.warning("scheduler.reschedule failed after settings commit", exc_info=True)
    return True


def is_setup_complete(settings: AppSettings) -> bool:
    """
    True when onboarding is complete enough that the Setup Wizard can be hidden.

    Source of truth is saved configuration state (not a one-time flag):
    - Auth must be configured (password set).
    - At least one integration must be configured (URL + API key), OR
      if any integration is enabled it must have its required fields present.
    """
    if not (settings.auth_password_hash or "").strip():
        return False

    def has_url_and_key(url: str | None, key: str | None) -> bool:
        return bool((url or "").strip() and (key or "").strip())

    son_cfg = has_url_and_key(settings.sonarr_url, settings.sonarr_api_key)
    rad_cfg = has_url_and_key(settings.radarr_url, settings.radarr_api_key)
    em_cfg = has_url_and_key(settings.emby_url, settings.emby_api_key)

    # If the user explicitly enabled an integration, its required fields must be present.
    if bool(settings.sonarr_enabled) and not son_cfg:
        return False
    if bool(settings.radarr_enabled) and not rad_cfg:
        return False
    if bool(settings.emby_enabled) and not em_cfg:
        return False

    # Otherwise, consider setup complete when *any* integration is configured.
    return bool(son_cfg or rad_cfg or em_cfg)


def movie_credit_types_summary(types: frozenset[str]) -> str:
    short = {
        "actor": "Cast",
        "director": "Director",
        "writer": "Writer",
        "producer": "Producer",
        "gueststar": "Guest",
    }
    order = ("actor", "director", "writer", "producer", "gueststar")
    parts = [short[k] for k in order if k in types]
    return "+".join(parts) if parts else "Cast"


def schedule_days_csv_from_named_day_checks(
    mon: int,
    tue: int,
    wed: int,
    thu: int,
    fri: int,
    sat: int,
    sun: int,
) -> str:
    """One checkbox per day (`name=prefix_Mon` value=1). Uncheck all → store "" (not full week)."""
    flags = (mon, tue, wed, thu, fri, sat, sun)
    parts = [DAY_NAMES[i] for i, v in enumerate(flags) if int(v or 0) != 0]
    if not parts:
        return ""
    return normalize_schedule_days_csv(",".join(parts))


def schedule_weekdays_selected_dict(days_csv: str) -> dict[str, bool]:
    """Per-day flags from DB column (raw). Empty stored value → all False."""
    n = normalize_schedule_days_csv((days_csv or "").strip())
    if not n.strip():
        return {d: False for d in DAY_NAMES}
    allowed = {p.strip() for p in n.split(",") if p.strip() in DAY_NAMES}
    return {d: (d in allowed) for d in DAY_NAMES}


def effective_emby_rules(settings: AppSettings) -> dict[str, int | bool]:
    global_rating = max(0, int(settings.emby_rule_watched_rating_below or 0))
    global_unwatched = max(0, int(settings.emby_rule_unwatched_days or 0))

    movie_rating = max(0, int(settings.emby_rule_movie_watched_rating_below or 0)) or global_rating
    movie_unwatched = max(0, int(settings.emby_rule_movie_unwatched_days or 0)) or global_unwatched
    tv_delete_watched = bool(settings.emby_rule_tv_delete_watched)
    tv_unwatched = max(0, int(settings.emby_rule_tv_unwatched_days or 0)) or global_unwatched

    return {
        "movie_rating_below": movie_rating,
        "movie_unwatched_days": movie_unwatched,
        "tv_delete_watched": tv_delete_watched,
        "tv_unwatched_days": tv_unwatched,
    }


# Total wizard screens (0 .. WIZARD_LAST_STEP_INDEX inclusive). Last index is the "done" page.
SETUP_WIZARD_STEPS = 6
WIZARD_LAST_STEP_INDEX = SETUP_WIZARD_STEPS - 1


def setup_wizard_step_title(step: int) -> str:
    return {
        0: "Account",
        1: "Sonarr",
        2: "Radarr",
        3: "Emby",
        4: "Schedule & timezone",
        5: "What's next",
    }.get(step, "Setup")
