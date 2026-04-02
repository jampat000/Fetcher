"""User-facing copy for failed-import queue cleanup (Sonarr / Radarr *arr download queue only).

Fetcher calls ``DELETE /api/v3/queue/{id}`` with per-toggle ``blocklist``, ``skipRedownload=true``
(so blocklist does not immediately trigger a replacement grab), and ``removeFromClient=false`` by
default (download may remain in the client; see README).
"""

from __future__ import annotations

from app.log_sanitize import redact_sensitive_text

# Versioned marker so ``web_common`` can parse headline + summary without heuristics.
FAILED_IMPORT_ACTIVITY_V1 = "fetcher.fi_cleanup.v1"


def format_failed_import_cleanup_activity_detail(
    arr_app: str,
    *,
    blocklist_applied: bool,
    title: str = "",
    reason: str = "",
    queue_signal: str | None = None,
) -> str:
    """
    Persisted on ``ActivityLog.detail`` for ``kind=cleanup``.

    * ``blocklist_applied`` — True when delete with blocklist succeeded (first API call).
    * When False, queue item was removed without blocklist after blocklist attempt failed.
    """
    app = (arr_app or "").strip().lower()
    arr = "Sonarr" if app == "sonarr" else "Radarr"
    if blocklist_applied:
        headline = "Failed import cleaned up"
        summary = f"Removed download and blocklisted release after terminal import failure in {arr}."
    else:
        headline = "Failed import removed"
        summary = (
            f"Removed download after terminal import failure in {arr}. Blocklist was not applied."
        )
    body: list[str] = [FAILED_IMPORT_ACTIVITY_V1, headline, summary]
    tail: list[str] = []
    t = (title or "").strip()
    if t:
        tail.append(t[:500])
    r = (reason or "").strip()
    if r:
        tail.append(f"Reason: {r[:2000]}")
    qs = (queue_signal or "").strip()
    if qs:
        tail.append(f"Matched: {qs[:500]}")
    if tail:
        body.append("")
        body.extend(tail)
    return redact_sensitive_text("\n".join(body))


def parse_failed_import_cleanup_activity_detail(detail: str) -> tuple[str, str, str] | None:
    """
    Return ``(headline, summary, remainder)`` for feed rendering, or None if not v1 payload.
    ``remainder`` is optional title/reason/matched lines (may be empty).
    """
    raw_lines = (detail or "").splitlines()
    if not raw_lines or raw_lines[0].strip() != FAILED_IMPORT_ACTIVITY_V1:
        return None
    if len(raw_lines) < 3:
        return None
    headline = raw_lines[1].strip()
    summary = raw_lines[2].strip()
    rest = raw_lines[3:]
    while rest and not rest[0].strip():
        rest = rest[1:]
    remainder = "\n".join(rest)
    return headline, summary, remainder


def failed_import_cleanup_action_success(arr_name: str, *, blocklist_applied: bool, label: str) -> str:
    """One line for job-run ``actions`` summaries; aligns with Activity primary titles."""
    head = "Failed import cleaned up" if blocklist_applied else "Failed import removed"
    return f"{arr_name}: {head} — {label}"
