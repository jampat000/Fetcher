"""Failed-import cleanup activity payload + feed rendering."""

from __future__ import annotations

from datetime import datetime

from app.failed_import_activity import (
    FAILED_IMPORT_ACTIVITY_V1,
    format_failed_import_cleanup_activity_detail,
    parse_failed_import_cleanup_activity_detail,
)
from app.models import ActivityLog
from app.web_common import activity_display_row


def test_format_parse_roundtrip() -> None:
    d = format_failed_import_cleanup_activity_detail(
        "radarr",
        blocklist_applied=True,
        title="Test Movie",
        reason="corrupt",
    )
    assert d.startswith(FAILED_IMPORT_ACTIVITY_V1)
    p = parse_failed_import_cleanup_activity_detail(d)
    assert p is not None
    headline, summary, rest = p
    assert headline == "Failed import cleaned up"
    assert "Radarr" in summary
    assert "blocklisted release" in summary
    assert "Test Movie" in rest
    assert "Reason: corrupt" in rest


def test_remove_only_copy() -> None:
    d = format_failed_import_cleanup_activity_detail(
        "sonarr",
        blocklist_applied=False,
        title="Ep 1",
        reason="hash mismatch",
    )
    p = parse_failed_import_cleanup_activity_detail(d)
    assert p is not None
    assert p[0] == "Failed import removed"
    assert "Blocklist was not applied" in p[1]
    assert "Sonarr" in p[1]


def test_activity_display_row_cleanup_combined() -> None:
    detail = format_failed_import_cleanup_activity_detail(
        "radarr",
        blocklist_applied=True,
        title="Movie (2020)",
        reason="corrupt or unreadable file",
    )
    row = activity_display_row(
        ActivityLog(
            id=1,
            app="radarr",
            kind="cleanup",
            count=1,
            status="ok",
            detail=detail,
            created_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
        "UTC",
        now=datetime(2026, 1, 1, 12, 1, 0),
    )
    assert row["primary_label"] == "Failed import cleaned up"
    assert "Removed download and blocklisted release" in row["detail_lines"][0]
    assert "Radarr" in row["detail_lines"][0]
    assert any("Movie (2020)" in ln for ln in row["detail_lines"])


def test_activity_display_row_cleanup_remove_only() -> None:
    detail = format_failed_import_cleanup_activity_detail(
        "sonarr",
        blocklist_applied=False,
        title="Show S01E01",
        reason="",
        queue_signal=None,
    )
    row = activity_display_row(
        ActivityLog(
            id=2,
            app="sonarr",
            kind="cleanup",
            count=1,
            status="ok",
            detail=detail,
            created_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
        "UTC",
        now=datetime(2026, 1, 1, 12, 1, 0),
    )
    assert row["primary_label"] == "Failed import removed"
    assert "Blocklist was not applied" in row["detail_lines"][0]
    assert "Sonarr" in row["detail_lines"][0]
