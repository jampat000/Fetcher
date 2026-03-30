"""Refiner activity card markup: media title, before/after, dry-run vs applied wording."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from jinja2 import ChoiceLoader, Environment, FileSystemLoader

from app.web_common import refiner_activity_display_row
from app.models import RefinerActivity


def _render_refiner_card(r: RefinerActivity) -> str:
    repo = Path(__file__).resolve().parent.parent
    loader = ChoiceLoader(
        [
            FileSystemLoader(repo / "tests" / "templates"),
            FileSystemLoader(repo / "app" / "templates"),
        ]
    )
    env = Environment(loader=loader, autoescape=True)
    tpl = env.get_template("test_refiner_activity_macro_wrapper.html")
    now = datetime(2026, 1, 1, 12, 0, 0)
    row = refiner_activity_display_row(r, "UTC", now)
    return tpl.render(e=row)


def _ctx(**kwargs: object) -> str:
    import json

    base = {
        "v": 1,
        "media_title": "",
        "refiner_title": "",
        "refiner_year": "",
        "audio_before": "",
        "audio_after": "",
        "subs_before": "",
        "subs_after": "",
        "commentary_removed": False,
        "failure_reason": "",
        "dry_run": False,
        "finalized": False,
        "source_removed": False,
    }
    base.update(kwargs)
    return json.dumps(base)


@pytest.mark.parametrize(
    ("fname", "canonical"),
    (("Movie.Name.2023.mkv", "Movie Name (2023)"),),
)
def test_refiner_card_renders_media_title_first(fname: str, canonical: str) -> None:
    r = RefinerActivity(
        file_name=fname,
        status="success",
        size_before_bytes=1024,
        size_after_bytes=900,
        audio_tracks_before=2,
        audio_tracks_after=2,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(media_title=canonical, finalized=True),
    )
    html = _render_refiner_card(r)
    pos_title = html.find("activity-refiner-media-title")
    pos_outcome = html.find("activity-refiner-outcome-label")
    assert pos_title != -1 and pos_outcome != -1
    assert pos_title < pos_outcome
    assert canonical in html
    assert "File:" in html and fname in html
    assert "Completed" in html
    assert "Applied" in html
    assert "Before" in html and "After" in html


def test_refiner_card_filename_fallback_without_metadata() -> None:
    r = RefinerActivity(
        file_name="Some.Release.2022.1080p.mkv",
        status="success",
        size_before_bytes=100,
        size_after_bytes=90,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(finalized=True),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 0))
    assert row["refiner_media_title"]
    assert row["refiner_media_title"] != "—"
    assert row["refiner_source_file_line"] is None


def test_refiner_card_shows_before_after_when_comparison_enabled() -> None:
    r = RefinerActivity(
        file_name="x.mkv",
        status="success",
        size_before_bytes=10_000,
        size_after_bytes=9000,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=1,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="English AAC",
            audio_after="English AAC",
            subs_before="English",
            subs_after="—",
            finalized=True,
        ),
    )
    html = _render_refiner_card(r)
    assert "activity-refiner-compare" in html
    assert "Audio" in html
    assert "Subtitles" in html
    assert "File size" in html


def test_refiner_card_no_change_skip_hides_comparison_grid() -> None:
    r = RefinerActivity(
        file_name="noop.mkv",
        status="skipped",
        size_before_bytes=1024,
        size_after_bytes=1024,
        audio_tracks_before=2,
        audio_tracks_after=2,
        subtitle_tracks_before=1,
        subtitle_tracks_after=1,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="English 2.0 AAC",
            audio_after="English 2.0 AAC",
            subs_before="English",
            subs_after="English",
        ),
    )
    html = _render_refiner_card(r)
    assert "No changes required" in html
    assert "activity-refiner-compare" not in html


def test_refiner_card_dry_run_projected_changes_not_no_changes_required() -> None:
    r = RefinerActivity(
        file_name="preview.mkv",
        status="skipped",
        size_before_bytes=1024,
        size_after_bytes=1024,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=2,
        subtitle_tracks_after=2,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="Eng",
            audio_after="Eng",
            subs_before="English · French",
            subs_after="English",
            dry_run=True,
        ),
    )
    html = _render_refiner_card(r)
    assert "Dry run" in html
    assert "Preview only" in html
    assert "No changes required" not in html
    assert "activity-refiner-compare" in html
    assert "Before" in html and "After" in html


def test_refiner_row_dict_true_no_change_consistent_no_comparison() -> None:
    """Logic: noop skip has no differing before/after — no comparison section."""
    r = RefinerActivity(
        file_name="same.mkv",
        status="skipped",
        size_before_bytes=100,
        size_after_bytes=100,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="A",
            audio_after="A",
            subs_before="",
            subs_after="",
        ),
    )
    now = datetime(2026, 1, 1, 12, 0, 1)
    row = refiner_activity_display_row(r, "UTC", now)
    assert row["refiner_outcome_label"] == "No changes required"
    assert row["refiner_show_comparison"] is False
    assert row["refiner_compare_rows"] == []


def test_refiner_row_dict_dry_run_with_diff_subs_has_preview_mode() -> None:
    r = RefinerActivity(
        file_name="d.mkv",
        status="skipped",
        size_before_bytes=100,
        size_after_bytes=100,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=2,
        subtitle_tracks_after=2,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            dry_run=True,
            subs_before="A · B",
            subs_after="A",
        ),
    )
    row = refiner_activity_display_row(r, "UTC", datetime(2026, 1, 1, 12, 0, 2))
    assert row["refiner_outcome_label"] == "Dry run"
    assert row["refiner_apply_mode"] == "preview"
    assert row["refiner_show_comparison"] is True
    sub = next(x for x in row["refiner_compare_rows"] if x["label"] == "Subtitles")
    assert sub["before"] != sub["after"]
