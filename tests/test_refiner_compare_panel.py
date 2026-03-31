"""Refiner compare presentation: helpers, enriched row dicts, decision-summary template."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from jinja2 import ChoiceLoader, Environment, FileSystemLoader

from app.models import RefinerActivity
from app.refiner_activity_row import build_refiner_activity_row_dict
from app.refiner_compare_present import (
    MAX_REMOVED_GROUPS_VISIBLE,
    build_refiner_compare_sections,
    build_summarized_removed_items,
    compare_row_change_state,
    group_ordered_counts,
    is_absent_compare_token,
    split_joined_display_line,
)
from app.web_common import refiner_activity_display_row


def _ctx(**kwargs: object) -> str:
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
        "no_change_bullets": [],
    }
    base.update(kwargs)
    return json.dumps(base)


def test_absent_compare_token_treats_none_and_dash() -> None:
    assert is_absent_compare_token("")
    assert is_absent_compare_token("None")
    assert is_absent_compare_token("—")
    assert not is_absent_compare_token("English AAC")


def test_split_joined_display_line_splits_probe_style_blob() -> None:
    s = "English 2.0 AAC · Japanese 2.0 AAC · English 5.1 DTS"
    parts = split_joined_display_line(s)
    assert len(parts) == 3
    assert "Japanese 2.0 AAC" in parts


def test_split_joined_display_line_falls_back_to_bare_middle_dot() -> None:
    parts = split_joined_display_line("English·Spanish")
    assert parts == ["English", "Spanish"]


def test_split_joined_single_line_unchanged() -> None:
    assert split_joined_display_line("English 5.1 DTS") == ["English 5.1 DTS"]


def test_group_ordered_counts_merges_duplicates_preserves_order() -> None:
    g = group_ordered_counts(["French", "French", "Spanish", "Spanish", "Arabic"])
    assert g == [("French", 2), ("Spanish", 2), ("Arabic", 1)]


def test_build_summarized_removed_items_short_list_no_expand() -> None:
    vis, more, full = build_summarized_removed_items(["English", "French"])
    assert vis == ["English", "French"]
    assert more == 0
    assert full == []


def test_build_summarized_removed_items_groups_duplicates() -> None:
    vis, more, full = build_summarized_removed_items(["French", "French", "Spanish"])
    assert vis == ["French (2)", "Spanish"]
    assert more == 0
    assert full == []


def test_build_summarized_removed_items_long_list_summary_and_expand() -> None:
    langs = [f"Lang{i}" for i in range(MAX_REMOVED_GROUPS_VISIBLE + 3)]
    vis, more, full = build_summarized_removed_items(langs)
    assert len(vis) == MAX_REMOVED_GROUPS_VISIBLE
    assert more == 3
    assert len(full) == len(langs)
    assert langs[-1] in full


def test_compare_row_change_state_file_size_unchanged_vs_changed() -> None:
    assert (
        compare_row_change_state(label="File size", before="10 KiB", after="10 KiB", sb=10240, sa=10240)
        == "unchanged"
    )
    assert (
        compare_row_change_state(label="File size", before="10 KiB", after="9 KiB", sb=10240, sa=9216)
        == "changed"
    )


def test_compare_row_change_state_subtitles_removed_vs_unchanged() -> None:
    assert (
        compare_row_change_state(label="Subtitles", before="English", after="None", sb=0, sa=0)
        == "removed"
    )
    assert (
        compare_row_change_state(label="Subtitles", before="English", after="English", sb=0, sa=0)
        == "unchanged"
    )


def test_compare_row_change_state_audio_added_track_display() -> None:
    assert (
        compare_row_change_state(label="Audio", before="None", after="1 track(s)", sb=0, sa=0) == "added"
    )


def test_success_compare_rows_include_change_and_size_delta() -> None:
    r = RefinerActivity(
        file_name="x.mkv",
        status="success",
        size_before_bytes=10_000_000,
        size_after_bytes=9_000_000,
        audio_tracks_before=2,
        audio_tracks_after=2,
        subtitle_tracks_before=2,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="English AAC",
            audio_after="English AAC",
            subs_before="English · French",
            subs_after="",
            finalized=True,
        ),
    )
    row = build_refiner_activity_row_dict(r, "UTC", datetime(2026, 1, 1, 12, 0, 0))
    rows = row["refiner_compare_rows"]
    assert len(rows) == 3
    audio = next(x for x in rows if x["label"] == "Audio")
    assert audio["change"] == "unchanged"
    assert audio["before"] == audio["after"]
    sub = next(x for x in rows if x["label"] == "Subtitles")
    assert sub["after"] == "—"
    assert sub["change"] == "removed"
    sz = next(x for x in rows if x["label"] == "File size")
    assert sz["change"] == "changed"
    assert sz["size_delta"] is not None
    assert sz["size_delta"].startswith("Δ")


def test_compare_sections_audio_removed_groups_duplicate_codecs() -> None:
    ctx = {
        "audio_before": "English 2.0 FLAC · English 2.0 FLAC · English 5.1 DTS",
        "audio_after": "English 5.1 DTS",
        "subs_before": "",
        "subs_after": "",
    }
    secs = build_refiner_compare_sections(
        ctx=ctx,
        sb=1,
        sa=1,
        failed=False,
        include_audio_subs=True,
        ab=3,
        aa=1,
        sbb=0,
        sba=0,
    )
    audio = next(s for s in secs if s["kind"] == "audio")
    assert audio["secondary_heading"] == "Removed (2 tracks)"
    assert audio["secondary_items"] == ["English 2.0 FLAC (2)"]
    assert audio["secondary_more_tracks"] == 0
    assert audio["secondary_expand_items"] == []


def test_compare_sections_multi_audio_removed_list() -> None:
    ctx = {
        "audio_before": "English 2.0 FLAC · English 2.0 AC-3 · English 5.1 DTS",
        "audio_after": "English 5.1 DTS",
        "subs_before": "",
        "subs_after": "",
    }
    secs = build_refiner_compare_sections(
        ctx=ctx,
        sb=10_000_000_000,
        sa=9_000_000_000,
        failed=False,
        include_audio_subs=True,
        ab=3,
        aa=1,
        sbb=0,
        sba=0,
    )
    audio = next(s for s in secs if s["kind"] == "audio")
    assert audio["primary_label"] == "Selected"
    assert "English 5.1 DTS" in audio["primary_lines"]
    assert audio["secondary_heading"] == "Removed (2 tracks)"
    assert set(audio["secondary_items"]) == {"English 2.0 FLAC", "English 2.0 AC-3"}
    assert audio["secondary_more_tracks"] == 0


def test_compare_sections_subtitles_explicit_removed_none_kept() -> None:
    ctx = {
        "audio_before": "",
        "audio_after": "",
        "subs_before": "English · Spanish",
        "subs_after": "None",
    }
    secs = build_refiner_compare_sections(
        ctx=ctx,
        sb=1000,
        sa=1000,
        failed=False,
        include_audio_subs=True,
        ab=1,
        aa=1,
        sbb=2,
        sba=0,
    )
    sub = next(s for s in secs if s["kind"] == "subtitles")
    assert "All subtitles removed" in sub["primary_lines"]
    assert sub["secondary_heading"] == "Removed (2 tracks)"
    assert "English" in sub["secondary_items"] and "Spanish" in sub["secondary_items"]
    assert sub["secondary_more_tracks"] == 0
    assert sub["secondary_expand_items"] == []


def test_compare_sections_subtitles_long_removal_summary_and_expand_payload() -> None:
    langs = [f"L{i}" for i in range(MAX_REMOVED_GROUPS_VISIBLE + 5)]
    blob = " · ".join(langs)
    ctx = {
        "audio_before": "",
        "audio_after": "",
        "subs_before": blob,
        "subs_after": "",
    }
    secs = build_refiner_compare_sections(
        ctx=ctx,
        sb=1,
        sa=1,
        failed=False,
        include_audio_subs=True,
        ab=1,
        aa=1,
        sbb=len(langs),
        sba=0,
    )
    sub = next(s for s in secs if s["kind"] == "subtitles")
    assert sub["secondary_heading"] == f"Removed ({len(langs)} tracks)"
    assert len(sub["secondary_items"]) == MAX_REMOVED_GROUPS_VISIBLE
    assert sub["secondary_more_tracks"] == 5
    assert len(sub["secondary_expand_items"]) == len(langs)


def test_compare_sections_file_size_final_and_saved() -> None:
    ctx: dict = {"audio_before": "", "audio_after": "", "subs_before": "", "subs_after": ""}
    secs = build_refiner_compare_sections(
        ctx=ctx,
        sb=100_000_000,
        sa=90_000_000,
        failed=False,
        include_audio_subs=False,
        ab=0,
        aa=0,
        sbb=0,
        sba=0,
    )
    assert len(secs) == 1
    fs = secs[0]
    assert fs["kind"] == "file_size"
    assert fs["primary_label"] == "Final"
    assert fs["secondary_heading"] == "Saved"
    assert any("saved" in x.lower() for x in fs["secondary_items"])


def test_compare_sections_minimal_payload_safe() -> None:
    r = RefinerActivity(
        file_name="y.mkv",
        status="success",
        size_before_bytes=500,
        size_after_bytes=500,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(finalized=True),
    )
    row = build_refiner_activity_row_dict(r, "UTC", datetime(2026, 1, 1, 12, 0, 0))
    rows = row["refiner_compare_rows"]
    secs = row["refiner_compare_sections"]
    for line in rows:
        assert "label" in line and "before" in line and "after" in line
        assert line.get("change") in ("unchanged", "changed", "removed", "added", "unknown")
        assert "size_delta" in line
    assert len(secs) >= 1
    assert any(s["kind"] == "file_size" for s in secs)


def test_compare_sections_unchanged_audio_reassuring_note() -> None:
    ctx = {
        "audio_before": "English 5.1 DTS",
        "audio_after": "English 5.1 DTS",
        "subs_before": "",
        "subs_after": "",
    }
    secs = build_refiner_compare_sections(
        ctx=ctx,
        sb=100,
        sa=100,
        failed=False,
        include_audio_subs=True,
        ab=1,
        aa=1,
        sbb=0,
        sba=0,
    )
    audio = next(s for s in secs if s["kind"] == "audio")
    assert audio["variant"] == "unchanged"
    assert audio["note"] == "No changes needed."


def test_compare_rows_safe_when_context_strings_missing() -> None:
    r = RefinerActivity(
        file_name="y.mkv",
        status="success",
        size_before_bytes=500,
        size_after_bytes=500,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=0,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(finalized=True),
    )
    row = build_refiner_activity_row_dict(r, "UTC", datetime(2026, 1, 1, 12, 0, 0))
    rows = row["refiner_compare_rows"]
    for line in rows:
        assert "label" in line and "before" in line and "after" in line
        assert line.get("change") in ("unchanged", "changed", "removed", "added", "unknown")
        assert "size_delta" in line


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
    erow = refiner_activity_display_row(r, "UTC", now)
    return tpl.render(e=erow)


def test_template_long_removed_subtitles_shows_more_and_full_list_details() -> None:
    langs = [f"L{i}" for i in range(MAX_REMOVED_GROUPS_VISIBLE + 3)]
    r = RefinerActivity(
        file_name="many-subs.mkv",
        status="success",
        size_before_bytes=100_000,
        size_after_bytes=90_000,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=len(langs),
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="Eng",
            audio_after="Eng",
            subs_before=" · ".join(langs),
            subs_after="",
            finalized=True,
        ),
    )
    html = _render_refiner_card(r)
    assert "All subtitles removed" in html
    assert "+ 3 more" in html
    assert "Show full list" in html
    assert "activity-refiner-compare-expand" in html


def test_template_renders_decision_summary_panel() -> None:
    r = RefinerActivity(
        file_name="z.mkv",
        status="success",
        size_before_bytes=100_000,
        size_after_bytes=90_000,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=1,
        subtitle_tracks_after=0,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=_ctx(
            audio_before="Eng",
            audio_after="Eng",
            subs_before="English",
            subs_after="",
            finalized=True,
        ),
    )
    html = _render_refiner_card(r)
    assert "activity-refiner-compare-panel--summary" in html
    assert "activity-refiner-compare-section" in html
    assert "activity-refiner-compare-section--subtitles" in html
    assert "Removed (1 track" in html
    assert "All subtitles removed" in html
    assert "activity-refiner-compare-section--file_size" in html
    assert "Final" in html
    assert "saved" in html.lower()
