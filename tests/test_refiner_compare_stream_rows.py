"""Refiner compare rows: expandable audio/subtitle cells."""

from __future__ import annotations

import json
from datetime import datetime

from app.models import RefinerActivity
from app.refiner_activity_row import _stream_compare_row, build_refiner_activity_row_dict
def test_stream_compare_row_long_audio_has_full_keys() -> None:
    long = " · ".join([f"Lang {i}" for i in range(10)])
    ctx = {
        "audio_before": long,
        "audio_after": long,
        "subs_before": "",
        "subs_after": "",
    }
    row = _stream_compare_row("Audio", ctx, 2, 2)
    assert row["label"] == "Audio"
    assert "10 tracks" in row["before"]
    assert row.get("before_full")
    assert row["before_full"] == long
    assert row.get("after_full") == long


def test_stream_compare_row_subtitles_removed_after() -> None:
    ctx = {
        "audio_before": "",
        "audio_after": "",
        "subs_before": "English · French",
        "subs_after": "",
    }
    row = _stream_compare_row("Subtitles", ctx, 2, 0)
    assert row["before"] != "Removed"
    assert row["after"] == "Removed"
    assert "before_full" not in row or row.get("before_full") is None


def test_refiner_success_compare_includes_expandable_subtitles() -> None:
    long = " · ".join([f"S{i}" for i in range(6)])
    r = RefinerActivity(
        file_name="x.mkv",
        status="success",
        size_before_bytes=2000,
        size_after_bytes=1000,
        audio_tracks_before=1,
        audio_tracks_after=1,
        subtitle_tracks_before=6,
        subtitle_tracks_after=6,
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        activity_context=json.dumps(
            {
                "v": 1,
                "media_title": "",
                "audio_before": "English AAC",
                "audio_after": "English AAC",
                "subs_before": long,
                "subs_after": long,
                "finalized": True,
            }
        ),
    )
    d = build_refiner_activity_row_dict(r, "UTC", datetime(2026, 1, 1, 12, 0, 1))
    sub = next(x for x in d["refiner_compare_rows"] if x["label"] == "Subtitles")
    assert sub.get("before_full") == long
    assert "6 tracks" in sub["before"]
