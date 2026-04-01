"""Refiner activity compare row: compact subtitle lists."""

from __future__ import annotations

from app.refiner_activity_row import compact_subtitle_line_for_compare


def test_compact_subtitle_short_list_unchanged() -> None:
    s = "English · Spanish"
    assert compact_subtitle_line_for_compare(s) == s


def test_compact_subtitle_empty() -> None:
    assert compact_subtitle_line_for_compare("") == "—"
    assert compact_subtitle_line_for_compare("   ") == "—"


def test_compact_subtitle_many_tracks_summarized() -> None:
    long = " · ".join([f"Lang {i}" for i in range(12)])
    out = compact_subtitle_line_for_compare(long)
    assert "12 tracks" in out
    assert "(+9 more)" in out
    assert "Lang 0" in out and "Lang 2" in out


def test_compact_subtitle_no_separator_long_string_truncated() -> None:
    raw = "X" * 120
    out = compact_subtitle_line_for_compare(raw)
    assert out.endswith("…")
    assert len(out) <= 96
