"""ffprobe-derived media identity and activity title resolution."""

from __future__ import annotations

import json

from app.refiner_media_identity import (
    MediaIdentity,
    conservative_filename_display,
    provisional_media_title_before_probe,
    resolve_activity_card_title,
    should_show_raw_source_filename,
)


def _parse_ctx(**kwargs: str) -> dict:
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
    return json.loads(json.dumps(base))


def test_media_identity_from_ffprobe_title_and_date() -> None:
    probe = {
        "format": {
            "tags": {
                "title": "The Grifters",
                "DATE_RELEASED": "1990-01-01",
            }
        }
    }
    ident = MediaIdentity.from_ffprobe(probe)
    assert ident.media_title == "The Grifters (1990)"
    assert ident.refiner_title == "The Grifters"
    assert ident.refiner_year == "1990"
    assert ident.persisted_media_title_column() == "The Grifters (1990)"
    snap = ident.snapshot_identity_fields()
    assert snap["media_title"] == "The Grifters (1990)"


def test_media_identity_title_already_has_year_parens() -> None:
    probe = {"format": {"tags": {"title": "The Grifters (1990)"}}}
    ident = MediaIdentity.from_ffprobe(probe)
    assert ident.media_title == "The Grifters (1990)"


def test_media_identity_show_tag() -> None:
    probe = {"format": {"tags": {"show": "Severance", "date": "2022"}}}
    ident = MediaIdentity.from_ffprobe(probe)
    assert ident.refiner_title == "Severance"
    assert ident.media_title == "Severance (2022)"


def test_resolve_prefers_orm_media_title_over_filename() -> None:
    ctx = _parse_ctx()
    t = resolve_activity_card_title(
        "The.Grifters.1990.1080p.BluRay.x264.mkv",
        ctx,
        orm_media_title="The Grifters (1990)",
    )
    assert t == "The Grifters (1990)"


def test_resolve_title_year_from_context_without_media_title() -> None:
    ctx = _parse_ctx(
        refiner_title="The Grifters",
        refiner_year="1990",
    )
    t = resolve_activity_card_title("release-name.mkv", ctx, orm_media_title="")
    assert t == "The Grifters (1990)"


def test_resolve_conservative_filename_when_no_metadata() -> None:
    ctx = {}
    t = resolve_activity_card_title("Movie.Name.2023.1080p.mkv", ctx, orm_media_title="")
    assert t == "Movie Name 2023 1080p"
    # trailing release token may strip 1080p depending on pattern - check non-empty and not raw dots
    assert "." not in t


def test_resolve_never_empty_uses_em_dash_for_missing_file_name() -> None:
    assert resolve_activity_card_title("", {}, orm_media_title="") == "—"


def test_provisional_media_title_before_probe_non_empty() -> None:
    t = provisional_media_title_before_probe("The.Matrix.1999.1080p.BluRay.x264.mkv")
    assert t
    assert "." not in t


def test_should_show_raw_file_when_upstream_differs() -> None:
    assert should_show_raw_source_filename(
        display_title="The Grifters (1990)",
        file_name="The.Grifters.1990.mkv",
        ctx={"media_title": "The Grifters (1990)"},
        orm_media_title="",
    )
    assert not should_show_raw_source_filename(
        display_title="The Grifters (1990)",
        file_name="The.Grifters.1990.mkv",
        ctx={},
        orm_media_title="",
    )


def test_should_hide_raw_line_when_orm_is_only_filename_provisional() -> None:
    raw = "Movie.Name.2023.1080p.mkv"
    prov = provisional_media_title_before_probe(raw)
    assert not should_show_raw_source_filename(
        display_title=prov,
        file_name=raw,
        ctx={},
        orm_media_title=prov,
    )


def test_conservative_filename_display_basic() -> None:
    assert conservative_filename_display("foo_bar.mkv") == "foo bar"
