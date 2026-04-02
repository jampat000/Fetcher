"""Unit tests for external subtitle sidecar preservation (keep_selected)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from app.refiner_rules import RefinerRulesConfig
from app.refiner_subtitle_sidecars import (
    discover_matching_external_subtitle_paths,
    preserve_external_subtitle_sidecars_if_configured,
    should_preserve_external_subtitle_sidecars,
)


def _cfg(*, mode: str) -> RefinerRulesConfig:
    return RefinerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        tertiary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode=mode,  # type: ignore[arg-type]
        subtitle_langs=("eng",),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="preferred_langs_quality",
    )


def test_should_preserve_only_keep_selected() -> None:
    assert should_preserve_external_subtitle_sidecars(_cfg(mode="keep_selected")) is True
    assert should_preserve_external_subtitle_sidecars(_cfg(mode="remove_all")) is False


def test_discover_same_stem_srt_and_tagged_variants(tmp_path: Path) -> None:
    job = tmp_path / "j"
    job.mkdir()
    media = job / "Movie.Name.2024.mkv"
    media.write_bytes(b"m")
    (job / "Movie.Name.2024.srt").write_text("a", encoding="utf-8")
    (job / "Movie.Name.2024.en.srt").write_text("b", encoding="utf-8")
    (job / "Movie.Name.2024.eng.forced.ass").write_text("c", encoding="utf-8")
    (job / "Movie.Name.2024.forced.idx").write_bytes(b"\x00")
    (job / "Movie.Name.2024.forced.sub").write_bytes(b"\x00")
    found = discover_matching_external_subtitle_paths(media)
    names = [p.name for p in found]
    assert names == [
        "Movie.Name.2024.en.srt",
        "Movie.Name.2024.eng.forced.ass",
        "Movie.Name.2024.forced.idx",
        "Movie.Name.2024.forced.sub",
        "Movie.Name.2024.srt",
    ]


def test_discover_excludes_unrelated_sidecars(tmp_path: Path) -> None:
    job = tmp_path / "j"
    job.mkdir()
    media = job / "Movie.Name.2024.mkv"
    media.write_bytes(b"m")
    (job / "Other.Release.srt").write_text("x", encoding="utf-8")
    (job / "Unrelated.en.srt").write_text("y", encoding="utf-8")
    (job / "Movie.Name.2024.nfo").write_text("nfo", encoding="utf-8")
    assert discover_matching_external_subtitle_paths(media) == []


def test_discover_excludes_non_subtitle_suffix(tmp_path: Path) -> None:
    job = tmp_path / "j"
    job.mkdir()
    media = job / "Show.S01E01.mkv"
    media.write_bytes(b"m")
    (job / "Show.S01E01.jpg").write_bytes(b"img")
    (job / "Show.S01E01.en.txt").write_text("t", encoding="utf-8")
    assert discover_matching_external_subtitle_paths(media) == []


def test_preserve_remove_all_mode_no_ops(tmp_path: Path) -> None:
    job = tmp_path / "j"
    job.mkdir()
    media = job / "a.mkv"
    media.write_bytes(b"m")
    (job / "a.srt").write_text("s", encoding="utf-8")
    out = tmp_path / "out"
    out.mkdir()
    dest = out / "a.mkv"
    dest.write_bytes(b"pending")
    n = preserve_external_subtitle_sidecars_if_configured(
        source_media_path=media,
        destination_media_path=dest,
        cfg=_cfg(mode="remove_all"),
    )
    assert n == []
    assert (job / "a.srt").exists()
    assert not (out / "a.srt").exists()


def test_preserve_collision_strict_failure(tmp_path: Path) -> None:
    job = tmp_path / "j"
    job.mkdir()
    media = job / "a.mkv"
    media.write_bytes(b"m")
    (job / "a.srt").write_text("s", encoding="utf-8")
    out = tmp_path / "out"
    out.mkdir()
    (out / "a.srt").write_text("existing", encoding="utf-8")
    dest = out / "a.mkv"
    dest.write_bytes(b"v")
    with pytest.raises(RuntimeError, match="already has a file named"):
        preserve_external_subtitle_sidecars_if_configured(
            source_media_path=media,
            destination_media_path=dest,
            cfg=_cfg(mode="keep_selected"),
        )


def test_preserve_anchors_sidecars_to_destination_stem(tmp_path: Path) -> None:
    job = tmp_path / "j"
    job.mkdir()
    media = job / "Movie.Name.2024.mkv"
    media.write_bytes(b"m")
    (job / "Movie.Name.2024.en.srt").write_text("s", encoding="utf-8")
    (job / "Movie.Name.2024.forced.idx").write_bytes(b"\x01")
    out = tmp_path / "out"
    out.mkdir()
    dest = out / "Movie.Name.2024.REMUX.mkv"
    dest.write_bytes(b"v")
    copied = preserve_external_subtitle_sidecars_if_configured(
        source_media_path=media,
        destination_media_path=dest,
        cfg=_cfg(mode="keep_selected"),
    )
    assert copied == ["Movie.Name.2024.en.srt", "Movie.Name.2024.forced.idx"]
    assert (out / "Movie.Name.2024.REMUX.en.srt").read_text(encoding="utf-8") == "s"
    assert (out / "Movie.Name.2024.REMUX.forced.idx").read_bytes() == b"\x01"


def test_preserve_copy_rollback_on_partial_failure(tmp_path: Path) -> None:
    job = tmp_path / "j"
    job.mkdir()
    media = job / "a.mkv"
    media.write_bytes(b"m")
    (job / "a.srt").write_text("s", encoding="utf-8")
    (job / "a.en.srt").write_text("s2", encoding="utf-8")
    out = tmp_path / "out"
    out.mkdir()
    dest = out / "a.mkv"
    dest.write_bytes(b"v")

    real_copy2 = __import__("shutil").copy2
    calls: list[str] = []

    def flaky_copy2(src: Path, dst: Path, *a, **k):  # noqa: ANN001, ANN002
        calls.append(src.name)
        if len(calls) >= 2:
            raise OSError("disk full")
        return real_copy2(src, dst, *a, **k)

    with patch("app.refiner_subtitle_sidecars.shutil.copy2", flaky_copy2):
        with pytest.raises(RuntimeError, match="preservation failed while copying"):
            preserve_external_subtitle_sidecars_if_configured(
                source_media_path=media,
                destination_media_path=dest,
                cfg=_cfg(mode="keep_selected"),
            )
    assert not (out / "a.srt").exists()
    assert not (out / "a.en.srt").exists()
    assert len(calls) == 2


def test_preserve_idx_sub_pair_collision_fails_without_partial_copy(tmp_path: Path) -> None:
    job = tmp_path / "j"
    job.mkdir()
    media = job / "Film.mkv"
    media.write_bytes(b"m")
    (job / "Film.forced.idx").write_bytes(b"\x01")
    (job / "Film.forced.sub").write_bytes(b"\x02")
    out = tmp_path / "out"
    out.mkdir()
    dest = out / "Film.REMUX.mkv"
    dest.write_bytes(b"v")
    (out / "Film.REMUX.forced.idx").write_bytes(b"exists")
    with pytest.raises(RuntimeError, match="already has a file named"):
        preserve_external_subtitle_sidecars_if_configured(
            source_media_path=media,
            destination_media_path=dest,
            cfg=_cfg(mode="keep_selected"),
        )
    assert not (out / "Film.REMUX.forced.sub").exists()

