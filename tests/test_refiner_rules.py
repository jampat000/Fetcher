from __future__ import annotations

from app.refiner_rules import (
    RefinerRulesConfig,
    _CODEC_UNKNOWN_RANK,
    _audio_codec_quality_rank,
    is_commentary_audio,
    is_remux_required,
    normalize_audio_preference_mode,
    normalize_lang,
    parse_subtitle_langs_csv,
    plan_remux,
    split_streams,
)


def _cfg(**over: object) -> RefinerRulesConfig:
    base: dict[str, object] = dict(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        tertiary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="preferred_langs_quality",
    )
    base.update(over)
    return RefinerRulesConfig(**base)  # type: ignore[arg-type]


def test_normalize_lang_trims_and_lower() -> None:
    assert normalize_lang("ENG") == "eng"
    assert normalize_lang("eng-us") == "eng"


def test_normalize_audio_preference_mode_unknown_and_canonical() -> None:
    assert normalize_audio_preference_mode("best_available") == "preferred_langs_quality"
    assert normalize_audio_preference_mode("prefer_surround") == "preferred_langs_quality"
    assert normalize_audio_preference_mode("highest_quality") == "preferred_langs_quality"
    assert normalize_audio_preference_mode("track_order") == "preferred_langs_quality"
    assert normalize_audio_preference_mode("prefer_default") == "preferred_langs_quality"
    assert normalize_audio_preference_mode("preferred_langs_quality") == "preferred_langs_quality"
    assert normalize_audio_preference_mode("preferred_langs_strict") == "preferred_langs_strict"
    assert normalize_audio_preference_mode("quality_all_languages") == "quality_all_languages"
    assert normalize_audio_preference_mode(None) == "preferred_langs_quality"
    assert normalize_audio_preference_mode("  ") == "preferred_langs_quality"


def test_audio_codec_quality_rank_explicit_order() -> None:
    assert _audio_codec_quality_rank("flac") < _audio_codec_quality_rank("aac")
    assert _audio_codec_quality_rank("aac") < _audio_codec_quality_rank("mp3")


def test_audio_codec_quality_rank_unknown_is_deterministic() -> None:
    u = _CODEC_UNKNOWN_RANK
    assert _audio_codec_quality_rank(None) == u
    assert _audio_codec_quality_rank("not_a_real_codec_xyz") == u
    assert u > _audio_codec_quality_rank("mp3")


def test_parse_subtitle_langs_csv_order_unique() -> None:
    assert parse_subtitle_langs_csv("eng, spa, eng") == ("eng", "spa")


def test_single_winning_track_retained_primary_beats_secondary_tier() -> None:
    """English tier has candidates; Japanese is ignored for selection."""
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "flac", "channels": 6, "tags": {"language": "jpn"}, "disposition": {}},
    ]
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=[],
        config=_cfg(secondary_audio_lang="jpn"),
    )
    assert plan is not None
    assert len(plan.audio) == 1
    assert plan.audio[0].input_index == 1
    assert plan.audio[0].lang_label == "eng"
    assert any("Ignored" in n for n in plan.audio_selection_notes)
    assert plan.default_audio_output_index == 0


def test_secondary_tier_used_when_primary_missing() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 2, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "jpn"}, "disposition": {}},
    ]
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=[],
        config=_cfg(secondary_audio_lang="jpn"),
    )
    assert plan is not None
    assert len(plan.audio) == 1
    assert plan.audio[0].lang_label == "jpn"


def test_tertiary_tier_used_when_primary_secondary_missing() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 3, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "spa"}, "disposition": {}},
    ]
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=[],
        config=_cfg(tertiary_audio_lang="spa"),
    )
    assert plan is not None
    assert plan.audio[0].lang_label == "spa"


def test_fallback_when_no_preferred_language_matches() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "spa"}, "disposition": {}},
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg(secondary_audio_lang="jpn"))
    assert plan is not None
    assert plan.audio[0].lang_label == "spa"
    assert any("Fell back" in n for n in plan.audio_selection_notes)


def test_preferred_langs_strict_no_primary_returns_none() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "flac", "channels": 6, "tags": {"language": "jpn"}, "disposition": {}},
    ]
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=[],
        config=_cfg(audio_preference_mode="preferred_langs_strict", secondary_audio_lang="jpn"),
    )
    assert plan is None


def test_quality_all_languages_ignores_tier_priority() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "flac", "channels": 6, "tags": {"language": "jpn"}, "disposition": {}},
    ]
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=[],
        config=_cfg(audio_preference_mode="quality_all_languages"),
    )
    assert plan is not None
    assert plan.audio[0].input_index == 2


def test_plan_removes_commentary_when_enabled() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}},
        {
            "index": 2,
            "codec_type": "audio",
            "tags": {"language": "eng", "title": "Director commentary"},
            "disposition": {},
        },
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg(remove_commentary=True))
    assert plan is not None
    assert len(plan.audio) == 1
    assert plan.audio[0].input_index == 1
    assert any("commentary" in x.lower() for x in plan.removed_audio)
    assert any("Excluded commentary" in n for n in plan.audio_selection_notes)


def test_commentary_ranks_below_program_when_kept() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {
            "index": 2,
            "codec_type": "audio",
            "codec_name": "flac",
            "channels": 6,
            "tags": {"language": "eng", "title": "Director commentary"},
            "disposition": {},
        },
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert plan.audio[0].input_index == 1


def test_is_commentary_audio_title_match() -> None:
    s = {"tags": {"title": "English Commentary"}}
    assert is_commentary_audio(s) is True
    assert is_commentary_audio({"tags": {"title": "Main"}}) is False


def test_subtitle_remove_all() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [{"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}}]
    subs = [{"index": 3, "codec_type": "subtitle", "tags": {"language": "spa"}, "disposition": {}}]
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=subs,
        config=_cfg(subtitle_langs=("eng",)),
    )
    assert plan is not None
    assert plan.subtitles == []
    assert "spa" in plan.removed_subtitles


def test_subtitle_keep_selected_and_order() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [{"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}}]
    subs = [
        {"index": 4, "codec_type": "subtitle", "tags": {"language": "jpn"}, "disposition": {}},
        {"index": 3, "codec_type": "subtitle", "tags": {"language": "eng"}, "disposition": {}},
    ]
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=subs,
        config=_cfg(subtitle_mode="keep_selected", subtitle_langs=("jpn", "eng")),
    )
    assert plan is not None
    assert [t.input_index for t in plan.subtitles] == [4, 3]


def test_is_remux_required_detects_audio_default_change() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {"default": 1}},
        {"index": 2, "codec_type": "audio", "tags": {"language": "jpn"}, "disposition": {"default": 0}},
    ]
    subs: list[dict] = []
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=subs,
        config=_cfg(secondary_audio_lang="jpn", default_audio_slot="secondary"),
    )
    assert plan is not None
    assert is_remux_required(plan, audio, subs) is True


def test_split_streams_groups_by_codec_type() -> None:
    probe = {
        "streams": [
            {"index": 1, "codec_type": "audio"},
            {"index": 0, "codec_type": "video"},
        ]
    }
    v, a, s = split_streams(probe)
    assert [x["index"] for x in v] == [0]
    assert [x["index"] for x in a] == [1]


def test_within_tier_prefers_higher_channel_count() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "aac", "channels": 6, "tags": {"language": "eng"}, "disposition": {}},
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert plan.audio[0].input_index == 2


def test_within_tier_known_channels_before_unknown() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 0, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert plan.audio[0].input_index == 2


def test_within_tier_prefers_better_codec_when_channels_match() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "mp3", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "flac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert plan.audio[0].input_index == 2


def test_within_tier_prefers_higher_bitrate_when_codec_channels_match() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {
            "index": 1,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 2,
            "bit_rate": 128000,
            "tags": {"language": "eng"},
            "disposition": {},
        },
        {
            "index": 2,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 2,
            "bit_rate": 256000,
            "tags": {"language": "eng"},
            "disposition": {},
        },
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert plan.audio[0].input_index == 2


def test_within_tier_channels_outrank_codec() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 6, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "flac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert plan.audio[0].input_index == 1


def test_within_tier_default_disposition_is_weak_tiebreak() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {
            "index": 1,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 2,
            "bit_rate": 192000,
            "tags": {"language": "eng"},
            "disposition": {"default": 0},
        },
        {
            "index": 2,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 2,
            "bit_rate": 192000,
            "tags": {"language": "eng"},
            "disposition": {"default": 1},
        },
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert plan.audio[0].input_index == 2


def test_track_order_final_tiebreak() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {
            "index": 5,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 2,
            "bit_rate": 128000,
            "tags": {"language": "eng"},
            "disposition": {},
        },
        {
            "index": 9,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 2,
            "bit_rate": 128000,
            "tags": {"language": "eng"},
            "disposition": {},
        },
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert plan.audio[0].input_index == 5


def test_non_selected_tracks_in_removed_audio() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
    ]
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=_cfg())
    assert plan is not None
    assert len(plan.removed_audio) >= 1
    assert any("removed" in x.lower() for x in plan.removed_audio)


def test_deterministic_repeated_plan() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "aac", "channels": 6, "tags": {"language": "eng"}, "disposition": {}},
    ]
    cfg = _cfg()
    p1 = plan_remux(video=video, audio=audio, subtitles=[], config=cfg)
    p2 = plan_remux(video=video, audio=audio, subtitles=[], config=cfg)
    assert p1 is not None and p2 is not None
    assert p1.audio[0].input_index == p2.audio[0].input_index


def test_unknown_audio_preference_mode_string_uses_default_tiered_policy() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "mp3", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "flac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
    ]
    plan = plan_remux(
        video=video,
        audio=audio,
        subtitles=[],
        config=_cfg(audio_preference_mode="prefer_lossless"),  # type: ignore[arg-type]
    )
    assert plan is not None
    assert plan.audio[0].input_index == 2

