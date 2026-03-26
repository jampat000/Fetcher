from __future__ import annotations

from app.stream_manager_rules import (
    StreamManagerRulesConfig,
    is_commentary_audio,
    is_remux_required,
    normalize_lang,
    parse_subtitle_langs_csv,
    plan_remux,
    split_streams,
)


def test_normalize_lang_trims_and_lower() -> None:
    assert normalize_lang("ENG") == "eng"
    assert normalize_lang("eng-us") == "eng"


def test_parse_subtitle_langs_csv_order_unique() -> None:
    assert parse_subtitle_langs_csv("eng, spa, eng") == ("eng", "spa")


def test_plan_audio_ordering_primary_before_secondary() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 2, "codec_type": "audio", "tags": {"language": "jpn"}, "disposition": {}},
        {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}},
    ]
    subs: list[dict] = []
    cfg = StreamManagerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="jpn",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="prefer_surround",
    )
    plan = plan_remux(video=video, audio=audio, subtitles=subs, config=cfg)
    assert plan is not None
    assert [t.input_index for t in plan.audio] == [1, 2]
    assert plan.default_audio_output_index == 0


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
    subs: list[dict] = []
    cfg = StreamManagerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=True,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="prefer_surround",
    )
    plan = plan_remux(video=video, audio=audio, subtitles=subs, config=cfg)
    assert plan is not None
    assert len(plan.audio) == 1
    assert plan.audio[0].input_index == 1
    assert any("commentary" in x for x in plan.removed_audio)


def test_is_commentary_audio_title_match() -> None:
    s = {"tags": {"title": "English Commentary"}}
    assert is_commentary_audio(s) is True
    assert is_commentary_audio({"tags": {"title": "Main"}}) is False


def test_subtitle_remove_all() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [{"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {}}]
    subs = [{"index": 3, "codec_type": "subtitle", "tags": {"language": "spa"}, "disposition": {}}]
    cfg = StreamManagerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=("eng",),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="prefer_surround",
    )
    plan = plan_remux(video=video, audio=audio, subtitles=subs, config=cfg)
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
    cfg = StreamManagerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="keep_selected",
        subtitle_langs=("jpn", "eng"),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="prefer_surround",
    )
    plan = plan_remux(video=video, audio=audio, subtitles=subs, config=cfg)
    assert plan is not None
    assert [t.input_index for t in plan.subtitles] == [4, 3]


def test_is_remux_required_detects_audio_default_change() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "tags": {"language": "eng"}, "disposition": {"default": 1}},
        {"index": 2, "codec_type": "audio", "tags": {"language": "jpn"}, "disposition": {"default": 0}},
    ]
    subs: list[dict] = []
    cfg = StreamManagerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="jpn",
        default_audio_slot="secondary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="prefer_surround",
    )
    plan = plan_remux(video=video, audio=audio, subtitles=subs, config=cfg)
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


def test_audio_preference_mode_prefer_surround() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {
            "index": 1,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 2,
            "tags": {"language": "eng"},
            "disposition": {},
        },
        {
            "index": 2,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 6,
            "tags": {"language": "eng"},
            "disposition": {},
        },
    ]
    cfg = StreamManagerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="prefer_surround",
    )
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=cfg)
    assert plan is not None
    assert [t.input_index for t in plan.audio] == [2, 1]


def test_audio_preference_mode_prefer_stereo() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 6, "tags": {"language": "eng"}, "disposition": {}},
        {"index": 2, "codec_type": "audio", "codec_name": "aac", "channels": 2, "tags": {"language": "eng"}, "disposition": {}},
    ]
    cfg = StreamManagerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="prefer_stereo",
    )
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=cfg)
    assert plan is not None
    assert [t.input_index for t in plan.audio] == [2, 1]


def test_audio_preference_mode_prefer_lossless() -> None:
    video = [{"index": 0, "codec_type": "video"}]
    audio = [
        {
            "index": 1,
            "codec_type": "audio",
            "codec_name": "aac",
            "channels": 6,
            "tags": {"language": "eng"},
            "disposition": {},
        },
        {
            "index": 2,
            "codec_type": "audio",
            "codec_name": "flac",
            "channels": 2,
            "tags": {"language": "eng"},
            "disposition": {},
        },
    ]
    cfg = StreamManagerRulesConfig(
        primary_audio_lang="eng",
        secondary_audio_lang="",
        default_audio_slot="primary",
        remove_commentary=False,
        subtitle_mode="remove_all",
        subtitle_langs=(),
        preserve_forced_subs=True,
        preserve_default_subs=True,
        audio_preference_mode="prefer_lossless",
    )
    plan = plan_remux(video=video, audio=audio, subtitles=[], config=cfg)
    assert plan is not None
    assert [t.input_index for t in plan.audio] == [2, 1]
