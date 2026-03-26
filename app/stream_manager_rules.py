from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

SubtitleMode = Literal["remove_all", "keep_selected"]
DefaultAudioSlot = Literal["primary", "secondary"]
AudioPreferenceMode = Literal[
    "best_available",
    "prefer_surround",
    "prefer_stereo",
    "prefer_lossless",
]

_MEDIA_EXTENSIONS = frozenset({".mkv", ".mp4", ".m4v", ".webm", ".avi"})


def normalize_lang(tag: str | None) -> str:
    if not tag:
        return ""
    s = tag.strip().lower()
    if not s:
        return ""
    # ISO 639-2 common in Matroska; allow 2–3 letters or 3-letter + hyphen region
    m = re.match(r"^([a-z]{2,3})(?:-[a-z0-9]+)?$", s)
    if m:
        return m.group(1)
    return s[:12]


def parse_subtitle_langs_csv(raw: str) -> tuple[str, ...]:
    parts = [normalize_lang(p) for p in (raw or "").replace("\n", ",").split(",")]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return tuple(out)


def _stream_tags(stream: dict[str, Any]) -> dict[str, str]:
    tags = stream.get("tags") or {}
    if not isinstance(tags, dict):
        return {}
    return {str(k): str(v) for k, v in tags.items()}


def _stream_disposition(stream: dict[str, Any]) -> dict[str, int]:
    disp = stream.get("disposition") or {}
    if not isinstance(disp, dict):
        return {}
    out: dict[str, int] = {}
    for k, v in disp.items():
        try:
            out[str(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return out


def is_commentary_audio(stream: dict[str, Any]) -> bool:
    tags = _stream_tags(stream)
    title = (tags.get("title") or "").lower()
    if "commentary" in title:
        return True
    # Simple alternate tag (keep narrow to avoid false positives)
    if tags.get("comment") and "commentary" in (tags.get("comment") or "").lower():
        return True
    return False


@dataclass(frozen=True)
class StreamManagerRulesConfig:
    primary_audio_lang: str
    secondary_audio_lang: str
    default_audio_slot: DefaultAudioSlot
    remove_commentary: bool
    subtitle_mode: SubtitleMode
    subtitle_langs: tuple[str, ...]
    preserve_forced_subs: bool
    preserve_default_subs: bool
    audio_preference_mode: AudioPreferenceMode


@dataclass
class PlannedTrack:
    input_index: int
    lang_label: str
    commentary: bool = False
    forced: bool = False
    default: bool = False
    channels: int = 0
    lossless: bool = False
    bitrate: int = 0
    kind: Literal["audio", "subtitle"] = "audio"


@dataclass
class RemuxPlan:
    video_indices: list[int]
    audio: list[PlannedTrack]
    subtitles: list[PlannedTrack]
    removed_audio: list[str] = field(default_factory=list)
    removed_subtitles: list[str] = field(default_factory=list)
    default_audio_output_index: int = 0


def is_remux_required(plan: RemuxPlan, audio_probe: list[dict[str, Any]], sub_probe: list[dict[str, Any]]) -> bool:
    if [t.input_index for t in plan.audio] != [int(s["index"]) for s in audio_probe]:
        return True
    if [t.input_index for t in plan.subtitles] != [int(s["index"]) for s in sub_probe]:
        return True
    old_audio_disp = [(int(s["index"]), int(_stream_disposition(s).get("default", 0))) for s in audio_probe]
    new_audio_disp = [(t.input_index, int(t.default)) for t in plan.audio]
    if old_audio_disp != new_audio_disp:
        return True
    old_sub = [
        (int(s["index"]), int(_stream_disposition(s).get("forced", 0)), int(_stream_disposition(s).get("default", 0)))
        for s in sub_probe
    ]
    new_sub = [(t.input_index, int(t.forced), int(t.default)) for t in plan.subtitles]
    if old_sub != new_sub:
        return True
    return False


def split_streams(probe: dict[str, Any]) -> tuple[list[dict], list[dict], list[dict]]:
    streams = probe.get("streams")
    if not isinstance(streams, list):
        return [], [], []
    video: list[dict] = []
    audio: list[dict] = []
    subs: list[dict] = []
    for s in streams:
        if not isinstance(s, dict):
            continue
        ct = (s.get("codec_type") or "").strip().lower()
        if ct == "video":
            video.append(s)
        elif ct == "audio":
            audio.append(s)
        elif ct == "subtitle":
            subs.append(s)
    video.sort(key=lambda x: int(x.get("index", 0)))
    audio.sort(key=lambda x: int(x.get("index", 0)))
    subs.sort(key=lambda x: int(x.get("index", 0)))
    return video, audio, subs


def _is_lossless_audio(codec_name: str | None) -> bool:
    c = (codec_name or "").strip().lower()
    return c in {"flac", "truehd", "alac", "pcm_s16le", "pcm_s24le", "pcm_s32le", "wavpack"}


def _audio_sort_key(
    track: PlannedTrack,
    *,
    allowed_langs: list[tuple[str, Literal["primary", "secondary"]]],
    preference: AudioPreferenceMode,
) -> tuple[int, int, int, int, int]:
    tier_order = {"primary": 0, "secondary": 1}
    tier = "secondary"
    for lg, name in allowed_langs:
        if lg == track.lang_label:
            tier = name
            break
    surround = 1 if track.channels >= 6 else 0
    stereo = 1 if 2 <= track.channels < 6 else 0
    lossless = 1 if track.lossless else 0
    if preference == "best_available":
        pref_tuple = (0, -surround, -lossless)
    elif preference == "prefer_surround":
        pref_tuple = (0, -surround, -lossless)
    elif preference == "prefer_stereo":
        pref_tuple = (0, -stereo, -lossless)
    elif preference == "prefer_lossless":
        pref_tuple = (0, -lossless, -surround)
    else:
        pref_tuple = (0, -surround, -lossless)
    return (tier_order[tier], pref_tuple[1], pref_tuple[2], -track.channels, track.input_index)


def plan_remux(
    *,
    video: list[dict[str, Any]],
    audio: list[dict[str, Any]],
    subtitles: list[dict[str, Any]],
    config: StreamManagerRulesConfig,
) -> RemuxPlan | None:
    """
    Returns None if no audio would remain (unsafe).
    """
    video_indices = [int(s["index"]) for s in video]

    allowed_langs: list[tuple[str, Literal["primary", "secondary"]]] = []
    p = normalize_lang(config.primary_audio_lang)
    sec = normalize_lang(config.secondary_audio_lang)
    if p:
        allowed_langs.append((p, "primary"))
    if sec:
        allowed_langs.append((sec, "secondary"))
    allowed_set = {lang for lang, _ in allowed_langs}

    removed_audio_labels: list[str] = []

    kept_audio: list[PlannedTrack] = []
    for s in audio:
        tags = _stream_tags(s)
        idx = int(s["index"])
        lang_raw = tags.get("language") or ""
        lang = normalize_lang(lang_raw)
        com = is_commentary_audio(s)
        if config.remove_commentary and com:
            removed_audio_labels.append(f"{lang or 'und'} (commentary)")
            continue
        if not lang or lang not in allowed_set:
            removed_audio_labels.append(lang or "und")
            continue
        disp = _stream_disposition(s)
        kept_audio.append(
            PlannedTrack(
                input_index=idx,
                lang_label=lang,
                commentary=com,
                forced=bool(disp.get("forced")),
                default=bool(disp.get("default")),
                channels=int(s.get("channels") or 0),
                lossless=_is_lossless_audio(str(s.get("codec_name") or "")),
                bitrate=int(s.get("bit_rate") or 0),
                kind="audio",
            )
        )

    pref_mode: AudioPreferenceMode = (
        config.audio_preference_mode
        if config.audio_preference_mode in (
            "best_available",
            "prefer_surround",
            "prefer_stereo",
            "prefer_lossless",
        )
        else "best_available"
    )
    kept_audio.sort(key=lambda t: _audio_sort_key(t, allowed_langs=allowed_langs, preference=pref_mode))

    if not kept_audio:
        return None

    slot: DefaultAudioSlot = (
        config.default_audio_slot if config.default_audio_slot in ("primary", "secondary") else "primary"
    )
    matching = [
        i
        for i, t in enumerate(kept_audio)
        if any(lg == t.lang_label and nm == slot for lg, nm in allowed_langs)
    ]
    default_out = matching[0] if matching else 0
    for i, t in enumerate(kept_audio):
        t.default = i == default_out

    # Subtitles
    kept_subs: list[PlannedTrack] = []
    removed_sub_labels: list[str] = []
    if config.subtitle_mode == "remove_all":
        for s in subtitles:
            tags = _stream_tags(s)
            removed_sub_labels.append(normalize_lang(tags.get("language")) or "und")
    else:
        sel = set(config.subtitle_langs)
        if not sel:
            for s in subtitles:
                tags = _stream_tags(s)
                removed_sub_labels.append(normalize_lang(tags.get("language")) or "und")
        else:
            for s in subtitles:
                tags = _stream_tags(s)
                idx = int(s["index"])
                lang = normalize_lang(tags.get("language"))
                disp = _stream_disposition(s)
                if not lang or lang not in sel:
                    removed_sub_labels.append(lang or "und")
                    continue
                t = PlannedTrack(
                    input_index=idx,
                    lang_label=lang,
                    forced=bool(disp.get("forced")),
                    default=bool(disp.get("default")),
                    kind="subtitle",
                )
                if not config.preserve_forced_subs:
                    t.forced = False
                if not config.preserve_default_subs:
                    t.default = False
                kept_subs.append(t)
            rank = {l: n for n, l in enumerate(config.subtitle_langs)}
            kept_subs.sort(key=lambda t: (rank.get(t.lang_label, 99), t.input_index))

    return RemuxPlan(
        video_indices=video_indices,
        audio=kept_audio,
        subtitles=kept_subs,
        removed_audio=removed_audio_labels,
        removed_subtitles=removed_sub_labels,
        default_audio_output_index=default_out,
    )


def collect_media_files_under_path(path_str: str) -> list[str]:
    """Expand a path line to files (file itself or recursive media extensions under a directory)."""
    root = Path(path_str.strip()).expanduser()
    if not root.exists():
        return []
    if root.is_file():
        return [str(root.resolve())] if root.suffix.lower() in _MEDIA_EXTENSIONS else []
    if not root.is_dir():
        return []
    out: list[str] = []
    try:
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in _MEDIA_EXTENSIONS:
                try:
                    out.append(str(p.resolve()))
                except OSError:
                    out.append(str(p))
    except OSError:
        return []
    out.sort()
    return out


def parse_path_lines(raw: str) -> list[str]:
    lines: list[str] = []
    for line in (raw or "").splitlines():
        s = line.strip()
        if s:
            lines.append(s)
    return lines
