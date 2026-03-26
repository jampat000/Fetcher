from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from app.stream_manager_rules import RemuxPlan
from app.paths import BASE_DIR

logger = logging.getLogger(__name__)


def resolve_ffprobe_ffmpeg() -> tuple[str, str]:
    # Prefer bundled tools for packaged builds, then PATH.
    bundled = [
        BASE_DIR / "bin" / "ffmpeg" / "ffprobe.exe",
        BASE_DIR / "bin" / "ffmpeg" / "ffmpeg.exe",
    ]
    if bundled[0].is_file() and bundled[1].is_file():
        return str(bundled[0]), str(bundled[1])

    ffprobe = shutil.which("ffprobe")
    ffmpeg = shutil.which("ffmpeg")
    if not ffprobe or not ffmpeg:
        raise RuntimeError(
            "Stream Manager requires ffprobe/ffmpeg. In packaged Windows builds, place them under "
            "'bin/ffmpeg' (or package with build); in non-packaged environments ensure both are on PATH."
        )
    return ffprobe, ffmpeg


def ffprobe_json(path: Path, *, timeout_s: int = 120) -> dict[str, Any]:
    ffprobe, _ = resolve_ffprobe_ffmpeg()
    r = subprocess.run(
        [
            ffprobe,
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            str(path),
        ],
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout or "").strip() or "ffprobe failed")
    return json.loads(r.stdout)


def validate_remux_output(path: Path) -> None:
    data = ffprobe_json(path)
    streams = data.get("streams") or []
    if not isinstance(streams, list):
        raise RuntimeError("validation failed: invalid ffprobe output")
    n_audio = 0
    for s in streams:
        if isinstance(s, dict) and (s.get("codec_type") or "").lower() == "audio":
            n_audio += 1
    if n_audio < 1:
        raise RuntimeError("validation failed: output has no audio stream")


def build_ffmpeg_argv(*, ffmpeg_bin: str, src: Path, dst: Path, plan: RemuxPlan) -> list[str]:
    args = [ffmpeg_bin, "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-i", str(src)]
    for vi in plan.video_indices:
        args.extend(["-map", f"0:{vi}"])
    for t in plan.audio:
        args.extend(["-map", f"0:{t.input_index}"])
    for t in plan.subtitles:
        args.extend(["-map", f"0:{t.input_index}"])
    args.extend(["-c", "copy"])
    for i, t in enumerate(plan.audio):
        args.extend(["-disposition:a:%d" % i, "default" if t.default else "0"])
    for i, t in enumerate(plan.subtitles):
        flags: list[str] = []
        if t.default:
            flags.append("default")
        if t.forced:
            flags.append("forced")
        args.extend(["-disposition:s:%d" % i, "+".join(flags) if flags else "0"])
    args.append(str(dst))
    return args


def run_ffmpeg(argv: list[str], *, timeout_s: int | None = 3600) -> None:
    r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_s)
    if r.returncode != 0:
        msg = (r.stderr or r.stdout or "").strip()
        raise RuntimeError(msg or "ffmpeg failed")


def remux_to_temp_file(*, src: Path, work_dir: Path, plan: RemuxPlan) -> Path:
    """Write remux output into work_dir and validate it. Caller owns move/delete decisions."""
    _, ffmpeg_bin = resolve_ffprobe_ffmpeg()
    work_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        suffix=src.suffix or ".mkv",
        prefix=f"{src.stem}.streammgr.",
        dir=str(work_dir),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        argv = build_ffmpeg_argv(ffmpeg_bin=ffmpeg_bin, src=src, dst=tmp_path, plan=plan)
        logger.debug("Stream Manager: ffmpeg %s", " ".join(argv[:8]) + " ...")
        run_ffmpeg(argv)
        validate_remux_output(tmp_path)
    except Exception:
        try:
            if tmp_path.is_file():
                tmp_path.unlink()
        except OSError:
            logger.warning("Stream Manager: could not remove temp file %s", tmp_path, exc_info=True)
        raise
    return tmp_path
