"""Per-file source readiness before Refiner probe/remux (stable, unlocked, not actively written)."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

logger = logging.getLogger(__name__)

# Must be this many seconds since last content modification before lock/stability checks.
# Torrent/usenet clients often pause between writes; 15s was too aggressive for production.
REFINER_MIN_SILENCE_AFTER_MTIME_SEC: Final[float] = 60.0
# After exclusive read, require this many matching (size,mtime) samples spaced by this interval.
REFINER_STABILITY_EXTRA_SAMPLES: Final[int] = 2
REFINER_STABILITY_SAMPLE_INTERVAL_SEC: Final[float] = 1.5
# Back-compat alias (tests): one “stability window” duration order-of-magnitude.
REFINER_STABILITY_INTERVAL_SEC: Final[float] = REFINER_STABILITY_SAMPLE_INTERVAL_SEC

# Log at most once per path+code within this window (scheduler tick spam control).
READINESS_LOG_THROTTLE_SEC: Final[float] = 75.0

_last_readiness_log: dict[str, float] = {}

# Conservative: refuse obvious in-progress client filenames / sidecars (not a substitute for *arr queue).
_INCOMPLETE_NAME_SUFFIXES: Final[tuple[str, ...]] = (
    ".part",
    ".partial",
    ".download",
    ".downloading",
    ".!ut",
    ".!qb",
    ".bc!",
    ".tmp",
    ".temp",
    ".aria2",
    ".ftnh",
)
_INCOMPLETE_NAME_SUBSTRINGS: Final[tuple[str, ...]] = (
    "__incomplete__",
    "_unpack_",
    "~incomplete~",
)


def path_looks_like_incomplete_download(path: Path) -> bool:
    """True when path name or a same-folder sidecar indicates a client still assembling the release."""
    name_lower = path.name.lower()
    for suf in _INCOMPLETE_NAME_SUFFIXES:
        if name_lower.endswith(suf):
            return True
    for frag in _INCOMPLETE_NAME_SUBSTRINGS:
        if frag in name_lower:
            return True
    try:
        parent = path.parent
        stem = path.name
        if parent.is_dir():
            for sibling in parent.iterdir():
                if not sibling.is_file():
                    continue
                sn = sibling.name
                if sn.startswith(stem) and ".!qb" in sn.lower():
                    return True
                if sn.startswith(stem) and sn.endswith(".part"):
                    return True
    except OSError:
        pass
    return False


@dataclass(frozen=True)
class SourceReadinessResult:
    """Outcome of ``check_source_readiness`` (canonical gate for the pipeline)."""

    ready: bool
    code: str
    operator_message: str


def _stat_pair(path: Path) -> tuple[int, int] | None:
    """Return (size_bytes, mtime_ns) or None on failure."""
    try:
        st = path.stat()
    except OSError:
        return None
    size = int(st.st_size)
    m_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
    return size, m_ns


def _try_exclusive_read(path: Path) -> bool:
    """True if we can open the file for read without sharing (no concurrent other handles)."""
    if os.name == "nt":
        return _win_try_exclusive_read(path)
    return _posix_try_exclusive_read(path)


def _win_try_exclusive_read(path: Path) -> bool:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    CreateFileW = kernel32.CreateFileW
    CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    CreateFileW.restype = wintypes.HANDLE

    GENERIC_READ = 0x80000000
    OPEN_EXISTING = 3
    FILE_ATTRIBUTE_NORMAL = 0x80

    abs_path = os.path.normpath(str(path.resolve(strict=False)))
    handle = CreateFileW(
        abs_path,
        GENERIC_READ,
        0,  # no sharing — another handle open => fail
        None,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        None,
    )
    try:
        h_int = int(handle)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        h_int = -1
    if h_int == -1 or h_int == 0xFFFFFFFFFFFFFFFF:
        return False
    kernel32.CloseHandle(handle)
    return True


def _posix_try_exclusive_read(path: Path) -> bool:
    import fcntl

    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass


def check_source_readiness(path: Path) -> SourceReadinessResult:
    """Return whether ``path`` is safe to probe/remux.

    Conservative: if uncertain, not ready. Bounded cost: at most one ``REFINER_STABILITY_INTERVAL_SEC`` sleep
    when the file passes the minimum-age check (otherwise exit immediately).
    """
    try:
        p = path.resolve(strict=False)
    except OSError:
        return SourceReadinessResult(
            False,
            "not_ready_missing",
            "Source path could not be resolved.",
        )
    if not p.is_file():
        return SourceReadinessResult(
            False,
            "not_ready_missing",
            "Source is missing or not a regular file.",
        )
    if path_looks_like_incomplete_download(p):
        return SourceReadinessResult(
            False,
            "not_ready_incomplete_marker",
            "File name or folder indicates an incomplete download — skipping until the client finishes.",
        )
    pair0 = _stat_pair(p)
    if pair0 is None:
        return SourceReadinessResult(
            False,
            "not_ready_missing",
            "Could not read file metadata.",
        )
    size0, _m_ns0 = pair0
    if size0 <= 0:
        return SourceReadinessResult(
            False,
            "not_ready_missing",
            "Source file is empty.",
        )

    try:
        mtime = p.stat().st_mtime
    except OSError:
        return SourceReadinessResult(
            False,
            "not_ready_missing",
            "Could not read modification time.",
        )
    age = time.time() - mtime
    if age < REFINER_MIN_SILENCE_AFTER_MTIME_SEC:
        return SourceReadinessResult(
            False,
            "not_ready_too_fresh",
            "Source was modified very recently — waiting for a quiet period before processing.",
        )

    if not _try_exclusive_read(p):
        return SourceReadinessResult(
            False,
            "not_ready_locked",
            "Source file is in use or locked — skipping until it is released.",
        )

    prev = _stat_pair(p)
    if prev is None:
        return SourceReadinessResult(
            False,
            "not_ready_missing",
            "Could not re-read file metadata before stability check.",
        )
    for _ in range(int(REFINER_STABILITY_EXTRA_SAMPLES)):
        time.sleep(float(REFINER_STABILITY_SAMPLE_INTERVAL_SEC))
        cur = _stat_pair(p)
        if cur is None:
            return SourceReadinessResult(
                False,
                "not_ready_unstable",
                "File metadata became unreadable during stability check.",
            )
        if cur != prev:
            return SourceReadinessResult(
                False,
                "not_ready_unstable",
                "Source file size or timestamp changed while checking — still being written.",
            )
        prev = cur

    return SourceReadinessResult(True, "ready", "")


def log_readiness_skip_throttled(path: Path, result: SourceReadinessResult) -> None:
    """Info-level skip line; throttled per resolved path + code to limit scheduler noise."""
    if result.ready:
        return
    try:
        key = f"{result.code}:{path.resolve(strict=False)}"
    except OSError:
        key = f"{result.code}:{path}"
    now = time.monotonic()
    prev = _last_readiness_log.get(key)
    if prev is not None and (now - prev) < READINESS_LOG_THROTTLE_SEC:
        return
    _last_readiness_log[key] = now
    if result.code == "not_ready_locked":
        msg = "source file locked or in use, skipping until stable"
    elif result.code == "not_ready_unstable":
        msg = "source still in progress (size or timestamp changed), skipping"
    elif result.code == "not_ready_too_fresh":
        msg = "source still settling after recent write, skipping"
    elif result.code == "not_ready_missing":
        msg = "source not ready (missing or unreadable), skipping"
    elif result.code == "not_ready_incomplete_marker":
        msg = "source looks like an incomplete download (name/sidecar), skipping"
    else:
        msg = "source not ready, skipping"
    logger.info("Refiner: %s — %s", msg, path.name)


def clear_readiness_log_throttle_for_tests() -> None:
    """Test helper: reset throttle map."""
    _last_readiness_log.clear()
