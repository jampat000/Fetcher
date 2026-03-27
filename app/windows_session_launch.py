"""Windows-only helpers to launch FetcherCompanion in the active user session."""

from __future__ import annotations

import ctypes
import ctypes.wintypes as wt
import logging
import os
import subprocess
import sys
from dataclasses import dataclass

logger = logging.getLogger(__name__)

if os.name == "nt":
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    userenv = ctypes.WinDLL("userenv", use_last_error=True)
    wtsapi32 = ctypes.WinDLL("wtsapi32", use_last_error=True)
else:
    kernel32 = None
    advapi32 = None
    userenv = None
    wtsapi32 = None


INVALID_SESSION_ID = 0xFFFFFFFF
WTS_CURRENT_SERVER_HANDLE = wt.HANDLE(0)

TOKEN_ASSIGN_PRIMARY = 0x0001
TOKEN_DUPLICATE = 0x0002
TOKEN_QUERY = 0x0008
TOKEN_ADJUST_DEFAULT = 0x0080
TOKEN_ADJUST_SESSIONID = 0x0100

MAXIMUM_ALLOWED = 0x02000000
SecurityImpersonation = 2
TokenPrimary = 1

CREATE_UNICODE_ENVIRONMENT = 0x00000400
CREATE_NO_WINDOW = 0x08000000
NORMAL_PRIORITY_CLASS = 0x00000020
STARTF_USESHOWWINDOW = 0x00000001
SW_HIDE = 0


class STARTUPINFOW(ctypes.Structure):
    _fields_ = [
        ("cb", wt.DWORD),
        ("lpReserved", wt.LPWSTR),
        ("lpDesktop", wt.LPWSTR),
        ("lpTitle", wt.LPWSTR),
        ("dwX", wt.DWORD),
        ("dwY", wt.DWORD),
        ("dwXSize", wt.DWORD),
        ("dwYSize", wt.DWORD),
        ("dwXCountChars", wt.DWORD),
        ("dwYCountChars", wt.DWORD),
        ("dwFillAttribute", wt.DWORD),
        ("dwFlags", wt.DWORD),
        ("wShowWindow", wt.WORD),
        ("cbReserved2", wt.WORD),
        ("lpReserved2", ctypes.POINTER(ctypes.c_byte)),
        ("hStdInput", wt.HANDLE),
        ("hStdOutput", wt.HANDLE),
        ("hStdError", wt.HANDLE),
    ]


class PROCESS_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("hProcess", wt.HANDLE),
        ("hThread", wt.HANDLE),
        ("dwProcessId", wt.DWORD),
        ("dwThreadId", wt.DWORD),
    ]


@dataclass(frozen=True)
class LaunchResult:
    attempted: bool
    launched: bool
    reason: str


def _close_handle(h: wt.HANDLE | int | None) -> None:
    if os.name != "nt" or not h:
        return
    try:
        kernel32.CloseHandle(h)
    except Exception:
        pass


def _configure_win32_signatures() -> None:
    if os.name != "nt":
        return
    kernel32.WTSGetActiveConsoleSessionId.restype = wt.DWORD

    wtsapi32.WTSQueryUserToken.argtypes = [wt.ULONG, ctypes.POINTER(wt.HANDLE)]
    wtsapi32.WTSQueryUserToken.restype = wt.BOOL

    advapi32.DuplicateTokenEx.argtypes = [
        wt.HANDLE,
        wt.DWORD,
        wt.LPVOID,
        wt.DWORD,
        wt.DWORD,
        ctypes.POINTER(wt.HANDLE),
    ]
    advapi32.DuplicateTokenEx.restype = wt.BOOL

    userenv.CreateEnvironmentBlock.argtypes = [
        ctypes.POINTER(wt.LPVOID),
        wt.HANDLE,
        wt.BOOL,
    ]
    userenv.CreateEnvironmentBlock.restype = wt.BOOL

    userenv.DestroyEnvironmentBlock.argtypes = [wt.LPVOID]
    userenv.DestroyEnvironmentBlock.restype = wt.BOOL

    advapi32.CreateProcessAsUserW.argtypes = [
        wt.HANDLE,
        wt.LPCWSTR,
        wt.LPWSTR,
        wt.LPVOID,
        wt.LPVOID,
        wt.BOOL,
        wt.DWORD,
        wt.LPVOID,
        wt.LPCWSTR,
        ctypes.POINTER(STARTUPINFOW),
        ctypes.POINTER(PROCESS_INFORMATION),
    ]
    advapi32.CreateProcessAsUserW.restype = wt.BOOL

    kernel32.CloseHandle.argtypes = [wt.HANDLE]
    kernel32.CloseHandle.restype = wt.BOOL


_configure_win32_signatures()


def resolve_companion_exe_path() -> str:
    """Best-effort path for FetcherCompanion.exe from service/runtime context."""
    env_override = (os.environ.get("FETCHER_COMPANION_EXE_PATH") or "").strip()
    if env_override:
        return env_override
    if getattr(sys, "frozen", False):
        return os.path.join(os.path.dirname(sys.executable), "FetcherCompanion.exe")
    # Dev fallback for local runs.
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(repo_root, "dist", "Fetcher", "FetcherCompanion.exe")


def launch_companion_in_active_session(companion_exe: str) -> LaunchResult:
    """
    Launch companion in the active interactive session from a service context.
    Uses WTSGetActiveConsoleSessionId + WTSQueryUserToken + CreateProcessAsUserW.
    """
    if os.name != "nt":
        return LaunchResult(False, False, "non_windows")
    if not os.path.isfile(companion_exe):
        return LaunchResult(False, False, f"missing_exe:{companion_exe}")

    sess_id = kernel32.WTSGetActiveConsoleSessionId()
    if sess_id == INVALID_SESSION_ID:
        return LaunchResult(False, False, "no_active_session")

    user_token = wt.HANDLE()
    primary_token = wt.HANDLE()
    env_block = wt.LPVOID()
    proc_info = PROCESS_INFORMATION()
    work_dir = os.path.dirname(companion_exe) or None
    cmdline = ctypes.create_unicode_buffer(f"\"{companion_exe}\"")

    try:
        if not wtsapi32.WTSQueryUserToken(sess_id, ctypes.byref(user_token)):
            err = ctypes.get_last_error()
            return LaunchResult(True, False, f"wts_query_user_token_failed:{err}")

        desired_access = (
            MAXIMUM_ALLOWED
            | TOKEN_ASSIGN_PRIMARY
            | TOKEN_DUPLICATE
            | TOKEN_QUERY
            | TOKEN_ADJUST_DEFAULT
            | TOKEN_ADJUST_SESSIONID
        )
        if not advapi32.DuplicateTokenEx(
            user_token,
            desired_access,
            None,
            SecurityImpersonation,
            TokenPrimary,
            ctypes.byref(primary_token),
        ):
            err = ctypes.get_last_error()
            return LaunchResult(True, False, f"duplicate_token_failed:{err}")

        if not userenv.CreateEnvironmentBlock(ctypes.byref(env_block), primary_token, False):
            # Non-fatal: continue with process environment.
            env_block = wt.LPVOID()

        startup = STARTUPINFOW()
        startup.cb = ctypes.sizeof(STARTUPINFOW)
        startup.lpDesktop = "winsta0\\default"
        startup.dwFlags = STARTF_USESHOWWINDOW
        startup.wShowWindow = SW_HIDE

        flags = CREATE_UNICODE_ENVIRONMENT | CREATE_NO_WINDOW | NORMAL_PRIORITY_CLASS
        if not advapi32.CreateProcessAsUserW(
            primary_token,
            None,
            cmdline,
            None,
            None,
            False,
            flags,
            env_block,
            work_dir,
            ctypes.byref(startup),
            ctypes.byref(proc_info),
        ):
            err = ctypes.get_last_error()
            return LaunchResult(True, False, f"create_process_as_user_failed:{err}")
        return LaunchResult(True, True, f"launched_pid:{int(proc_info.dwProcessId)}")
    finally:
        _close_handle(proc_info.hThread)
        _close_handle(proc_info.hProcess)
        if env_block:
            try:
                userenv.DestroyEnvironmentBlock(env_block)
            except Exception:
                pass
        _close_handle(primary_token)
        _close_handle(user_token)


def start_companion_best_effort(companion_exe: str) -> LaunchResult:
    """
    Launch helper used by refiner picker path.
    - Service/non-interactive Windows: launch into active user session.
    - Interactive Windows: local fallback via subprocess (dev/manual runs).
    """
    if os.name != "nt":
        return LaunchResult(False, False, "non_windows")
    if not os.path.isfile(companion_exe):
        return LaunchResult(False, False, f"missing_exe:{companion_exe}")
    if not os.environ.get("SESSIONNAME") or os.environ.get("SESSIONNAME", "").upper() == "SERVICES":
        return launch_companion_in_active_session(companion_exe)
    try:
        subprocess.Popen(
            [companion_exe],
            cwd=os.path.dirname(companion_exe) or None,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=CREATE_NO_WINDOW,
            close_fds=True,
        )
        return LaunchResult(True, True, "interactive_spawned")
    except Exception as exc:
        return LaunchResult(True, False, f"interactive_spawn_failed:{exc.__class__.__name__}")
