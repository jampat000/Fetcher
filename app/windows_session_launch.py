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
INVALID_HANDLE_VALUE = wt.HANDLE(-1).value

TOKEN_ASSIGN_PRIMARY = 0x0001
TOKEN_DUPLICATE = 0x0002
TOKEN_QUERY = 0x0008
TOKEN_ADJUST_DEFAULT = 0x0080
TOKEN_ADJUST_SESSIONID = 0x0100
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
TH32CS_SNAPPROCESS = 0x00000002

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


class PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", wt.DWORD),
        ("cntUsage", wt.DWORD),
        ("th32ProcessID", wt.DWORD),
        ("th32DefaultHeapID", ctypes.POINTER(ctypes.c_ulong)),
        ("th32ModuleID", wt.DWORD),
        ("cntThreads", wt.DWORD),
        ("th32ParentProcessID", wt.DWORD),
        ("pcPriClassBase", ctypes.c_long),
        ("dwFlags", wt.DWORD),
        ("szExeFile", wt.WCHAR * 260),
    ]


@dataclass(frozen=True)
class LaunchResult:
    attempted: bool
    launched: bool
    reason: str
    session_id: int | None = None
    companion_exe: str = ""
    working_dir: str = ""
    process_id: int | None = None
    environment_block_created: bool = False
    token_source: str = ""


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
    kernel32.ProcessIdToSessionId.argtypes = [wt.DWORD, ctypes.POINTER(wt.DWORD)]
    kernel32.ProcessIdToSessionId.restype = wt.BOOL
    kernel32.CreateToolhelp32Snapshot.argtypes = [wt.DWORD, wt.DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = wt.HANDLE
    kernel32.Process32FirstW.argtypes = [wt.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32FirstW.restype = wt.BOOL
    kernel32.Process32NextW.argtypes = [wt.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32NextW.restype = wt.BOOL
    kernel32.OpenProcess.argtypes = [wt.DWORD, wt.BOOL, wt.DWORD]
    kernel32.OpenProcess.restype = wt.HANDLE

    wtsapi32.WTSQueryUserToken.argtypes = [wt.ULONG, ctypes.POINTER(wt.HANDLE)]
    wtsapi32.WTSQueryUserToken.restype = wt.BOOL

    advapi32.OpenProcessToken.argtypes = [wt.HANDLE, wt.DWORD, ctypes.POINTER(wt.HANDLE)]
    advapi32.OpenProcessToken.restype = wt.BOOL
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


def _last_winerr() -> int:
    try:
        return int(ctypes.get_last_error())
    except Exception:
        return -1


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


def _duplicate_primary_token(token: wt.HANDLE, session_id: int, source: str) -> tuple[wt.HANDLE | None, str]:
    primary = wt.HANDLE()
    desired_access = (
        MAXIMUM_ALLOWED
        | TOKEN_ASSIGN_PRIMARY
        | TOKEN_DUPLICATE
        | TOKEN_QUERY
        | TOKEN_ADJUST_DEFAULT
        | TOKEN_ADJUST_SESSIONID
    )
    if not advapi32.DuplicateTokenEx(
        token,
        desired_access,
        None,
        SecurityImpersonation,
        TokenPrimary,
        ctypes.byref(primary),
    ):
        err = _last_winerr()
        logger.warning(
            "Companion launch/session: DuplicateTokenEx failed source=%s session_id=%s winerr=%s",
            source,
            int(session_id),
            err,
        )
        return None, "launch_failed"
    logger.info("Companion launch/session: DuplicateTokenEx ok source=%s session_id=%s", source, int(session_id))
    return primary, "ok"


def _get_primary_token_from_explorer(session_id: int) -> tuple[wt.HANDLE | None, str]:
    logger.info("Companion launch/session: fallback token lookup started session_id=%s", int(session_id))
    snap = kernel32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
    if not snap or int(snap) == int(INVALID_HANDLE_VALUE):
        return None, "fallback_token_not_found"

    found_explorer = False
    process_open_failed = False
    entry = PROCESSENTRY32W()
    entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)

    try:
        ok = kernel32.Process32FirstW(snap, ctypes.byref(entry))
        while ok:
            exe = (entry.szExeFile or "").lower()
            if exe == "explorer.exe":
                pid = int(entry.th32ProcessID)
                pid_session = wt.DWORD(0)
                if kernel32.ProcessIdToSessionId(pid, ctypes.byref(pid_session)) and int(pid_session.value) == int(session_id):
                    found_explorer = True
                    logger.info(
                        "Companion launch/session: fallback explorer.exe found pid=%s session_id=%s",
                        pid,
                        int(session_id),
                    )
                    ph = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
                    if not ph:
                        process_open_failed = True
                        logger.warning(
                            "Companion launch/session: fallback OpenProcess failed pid=%s winerr=%s",
                            pid,
                            _last_winerr(),
                        )
                    else:
                        imp_token = wt.HANDLE()
                        try:
                            if not advapi32.OpenProcessToken(ph, TOKEN_QUERY | TOKEN_DUPLICATE, ctypes.byref(imp_token)):
                                process_open_failed = True
                                logger.warning(
                                    "Companion launch/session: fallback OpenProcessToken failed pid=%s winerr=%s",
                                    pid,
                                    _last_winerr(),
                                )
                            else:
                                logger.info(
                                    "Companion launch/session: fallback OpenProcessToken ok pid=%s session_id=%s",
                                    pid,
                                    int(session_id),
                                )
                                primary, status = _duplicate_primary_token(imp_token, session_id, "fallback_explorer")
                                if primary is not None:
                                    logger.info(
                                        "Companion launch/session: fallback token path selected session_id=%s",
                                        int(session_id),
                                    )
                                    return primary, "ok"
                                return None, "launch_failed"
                        finally:
                            _close_handle(imp_token)
                            _close_handle(ph)
            ok = kernel32.Process32NextW(snap, ctypes.byref(entry))
    finally:
        _close_handle(snap)

    if not found_explorer:
        logger.warning(
            "Companion launch/session: fallback explorer.exe not found in active session session_id=%s",
            int(session_id),
        )
        return None, "fallback_token_not_found"
    if process_open_failed:
        return None, "fallback_token_open_failed"
    return None, "fallback_token_not_found"


def _get_primary_token_for_session(session_id: int) -> tuple[wt.HANDLE | None, str, str]:
    user_token = wt.HANDLE()
    if wtsapi32.WTSQueryUserToken(session_id, ctypes.byref(user_token)):
        logger.info("Companion launch/session: WTSQueryUserToken ok session_id=%s", int(session_id))
        try:
            primary, status = _duplicate_primary_token(user_token, session_id, "wts")
            if primary is not None:
                return primary, "ok", "wts"
            return None, "launch_failed", "wts"
        finally:
            _close_handle(user_token)

    err = _last_winerr()
    logger.warning(
        "Companion launch/session: WTSQueryUserToken failed session_id=%s winerr=%s",
        int(session_id),
        err,
    )
    primary, fallback_status = _get_primary_token_from_explorer(session_id)
    if primary is not None:
        return primary, "ok", "fallback_explorer"
    if fallback_status in ("fallback_token_not_found", "fallback_token_open_failed", "launch_failed"):
        return None, fallback_status, "fallback_explorer"
    return None, "wts_token_failed", "wts"


def launch_companion_in_active_session(companion_exe: str) -> LaunchResult:
    """
    Launch companion in the active interactive session from a service context.
    Uses WTSGetActiveConsoleSessionId + WTSQueryUserToken + CreateProcessAsUserW.
    """
    if os.name != "nt":
        return LaunchResult(False, False, "non_windows", companion_exe=companion_exe)
    work_dir = os.path.dirname(companion_exe) or ""
    logger.info(
        "Companion launch/session: resolved_exe=%s exists=%s work_dir=%s",
        companion_exe,
        os.path.isfile(companion_exe),
        work_dir,
    )
    if not os.path.isfile(companion_exe):
        return LaunchResult(
            False,
            False,
            f"invalid_companion_path:{companion_exe}",
            companion_exe=companion_exe,
            working_dir=work_dir,
        )

    sess_id = kernel32.WTSGetActiveConsoleSessionId()
    logger.info("Companion launch/session: active_console_session_id=%s", int(sess_id))
    if sess_id == INVALID_SESSION_ID or int(sess_id) == 0:
        # Session 0 is services, not an interactive desktop user.
        return LaunchResult(
            False,
            False,
            "no_active_session",
            session_id=int(sess_id),
            companion_exe=companion_exe,
            working_dir=work_dir,
        )

    primary_token = wt.HANDLE()
    env_block = wt.LPVOID()
    proc_info = PROCESS_INFORMATION()
    env_created = False
    proc_pid: int | None = None
    work_dir_arg = work_dir or None
    cmdline = ctypes.create_unicode_buffer(f"\"{companion_exe}\"")

    try:
        primary, token_status, token_source = _get_primary_token_for_session(int(sess_id))
        if primary is None:
            return LaunchResult(
                True,
                False,
                token_status,
                session_id=int(sess_id),
                companion_exe=companion_exe,
                working_dir=work_dir,
                token_source=token_source,
            )
        primary_token = primary
        logger.info(
            "Companion launch/session: using token_source=%s for CreateProcessAsUserW session_id=%s",
            token_source,
            int(sess_id),
        )

        if not userenv.CreateEnvironmentBlock(ctypes.byref(env_block), primary_token, False):
            # Non-fatal: continue with process environment.
            err = _last_winerr()
            logger.warning(
                "Companion launch/session: CreateEnvironmentBlock failed session_id=%s winerr=%s (continuing)",
                int(sess_id),
                err,
            )
            env_block = wt.LPVOID()
        else:
            env_created = True
            logger.info(
                "Companion launch/session: CreateEnvironmentBlock ok session_id=%s",
                int(sess_id),
            )

        startup = STARTUPINFOW()
        startup.cb = ctypes.sizeof(STARTUPINFOW)
        startup.lpDesktop = "winsta0\\default"
        startup.dwFlags = STARTF_USESHOWWINDOW
        startup.wShowWindow = SW_HIDE

        flags = CREATE_UNICODE_ENVIRONMENT | CREATE_NO_WINDOW | NORMAL_PRIORITY_CLASS
        if not advapi32.CreateProcessAsUserW(
            primary_token,
            companion_exe,
            cmdline,
            None,
            None,
            False,
            flags,
            env_block,
            work_dir_arg,
            ctypes.byref(startup),
            ctypes.byref(proc_info),
        ):
            err = _last_winerr()
            logger.warning(
                "Companion launch/session: CreateProcessAsUserW failed session_id=%s winerr=%s app=%s cwd=%s",
                int(sess_id),
                err,
                companion_exe,
                work_dir,
            )
            return LaunchResult(
                True,
                False,
                "launch_failed",
                session_id=int(sess_id),
                companion_exe=companion_exe,
                working_dir=work_dir,
                environment_block_created=env_created,
                token_source=token_source,
            )
        proc_pid = int(proc_info.dwProcessId)
        logger.info(
            "Companion launch/session: CreateProcessAsUserW ok session_id=%s pid=%s",
            int(sess_id),
            proc_pid,
        )
        return LaunchResult(
            True,
            True,
            "launch_succeeded",
            session_id=int(sess_id),
            companion_exe=companion_exe,
            working_dir=work_dir,
            process_id=proc_pid,
            environment_block_created=env_created,
            token_source=token_source,
        )
    finally:
        _close_handle(proc_info.hThread)
        _close_handle(proc_info.hProcess)
        if env_block:
            try:
                userenv.DestroyEnvironmentBlock(env_block)
            except Exception:
                pass
        _close_handle(primary_token)


def start_companion_best_effort(companion_exe: str) -> LaunchResult:
    """
    Launch helper used by refiner picker path.
    - Service/non-interactive Windows: launch into active user session.
    - Interactive Windows: local fallback via subprocess (dev/manual runs).
    """
    if os.name != "nt":
        return LaunchResult(False, False, "non_windows", companion_exe=companion_exe)
    work_dir = os.path.dirname(companion_exe) or ""
    if not os.path.isfile(companion_exe):
        return LaunchResult(
            False,
            False,
            f"invalid_companion_path:{companion_exe}",
            companion_exe=companion_exe,
            working_dir=work_dir,
        )
    if not os.environ.get("SESSIONNAME") or os.environ.get("SESSIONNAME", "").upper() == "SERVICES":
        return launch_companion_in_active_session(companion_exe)
    try:
        subprocess.Popen(
            [companion_exe],
            cwd=work_dir or None,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=CREATE_NO_WINDOW,
            close_fds=True,
        )
        return LaunchResult(
            True,
            True,
            "launch_succeeded_interactive",
            companion_exe=companion_exe,
            working_dir=work_dir,
        )
    except Exception as exc:
        return LaunchResult(
            True,
            False,
            f"launch_failed:interactive_spawn:{exc.__class__.__name__}",
            companion_exe=companion_exe,
            working_dir=work_dir,
        )
