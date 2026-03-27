"""
Canonical Refiner folder Browse capability by runtime (Windows companion, Linux zenity, headless/Docker).
Single source of truth for backend, API, and UI data attributes.
"""

from __future__ import annotations

import os
import sys
from typing import Literal

RefinerPickMode = Literal["windows_companion", "linux_desktop", "headless_unavailable"]

# Docker / container / forced headless — matches backend pick-folder and Browse UX.
HEADLESS_FOLDER_BROWSE_MESSAGE = (
    "Folder Browse is unavailable in this environment. Type or paste the path manually."
)

# Inline guidance (Windows service session, companion not reachable); two lines in UI.
WINDOWS_SERVICE_COMPANION_GUIDANCE_LINE1 = (
    "Folder Browse requires Fetcher Companion to be running in the logged-in Windows session."
)
WINDOWS_SERVICE_COMPANION_GUIDANCE_LINE2 = (
    "If Browse is unavailable, run \"Register Fetcher Companion (folder picker)\" from the "
    "Start Menu as the logged-in user."
)

WINDOWS_SERVICE_COMPANION_PREFLIGHT_MESSAGE = (
    f"{WINDOWS_SERVICE_COMPANION_GUIDANCE_LINE1} {WINDOWS_SERVICE_COMPANION_GUIDANCE_LINE2}"
)


def is_headless_pick_environment() -> bool:
    """True when native / companion Browse must not be attempted (Docker, OCI, explicit opt-out)."""
    v = (os.environ.get("FETCHER_HEADLESS_REFINER_PICK") or "").strip().lower()
    if v in ("1", "true", "yes"):
        return True
    try:
        if os.path.isfile("/.dockerenv"):
            return True
    except OSError:
        pass
    c = (os.environ.get("container") or "").strip().lower()
    if c in ("oci", "docker", "podman"):
        return True
    return False


def is_windows_noninteractive_service_session() -> bool:
    """True when Fetcher likely runs as a Windows service (Session 0 / no interactive desktop)."""
    if os.name != "nt":
        return False
    session = os.environ.get("SESSIONNAME", "")
    return not session or session.upper() == "SERVICES"


def get_refiner_pick_mode() -> RefinerPickMode:
    """
    Canonical Browse mode for this process.
    - headless_unavailable: Docker/OCI/explicit headless — manual paths only.
    - windows_companion: POST /pick-folder uses FetcherCompanion HTTP.
    - linux_desktop: zenity subprocess path (non-container Linux only).
    """
    if is_headless_pick_environment():
        return "headless_unavailable"
    if sys.platform == "win32":
        return "windows_companion"
    if sys.platform.startswith("linux"):
        return "linux_desktop"
    return "headless_unavailable"
