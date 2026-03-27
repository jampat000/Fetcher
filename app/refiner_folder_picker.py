"""Native folder picker for Refiner paths (desktop Windows / optional Linux)."""

from __future__ import annotations

import logging
import subprocess
import sys
from typing import Literal

logger = logging.getLogger(__name__)

PickerOutcome = Literal["ok", "cancelled", "unavailable"]


def pick_folder_sync() -> tuple[str | None, PickerOutcome]:
    """
    Block the current thread; open a folder dialog if possible.
    Returns (path, outcome). path is set only when outcome == \"ok\".
    """
    if sys.platform == "win32":
        try:
            import tkinter as tk
            from tkinter import filedialog
        except Exception:
            logger.debug("Refiner folder picker: tkinter unavailable", exc_info=True)
            return None, "unavailable"
        root = tk.Tk()
        root.withdraw()
        try:
            try:
                root.attributes("-topmost", True)
            except Exception:
                pass
            picked = filedialog.askdirectory(mustexist=True)
        finally:
            root.destroy()
        if picked and str(picked).strip():
            return str(picked).strip(), "ok"
        return None, "cancelled"

    if sys.platform.startswith("linux"):
        try:
            r = subprocess.run(
                ["zenity", "--file-selection", "--directory"],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            if r.returncode == 0 and (r.stdout or "").strip():
                return (r.stdout or "").strip(), "ok"
            if r.returncode != 0:
                return None, "cancelled"
        except Exception:
            logger.debug("Refiner folder picker: zenity failed", exc_info=True)
        return None, "unavailable"

    return None, "unavailable"
