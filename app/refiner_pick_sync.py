"""Synchronous native folder pick (tkinter on Windows, zenity on Linux). Used by companion + Linux subprocess worker."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from typing import Literal

logger = logging.getLogger(__name__)

REFINER_PICK_FOLDER_FAIL_MESSAGE = "Folder picker unavailable. Type or paste the path."

PickerOutcome = Literal["ok", "cancelled", "unavailable"]


def pick_folder_sync() -> tuple[str | None, PickerOutcome]:
    """
    Block the current thread; open a folder dialog if possible.
    Returns (path, outcome). path is set only when outcome == \"ok\".
    """
    try:
        if sys.platform == "win32":
            try:
                import tkinter as tk
                from tkinter import filedialog
            except Exception:
                logger.warning("Refiner folder picker: tkinter import failed", exc_info=True)
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
                try:
                    root.destroy()
                except Exception:
                    pass
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
                logger.warning("Refiner folder picker: zenity failed", exc_info=True)
            return None, "unavailable"

        return None, "unavailable"
    except Exception:
        logger.warning("Refiner folder picker: unexpected failure", exc_info=True)
        return None, "unavailable"


def run_picker_subprocess() -> None:
    """Linux dev / subprocess worker: one JSON line on stdout."""
    path, outcome = pick_folder_sync()
    line = json.dumps({"folder": path, "outcome": outcome, "error": outcome if outcome != "ok" else None})
    print(line, flush=True)


if __name__ == "__main__":
    run_picker_subprocess()
