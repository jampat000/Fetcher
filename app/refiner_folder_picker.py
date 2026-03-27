"""Native folder picker for Refiner paths (desktop Windows / optional Linux)."""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
from typing import Any, Literal

logger = logging.getLogger(__name__)

# Single user-facing message for all pick-folder failure paths (timeout, parse, subprocess, etc.).
REFINER_PICK_FOLDER_FAIL_MESSAGE = "Folder picker unavailable. Type or paste the path."

PickerOutcome = Literal["ok", "cancelled", "unavailable"]


def pick_folder_sync() -> tuple[str | None, PickerOutcome]:
    """
    Block the current thread; open a folder dialog if possible.
    Returns (path, outcome). path is set only when outcome == \"ok\".

    Any unexpected error (no display session, service account, frozen-app Tcl init,
    etc.) must return \"unavailable\" — never raise to the HTTP layer.
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
    """Child-process entry: one JSON line on stdout (picked up by refiner_pick_folder_subprocess)."""
    path, outcome = pick_folder_sync()
    line = json.dumps({"folder": path, "outcome": outcome, "error": outcome if outcome != "ok" else None})
    print(line, flush=True)


def _pick_folder_subprocess_argv() -> list[str]:
    """Frozen exe cannot run ``python -c``; use a dedicated argv consumed by app.cli.main."""
    if getattr(sys, "frozen", False):
        return [sys.executable, "--refiner-pick-folder-worker"]
    return [sys.executable, "-m", "app.refiner_folder_picker"]


async def refiner_pick_folder_subprocess() -> dict[str, Any]:
    """Run ``pick_folder_sync`` in a subprocess so timeouts can kill the process (no stuck tk thread)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *_pick_folder_subprocess_argv(),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            logger.warning("Refiner folder picker subprocess timed out")
            return {
                "ok": False,
                "reason": "unavailable",
                "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
            }

        if proc.returncode != 0:
            err_txt = (stderr or b"").decode(errors="replace").strip()
            logger.warning(
                "Refiner folder picker subprocess failed (rc=%s): %s",
                proc.returncode,
                err_txt,
            )
            return {
                "ok": False,
                "reason": "unavailable",
                "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
            }

        raw_out = stdout or b""
        decoded = raw_out.decode(errors="replace").strip()
        lines = [ln.strip() for ln in decoded.splitlines() if ln.strip()]
        if not lines:
            return {
                "ok": False,
                "reason": "unavailable",
                "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
            }
        try:
            data = json.loads(lines[-1])
        except Exception:
            logger.warning("Invalid subprocess output: %r", raw_out[:2048])
            return {
                "ok": False,
                "reason": "unavailable",
                "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
            }

        path = data.get("folder")
        outcome = data.get("outcome")
        if outcome == "ok" and path:
            return {"ok": True, "path": str(path)}
        if outcome == "cancelled":
            return {"ok": False, "reason": "cancelled"}
        return {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
        }
    except Exception:
        logger.exception("Refiner pick-folder: subprocess execution failed")
        return {
            "ok": False,
            "reason": "unavailable",
            "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
        }


if __name__ == "__main__":
    run_picker_subprocess()
