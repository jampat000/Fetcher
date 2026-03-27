from __future__ import annotations

import argparse
import sys

import uvicorn


def main() -> None:
    # PyInstaller / packaged exe: folder picker runs in a child process (see refiner_pick_folder_subprocess).
    if len(sys.argv) > 1 and sys.argv[1] == "--refiner-pick-folder-worker":
        if sys.argv.count("--refiner-pick-folder-worker") > 1:
            return
        from app.refiner_pick_sync import run_picker_subprocess

        run_picker_subprocess()
        return

    p = argparse.ArgumentParser(prog="Fetcher")
    # Defaulting to localhost for security. Bind to 0.0.0.0 only on trusted private networks.
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    args = p.parse_args()

    # Import the ASGI app directly so packaging tools include it.
    from app.main import app as asgi_app

    uvicorn.run(asgi_app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()

