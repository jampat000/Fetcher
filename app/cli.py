from __future__ import annotations

import argparse

import uvicorn


def main() -> None:
    p = argparse.ArgumentParser(prog="Grabby")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    args = p.parse_args()

    # Import the ASGI app directly so packaging tools include it.
    from app.main import app as asgi_app

    uvicorn.run(asgi_app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()

