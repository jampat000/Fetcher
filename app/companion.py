"""
Fetcher Companion — runs in the logged-in user session only.

Localhost HTTP server that opens the native folder dialog (tkinter) so the
Windows service can remain headless and proxy picker requests here.
"""

from __future__ import annotations

import json
import logging
import sys
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from app.refiner_pick_sync import REFINER_PICK_FOLDER_FAIL_MESSAGE, pick_folder_sync

logger = logging.getLogger(__name__)

COMPANION_DEFAULT_HOST = "127.0.0.1"
COMPANION_DEFAULT_PORT = 8767


def _pick_folder_json() -> dict:
    path, outcome = pick_folder_sync()
    if outcome == "ok" and path:
        return {"ok": True, "path": str(path)}
    if outcome == "cancelled":
        return {"ok": False, "reason": "cancelled"}
    return {
        "ok": False,
        "reason": "unavailable",
        "message": REFINER_PICK_FOLDER_FAIL_MESSAGE,
    }


def _make_handler() -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        timeout = 120

        def log_message(self, fmt: str, *args: object) -> None:
            logger.info("%s - " + fmt, self.address_string(), *args)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.rstrip("/") != "/pick-folder":
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            body = _pick_folder_json()
            raw = json.dumps(body).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.rstrip("/") == "/health":
                raw = json.dumps({"ok": True}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    return Handler


def _companion_health_ok(host: str, port: int) -> bool:
    try:
        url = f"http://{host}:{int(port)}/health"
        with urllib.request.urlopen(url, timeout=0.75) as resp:  # noqa: S310 — localhost only
            if resp.status != 200:
                return False
            body = resp.read(256)
        data = json.loads(body.decode("utf-8", errors="replace"))
        return isinstance(data, dict) and data.get("ok") is True
    except (OSError, urllib.error.URLError, ValueError, json.JSONDecodeError):
        return False


def run_server(host: str = COMPANION_DEFAULT_HOST, port: int = COMPANION_DEFAULT_PORT) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    if _companion_health_ok(host, port):
        logger.info("Fetcher Companion already listening on http://%s:%s; exiting (single instance).", host, port)
        return

    Handler = _make_handler()
    try:
        server = ThreadingHTTPServer((host, int(port)), Handler)
    except OSError as exc:
        if "Address already in use" in str(exc) or "Only one usage" in str(exc) or getattr(exc, "winerror", None) == 10048:
            logger.warning("Port %s in use on %s; assuming another companion — exiting.", port, host)
            return
        logger.error("Companion could not bind %s:%s: %s", host, port, exc)
        sys.exit(1)
    server.daemon_threads = True
    logger.info(
        "Fetcher Companion listening on http://%s:%s (GET /health, POST /pick-folder)",
        host,
        port,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Companion shutting down")
        server.shutdown()


def main() -> None:
    run_server()


if __name__ == "__main__":
    main()
