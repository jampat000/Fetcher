"""E2E fixtures: real HTTP server for Playwright (separate process, own SQLite)."""

from __future__ import annotations

import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT: Path = Path(__file__).resolve().parents[2]
BASE = "http://127.0.0.1:8767"


@pytest.fixture(scope="session")
def e2e_server() -> str:
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8767",
            "--log-level",
            "warning",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 30
        last_exc: BaseException | None = None
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{BASE}/healthz", timeout=1) as r:
                    if r.status == 200:
                        break
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last_exc = e
                time.sleep(0.25)
        else:
            proc.terminate()
            pytest.fail(f"E2E server did not become ready: {last_exc!r}")
        yield BASE
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
