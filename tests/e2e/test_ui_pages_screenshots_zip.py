"""Capture full-page PNGs of main Fetcher UI routes and write ``build/Fetcher-ui-screenshots.zip``.

Opt-in only (does not run in default ``pytest tests/`` or CI unless env is set).

From repo root (PowerShell):

    $env:CAPTURE_UI_SCREENSHOTS_ZIP='1'
    pytest tests/e2e/test_ui_pages_screenshots_zip.py -v

Output:

- ``build/ui-screenshots/*.png`` — one file per route (numbered for sort order)
- ``build/Fetcher-ui-screenshots.zip`` — archive of those PNGs + ``README.txt``
"""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest
from playwright.sync_api import Page, sync_playwright

from tests.e2e.test_smoke import _e2e_sign_in

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD_DIR = REPO_ROOT / "build"
SHOT_SUBDIR = BUILD_DIR / "ui-screenshots"
ZIP_PATH = BUILD_DIR / "Fetcher-ui-screenshots.zip"


@pytest.mark.regen_screenshots
@pytest.mark.skipif(
    not (os.environ.get("CAPTURE_UI_SCREENSHOTS_ZIP") or "").strip(),
    reason="Set CAPTURE_UI_SCREENSHOTS_ZIP=1 to capture pages and create build/Fetcher-ui-screenshots.zip",
)
def test_capture_ui_pages_zip(e2e_server: str) -> None:
    base = e2e_server.rstrip("/")

    def _shot(page: Page, filename: str) -> None:
        path = SHOT_SUBDIR / filename
        page.screenshot(path=str(path), full_page=True)
        assert path.is_file() and path.stat().st_size > 1000, f"Bad screenshot: {path}"

    SHOT_SUBDIR.mkdir(parents=True, exist_ok=True)
    if ZIP_PATH.is_file():
        ZIP_PATH.unlink()

    captures: list[tuple[str, str]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        try:
            page.goto(f"{base}/setup/1", wait_until="domcontentloaded")
            page.wait_for_selector("text=First-run setup", timeout=20000)
            page.wait_for_timeout(300)
            _shot(page, "01-setup-wizard.png")
            captures.append(("01-setup-wizard.png", "/setup/1"))

            page.goto(f"{base}/login", wait_until="domcontentloaded")
            page.wait_for_selector("#username", timeout=20000)
            page.wait_for_timeout(200)
            _shot(page, "02-login.png")
            captures.append(("02-login.png", "/login"))

            _e2e_sign_in(page, base)
            page.locator(".sidebar-logo").filter(has_text="Fetcher").wait_for(timeout=15000)

            routes: list[tuple[str, str, str]] = [
                ("03-dashboard.png", "/", "h1.ph-title:has-text('Dashboard')"),
                ("04-settings.png", "/settings", "text=Fetcher settings"),
                ("05-refiner.png", "/refiner", "h1.ph-title:has-text('Refiner')"),
                ("06-refiner-settings.png", "/refiner/settings", "h1.ph-title:has-text('Movies Settings')"),
                ("07-trimmer.png", "/trimmer", "h1.ph-title:has-text('Trimmer')"),
                ("08-trimmer-settings.png", "/trimmer/settings", "text=Trimmer"),
                ("09-activity.png", "/activity", "text=Activity"),
                ("10-logs.png", "/logs", "h1.ph-title:has-text('Logs')"),
            ]
            for fname, path_suffix, sel in routes:
                page.goto(f"{base}{path_suffix}", wait_until="domcontentloaded")
                page.wait_for_selector(sel, timeout=20000)
                page.wait_for_timeout(400)
                _shot(page, fname)
                captures.append((fname, path_suffix))
        finally:
            browser.close()

    lines = [
        "Fetcher UI screenshots (full-page PNGs).",
        f"Base URL used: {base}",
        "",
        "Files:",
    ]
    for fname, path_suffix in captures:
        lines.append(f"  {fname}  ->  {path_suffix}")
    readme = BUILD_DIR / "ui-screenshots" / "README.txt"
    readme.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with zipfile.ZipFile(ZIP_PATH, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(readme, arcname="README.txt")
        for fname, _ in captures:
            fp = SHOT_SUBDIR / fname
            zf.write(fp, arcname=fname)

    assert ZIP_PATH.is_file() and ZIP_PATH.stat().st_size > 5000, "zip missing or empty"
