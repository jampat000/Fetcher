"""Regenerate README screenshots under docs/screenshots/.

**Default verification:** this module is not collected when running ``pytest tests/``
(see ``pytest_ignore_collect`` in ``tests/conftest.py``), so it does not add skip noise.

**Direct invocation:** pytest always collects this file if you pass its path. Without
``REGEN_README_SCREENSHOTS``, the test is **skipped** with an explicit reason (not “no tests ran”).
With the env var set, the test runs and overwrites PNGs under ``docs/screenshots/``.

Run (from repo root, dev deps + Chromium installed):

    set REGEN_README_SCREENSHOTS=1
    pytest tests/e2e/test_readme_screenshots.py -v

Or PowerShell:

    $env:REGEN_README_SCREENSHOTS='1'
    pytest tests/e2e/test_readme_screenshots.py -v
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from playwright.sync_api import Page, sync_playwright

from tests.e2e.test_smoke import _e2e_sign_in

REPO_ROOT = Path(__file__).resolve().parents[2]
SHOT_DIR = REPO_ROOT / "docs" / "screenshots"


@pytest.mark.regen_screenshots
@pytest.mark.skipif(
    not (os.environ.get("REGEN_README_SCREENSHOTS") or "").strip(),
    reason="Set REGEN_README_SCREENSHOTS=1 to regenerate docs/screenshots/*.png (maintenance only)",
)
def test_regenerate_readme_screenshots(e2e_server: str) -> None:
    SHOT_DIR.mkdir(parents=True, exist_ok=True)

    def _shot(page: Page, name: str) -> None:
        path = SHOT_DIR / name
        page.screenshot(path=str(path), full_page=True)
        assert path.is_file() and path.stat().st_size > 1000, f"Bad screenshot: {path}"

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        try:
            _e2e_sign_in(page, e2e_server)
            # Sidebar must show Fetcher product name.
            page.locator(".sidebar-logo").filter(has_text="Fetcher").wait_for(timeout=15000)

            page.goto(f"{e2e_server}/", wait_until="domcontentloaded")
            page.wait_for_selector("h1.ph-title:has-text('Dashboard')", timeout=20000)
            page.wait_for_timeout(500)  # hero counters settle
            _shot(page, "dashboard.png")

            page.goto(f"{e2e_server}/settings", wait_until="domcontentloaded")
            page.wait_for_selector("text=Fetcher settings", timeout=20000)
            page.wait_for_timeout(300)
            _shot(page, "settings.png")

            page.goto(f"{e2e_server}/trimmer/settings", wait_until="domcontentloaded")
            page.wait_for_selector("text=Trimmer", timeout=20000)
            page.wait_for_timeout(300)
            _shot(page, "trimmer-settings.png")

            page.goto(f"{e2e_server}/activity", wait_until="domcontentloaded")
            page.wait_for_selector("text=Activity", timeout=20000)
            page.wait_for_timeout(300)
            _shot(page, "activity.png")
        finally:
            browser.close()
