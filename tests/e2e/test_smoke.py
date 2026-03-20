"""Browser smoke tests (Playwright) — run against a live uvicorn process."""

from __future__ import annotations

from playwright.sync_api import sync_playwright


def test_healthz_visible(e2e_server: str) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        try:
            page.goto(f"{e2e_server}/healthz")
            text = page.inner_text("body")
            assert "ok" in text.lower()
            assert "grabby" in text.lower()
        finally:
            browser.close()


def test_setup_wizard_first_step(e2e_server: str) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        try:
            page.goto(f"{e2e_server}/setup/1")
            assert page.locator("text=First-run setup").first.is_visible()
            assert page.locator("text=Sonarr").first.is_visible()
        finally:
            browser.close()


def test_cleaner_fast_path(e2e_server: str) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        try:
            page.goto(f"{e2e_server}/cleaner")
            assert page.locator("text=Cleaner").first.is_visible()
            assert page.locator("text=Scan Emby for matches").first.is_visible()
        finally:
            browser.close()
