"""Browser smoke tests (Playwright) — run against a live uvicorn process."""

from __future__ import annotations

from playwright.sync_api import Page, sync_playwright

from tests.e2e.constants import E2E_AUTH_PASSWORD, E2E_AUTH_USERNAME


def _e2e_sign_in(page: Page, base: str) -> None:
    """Session cookie for protected routes (matches seeded DB in e2e conftest)."""
    root = base.rstrip("/") + "/"
    page.goto(f"{base}/login")
    page.locator("#username").fill(E2E_AUTH_USERNAME)
    page.locator("#password").fill(E2E_AUTH_PASSWORD)
    page.get_by_role("button", name="Sign in").click()
    page.wait_for_url(root, timeout=15000)


def test_healthz_visible(e2e_server: str) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        try:
            page.goto(f"{e2e_server}/healthz")
            text = page.inner_text("body")
            assert "ok" in text.lower()
            assert "fetcher" in text.lower()
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


def test_trimmer_fast_path(e2e_server: str) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        try:
            _e2e_sign_in(page, e2e_server)
            page.goto(f"{e2e_server}/trimmer")
            assert page.locator("text=Trimmer").first.is_visible()
            assert page.locator("text=Scan Emby for matches").first.is_visible()
        finally:
            browser.close()
