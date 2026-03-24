"""Visual/layout QA: Automation 4-card grid at common viewports (Playwright)."""

from __future__ import annotations

from playwright.sync_api import Page, sync_playwright

from tests.e2e.constants import E2E_AUTH_PASSWORD, E2E_AUTH_USERNAME


def _sign_in(page: Page, base: str) -> None:
    root = base.rstrip("/") + "/"
    page.goto(f"{base}/login")
    page.locator("#username").fill(E2E_AUTH_USERNAME)
    page.locator("#password").fill(E2E_AUTH_PASSWORD)
    page.get_by_role("button", name="Sign in").click()
    page.wait_for_url(root, timeout=15000)


def _first_row_card_count(page: Page) -> int:
    return page.evaluate(
        """() => {
          const el = document.querySelector('.automation-cards');
          if (!el) return 0;
          const cards = [...el.querySelectorAll('.automation-card')];
          if (!cards.length) return 0;
          const y0 = cards[0].getBoundingClientRect().top;
          return cards.filter(
            (c) => Math.abs(c.getBoundingClientRect().top - y0) < 4
          ).length;
        }"""
    )


def test_automation_cards_layout_at_viewports(e2e_server: str) -> None:
    """Desktop 4-up, tablet 2x2, mobile single column — matches app.css breakpoints."""
    cases: list[tuple[int, int, int]] = [
        (1440, 900, 4),
        (900, 800, 2),
        (500, 800, 1),
    ]
    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            for width, height, expected_first_row in cases:
                page = browser.new_page(viewport={"width": width, "height": height})
                try:
                    _sign_in(page, e2e_server)
                    page.goto(f"{e2e_server}/")
                    page.wait_for_selector(".automation-cards", timeout=15000)
                    assert page.locator(".automation-card").count() == 4
                    assert page.get_by_text("Latest event", exact=True).first.is_visible()
                    got = _first_row_card_count(page)
                    assert got == expected_first_row, (
                        f"viewport {width}x{height}: expected {expected_first_row} cards in first row, got {got}"
                    )
                finally:
                    page.close()
        finally:
            browser.close()
