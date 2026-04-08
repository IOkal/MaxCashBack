"""Aeroplan eStore scraper for the RewardOps storefront."""

import re
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright

from base_scraper import BaseScraper, ScrapedRate

AEROPLAN_ESTORE_URL = "https://aeroplan.rewardops.com/en-CA/home/brands?view=list"
AEROPLAN_BASE_URL = "https://aeroplan.rewardops.com"
RETAILER_ROW_SELECTOR = 'div[data-testid^="retailer-row-"]'


def parse_aeroplan_rate(text: str) -> tuple[float, bool]:
    """
    Parse Aeroplan rate text like "Earn 2 pts/$" or "Earn up to 1 pt/$".
    Returns (points_per_dollar, is_up_to).
    """
    text = re.sub(r"\s+", " ", text.strip())
    is_up_to = "up to" in text.lower()

    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:pts?|points?)", text, re.IGNORECASE)
    if match:
        return float(match.group(1)), is_up_to

    return 0.0, is_up_to


def normalize_rate_text(text: str) -> str:
    """Collapse whitespace and strip duplicated 'Earn' prefixes from ARIA text."""
    text = re.sub(r"\s+", " ", text.strip())
    return re.sub(r"^(Earn\s+)+", "Earn ", text, flags=re.IGNORECASE)


class AeroplanScraper(BaseScraper):
    SOURCE_SLUG = "aeroplan"
    SOURCE_NAME = "Aeroplan eStore"

    def _wait_for_results(self, page) -> None:
        self.logger.info(f"Navigating to {AEROPLAN_ESTORE_URL}")
        page.goto(AEROPLAN_ESTORE_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_selector(RETAILER_ROW_SELECTOR, timeout=45000)

    def _scroll_until_loaded(self, page) -> None:
        stable_count = 0
        previous_count = 0

        for _ in range(40):
            current_count = page.locator(RETAILER_ROW_SELECTOR).count()
            if current_count == previous_count:
                stable_count += 1
            else:
                stable_count = 0
                previous_count = current_count

            if stable_count >= 3:
                break

            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1200)

        self.logger.info(f"Row count stabilized at {previous_count}")

    def scrape(self) -> list[ScrapedRate]:
        rates: list[ScrapedRate] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            self._wait_for_results(page)
            self._scroll_until_loaded(page)

            store_data = page.evaluate(
                """
                ({ baseUrl, rowSelector }) => {
                    const stores = [];
                    const rows = document.querySelectorAll(rowSelector);

                    rows.forEach((row) => {
                        const linkEl = row.querySelector('a[href^="/en-CA/affiliate-details/"]');
                        if (!linkEl) return;

                        const rateEl = Array.from(row.querySelectorAll('[aria-label]')).find(
                            (el) => el.getAttribute('aria-label')?.startsWith('Earn ')
                        );

                        const name = (
                            linkEl.getAttribute('aria-label') ||
                            linkEl.textContent ||
                            ''
                        ).trim();
                        const href = linkEl.getAttribute('href');
                        const rateText = (
                            rateEl?.textContent ||
                            rateEl?.getAttribute('aria-label') ||
                            ''
                        ).trim();

                        if (!name) return;

                        stores.push({
                            name,
                            rate_text: rateText,
                            url: href ? new URL(href, baseUrl).toString() : null,
                        });
                    });

                    return stores;
                }
                """,
                {"baseUrl": AEROPLAN_BASE_URL, "rowSelector": RETAILER_ROW_SELECTOR},
            )

            self.logger.info(f"Found {len(store_data)} Aeroplan retailer rows")

            seen_keys: set[str] = set()
            for store in store_data:
                try:
                    name = store["name"].strip()
                    source_url = store.get("url")
                    dedupe_key = source_url or name.lower()
                    if dedupe_key in seen_keys:
                        continue
                    seen_keys.add(dedupe_key)

                    rate_text = normalize_rate_text(store.get("rate_text", ""))
                    rate_value, is_up_to = parse_aeroplan_rate(rate_text)
                    rates.append(
                        ScrapedRate(
                            store_name=name,
                            rate_value=rate_value,
                            rate_type="points_per_dollar",
                            rate_display=rate_text,
                            is_up_to=is_up_to,
                            source_url=urljoin(AEROPLAN_BASE_URL, source_url) if source_url else None,
                        )
                    )
                except Exception as e:
                    self.logger.error(f"Error parsing store {store.get('name', '?')}: {e}")

            browser.close()

        return rates


if __name__ == "__main__":
    scraper = AeroplanScraper()

    import sys
    if "--dry-run" in sys.argv:
        rates = scraper.scrape()
        print(f"\nScraped {len(rates)} stores from Aeroplan eStore:\n")
        for rate in sorted(rates, key=lambda r: r.rate_value, reverse=True)[:20]:
            up_to = " (up to)" if rate.is_up_to else ""
            print(f"  {rate.store_name}: {rate.rate_value} pts/${up_to} — {rate.rate_display}")
    else:
        scraper.run()
