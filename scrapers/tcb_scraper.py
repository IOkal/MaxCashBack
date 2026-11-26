"""
TopCashback Canada scraper.

TCB's Canada category pages are paginated with `?page=N&orderBy=popularity`.
As of the site's latest redesign, plain `requests` calls get a 403 from their
bot protection, so we fetch with Playwright (a real browser) instead. We scrape
the category listing rate rather than the retailer detail page because the
current data model stores one current rate per retailer/source.
"""

from urllib.parse import urljoin
import re

from playwright.sync_api import sync_playwright

from base_scraper import BaseScraper, ScrapedRate

TCB_CANADA_URL = "https://www.topcashback.com/category/canada-retailers/"
TCB_BASE_URL = "https://www.topcashback.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
MAX_PAGES = 30


def parse_tcb_rate(text: str) -> tuple[float, bool, str]:
    """
    Parse TCB cashback text like "4% Cash Back", "Up to 6% Cash Back",
    "$13.97 Cash Back", or "No Cash Back".
    Returns (rate_value, is_up_to, rate_type).
    """
    normalized = " ".join(text.split()).strip()
    lowered = normalized.lower()
    is_up_to = "up to" in lowered

    if "no cash back" in lowered:
        return 0.0, False, "percentage"

    dollar_match = re.search(r"\$([\d.]+)", normalized)
    if dollar_match:
        return float(dollar_match.group(1)), is_up_to, "fixed"

    pct_match = re.search(r"([\d.]+)\s*%", normalized)
    if pct_match:
        return float(pct_match.group(1)), is_up_to, "percentage"

    return 0.0, is_up_to, "percentage"


class TCBScraper(BaseScraper):
    SOURCE_SLUG = "tcb"
    SOURCE_NAME = "TopCashback"

    def scrape(self) -> list[ScrapedRate]:
        rates: list[ScrapedRate] = []
        seen_keys: set[str] = set()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()

            for page_number in range(1, MAX_PAGES + 1):
                url = f"{TCB_CANADA_URL}?page={page_number}&orderBy=popularity"
                self.logger.info(f"Fetching TCB Canada page {page_number}: {url}")

                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)
                except Exception as e:
                    self.logger.error(f"Failed to load {url}: {e}")
                    break

                cards = page.evaluate(
                    """
                    () => Array.from(document.querySelectorAll('a.cat-result')).map(a => ({
                        name: (a.querySelector('.cat-result-info__name')?.textContent || '').trim(),
                        rate: (a.querySelector('.cat-result-info__rate')?.textContent || '').trim(),
                        href: a.getAttribute('href') || '',
                    }))
                    """
                )
                self.logger.info(f"Found {len(cards)} TCB retailer cards on page {page_number}")

                if not cards:
                    break

                for card in cards:
                    try:
                        store_name = " ".join(card["name"].split())
                        rate_text = " ".join(card["rate"].split())
                        if not store_name or not rate_text:
                            continue

                        href = card["href"].strip()
                        source_url = urljoin(TCB_BASE_URL, href) if href else None
                        dedupe_key = source_url or store_name.lower()
                        if dedupe_key in seen_keys:
                            continue
                        seen_keys.add(dedupe_key)

                        rate_value, is_up_to, rate_type = parse_tcb_rate(rate_text)
                        rates.append(
                            ScrapedRate(
                                store_name=store_name,
                                rate_value=rate_value,
                                rate_type=rate_type,
                                rate_display=rate_text,
                                is_up_to=is_up_to,
                                source_url=source_url,
                            )
                        )
                    except Exception as e:
                        self.logger.error(f"Error parsing TCB retailer card: {e}")

            browser.close()

        return rates


if __name__ == "__main__":
    scraper = TCBScraper()

    import sys

    if "--dry-run" in sys.argv:
        rates = scraper.scrape()
        print(f"\nScraped {len(rates)} stores from TopCashback Canada:\n")
        for rate in sorted(rates, key=lambda r: r.rate_value, reverse=True)[:20]:
            up_to = " (up to)" if rate.is_up_to else ""
            value_str = f"{rate.rate_value}%" if rate.rate_type == "percentage" else f"${rate.rate_value}"
            print(f"  {rate.store_name}: {value_str}{up_to} — {rate.rate_display}")
    else:
        scraper.run()
