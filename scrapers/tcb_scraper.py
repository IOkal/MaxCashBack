"""
TopCashback Canada scraper.

TCB's Canada category pages are server-rendered and paginated with `?page=N`.
We scrape the category listing rate rather than the retailer detail page because
the current data model stores one current rate per retailer/source.
"""

import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from base_scraper import BaseScraper, ScrapedRate

TCB_CANADA_URL = "https://www.topcashback.com/category/canada-retailers/"
TCB_BASE_URL = "https://www.topcashback.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


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
        next_url: str | None = TCB_CANADA_URL
        visited_urls: set[str] = set()
        page_number = 1

        while next_url and next_url not in visited_urls:
            visited_urls.add(next_url)
            self.logger.info(f"Fetching TCB Canada page {page_number}: {next_url}")

            try:
                response = requests.get(next_url, timeout=30, headers=HEADERS)
                response.raise_for_status()
            except requests.RequestException as e:
                self.logger.error(f"Failed to fetch {next_url}: {e}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            cards = soup.select("a.category-panel")
            self.logger.info(f"Found {len(cards)} TCB retailer cards on page {page_number}")

            if not cards:
                break

            for card in cards:
                try:
                    name_el = card.select_one(".search-merchant-name")
                    cashback_el = card.select_one(".category-cashback-rate")

                    if not name_el or not cashback_el:
                        continue

                    store_name = " ".join(name_el.get_text(" ", strip=True).split())
                    rate_text = " ".join(cashback_el.get_text(" ", strip=True).split())

                    href = (card.get("href") or "").strip()
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

            next_link = soup.select_one('link[rel="next"]')
            next_href = (next_link.get("href") or "").strip() if next_link else ""
            next_url = urljoin(TCB_BASE_URL, next_href) if next_href else None
            page_number += 1

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
