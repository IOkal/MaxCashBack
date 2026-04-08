"""
Rakuten.ca scraper.

Rakuten's stores page includes the full merchant list in the server-rendered HTML,
so we can scrape it directly without Playwright.
"""

import re
import requests
from bs4 import BeautifulSoup
from base_scraper import BaseScraper, ScrapedRate

RAKUTEN_STORES_URL = "https://www.rakuten.ca/stores"
RAKUTEN_BASE_URL = "https://www.rakuten.ca"


def parse_rakuten_rate(text: str) -> tuple[float, bool, str]:
    """
    Parse Rakuten cashback text like "Up to 5% Cash Back", "3% Cash Back",
    "$2 Cash Back", or "Coupons Only".
    Returns (rate_value, is_up_to, rate_type).
    """
    normalized = " ".join(text.split()).strip()
    lowered = normalized.lower()
    is_up_to = "up to" in lowered

    if "coupons only" in lowered:
        return 0.0, False, "percentage"

    pct_match = re.search(r"([\d.]+)\s*%", normalized)
    if pct_match:
        return float(pct_match.group(1)), is_up_to, "percentage"

    dollar_match = re.search(r"\$([\d.]+)", normalized)
    if dollar_match:
        return float(dollar_match.group(1)), is_up_to, "fixed"

    return 0.0, is_up_to, "percentage"


class RakutenScraper(BaseScraper):
    SOURCE_SLUG = "rakuten"
    SOURCE_NAME = "Rakuten.ca"

    def scrape(self) -> list[ScrapedRate]:
        rates: list[ScrapedRate] = []
        seen_stores: set[str] = set()

        self.logger.info(f"Fetching {RAKUTEN_STORES_URL}")

        try:
            response = requests.get(
                RAKUTEN_STORES_URL,
                timeout=30,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36"
                    )
                },
            )
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {RAKUTEN_STORES_URL}: {e}")
            return rates

        soup = BeautifulSoup(response.text, "html.parser")
        cards = soup.select("div.promo-store-block")
        self.logger.info(f"Found {len(cards)} Rakuten store cards")

        for card in cards:
            try:
                name_el = card.select_one("a.store-name")
                rate_el = card.select_one("span.now_rebate")

                if not name_el or not rate_el:
                    continue

                store_name = " ".join(name_el.get_text(" ", strip=True).split())
                if store_name in seen_stores:
                    continue
                seen_stores.add(store_name)

                rate_text = " ".join(rate_el.get_text(" ", strip=True).split())
                rate_value, is_up_to, rate_type = parse_rakuten_rate(rate_text)

                href = name_el.get("href", "").strip()
                source_url = f"{RAKUTEN_BASE_URL}{href}" if href.startswith("/") else href or None

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
                self.logger.error(f"Error parsing Rakuten store card: {e}")

        return rates


if __name__ == "__main__":
    scraper = RakutenScraper()

    import sys

    if "--dry-run" in sys.argv:
        rates = scraper.scrape()
        print(f"\nScraped {len(rates)} stores from Rakuten.ca:\n")
        for rate in sorted(rates, key=lambda r: r.rate_value, reverse=True)[:20]:
            up_to = " (up to)" if rate.is_up_to else ""
            value_str = f"{rate.rate_value}%" if rate.rate_type == "percentage" else f"${rate.rate_value}"
            print(f"  {rate.store_name}: {value_str}{up_to} — {rate.rate_display}")
    else:
        scraper.run()
