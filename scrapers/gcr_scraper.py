"""
Great Canadian Rebates scraper.

GCR uses traditional server-rendered HTML, so we can use requests directly.
The "All Merchants" page is the best discovery source because it includes
stores with 0% cashback that do not appear on category pages.
"""

import html
import re
import requests
from base_scraper import BaseScraper, ScrapedRate

GCR_BASE_URL = "https://www.greatcanadianrebates.ca"
GCR_ALL_MERCHANTS_URL = f"{GCR_BASE_URL}/display/All/"

MERCHANT_BLOCK_RE = re.compile(
    r"<b style='color:#8b0000;'>(?P<name>.*?)</b>(?P<header>.*?)<br>.*?"
    r"<a class='listshopname' href='/details/(?P<detail_slug>[^']+)/'>GCR Page</a>\s*&nbsp;&nbsp; OR &nbsp;&nbsp;\s*"
    r"<a class='listshopname' href='javascript:goShopping\(\"(?P<shop_path>/shop/[^\"]+/)\"\);' title=\"Shop @ (?P<title>[^\"]+)\">Vendor Page</a>",
    re.S,
)


def parse_rate_text(rate_text: str) -> tuple[float, bool]:
    """
    Parse a GCR rate string like '5%', 'Up to 8%', '$2.50', etc.
    Returns (rate_value, is_up_to).
    """
    text = rate_text.strip()
    is_up_to = "up to" in text.lower()

    # Remove "Up to" prefix
    cleaned = re.sub(r"(?i)up\s+to\s+", "", text).strip()

    # Try to extract a number
    match = re.search(r"[\d.]+", cleaned)
    if match:
        return float(match.group()), is_up_to

    return 0.0, is_up_to


class GCRScraper(BaseScraper):
    SOURCE_SLUG = "gcr"
    SOURCE_NAME = "Great Canadian Rebates"

    def scrape(self) -> list[ScrapedRate]:
        all_rates: list[ScrapedRate] = []
        seen_stores: set[str] = set()

        self.logger.info("Scraping GCR all merchants page")

        try:
            response = requests.get(GCR_ALL_MERCHANTS_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {GCR_ALL_MERCHANTS_URL}: {e}")
            return all_rates

        for match in MERCHANT_BLOCK_RE.finditer(response.text):
            try:
                store_name = html.unescape(match.group("name")).strip()
                if store_name in seen_stores:
                    continue
                seen_stores.add(store_name)

                header_html = match.group("header")
                rate_match = re.search(r"<span class='listrebate'>(.*?)</span>", header_html, re.S)
                raw_rate = html.unescape(rate_match.group(1)).strip() if rate_match else None
                rate_text = raw_rate or "0.0%"
                if "up to" in html.unescape(header_html).lower() and not rate_text.lower().startswith("up to"):
                    rate_text = f"Up to {rate_text}"

                rate_value, is_up_to = parse_rate_text(rate_text)
                rate_type = "percentage" if "%" in rate_text or raw_rate is None else "fixed"
                source_url = f"{GCR_BASE_URL}/details/{match.group('detail_slug')}/"

                all_rates.append(ScrapedRate(
                    store_name=store_name,
                    rate_value=rate_value,
                    rate_type=rate_type,
                    rate_display=rate_text,
                    is_up_to=is_up_to,
                    source_url=source_url,
                ))
            except Exception as e:
                self.logger.error(f"Error parsing merchant block: {e}")

        return all_rates


if __name__ == "__main__":
    scraper = GCRScraper()

    # For testing: just scrape and print without DB
    import sys
    if "--dry-run" in sys.argv:
        rates = scraper.scrape()
        print(f"\nScraped {len(rates)} stores from GCR:\n")
        for rate in sorted(rates, key=lambda r: r.rate_value, reverse=True)[:20]:
            up_to = " (up to)" if rate.is_up_to else ""
            value_str = f"{rate.rate_value}%" if rate.rate_type == "percentage" else f"${rate.rate_value}"
            print(f"  {rate.store_name}: {value_str}{up_to} — {rate.rate_display}")
    else:
        scraper.run()
