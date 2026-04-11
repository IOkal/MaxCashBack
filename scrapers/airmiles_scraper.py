"""
Air Miles Shops scraper.

The Air Miles shopping directory at airmilesshops.ca is a Next.js app that
embeds all shop data (names, rates, slugs) in a __NEXT_DATA__ script tag.
We extract this JSON directly with a single HTTP request — no Playwright needed.

Rate types:
  - Per-dollar: baseMiles / amountPerBaseMiles = miles per dollar
    e.g. 1 mile for $20 = 0.05 miles/$
  - Flat bonus: baseMiles with no amountPerBaseMiles (one-time reward)
    e.g. 50 Miles for signing up

Air Miles Cash redemption: 95 miles = $10 → ~10.5¢ per mile.
"""

import json
import re

import requests
from bs4 import BeautifulSoup

from base_scraper import BaseScraper, ScrapedRate

AIRMILES_DIRECTORY_URL = "https://www.airmilesshops.ca/en/directory"
AIRMILES_BASE_URL = "https://www.airmilesshops.ca"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


class AirMilesScraper(BaseScraper):
    SOURCE_SLUG = "airmiles"
    SOURCE_NAME = "Air Miles Shops"

    def scrape(self) -> list[ScrapedRate]:
        rates: list[ScrapedRate] = []

        self.logger.info(f"Fetching {AIRMILES_DIRECTORY_URL}")
        try:
            resp = requests.get(AIRMILES_DIRECTORY_URL, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch directory: {e}")
            return []

        # Extract __NEXT_DATA__ JSON from the HTML
        soup = BeautifulSoup(resp.text, "html.parser")
        script_tag = soup.find("script", id="__NEXT_DATA__")
        if not script_tag or not script_tag.string:
            self.logger.error("No __NEXT_DATA__ script tag found")
            return []

        try:
            next_data = json.loads(script_tag.string)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse __NEXT_DATA__: {e}")
            return []

        shop_order = (
            next_data.get("props", {})
            .get("pageProps", {})
            .get("pageData", {})
            .get("shopOrder", {})
        )

        if not shop_order:
            self.logger.error("No shopOrder found in __NEXT_DATA__")
            return []

        # Flatten all shops from the letter-keyed dict
        all_shops = []
        for key, shops in shop_order.items():
            if isinstance(shops, list):
                all_shops.extend(shops)

        self.logger.info(f"Found {len(all_shops)} shops in __NEXT_DATA__")

        seen_slugs: set[str] = set()
        for shop in all_shops:
            try:
                if not isinstance(shop, dict):
                    continue
                if not shop.get("active", True):
                    continue

                name = (shop.get("name") or shop.get("displayName") or "").strip()
                slug = (shop.get("slug") or "").strip()
                if not name or not slug:
                    continue

                if slug.lower() in seen_slugs:
                    continue
                seen_slugs.add(slug.lower())

                base_miles = shop.get("baseMiles")
                amount_per = shop.get("amountPerBaseMiles")
                source_url = f"{AIRMILES_BASE_URL}/en/shops/{slug}"

                if amount_per and float(amount_per) > 0:
                    # Per-dollar rate: e.g. 1 mile for $20
                    miles_per_dollar = float(base_miles) / float(amount_per)
                    rate_type = "points_per_dollar"
                    rate_value = miles_per_dollar

                    # Build display text
                    amt = int(amount_per) if float(amount_per) == int(amount_per) else amount_per
                    if base_miles == 1:
                        rate_display = f"1 Mile for ${amt}"
                    else:
                        rate_display = f"{base_miles} Miles for ${amt}"
                else:
                    # Flat bonus miles (one-time, not per dollar)
                    rate_type = "fixed"
                    rate_value = float(base_miles) if base_miles else 0
                    if base_miles == 1:
                        rate_display = "1 Bonus Mile"
                    else:
                        rate_display = f"{base_miles} Bonus Miles"

                rates.append(
                    ScrapedRate(
                        store_name=name,
                        rate_value=rate_value,
                        rate_type=rate_type,
                        rate_display=rate_display,
                        is_up_to=False,
                        source_url=source_url,
                    )
                )
            except Exception as e:
                shop_name = shop.get("name", "?") if isinstance(shop, dict) else "?"
                self.logger.error(f"Error parsing shop {shop_name}: {e}")

        return rates


if __name__ == "__main__":
    scraper = AirMilesScraper()

    import sys

    if "--dry-run" in sys.argv:
        rates = scraper.scrape()
        print(f"\nScraped {len(rates)} stores from Air Miles Shops:\n")
        per_dollar = [r for r in rates if r.rate_type == "points_per_dollar"]
        flat = [r for r in rates if r.rate_type == "fixed"]
        print(f"  Per-dollar rates: {len(per_dollar)}, Flat bonuses: {len(flat)}\n")
        for rate in sorted(per_dollar, key=lambda r: r.rate_value, reverse=True)[:20]:
            print(
                f"  {rate.store_name}: {rate.rate_value:.4f} miles/$"
                f" — {rate.rate_display}"
            )
    else:
        scraper.run()
