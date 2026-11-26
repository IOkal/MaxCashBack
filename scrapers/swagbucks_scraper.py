"""
Swagbucks scraper.

Swagbucks shows cashback as a percentage. The site previously embedded a full
alphabetical store directory via a `window.initialCardLoad` global on 27
letter-indexed pages; that global no longer exists after Swagbucks' frontend
redesign (a Next.js rebuild, data-revision ~2026-06-16).

Store data is now loaded client-side via `POST /?cmd=mp-sh-stream-data`, but
for signed-out visitors it only returns a fixed popularity-ranked list (~20
stores) — the per-letter directory now requires login (the letter links
redirect to /p/login). Until we're willing to scrape behind an authenticated
session, this is the practical ceiling for anonymous scraping: coverage has
dropped from ~1,500 stores to ~20 of the most popular ones.
"""

from playwright.sync_api import sync_playwright

from base_scraper import BaseScraper, ScrapedRate

ALL_STORES_URL = "https://www.swagbucks.com/shop/all-stores-coupons?type=all"
SWAGBUCKS_BASE = "https://www.swagbucks.com"
STREAM_DATA_BODY = "_ajax=1&streamType=all-coupons&sort=100&view=cards"


class SwagbucksScraper(BaseScraper):
    SOURCE_SLUG = "swagbucks"
    SOURCE_NAME = "Swagbucks"

    def scrape(self) -> list[ScrapedRate]:
        rates: list[ScrapedRate] = []
        seen_names: set[str] = set()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            try:
                page.goto(ALL_STORES_URL, wait_until="networkidle", timeout=30000)
            except Exception as e:
                self.logger.error(f"Failed to load {ALL_STORES_URL}: {e}")
                browser.close()
                return rates

            data = page.evaluate(
                """
                async (body) => {
                    const resp = await fetch('/?cmd=mp-sh-stream-data', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-Requested-With': 'XMLHttpRequest',
                        },
                        body,
                    });
                    if (!resp.ok) return null;
                    return await resp.json();
                }
                """,
                STREAM_DATA_BODY,
            )

            browser.close()

        if not data or not data.get("success"):
            self.logger.warning("No data returned from Swagbucks stream-data endpoint")
            return rates

        cards = data.get("cards", [])
        self.logger.info(f"Fetched {len(cards)} stores from Swagbucks all-stores stream")

        for card in cards:
            try:
                name = (card.get("header") or "").strip()
                if not name:
                    continue

                dedupe_key = name.lower()
                if dedupe_key in seen_names:
                    continue
                seen_names.add(dedupe_key)

                cashback_pct = float(card.get("cashBackPercent", 0) or 0)
                earn = float(card.get("earn", 0) or 0)
                is_up_to = bool(card.get("upTo", False))
                sb_cash_value = card.get("sbCashValueFormatted", "")
                details_link = card.get("detailsLink", "")

                # Determine if this is a percentage rate or a flat SB reward.
                # Flat rewards have very high cashBackPercent values (100+)
                # and sbCashValueFormatted > 1.00 (meaning > $1 per dollar,
                # which is impossible as a percentage).
                is_flat = False
                try:
                    cash_val = float(sb_cash_value) if sb_cash_value else 0
                    is_flat = cash_val > 1.0
                except ValueError:
                    is_flat = cashback_pct > 100

                if is_flat:
                    # Flat SB reward (e.g. "10,000 SB" = $100 one-time)
                    # Convert SB to dollars: 100 SB = $1
                    dollar_value = earn / 100.0
                    rate_type = "fixed"
                    prefix = "Up to " if is_up_to else ""
                    rate_display = f"{prefix}${dollar_value:.2f} Cash Back"
                    rate_value = dollar_value
                else:
                    rate_type = "percentage"
                    rate_value = cashback_pct
                    prefix = "Up to " if is_up_to else ""
                    if cashback_pct == int(cashback_pct):
                        rate_display = f"{prefix}{int(cashback_pct)}% Cash Back"
                    else:
                        rate_display = f"{prefix}{cashback_pct}% Cash Back"

                source_url = (
                    f"{SWAGBUCKS_BASE}{details_link}"
                    if details_link
                    else None
                )

                rates.append(
                    ScrapedRate(
                        store_name=name,
                        rate_value=rate_value,
                        rate_type=rate_type,
                        rate_display=rate_display,
                        is_up_to=is_up_to,
                        source_url=source_url,
                    )
                )
            except Exception as e:
                self.logger.error(
                    f"Error parsing card {card.get('header', '?')}: {e}"
                )

        return rates


if __name__ == "__main__":
    scraper = SwagbucksScraper()

    import sys

    if "--dry-run" in sys.argv:
        rates = scraper.scrape()
        print(f"\nScraped {len(rates)} stores from Swagbucks:\n")
        pct_rates = [r for r in rates if r.rate_type == "percentage"]
        fixed_rates = [r for r in rates if r.rate_type == "fixed"]
        print(f"  Percentage rates: {len(pct_rates)}, Fixed rates: {len(fixed_rates)}\n")
        for rate in sorted(pct_rates, key=lambda r: r.rate_value, reverse=True)[:20]:
            up_to = " (up to)" if rate.is_up_to else ""
            print(f"  {rate.store_name}: {rate.rate_value}%{up_to}")
    else:
        scraper.run()
