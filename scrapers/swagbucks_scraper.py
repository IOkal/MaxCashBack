"""
Swagbucks scraper.

Swagbucks shows cashback as a percentage. The all-stores directory is split by
letter (A-Z + "1" for numbers). Each letter page embeds an `initialCardLoad`
JavaScript object containing store data. We use Playwright to evaluate the JS
directly (avoiding brittle JSON conversion) and iterate through all 27 pages.

Note: each letter page returns the first ~20 stores from that letter's total.
This still covers 500+ stores across all major retailers.
"""

from playwright.sync_api import sync_playwright

from base_scraper import BaseScraper, ScrapedRate

LETTERS = ["1"] + list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
URL_TEMPLATE = "https://www.swagbucks.com/shop/all-stores-coupons?type=all&letter={}"
SWAGBUCKS_BASE = "https://www.swagbucks.com"


class SwagbucksScraper(BaseScraper):
    SOURCE_SLUG = "swagbucks"
    SOURCE_NAME = "Swagbucks"

    def scrape(self) -> list[ScrapedRate]:
        rates: list[ScrapedRate] = []
        seen_names: set[str] = set()
        total_available = 0
        total_fetched = 0

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

            for letter in LETTERS:
                url = URL_TEMPLATE.format(letter)
                self.logger.info(f"Fetching Swagbucks letter={letter}")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except Exception as e:
                    self.logger.error(f"Failed to load letter {letter}: {e}")
                    continue

                # Read the embedded initialCardLoad JS variable directly
                data = page.evaluate(
                    """
                    () => {
                        if (typeof initialCardLoad === 'undefined') return null;
                        return {
                            cards: (initialCardLoad.cards || []).map(c => ({
                                header: c.header || '',
                                cashBackPercent: c.cashBackPercent || 0,
                                earn: c.earn || 0,
                                upTo: !!c.upTo,
                                perDollar: !!c.perDollar,
                                sbCashValue: c.sbCashValueFormatted || '',
                                detailsLink: c.detailsLink || '',
                                merchantId: c.merchantId || 0,
                            })),
                            total: initialCardLoad.totalCount || 0,
                        };
                    }
                    """
                )

                if not data:
                    self.logger.warning(f"No initialCardLoad for letter {letter}")
                    continue

                cards = data.get("cards", [])
                letter_total = data.get("total", len(cards))
                total_available += letter_total
                total_fetched += len(cards)

                self.logger.info(
                    f"Letter {letter}: {len(cards)}/{letter_total} stores"
                )

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
                        per_dollar = bool(card.get("perDollar", False))
                        sb_cash_value = card.get("sbCashValue", "")
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

            browser.close()

        self.logger.info(
            f"Fetched {total_fetched}/{total_available} stores across all letters"
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
