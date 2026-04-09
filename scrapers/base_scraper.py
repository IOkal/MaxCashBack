"""Base scraper class for MaxCashBack."""

import abc
import logging
from dataclasses import dataclass
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")


@dataclass
class ScrapedRate:
    """A single cashback rate scraped from a source."""
    store_name: str
    rate_value: float
    rate_type: str           # 'percentage', 'fixed', 'points_per_dollar'
    rate_display: str        # Raw display text, e.g. "Up to 5% Cash Back"
    is_up_to: bool
    source_url: Optional[str] = None  # Store's URL on the portal


class BaseScraper(abc.ABC):
    """Abstract base class for all cashback scrapers."""

    # Subclasses must set these
    SOURCE_SLUG: str = ""    # e.g. 'rakuten', 'gcr', 'aeroplan'
    SOURCE_NAME: str = ""    # e.g. 'Rakuten.ca'

    def __init__(self):
        self.logger = logging.getLogger(self.SOURCE_SLUG)

    @abc.abstractmethod
    def scrape(self) -> list[ScrapedRate]:
        """Scrape all cashback rates from the source. Must be implemented by subclasses."""
        ...

    def run(self):
        """Full pipeline: scrape, match retailers, store in DB."""
        self.logger.info(f"Starting scrape for {self.SOURCE_NAME}")

        rates = self.scrape()
        self.logger.info(f"Scraped {len(rates)} rates from {self.SOURCE_NAME}")

        if not rates:
            self.logger.warning("No rates scraped — skipping DB update")
            return

        from db import get_connection, get_source_id, find_or_create_retailer, upsert_cashback_rate

        with get_connection() as conn:
            source_id = get_source_id(conn, self.SOURCE_SLUG)
            inserted = 0
            refreshed = 0

            for rate in rates:
                try:
                    retailer_id = find_or_create_retailer(
                        conn, rate.store_name, source_id, rate.source_url
                    )
                    changed = upsert_cashback_rate(
                        conn,
                        retailer_id=retailer_id,
                        source_id=source_id,
                        rate_value=rate.rate_value,
                        rate_type=rate.rate_type,
                        rate_display=rate.rate_display,
                        is_up_to=rate.is_up_to,
                    )
                    if changed:
                        inserted += 1
                    else:
                        refreshed += 1
                except Exception as e:
                    self.logger.error(f"Error processing {rate.store_name}: {e}")

            self.logger.info(
                f"Inserted {inserted} changed rates and refreshed {refreshed} unchanged rates out of {len(rates)}"
            )
