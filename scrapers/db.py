"""Database connection and helper functions for MaxCashBack scrapers."""

import os
import re
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Optional
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")


@contextmanager
def get_connection():
    """Context manager for database connections."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_source_id(conn, source_slug: str) -> int:
    """Get the source ID for a given slug (e.g. 'rakuten', 'gcr', 'aeroplan')."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM sources WHERE slug = %s", (source_slug,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Unknown source: {source_slug}")
        return row[0]


def find_retailer_by_alias(conn, source_id: int, alias_name: str) -> Optional[int]:
    """Look up a retailer ID by their name on a specific source."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT retailer_id FROM retailer_aliases WHERE source_id = %s AND alias_name = %s",
            (source_id, alias_name),
        )
        row = cur.fetchone()
        return row[0] if row else None


def normalize_name(name: str) -> str:
    """Normalize a retailer name for matching purposes."""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [".ca", ".com", " canada", " inc", " inc.", " ltd", " ltd."]:
        name = name.replace(suffix, "")
    # Remove "the " prefix
    if name.startswith("the "):
        name = name[4:]
    # Remove non-alphanumeric (keep spaces)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def slugify(name: str) -> str:
    """Create a URL-friendly slug from a retailer name."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug


def get_domain_variant(name: str) -> Optional[str]:
    """Return an explicit domain variant marker when present in the store name."""
    lowered = name.lower()
    match = re.search(r"\.(ca|com)\b", lowered)
    return match.group(1) if match else None


def find_or_create_retailer(conn, store_name: str, source_id: int, source_url: str = None) -> int:
    """
    Find an existing retailer by alias or normalized name, or create a new one.
    Also ensures the alias mapping exists for this source.
    Returns the retailer ID.
    """
    # First check if we already have an alias for this exact name + source
    retailer_id = find_retailer_by_alias(conn, source_id, store_name)
    if retailer_id:
        if source_url:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE retailer_aliases
                       SET source_url = %s
                       WHERE source_id = %s AND alias_name = %s""",
                    (source_url, source_id, store_name),
                )
        return retailer_id

    # Try to find by normalized name
    normalized = normalize_name(store_name)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, name FROM retailers WHERE normalized_name = %s ORDER BY id LIMIT 1",
            (normalized,),
        )
        row = cur.fetchone()

        if row:
            existing_retailer_id, existing_name = row
            existing_variant = get_domain_variant(existing_name)
            incoming_variant = get_domain_variant(store_name)

            # Keep explicit .ca and .com storefronts separate unless they match exactly.
            if existing_variant and incoming_variant and existing_variant != incoming_variant:
                row = None
            else:
                retailer_id = existing_retailer_id

        if row is None:
            # Create new retailer
            cur.execute(
                """INSERT INTO retailers (slug, name, normalized_name)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (slug) DO UPDATE SET slug = retailers.slug
                   RETURNING id""",
                (slugify(store_name), store_name, normalized),
            )
            retailer_id = cur.fetchone()[0]
        else:
            retailer_id = row[0]

        # Create the alias mapping
        cur.execute(
            """INSERT INTO retailer_aliases (retailer_id, source_id, alias_name, source_url)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (source_id, alias_name) DO UPDATE SET source_url = EXCLUDED.source_url""",
            (retailer_id, source_id, store_name, source_url),
        )

    return retailer_id


def upsert_cashback_rate(
    conn,
    retailer_id: int,
    source_id: int,
    rate_value: float,
    rate_type: str = "percentage",
    rate_display: str = None,
    is_up_to: bool = False,
):
    """
    Insert a new cashback rate and mark it as current.
    Marks previous rates for the same retailer+source as not current.
    """
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        # Mark old rates as not current
        cur.execute(
            """UPDATE cashback_rates
               SET is_current = false
               WHERE retailer_id = %s AND source_id = %s AND is_current = true""",
            (retailer_id, source_id),
        )
        # Insert new rate
        cur.execute(
            """INSERT INTO cashback_rates
               (retailer_id, source_id, rate_value, rate_type, rate_display, is_up_to, is_current, scraped_at)
               VALUES (%s, %s, %s, %s, %s, %s, true, %s)""",
            (retailer_id, source_id, rate_value, rate_type, rate_display, is_up_to, now),
        )
