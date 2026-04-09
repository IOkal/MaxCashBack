-- MaxCashBack Database Schema
-- PostgreSQL (designed for Supabase)

-- Sources: the cashback portals we track
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    slug VARCHAR(50) UNIQUE NOT NULL,          -- e.g. 'rakuten', 'gcr', 'aeroplan'
    name VARCHAR(100) NOT NULL,                -- e.g. 'Rakuten.ca'
    url VARCHAR(255) NOT NULL,                 -- e.g. 'https://www.rakuten.ca'
    rate_type VARCHAR(20) NOT NULL DEFAULT 'percentage',  -- 'percentage', 'fixed', 'points'
    points_value_cents NUMERIC(5,2),           -- value of 1 point in cents (e.g. 1.5 for Aeroplan)
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Retailers: canonical store records
CREATE TABLE IF NOT EXISTS retailers (
    id SERIAL PRIMARY KEY,
    slug VARCHAR(200) UNIQUE NOT NULL,         -- URL-friendly name: 'amazon', 'the-bay'
    name VARCHAR(200) NOT NULL,                -- Display name: 'Amazon.ca'
    normalized_name VARCHAR(200) NOT NULL,     -- For matching: 'amazon'
    category VARCHAR(100),
    logo_url VARCHAR(500),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Retailer aliases: maps variant names to canonical retailers
CREATE TABLE IF NOT EXISTS retailer_aliases (
    id SERIAL PRIMARY KEY,
    retailer_id INTEGER NOT NULL REFERENCES retailers(id) ON DELETE CASCADE,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    alias_name VARCHAR(200) NOT NULL,          -- Name as it appears on the source site
    source_url VARCHAR(500),                   -- URL on the source site (e.g. rakuten.ca/amazon)
    UNIQUE(source_id, alias_name)
);

-- Cashback rates: current and historical rates
CREATE TABLE IF NOT EXISTS cashback_rates (
    id SERIAL PRIMARY KEY,
    retailer_id INTEGER NOT NULL REFERENCES retailers(id) ON DELETE CASCADE,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    rate_value NUMERIC(10,2) NOT NULL,         -- The numeric rate (e.g. 5.0 for 5%)
    rate_type VARCHAR(20) NOT NULL DEFAULT 'percentage',  -- 'percentage', 'fixed', 'points_per_dollar'
    rate_display VARCHAR(100),                 -- Raw display text (e.g. "Up to 5% Cash Back")
    is_up_to BOOLEAN NOT NULL DEFAULT false,   -- Whether the rate is an "up to" maximum
    is_current BOOLEAN NOT NULL DEFAULT true,  -- Flag for the latest rate
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE cashback_rates
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

UPDATE cashback_rates
SET last_seen_at = COALESCE(last_seen_at, scraped_at)
WHERE last_seen_at IS NULL;

ALTER TABLE cashback_rates
    ALTER COLUMN last_seen_at SET DEFAULT NOW();

ALTER TABLE cashback_rates
    ALTER COLUMN last_seen_at SET NOT NULL;

-- Indexes for the main query patterns
CREATE INDEX IF NOT EXISTS idx_cashback_rates_retailer_current
    ON cashback_rates(retailer_id, is_current) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_cashback_rates_source
    ON cashback_rates(source_id);
CREATE INDEX IF NOT EXISTS idx_cashback_rates_retailer_scraped
    ON cashback_rates(retailer_id, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_retailers_normalized_name
    ON retailers(normalized_name);
CREATE INDEX IF NOT EXISTS idx_retailers_slug
    ON retailers(slug);
CREATE INDEX IF NOT EXISTS idx_retailer_aliases_alias
    ON retailer_aliases(alias_name);

-- Seed the initial sources
INSERT INTO sources (slug, name, url, rate_type, points_value_cents) VALUES
    ('rakuten', 'Rakuten.ca', 'https://www.rakuten.ca', 'percentage', NULL),
    ('gcr', 'Great Canadian Rebates', 'https://www.greatcanadianrebates.ca', 'percentage', NULL),
    ('aeroplan', 'Aeroplan eStore', 'https://aeroplan.rewardops.com/en-CA/home/brands?view=list', 'points_per_dollar', 1.50)
ON CONFLICT (slug) DO UPDATE SET
    name = EXCLUDED.name,
    url = EXCLUDED.url,
    rate_type = EXCLUDED.rate_type,
    points_value_cents = EXCLUDED.points_value_cents;

-- Helper view: current rates with retailer and source info joined
CREATE OR REPLACE VIEW current_rates AS
SELECT
    r.id AS retailer_id,
    r.slug AS retailer_slug,
    r.name AS retailer_name,
    r.category,
    s.slug AS source_slug,
    s.name AS source_name,
    COALESCE(
        CASE
            WHEN s.slug = 'gcr' AND ra.source_url LIKE 'javascript:goShopping("/shop/%");'
                THEN s.url || '/details/' || substring(ra.source_url from 'goShopping\("/shop/([^"]+)/"\);') || '/'
            WHEN s.slug = 'gcr' AND ra.source_url LIKE 'https://www.greatcanadianrebates.ca/shop/%'
                THEN replace(ra.source_url, '/shop/', '/details/')
            ELSE ra.source_url
        END,
        s.url
    )::VARCHAR(255) AS source_url,
    cr.rate_value,
    cr.rate_type,
    cr.rate_display,
    cr.is_up_to,
    cr.scraped_at,
    -- Compute an effective cash percentage for comparison
    CASE
        WHEN cr.rate_type = 'percentage' THEN cr.rate_value
        WHEN cr.rate_type = 'points_per_dollar' AND s.points_value_cents IS NOT NULL
            THEN cr.rate_value * s.points_value_cents
        ELSE NULL
    END AS effective_cash_percentage,
    cr.last_seen_at
FROM cashback_rates cr
JOIN retailers r ON r.id = cr.retailer_id
JOIN sources s ON s.id = cr.source_id
LEFT JOIN LATERAL (
    SELECT retailer_aliases.source_url
    FROM retailer_aliases
    WHERE retailer_aliases.retailer_id = r.id
      AND retailer_aliases.source_id = s.id
      AND retailer_aliases.source_url IS NOT NULL
    ORDER BY CASE WHEN retailer_aliases.alias_name = r.name THEN 0 ELSE 1 END,
             retailer_aliases.id DESC
    LIMIT 1
) ra ON true
WHERE cr.is_current = true;

CREATE OR REPLACE VIEW historical_rates AS
SELECT
    r.id AS retailer_id,
    r.slug AS retailer_slug,
    r.name AS retailer_name,
    s.slug AS source_slug,
    s.name AS source_name,
    cr.rate_value,
    cr.rate_type,
    cr.rate_display,
    cr.is_up_to,
    cr.scraped_at,
    cr.last_seen_at,
    CASE
        WHEN cr.rate_type = 'percentage' THEN cr.rate_value
        WHEN cr.rate_type = 'points_per_dollar' AND s.points_value_cents IS NOT NULL
            THEN cr.rate_value * s.points_value_cents
        ELSE NULL
    END AS effective_cash_percentage
FROM cashback_rates cr
JOIN retailers r ON r.id = cr.retailer_id
JOIN sources s ON s.id = cr.source_id;

GRANT SELECT ON current_rates TO anon, authenticated;
GRANT SELECT ON historical_rates TO anon, authenticated;
