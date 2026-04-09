-- Store popularity tracking
-- Run this in the Supabase SQL editor after schema.sql and rls.sql

-- One row per retailer with aggregated visit count
CREATE TABLE IF NOT EXISTS store_visits (
    retailer_id INTEGER PRIMARY KEY REFERENCES retailers(id) ON DELETE CASCADE,
    visit_count INTEGER NOT NULL DEFAULT 0,
    last_visited_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_store_visits_count
    ON store_visits(visit_count DESC);

-- RLS: anon/authenticated can read, but not write
ALTER TABLE store_visits ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_read_store_visits"
    ON store_visits FOR SELECT
    TO anon
    USING (true);

CREATE POLICY "authenticated_read_store_visits"
    ON store_visits FOR SELECT
    TO authenticated
    USING (true);

-- Atomic upsert function (SECURITY DEFINER runs as the function owner,
-- bypassing RLS — callable via supabaseAdmin.rpc())
CREATE OR REPLACE FUNCTION increment_store_visit(p_retailer_id INTEGER)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    INSERT INTO store_visits (retailer_id, visit_count, last_visited_at)
    VALUES (p_retailer_id, 1, NOW())
    ON CONFLICT (retailer_id)
    DO UPDATE SET
        visit_count = store_visits.visit_count + 1,
        last_visited_at = NOW();
END;
$$;

-- View for popularity-sorted retailers (used by getFeaturedRetailers)
CREATE OR REPLACE VIEW popular_retailers AS
SELECT
    r.id,
    r.name,
    r.slug,
    r.category,
    COALESCE(sv.visit_count, 0) AS visit_count
FROM retailers r
LEFT JOIN store_visits sv ON sv.retailer_id = r.id
ORDER BY COALESCE(sv.visit_count, 0) DESC, r.name ASC;

GRANT SELECT ON popular_retailers TO anon, authenticated;
