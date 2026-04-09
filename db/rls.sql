-- Row Level Security policies for MaxCashBack
-- Run this in the Supabase SQL editor after schema.sql
--
-- Goal: the anon key (used by the frontend) can only READ.
-- Scrapers connect via DATABASE_URL (postgres role), which bypasses RLS.

-- 1. Enable RLS on every table
ALTER TABLE sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE retailers ENABLE ROW LEVEL SECURITY;
ALTER TABLE retailer_aliases ENABLE ROW LEVEL SECURITY;
ALTER TABLE cashback_rates ENABLE ROW LEVEL SECURITY;

-- 2. Read-only policies for the anon role
CREATE POLICY "anon_read_sources"
  ON sources FOR SELECT
  TO anon
  USING (true);

CREATE POLICY "anon_read_retailers"
  ON retailers FOR SELECT
  TO anon
  USING (true);

CREATE POLICY "anon_read_retailer_aliases"
  ON retailer_aliases FOR SELECT
  TO anon
  USING (true);

CREATE POLICY "anon_read_cashback_rates"
  ON cashback_rates FOR SELECT
  TO anon
  USING (true);

-- 3. The authenticated role also gets read-only (no user auth in this app)
CREATE POLICY "authenticated_read_sources"
  ON sources FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "authenticated_read_retailers"
  ON retailers FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "authenticated_read_retailer_aliases"
  ON retailer_aliases FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "authenticated_read_cashback_rates"
  ON cashback_rates FOR SELECT
  TO authenticated
  USING (true);

-- No INSERT, UPDATE, or DELETE policies exist for anon/authenticated,
-- so those operations are denied by default once RLS is enabled.
