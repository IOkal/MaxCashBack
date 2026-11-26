-- Waitlist signups for upcoming features (e.g. credit-card stacking)
-- Run this in the Supabase SQL editor after schema.sql

CREATE TABLE IF NOT EXISTS waitlist_signups (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    feature TEXT NOT NULL DEFAULT 'card_stacking',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (email, feature)
);

-- RLS: no public policies — inserts only go through the API route's
-- service-role client, which bypasses RLS entirely.
ALTER TABLE waitlist_signups ENABLE ROW LEVEL SECURITY;
