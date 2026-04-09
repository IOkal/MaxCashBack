# MaxCashBack

Canadian cashback and rewards comparison tool. Search for any retailer and instantly see which cashback portal offers the best rate — across Rakuten.ca, Great Canadian Rebates, Aeroplan eStore, TopCashback Canada, and more.

## Stack

- **Scrapers** — Python + Playwright, scheduled via GitHub Actions
- **Database** — PostgreSQL on Supabase
- **Frontend** — Next.js on Vercel
- **DNS** — AWS Route 53

## Development

See [CLAUDE.md](CLAUDE.md) for full architecture docs, scraping notes, and setup instructions.

```bash
# Run a scraper locally (dry run, no DB needed)
cd scrapers
pip install -r requirements.txt
python gcr_scraper.py --dry-run
python tcb_scraper.py --dry-run

# Run the frontend
cd web
npm install
npm run dev
```

## Deployment

### Vercel

Deploy the `web/` app to Vercel and set these production environment variables:

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- optionally the AdSense vars later, once the production domain is approved

Recommended Vercel project settings:

- Framework preset: `Next.js`
- Root directory: `web`
- Build command: `npm run build`
- Output directory: leave default for Next.js

### GitHub Actions

The daily scraper workflow lives in `.github/workflows/scrape.yml` and requires this repository secret:

- `DATABASE_URL`

That secret should point at the Supabase Postgres connection string used by the scrapers.

## AdSense Status

AdSense support is partially implemented in the frontend, but rollout is intentionally deferred until the production domain is ready for AdSense review and live ad serving.

Current state:

- AdSense script loading and page placements are already wired into `web/`
- ads are disabled by default unless the AdSense env vars are set
- local development can verify layout and script wiring, but not real approval or reliable ad fill

What we learned:

- AdSense is still the best first ad network for this project because it has the lowest operational overhead
- we should use manual display ad units, not Auto ads, because the app already has explicit placements
- real setup depends on a deployed production domain, site verification, and AdSense review
- localhost is only useful for validating the integration path, not for true monetization testing
- the app now supports env-driven Google verification metadata and a generated `/ads.txt` endpoint

Future task when deployment is ready:

1. Add the production site in AdSense and complete site verification
2. Create 3 responsive display ad units: homepage top, homepage inline, store inline
3. Set the production env vars in the hosting platform
4. Verify `/ads.txt` on the live domain
5. Turn on `NEXT_PUBLIC_ENABLE_ADS`
