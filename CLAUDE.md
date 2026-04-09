# MaxCashBack

A Canadian cashback/rewards comparison site — like CashbackMonitor.com but focused on the Canadian market.

## Project Goal

Users search for a retailer (e.g. "Amazon") and see a comparison table of cashback/points rates across all Canadian cashback portals, sorted best-to-worst. This helps Canadians maximize their rewards when shopping online.

## Target Cashback Portals (Initial)

1. **Rakuten.ca** — Cash back (percentage). ~750 stores. JS-rendered site.
2. **Great Canadian Rebates (GCR)** — Cash back (percentage). ~900 stores. Traditional HTML, category-based pages.
3. **Aeroplan eStore** — Points per dollar (Aeroplan points). ~200 stores. JS-rendered.
4. **TopCashback Canada** — Cash back (percentage or fixed amount). ~220 stores in the Canada category. Traditional HTML, paginated category pages.

Future additions: Swagbucks Canada, credit card shopping portals (RBC, TD, etc.)

## Architecture

### Tech Stack
- **Scrapers**: Python + Playwright (headless browser for JS-rendered sites)
- **Database**: PostgreSQL (Supabase free tier)
- **Frontend**: Next.js (deployed on Vercel free tier)
- **Scraper scheduling**: GitHub Actions cron (private repo — 2,000 free min/month, we use ~100-150)
- **DNS**: AWS Route 53 (maxcashback.ca — already owned)
- **Monorepo**: Everything lives in this repo (private)

### Hosting & Infrastructure (All Free Tier)

| Component        | Service           | Cost     | Notes                                      |
|------------------|-------------------|----------|--------------------------------------------|
| Database         | Supabase          | Free     | 500MB storage, unlimited API requests      |
| Frontend         | Vercel            | Free     | Auto-deploy from GitHub, SSR, custom domain|
| Scraper cron     | GitHub Actions    | Free     | 2,000 min/month for private repos          |
| DNS              | AWS Route 53      | ~$0.50/mo| Already owned: maxcashback.ca              |

#### DNS Setup (Route 53 → Vercel)
- Add a CNAME record: `maxcashback.ca` → `cname.vercel-dns.com`
- Add a CNAME record: `www.maxcashback.ca` → `cname.vercel-dns.com`
- Configure the custom domain in Vercel project settings (Vercel handles SSL automatically)

### Project Structure
```
MaxCashBack/
├── CLAUDE.md                  # This file
├── scrapers/                  # Python scraper modules
│   ├── requirements.txt
│   ├── base_scraper.py        # Abstract base class for scrapers
│   ├── rakuten_scraper.py     # Rakuten.ca scraper
│   ├── gcr_scraper.py         # Great Canadian Rebates scraper
│   ├── aeroplan_scraper.py    # Aeroplan eStore scraper
│   ├── tcb_scraper.py         # TopCashback Canada scraper
│   ├── retailer_identity.py   # Shared alias resolution + normalization helpers
│   └── db.py                  # Database connection & upsert helpers
├── web/                       # Next.js frontend
│   ├── package.json
│   ├── next.config.js
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── page.tsx           # Home page with search
│   │   └── store/
│   │       └── [slug]/
│   │           └── page.tsx   # Store comparison page
│   └── lib/
│       └── db.ts              # Database queries
├── db/
│   └── schema.sql             # PostgreSQL schema
├── .github/
│   └── workflows/
│       └── scrape.yml         # GitHub Actions cron for scrapers
└── lambda_functions/          # Legacy code (kept for reference)
```

### Database Schema (PostgreSQL)

Four main tables:
- **sources** — Cashback portals (Rakuten, GCR, Aeroplan, TopCashback, etc.)
- **retailers** — Canonical retailer records (one per real store)
- **retailer_aliases** — Maps variant names ("The Bay", "Hudson's Bay") to a single retailer
- **cashback_rates** — Current and historical rates per retailer × source

### Key Design Decisions

1. **Use the lightest scraper that fits the site**: Rakuten, GCR, and TopCashback category pages are available in server-rendered HTML, so `requests` plus HTML parsing is enough. Aeroplan still requires Playwright because its retailer list is client-rendered.

2. **PostgreSQL over DynamoDB**: The core query is relational — "give me all rates for retailer X across all sources." JOINs make this natural. Supabase gives us a free hosted Postgres with a REST API.

3. **Retailer matching strategy**: Normalize names (lowercase, strip "the", ".ca", "Canada", "Inc", etc.), then fuzzy match with a threshold. Store confirmed matches in retailer_aliases. A manual overrides table handles known tricky cases (e.g. "The Bay" = "Hudson's Bay" = "HBC").

4. **Rate types**: Cashback can be a percentage (5%), a fixed amount ($2 per order), or points (3 pts/$). The `rate_type` enum handles all three. For the comparison view, points need a configurable value (e.g. Aeroplan points at ~1.5¢ each) to compare against cash percentages.

## Scraping Notes

### Rakuten.ca
- URL: https://www.rakuten.ca/stores
- JS-rendered — need Playwright
- Store blocks contain: store name, cashback rate text (e.g. "Up to 5% Cash Back")
- Parse out: rate value, is_up_to flag

### Great Canadian Rebates
- Category pages: https://www.greatcanadianrebates.ca/display/{Category}/
- More traditional HTML — could use requests+BS4 but Playwright is fine too
- Categories: Apparel, Automotive, Baby, BMM, Business, Computers, Electronics, Finance, Flowers, Gift-Cards, Groceries, Health, Hobbies, Home, Jewelry, Pets, Services, Sporting, Toys, Travel
- Store blocks use `fieldset.smallbox.nolegend` with `a.listshopname` and `span.listrebate`

### Aeroplan eStore
- URL: https://www.aircanada.com/ca/en/aco/home/aeroplan/estore.html
- JS-rendered — need Playwright
- Shows points per dollar (1-10+ pts/$)
- Must be logged out to see base rates (logged-in shows elite bonus rates)

### TopCashback Canada
- URL: https://www.topcashback.com/category/canada-retailers/
- Server-rendered HTML with `?page=N` pagination
- Category cards contain store name, listing cashback rate, and merchant detail URL
- Use the category listing rate for now because the detail page can expose multiple sub-rates

## Legacy Code

The `lambda_functions/` directory contains the original Lambda+DynamoDB approach. It's kept for reference but is NOT part of the new architecture. Key issues with the old approach:
- BeautifulSoup couldn't handle JS-rendered sites
- DynamoDB was wrong data model for relational queries
- Retailer ID generation was incomplete (GCR scraper has a placeholder)
- No frontend was ever built

## Development Commands

```bash
# Scrapers
cd scrapers
pip install -r requirements.txt
python gcr_scraper.py          # Run GCR scraper
python rakuten_scraper.py      # Run Rakuten scraper
python aeroplan_scraper.py     # Run Aeroplan scraper
python tcb_scraper.py          # Run TopCashback scraper

# Frontend
cd web
npm install
npm run dev                    # Start dev server at localhost:3000

# Database
psql $DATABASE_URL < db/schema.sql   # Initialize schema
```

## Environment Variables

```
DATABASE_URL=postgresql://...   # Supabase connection string (used by scrapers and Next.js server)
NEXT_PUBLIC_SUPABASE_URL=...    # Supabase project URL (used by frontend)
NEXT_PUBLIC_SUPABASE_ANON_KEY=... # Supabase anon key (used by frontend)
```

### GitHub Secrets (for Actions)
Set these in repo Settings → Secrets and variables → Actions:
- `DATABASE_URL` — Supabase Postgres connection string

## Setup Checklist

1. [ ] Create Supabase project (https://supabase.com) and run `db/schema.sql`
2. [ ] Set `DATABASE_URL` in `.env` locally and in GitHub repo secrets
3. [ ] Test GCR scraper: `cd scrapers && python gcr_scraper.py --dry-run`
4. [ ] Test Rakuten scraper (may need selector tuning against live DOM)
5. [ ] Test Aeroplan scraper (may need selector tuning against live DOM)
6. [ ] Create Next.js frontend in `web/`
7. [ ] Deploy frontend to Vercel, connect GitHub repo
8. [ ] In Route 53, add CNAME records pointing maxcashback.ca → Vercel
9. [ ] In Vercel, add maxcashback.ca as custom domain
10. [ ] Verify GitHub Actions scrape workflow runs on schedule
