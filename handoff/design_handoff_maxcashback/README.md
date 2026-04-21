# Handoff: MaxCashBack

A Canadian cashback-portal aggregator. Tracks live rates across 6 portals (Rakuten.ca, Great Canadian Rebates, Swagbucks, Aeroplan eStore, TopCashback, Drop) for ~2,000+ stores and shows shoppers the single best rate per store at any moment. Monetization is ads (leaderboard + sidebar units built into every page layout) and, eventually, credit-card stacking recommendations.

This package contains three designed screens:
1. **Homepage** — hero search, leaderboard ad, top-rates + popular boards (30 each), billboard ad, portal grid, categories, extension CTA, footer
2. **Store page** — single-store comparison (the core conversion screen): header, best-rate hero, 6-portal comparison table, sidebar ad, 30-day history chart + log, similar stores, footer
3. **Search autocomplete** — inline dropdown from the nav; name-only matching, 4 states + a live playground

The direction is **"Modern Fintech"**: warm off-white background, near-black ink, teal accent, Inter sans + Instrument Serif for display. Dense but airy, Wealthsimple-adjacent.

## About the design files

The files in `designs/` are **design references created in HTML** — prototypes built with React + Babel in the browser, rendered inside a pan/zoom design canvas. They are not production code and should not be shipped as-is.

Your task is to **recreate these designs in the target codebase** using its established patterns, framework, and libraries. If no codebase exists yet, Next.js (App Router) + Tailwind is a sensible default — the designs translate cleanly.

To view the designs, open the HTML files in a browser. Each one renders multiple artboards on an infinite canvas (drag to pan, scroll to zoom).

## Fidelity

**High-fidelity.** Exact colors, typography, spacing, and layout are defined below. Interactions are indicated in the prototypes (hover states are not all drawn — infer sensible ones from the visual language).

---

## Design tokens

All tokens are defined in [`designs/direction-a.jsx`](designs/direction-a.jsx) as `A_TOKENS`. Reproduce them as CSS custom properties / Tailwind theme extensions.

### Colors (oklch, with hex approximations)

| Token | Value | Hex ≈ | Use |
|---|---|---|---|
| `bg` | `oklch(0.985 0.004 90)` | `#f7f5f0` | Page background, warm off-white |
| `surface` | `#ffffff` | `#ffffff` | Cards, nav, table rows |
| `ink` | `oklch(0.18 0.01 80)` | `#1e1c18` | Primary text, logo mark |
| `inkSoft` | `oklch(0.45 0.01 80)` | `#6a6760` | Secondary text |
| `inkMute` | `oklch(0.62 0.01 80)` | `#9a968e` | Tertiary / labels |
| `line` | `oklch(0.92 0.005 80)` | `#e8e5df` | Borders, dividers |
| `lineSoft` | `oklch(0.95 0.004 80)` | `#efecea` | Subtle row separators |
| `accent` | `oklch(0.58 0.11 185)` | `#1c8e94` | Teal — primary CTAs, highlights |
| `accentSoft` | `oklch(0.94 0.03 185)` | `#d9ecec` | Tinted backgrounds (best-rate hero) |
| `accentInk` | `oklch(0.35 0.08 185)` | `#175a5f` | Teal text on light bg |
| `pos` | `oklch(0.55 0.13 155)` | `#2a9656` | Rate increase, live dot |
| `neg` | `oklch(0.58 0.18 25)` | `#d14b2a` | Rate decrease |
| `adBg` | `oklch(0.97 0.006 85)` | `#f3f0ea` | Ad slot background |

### Typography

- **Sans** — `Inter` (weights 400, 500, 600, 700). Ui text, tables, labels.
- **Serif** — `Instrument Serif` (weights 400, with italic variant). Hero headlines, section titles, large numerals.
- **Mono** — none in Direction A. Rate values in tables use Inter 500 with tabular-nums (`font-variant-numeric: tabular-nums`).

Import from Google Fonts:
```
family=Inter:wght@400;500;600;700&family=Instrument+Serif:ital@0;1
```

Display sizes used:
- Hero headline: Instrument Serif 62px, line-height 1.02, letter-spacing −1.6, weight 400
- Section title: Instrument Serif 34–38px, letter-spacing −0.8
- Large stat numeral: Instrument Serif 36–64px
- Body: Inter 15–16px, line-height 1.5
- Labels / kickers: Inter 11px uppercase, letter-spacing 0.8, color `accentInk` for section kickers, `inkMute` for table headers
- Table body: Inter 14px, line-height 1.4
- Meta / chrome: Inter 11–12px

### Spacing / layout

- Page content width: 1200px. Nav padding: 40px horizontal.
- Section vertical padding: 56–64px top/bottom.
- Card/button border-radius: 8–12px.
- Ad slot border-radius: 10px, border `1px dashed line`, background `adBg`.
- Logo tiles: `border-radius: 8` (small), `14` (store hero logo).
- Gap within clusters: 10–16px.
- Row vertical padding in dense tables: 10px.

### Portal brand colors (for dots/badges)

| Portal | Color |
|---|---|
| Rakuten.ca | `#BF0000` |
| Great Canadian Rebates | `#006B3C` |
| Swagbucks | `#0A7AA6` |
| Aeroplan eStore | `#D3273E` |
| TopCashback | `#E8344C` |
| Drop | `#6B4FBB` |
| Air Miles Shops | `#0066A4` |

### Store logo placeholders

Stores don't have real logos in the prototype — they're rendered as tinted square tiles with the first letter. Algorithm: hash the name to a hue, then `background: oklch(0.94 0.04 H)`, `color: oklch(0.38 0.08 H)`. In production, replace with real logos; keep the same rounded-square tile shape as the fallback.

---

## Screens

### 1. Homepage

File: `designs/MaxCashBack Homepage.html` → rendered from `direction-a.jsx`.

**Layout (top to bottom):**

1. **Nav (sticky, h:64, bg:surface, border-bottom:line)**
   - Left: logo mark (28×28 square, bg:ink, italic serif "m") + wordmark "MaxCashBack" (Inter 600, 15px) + "CA" pill (11px, bordered)
   - Center: nav links — Browse (active), Top rates, Popular, Categories, Portals, Extension (Inter 14, inkSoft)
   - Right: "Sign in" (text) + "Get alerts" (primary button: bg:ink, white text, 8px radius)

2. **Hero (padding 64/40/44, bg:surface)**
   - Status chip: green dot + "Live · Apr 17, 2026 · 9:09 PM UTC" (Inter 12, uppercase, letter-spacing 0.3)
   - Headline (Instrument Serif 62, line-height 1.02): "Every cashback rate in Canada, *in one place.*" (last phrase italic, color:accentInk)
   - Subhead (Inter 16, inkSoft): "Compare 2,081 stores across 6 portals."
   - **Prominent search** (Inter 17, height 64, border 1.5px ink, radius 12) with search icon, placeholder "Search 2,081 stores — adidas, HelloFresh, 1Password…", ⌘K hint. Right: teal primary "Search" button (h:64, radius 12, bg:accent).
   - **Trending pills** row: "Trending" label + 7 bordered pills (Amazon, Best Buy, Lululemon, Marriott, HelloFresh, adidas, Canva) — Inter 12, radius 99, padding 5/12.
   - **Stats strip** (4 cols, separated by vertical rules): 2,081 stores · 6 portals · 65% highest rate · 24h cadence. Numbers in Instrument Serif 36.

3. **Leaderboard ad** (bg:surface, centered): 970×120 placeholder, dashed border.

4. **Top rates + Popular dual boards** (padding 56/40, grid-template-columns: 1fr 1fr, gap 48)
   - Each column: kicker (accentInk uppercase 11px) + title (Instrument Serif 34) + "See all →" action.
   - **Top rates** (left): 30 rows. Columns: rank (01–30, inkMute tabular), store logo + name, category, rate (Inter 500, tabular-nums, 16px, right-aligned), trend delta (+/- in pos/neg).
   - **Popular** (right): 30 rows. Columns: rank, store logo + name, rate, no trend. Category column removed per design iteration.
   - Rows: h:~52, border-bottom:lineSoft, hover background:bg.

5. **Billboard ad**: 970×250 placeholder.

6. **Portal grid** (6 cards, 3-col grid)
   - Each card: portal logo badge (colored square, 40×40, white short code), portal name, tagline, stores tracked count, "Visit →" link. Border:line, radius:12, padding:24.

7. **Categories** (8-tile grid, 4 cols)
   - Tile: icon (emoji or glyph), category name (Inter 500), store count (inkMute). Padding 20, radius 10, border:line.

8. **Extension CTA** (full-width dark band, bg:ink, color:bg/surface)
   - Instrument Serif headline "Never miss a rate. Automatically." + Inter 15 body + primary teal CTA "Add to Chrome" + muted "Also: Firefox, Safari, Edge".

9. **Footer** (bg:surface, border-top:line, padding 48/40)
   - 4 columns of links (Product, Browse, Company, Legal) + small wordmark + "© 2026 MaxCashBack".

### 2. Store page (adidas Canada example)

File: `designs/MaxCashBack Store Page.html` → rendered from `store-page.jsx`.

**Layout:**

1. **Same nav.**
2. **Breadcrumb row** (padding 16/40, border-bottom:line): "Home / Apparel / adidas Canada" (Inter 13, inkSoft, current crumb in ink).
3. **Store header** (padding 32/40)
   - Left: large store logo (64×64, radius 14), name (Instrument Serif 44), meta line "Apparel · Tracked across 6 portals · Updated 3h ago" (Inter 13, inkSoft).
   - Right: "Save ♥" + "Set alert 🔔" icon buttons (bordered, radius 10) + primary "Visit store" button (bg:ink).
4. **Best-rate hero** (padding 36/40, bg:accentSoft, border-bottom:line)
   - Label: "Today's best rate" (uppercase kicker, accentInk).
   - Big numeral on its own line: "5 pts/$" (Instrument Serif ~96px, tabular, weight 400). Do NOT inline this with other content — it's on a dedicated row.
   - Second row: "≈ 7.50% effective" (Inter 20, inkSoft).
   - Third row: portal chip + "Winning on Aeroplan eStore · updated 3h ago" (Inter 14, whitespace: nowrap to prevent awkward wrapping).
   - Right side: primary CTA "Shop via Aeroplan eStore →" (bg:accent, Inter 600, h:56, radius 12).
5. **Comparison table** (padding 40, content max-width 900, with 300×600 sidebar ad to the right in a 2-col layout)
   - Columns: Portal (logo badge + name), Offer (raw string like "5 pts/$" or "Up to 6%"), Type (pill — Cashback or Points), Effective % (tabular numeric, right-aligned, Inter 500), Updated, action ("Shop →" button).
   - **Best row** (Aeroplan eStore here): background `accentSoft`, left border `3px solid accent`, effective % in `accentInk` weight 600.
   - 6 rows total — see `PORTALS` array in `store-page.jsx` for the exact data.
   - Row h:~68, border-bottom:lineSoft.
6. **Sidebar ad**: 300×600 placeholder, sticky within the table section.
7. **"Stack with your card" teaser** (under sidebar ad)
   - Small card, border:line, radius:12, padding 20. "Coming soon" pill badge. Instrument Serif 20 heading "Stack your credit card". Inter 13 body. No CTA.
8. **30-day history section** (full width, padding 56/40, bg:surface top border:line)
   - Title "30-day history" (Instrument Serif 34) + small CSV export link right-aligned.
   - **Chart**: area chart, width ~1120, height 220. X-axis: day labels (every ~5 days), Inter 11 inkMute. Y-axis: effective % gridlines. Area fill: `accent` at 20% opacity. Line: accent stroke 1.5px. Dot markers at portal-change points. No tooltips drawn (implementation detail).
   - **Stat tiles row**: 4 tiles (Current, 30d high, 30d low, 30d delta). Label (uppercase 11) + big value (Instrument Serif 34) + meta line. Use `pos` / `neg` colors for delta arrows.
   - **Daily log table**: scrollable or paginated. Columns: Date, Effective %, Portal, Label. Portal column uses colored dot + name. Dense (row h:36), tabular Inter 13.
9. **Similar stores grid** (4 cols): small cards with store logo, name, current best rate + portal dot. Border:line, radius:10.
10. **Footer** (same as homepage).

**Data for prototype** lives in `store-page.jsx` as `HISTORY` (30 entries) and `PORTALS` (6 entries). Wire up to real API in production.

### 3. Search autocomplete

File: `designs/MaxCashBack Search.html` → rendered from `search.jsx`.

**Component: inline dropdown from the nav's search input.**

- **Input** (width 520, h:42, radius 10, border 1.5px line default / ink on focus)
  - Search icon + placeholder "Search 2,081 stores…" + ⌘K hint pill (right).
  - On focus/typing, bottom radius collapses to 0 and border-bottom becomes transparent — the dropdown attaches seamlessly.
- **Dropdown** (absolute, top:42, same width as input, bg:surface, border 1.5px ink, border-top: none, border-radius 0 0 10 10, box-shadow `0 16px 36px rgba(0,0,0,0.08)`, z-index 20)
- **States** (prototype shows 4):
  1. **Focus, empty query** — centered muted message "Start typing to search stores." (Inter 12, inkMute).
  2. **Typing, matches** — up to 8 rows. Each row: store logo (24), store name (Inter 14, inkSoft) with matched substring in **bold ink**, ↵ pill on the first (highlighted) row. First row background:bg to indicate selection. Footer bar: "N matches" + "↑↓ navigate · ↵ open · esc close".
  3. **Typing, single-word match** (e.g. "vpn") — same structure, fewer rows.
  4. **No results** — "No stores found for '{query}'" (Inter 13, weight 500) + subline "Check your spelling, or *Request this store →*" (teal accent link).
- **Scoring** — name-only, single-pass O(n). See `scoreStore()` in `search.jsx`:
  - exact match → 1000
  - prefix match → 500 − name.length
  - word-prefix match → 300 − name.length
  - substring match → 100 − index − 0.1 × length
  - Sort descending, cap 8.
- **Keyboard**: ↑/↓ cycle, Enter opens highlighted store, Esc closes. Click dismisses on outside-click.
- **Interaction**: no separate "search results page" — users must pick from the dropdown. Empty result → request-store CTA.

---

## Interactions & behavior

- **Nav links** — standard client-side routing, active state = ink color + weight 500.
- **"Save" + "Set alert"** (store page) — auth-gated; if unauthenticated, open sign-in modal. If signed in, toggle state and persist.
- **Primary "Shop" buttons** — open affiliate portal link in a **new tab**, with redirect tracking for attribution. Use `rel="noopener noreferrer sponsored"`.
- **Chart** — static in the prototype; in production, make it responsive, add tooltips on hover (portal + exact date + effective %), allow 7d / 30d / 90d / 1y toggle.
- **Table sort** (homepage top-rates + store-page comparison) — not drawn, but expected. Default sorts: top rates → effective % desc; popular → rank asc; store comparison → effective % desc with best pinned.
- **Ad slots** — wire up to the chosen ad network (GAM, Kevel, etc). Sizes listed must be exact — don't reflow to arbitrary dimensions.

## State

- `stores` — large list (~2000), fetched once, cached aggressively. Search operates over this in-memory.
- `rates[storeId]` — current rates by portal. Stale-while-revalidate; 24h refresh.
- `history[storeId]` — 30-day daily snapshots per store.
- `user` — auth, savedStores[], alerts[]. Only needed for save/alert flows.
- `ui` — searchQuery, searchOpen, selectedResultIndex.

## Responsive

Designs are drawn at **1200px content width**. Breakpoints not yet designed — please flag if you need mobile/tablet layouts and we'll add them to this package. In the meantime, for a working responsive build:
- Below 1024px: collapse dual boards to stacked.
- Below 768px: full-width, nav collapses to hamburger, table becomes cards.

## Assets

No raster imagery in the prototype. Everything is SVG/CSS or procedurally generated (store logo fallback tiles). Before launch, replace with:
- Real store logos (served from your CDN, with colored-tile fallback using the hash algorithm).
- Real portal logos (or continue with the colored short-code badge — it works well visually).

## Files in this bundle

```
design_handoff_maxcashback/
├── README.md                          ← this file
└── designs/
    ├── MaxCashBack Homepage.html      ← open in browser
    ├── MaxCashBack Store Page.html    ← open in browser
    ├── MaxCashBack Search.html        ← open in browser
    ├── direction-a.jsx                ← homepage components + A_TOKENS
    ├── store-page.jsx                 ← store page components + data
    ├── search.jsx                     ← search autocomplete + scorer
    ├── data.js                        ← shared mock data (MCB_DATA)
    └── design-canvas.jsx              ← canvas scaffolding (not needed in prod)
```

`direction-a.jsx` is the source of truth for the token system and most UI atoms (buttons, nav, store logo, portal dot, ad slot, table rows). Mine it for spacing / radius / typography constants rather than re-deriving them from screenshots.
