import Link from 'next/link'
import { Suspense } from 'react'
import AdSlot from '@/components/AdSlot'
import SearchBox from '@/components/SearchBox'
import StoreLogo from '@/components/StoreLogo'
import {
  getFeaturedRetailers,
  getHighestEarnRates,
  getSiteStatus,
  type CashbackRate,
} from '@/lib/db'

function formatRate(rate: CashbackRate): string {
  const prefix = rate.is_up_to ? 'Up to ' : ''
  if (rate.rate_type === 'percentage') return `${prefix}${rate.rate_value}%`
  if (rate.rate_type === 'points_per_dollar') return `${prefix}${rate.rate_value} pts/$`
  return `${prefix}$${rate.rate_value}`
}

function formatStatusTimestamp(dateString: string | null): string {
  if (!dateString) return 'Unavailable'
  return new Intl.DateTimeFormat('en-CA', {
    dateStyle: 'medium',
    timeStyle: 'short',
    timeZone: 'UTC',
  }).format(new Date(dateString))
}

/* ─── Hero ───────────────────────────────────────────────── */

async function Hero() {
  const status = await getSiteStatus()
  const storeCount = status.trackedStores || 0
  const portalCount = status.trackedPortals || 0

  return (
    <section className="border-b border-mcb-line bg-mcb-surface px-5 pb-11 pt-16 md:px-10">
      <div className="mx-auto max-w-[1200px]">
        {/* Live chip */}
        <div className="mb-5 flex items-center gap-2">
          <span className="h-1.5 w-1.5 rounded-full bg-mcb-pos shadow-[0_0_0_3px_oklch(0.55_0.13_155/0.15)]" />
          <span className="text-[12px] uppercase tracking-wide text-mcb-ink-soft">
            Live · {status.latestUpdate ? formatStatusTimestamp(status.latestUpdate) + ' UTC' : 'Updating'}
          </span>
        </div>

        {/* Headline */}
        <h1
          className="max-w-[900px] font-serif text-mcb-ink"
          style={{ fontSize: 'clamp(36px, 5vw, 62px)', lineHeight: 1.02, letterSpacing: -1.6, fontWeight: 400 }}
        >
          Every cashback rate in Canada,{' '}
          <em className="text-mcb-accent-ink">in one place.</em>
        </h1>

        <p className="mt-4 max-w-[560px] text-[16px] leading-relaxed text-mcb-ink-soft">
          Compare {storeCount.toLocaleString()} stores across {portalCount} portals.
        </p>

        {/* Prominent search */}
        <div className="mt-7">
          <Suspense fallback={<SearchSkeleton />}>
            <SearchBox storeCount={storeCount} />
          </Suspense>
        </div>

        {/* Trending pills */}
        <div className="mt-4 flex flex-wrap items-center gap-2">
          <span className="mr-1 self-center text-[11px] uppercase tracking-wider text-mcb-ink-mute">
            Trending
          </span>
          {['Amazon', 'Best Buy', 'Lululemon', 'Marriott', 'HelloFresh', 'adidas', 'Canva'].map(t => (
            <Link
              key={t}
              href={`/store/${t.toLowerCase().replace(/\s+/g, '-')}`}
              className="rounded-full border border-mcb-line bg-mcb-surface px-3 py-1 text-[12px] text-mcb-ink-soft hover:border-mcb-ink-mute"
            >
              {t}
            </Link>
          ))}
        </div>

        {/* Stats strip */}
        <div className="mt-11 grid grid-cols-2 gap-0 border-t border-mcb-line pt-6 sm:grid-cols-4">
          {[
            { v: storeCount.toLocaleString(), l: 'Stores tracked' },
            { v: String(portalCount), l: 'Portals compared' },
            { v: '65%', l: 'Highest live rate' },
            { v: '24h', l: 'Update cadence' },
          ].map((s, i) => (
            <div
              key={s.l}
              className={i > 0 ? 'border-l border-mcb-line pl-6' : ''}
            >
              <div className="font-serif text-4xl text-mcb-ink" style={{ letterSpacing: -0.8, lineHeight: 1 }}>
                {s.v}
              </div>
              <div className="mt-1.5 text-[11px] uppercase tracking-wide text-mcb-ink-mute">
                {s.l}
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

/* ─── Leaderboard Ad ─────────────────────────────────────── */

function LeaderboardAd() {
  return (
    <div className="flex justify-center border-b border-mcb-line bg-mcb-surface px-5 py-7 md:px-10">
      <AdSlot placement="homepage_top" className="w-full max-w-[970px]" label="Advertisement · leaderboard" size="970 × 120" />
    </div>
  )
}

/* ─── Dual Boards: Top Rates + Popular ───────────────────── */

async function DualBoards() {
  const [rates, popular] = await Promise.all([
    getHighestEarnRates(30),
    getFeaturedRetailers(30),
  ])

  return (
    <section className="border-b border-mcb-line bg-mcb-surface px-5 py-14 md:px-10">
      <div className="mx-auto grid max-w-[1200px] grid-cols-1 items-start gap-10 lg:grid-cols-2 lg:gap-12">
        {/* Top rates */}
        <div id="top-rates" className="min-h-0">
          <div className="mb-4 flex items-baseline justify-between">
            <div>
              <div className="mb-1.5 text-[11px] uppercase tracking-wider text-mcb-accent-ink">
                Live · ranked by effective cash
              </div>
              <h2 className="font-serif text-[34px] text-mcb-ink" style={{ letterSpacing: -0.8, fontWeight: 400 }}>
                Top rates right now
              </h2>
            </div>
          </div>

          {/* Header */}
          <div className="grid grid-cols-[28px_1fr_90px] border-b border-mcb-line pb-2.5 text-[10px] uppercase tracking-wider text-mcb-ink-mute">
            <div>#</div>
            <div>Store</div>
            <div className="text-right">Rate</div>
          </div>

          {/* Rows */}
          {rates.map((r, i) => (
            <Link
              key={`${r.retailer_slug}-${r.source_slug}`}
              href={`/store/${r.retailer_slug}`}
              className="grid h-11 grid-cols-[28px_1fr_90px] items-center border-b border-mcb-line-soft hover:bg-mcb-bg"
            >
              <div className="tabular-nums text-[11px] text-mcb-ink-mute">
                {String(i + 1).padStart(2, '0')}
              </div>
              <div className="flex min-w-0 items-center gap-2.5">
                <StoreLogo name={r.retailer_name} size={26} />
                <span className="truncate text-[13px] font-medium text-mcb-ink">
                  {r.retailer_name}
                </span>
              </div>
              <div className="tabular-nums text-right text-[16px] font-semibold text-mcb-ink">
                {r.effective_cash_percentage != null
                  ? <>{r.effective_cash_percentage.toFixed(0)}<span className="ml-0.5 text-[12px] font-medium text-mcb-ink-mute">%</span></>
                  : formatRate(r)
                }
              </div>
            </Link>
          ))}
        </div>

        {/* Popular */}
        <div id="popular">
          <div className="mb-4 flex items-baseline justify-between">
            <div>
              <div className="mb-1.5 text-[11px] uppercase tracking-wider text-mcb-accent-ink">
                Most visited · last 7 days
              </div>
              <h2 className="font-serif text-[34px] text-mcb-ink" style={{ letterSpacing: -0.8, fontWeight: 400 }}>
                Popular right now
              </h2>
            </div>
          </div>

          {/* Header */}
          <div className="grid grid-cols-[28px_1fr_130px] border-b border-mcb-line pb-2.5 text-[10px] uppercase tracking-wider text-mcb-ink-mute">
            <div>#</div>
            <div>Store</div>
            <div className="text-right">Best rate</div>
          </div>

          {/* Rows */}
          {popular.map((r, i) => (
            <Link
              key={r.slug}
              href={`/store/${r.slug}`}
              className="grid h-11 grid-cols-[28px_1fr_130px] items-center border-b border-mcb-line-soft hover:bg-mcb-bg"
            >
              <div className="tabular-nums text-[11px] text-mcb-ink-mute">
                {String(i + 1).padStart(2, '0')}
              </div>
              <div className="flex min-w-0 items-center gap-2.5">
                <StoreLogo name={r.name} size={26} />
                <span className="truncate text-[13px] font-medium text-mcb-ink">
                  {r.name}
                </span>
              </div>
              <div className="tabular-nums truncate text-right text-[14px] font-semibold text-mcb-ink">
                {r.best_rate ?? '—'}
              </div>
            </Link>
          ))}
        </div>
      </div>
    </section>
  )
}

/* ─── Billboard Ad ───────────────────────────────────────── */

function BillboardAd() {
  return (
    <div className="flex justify-center border-b border-mcb-line bg-mcb-bg px-5 py-9 md:px-10">
      <AdSlot placement="homepage_inline" className="w-full max-w-[970px]" label="Advertisement · billboard" size="970 × 250" />
    </div>
  )
}

/* ─── Portal Grid ────────────────────────────────────────── */

function PortalGrid() {
  const portals = [
    { id: 'rakuten', name: 'Rakuten.ca', short: 'RK', color: '#BF0000' },
    { id: 'gcr', name: 'Great Canadian Rebates', short: 'GCR', color: '#006B3C' },
    { id: 'swagbucks', name: 'Swagbucks', short: 'SB', color: '#0A7AA6' },
    { id: 'aeroplan', name: 'Aeroplan eStore', short: 'AE', color: '#D3273E' },
    { id: 'topcashback', name: 'TopCashback', short: 'TCB', color: '#E8344C' },
    { id: 'airmiles', name: 'Air Miles Shops', short: 'AM', color: '#0066A4' },
  ]

  return (
    <section id="portals" className="border-b border-mcb-line bg-mcb-surface px-5 py-16 md:px-10">
      <div className="mx-auto max-w-[1200px]">
        <div className="mb-8 flex items-baseline justify-between">
          <div>
            <h2 className="font-serif text-[34px] text-mcb-ink" style={{ letterSpacing: -0.8 }}>
              Six portals, one scan.
            </h2>
            <p className="mt-1.5 max-w-[540px] text-[14px] text-mcb-ink-soft">
              We pull from every major Canadian cashback and rewards program so you never sign in twice.
            </p>
          </div>
        </div>
        <div className="grid grid-cols-1 gap-3.5 sm:grid-cols-2 lg:grid-cols-3">
          {portals.map(p => (
            <div
              key={p.id}
              className="flex items-center gap-3.5 rounded-xl border border-mcb-line bg-mcb-bg px-5 py-4"
            >
              <div
                className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[10px] text-[13px] font-bold text-white"
                style={{ background: p.color, letterSpacing: 0.3 }}
              >
                {p.short}
              </div>
              <div className="min-w-0 flex-1">
                <div className="text-[14px] font-medium text-mcb-ink">{p.name}</div>
                <div className="tabular-nums text-[12px] text-mcb-ink-mute">
                  Tracked · updated 2h ago
                </div>
              </div>
              <span className="text-[12px] text-mcb-ink-soft">&rarr;</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

/* ─── Categories ─────────────────────────────────────────── */

function Categories() {
  const categories = [
    { name: 'Apparel', count: 312 },
    { name: 'Electronics', count: 148 },
    { name: 'Travel', count: 97 },
    { name: 'Food & Grocery', count: 83 },
    { name: 'Software', count: 276 },
    { name: 'Beauty', count: 194 },
    { name: 'Home', count: 221 },
    { name: 'Learning', count: 64 },
  ]

  return (
    <section id="categories" className="border-b border-mcb-line bg-mcb-bg px-5 py-14 md:px-10">
      <div className="mx-auto max-w-[1200px]">
        <h2 className="mb-6 font-serif text-[30px] text-mcb-ink" style={{ letterSpacing: -0.6 }}>
          Browse by category
        </h2>
        <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-4">
          {categories.map(c => (
            <div
              key={c.name}
              className="flex flex-col gap-1 rounded-xl border border-mcb-line bg-mcb-surface px-4 py-4"
            >
              <div className="text-[14px] font-medium text-mcb-ink">{c.name}</div>
              <div className="tabular-nums text-[12px] text-mcb-ink-mute">{c.count} stores</div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

/* ─── Extension CTA ──────────────────────────────────────── */

function ExtensionCTA() {
  return (
    <section className="border-b border-mcb-line bg-mcb-surface px-5 py-16 md:px-10">
      <div className="mx-auto grid max-w-[1200px] grid-cols-1 items-center gap-12 lg:grid-cols-[1.1fr_1fr]">
        <div>
          <div className="mb-3 text-[11px] uppercase tracking-wider text-mcb-accent-ink">
            Browser extension
          </div>
          <h2 className="font-serif text-mcb-ink" style={{ fontSize: 'clamp(30px, 4vw, 42px)', letterSpacing: -1, lineHeight: 1.05 }}>
            Never miss a rate.{' '}
            <em className="text-mcb-accent-ink">Ever.</em>
          </h2>
          <p className="mt-4 max-w-[480px] text-[15px] leading-relaxed text-mcb-ink-soft">
            A quiet ribbon appears at checkout showing the best portal for the store you&apos;re on. One click, activated.
          </p>
          <div className="mt-5 flex gap-2.5">
            <span className="rounded-[10px] bg-mcb-ink px-5 py-3 text-[14px] font-medium text-mcb-bg">
              Add to Chrome
            </span>
            <span className="rounded-[10px] border border-mcb-line bg-mcb-bg px-5 py-3 text-[14px] font-medium text-mcb-ink">
              Firefox · Safari
            </span>
          </div>
        </div>

        {/* Browser mock */}
        <div className="rounded-2xl border border-mcb-line bg-mcb-bg p-5">
          <div className="mb-3.5 flex items-center gap-1.5">
            <div className="h-2.5 w-2.5 rounded-full" style={{ background: '#ED6A5E' }} />
            <div className="h-2.5 w-2.5 rounded-full" style={{ background: '#F5BE4F' }} />
            <div className="h-2.5 w-2.5 rounded-full" style={{ background: '#62C555' }} />
            <div className="flex-1 text-center text-[11px] text-mcb-ink-mute">lululemon.com</div>
          </div>
          <div className="flex items-center gap-3 rounded-[10px] border border-mcb-accent bg-mcb-accent-soft p-3.5">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-mcb-accent font-serif text-lg italic text-mcb-surface">
              m
            </div>
            <div className="min-w-0 flex-1">
              <div className="text-[13px] font-medium text-mcb-ink">
                Best rate: <b>2 pts/$</b> via Aeroplan eStore
              </div>
              <div className="mt-0.5 text-[11px] text-mcb-ink-soft">
                + 0.5% Rakuten · stackable with TD Cash Back Visa
              </div>
            </div>
            <span className="rounded-[7px] bg-mcb-accent px-3 py-1.5 text-[12px] font-medium text-mcb-surface">
              Activate
            </span>
          </div>
        </div>
      </div>
    </section>
  )
}

/* ─── Skeletons ──────────────────────────────────────────── */

function SearchSkeleton() {
  return (
    <div className="flex h-16 max-w-[760px] items-center rounded-xl border border-mcb-line bg-mcb-bg px-5">
      <span className="animate-pulse text-mcb-ink-mute">Loading search…</span>
    </div>
  )
}

function BoardsSkeleton() {
  return (
    <div className="border-b border-mcb-line bg-mcb-surface px-5 py-14 md:px-10">
      <div className="mx-auto grid max-w-[1200px] animate-pulse grid-cols-1 gap-10 lg:grid-cols-2">
        <div className="h-[800px] rounded-xl bg-mcb-bg" />
        <div className="h-[800px] rounded-xl bg-mcb-bg" />
      </div>
    </div>
  )
}

/* ─── Page ───────────────────────────────────────────────── */

export default function HomePage() {
  return (
    <>
      <Suspense fallback={
        <section className="border-b border-mcb-line bg-mcb-surface px-5 pb-11 pt-16 md:px-10">
          <div className="mx-auto max-w-[1200px] animate-pulse">
            <div className="mb-5 h-3 w-48 rounded bg-mcb-bg" />
            <div className="h-16 max-w-[700px] rounded bg-mcb-bg" />
          </div>
        </section>
      }>
        <Hero />
      </Suspense>

      <LeaderboardAd />

      <Suspense fallback={<BoardsSkeleton />}>
        <DualBoards />
      </Suspense>

      <BillboardAd />

      <PortalGrid />
      <Categories />
      <ExtensionCTA />
    </>
  )
}
