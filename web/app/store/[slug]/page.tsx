import { Suspense } from 'react'
import { after } from 'next/server'
import { notFound } from 'next/navigation'
import Link from 'next/link'
import type { Metadata } from 'next'
import AdSlot from '@/components/AdSlot'
import StoreLogo from '@/components/StoreLogo'
import PortalBadge, { PortalDot, getPortalColor } from '@/components/PortalBadge'
import { getRetailerBySlug, getStoreRateHistory, getStoreRates } from '@/lib/db'
import { getSupabaseAdmin } from '@/lib/supabase-admin'
import type { CashbackRate, StoreHistoryPoint, StoreRateHistory } from '@/lib/db'

type Props = { params: Promise<{ slug: string }> }

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params
  const [retailer, rates] = await Promise.all([
    getRetailerBySlug(slug),
    getStoreRates(slug),
  ])
  if (!retailer) return { title: 'Store not found' }

  const best = rates[0]
  const desc = best
    ? `Compare ${rates.length} cashback rate${rates.length > 1 ? 's' : ''} for ${retailer.name}. Best: ${best.rate_display ?? `${best.rate_value}%`} via ${best.source_name}.`
    : `Compare cashback rates for ${retailer.name} across Canadian portals.`

  return {
    title: `${retailer.name} Cashback Rates`,
    description: desc,
    openGraph: {
      title: `${retailer.name} — Best Canadian Cashback Rates`,
      description: desc,
      url: `https://maxcashback.ca/store/${slug}`,
      siteName: 'MaxCashBack',
      type: 'website',
    },
  }
}

function formatRate(rate: CashbackRate): string {
  const prefix = rate.is_up_to ? 'Up to ' : ''
  if (rate.rate_type === 'percentage') return `${prefix}${rate.rate_value}%`
  if (rate.rate_type === 'points_per_dollar') return `${prefix}${rate.rate_value} pts/$`
  return `${prefix}$${rate.rate_value}`
}

const RAKUTEN_REFERRAL = 'referrerid=5GYj2b3FkAQ%3D&src=Link'

function affiliateUrl(url: string): string {
  if (url.includes('rakuten.ca')) {
    return url + (url.includes('?') ? '&' : '?') + RAKUTEN_REFERRAL
  }
  return url
}

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime()
  const hours = Math.floor(diff / 3_600_000)
  if (hours < 1) return 'just now'
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}

function formatSignedPercentage(value: number | null): string {
  if (value == null) return 'N/A'
  if (value === 0) return '0.00%'
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
}

/* ─── Breadcrumb ─────────────────────────────────────────── */

function Breadcrumb({ category, name }: { category: string | null; name: string }) {
  return (
    <div className="flex items-center gap-2 text-[12px] text-mcb-ink-mute">
      <Link href="/" className="text-mcb-ink-soft hover:text-mcb-ink">Home</Link>
      <span>/</span>
      {category && (
        <>
          <span className="text-mcb-ink-soft">{category}</span>
          <span>/</span>
        </>
      )}
      <span className="text-mcb-ink">{name}</span>
    </div>
  )
}

/* ─── Comparison Table───────────────────────────────────── */

function ComparisonTable({ rates }: { rates: CashbackRate[] }) {
  return (
    <div className="px-5 pt-10 md:px-10">
      <div className="mx-auto grid max-w-[1200px] grid-cols-1 gap-8 lg:grid-cols-[1fr_300px]">
        {/* Table */}
        <div>
          <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-baseline sm:justify-between">
            <div>
              <h2 className="font-serif text-[28px] text-mcb-ink" style={{ letterSpacing: -0.6 }}>
                All portals
              </h2>
              <p className="mt-1 text-[13px] text-mcb-ink-soft">
                Sorted by effective cash rate. Verify before purchase.
              </p>
            </div>
          </div>

          <div className="overflow-hidden rounded-xl border border-mcb-line bg-mcb-surface">
            {/* Header */}
            <div className="grid grid-cols-[1.4fr_1fr_0.8fr_100px_80px_90px] border-b border-mcb-line bg-mcb-bg px-4 py-3.5 text-[10px] uppercase tracking-wider text-mcb-ink-mute max-md:hidden">
              <div>Portal</div>
              <div>Offer</div>
              <div>Type</div>
              <div className="text-right">Effective</div>
              <div className="text-right">Updated</div>
              <div />
            </div>

            {/* Rows */}
            {rates.map((rate, i) => {
              const isBest = i === 0
              return (
                <div
                  key={rate.source_slug}
                  className={`grid items-center gap-3 border-b border-mcb-line-soft px-4 py-4 last:border-b-0 max-md:grid-cols-2 md:grid-cols-[1.4fr_1fr_0.8fr_100px_80px_90px] ${
                    isBest ? 'bg-mcb-accent-soft' : 'bg-mcb-surface'
                  }`}
                  style={isBest ? { borderLeft: '3px solid var(--color-mcb-accent)' } : undefined}
                >
                  {/* Portal */}
                  <div className="flex items-center gap-3">
                    <PortalBadge name={rate.source_name} />
                    <div className="min-w-0">
                      <div className="flex items-center gap-2 text-[14px] font-medium text-mcb-ink">
                        {rate.source_name}
                        {isBest && (
                          <span className="rounded border border-mcb-accent bg-mcb-surface px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-mcb-accent-ink">
                            Best
                          </span>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Offer */}
                  <div className="tabular-nums font-serif text-[18px] text-mcb-ink" style={{ letterSpacing: -0.3 }}>
                    {formatRate(rate)}
                  </div>

                  {/* Type */}
                  <div className="flex items-center gap-1.5 text-[12px] text-mcb-ink-soft max-md:hidden">
                    <span
                      className="inline-block h-[5px] w-[5px] rounded-full"
                      style={{
                        background: rate.rate_type === 'points_per_dollar'
                          ? 'oklch(0.6 0.12 260)'
                          : 'var(--color-mcb-pos)',
                      }}
                    />
                    {rate.rate_type === 'points_per_dollar' ? 'Points' : 'Cashback'}
                  </div>

                  {/* Effective */}
                  <div className={`tabular-nums text-right font-serif text-[22px] text-mcb-ink max-md:hidden ${isBest ? 'font-semibold text-mcb-accent-ink' : ''}`}
                    style={{ letterSpacing: -0.3 }}
                  >
                    {rate.effective_cash_percentage != null
                      ? <>{rate.effective_cash_percentage.toFixed(2)}<span className="text-[12px] text-mcb-ink-mute">%</span></>
                      : '—'
                    }
                  </div>

                  {/* Updated */}
                  <div className="tabular-nums text-right text-[12px] text-mcb-ink-mute max-md:hidden">
                    {timeAgo(rate.last_seen_at)}
                  </div>

                  {/* Shop button */}
                  <div className="text-right max-md:col-span-2">
                    {rate.source_url && (
                      <a
                        href={affiliateUrl(rate.source_url)}
                        target="_blank"
                        rel="noopener noreferrer sponsored"
                        className={`inline-flex items-center gap-1.5 rounded-[7px] px-3 py-1.5 text-[12px] font-medium ${
                          isBest
                            ? 'bg-mcb-ink text-mcb-bg'
                            : 'border border-mcb-line bg-mcb-surface text-mcb-ink'
                        }`}
                      >
                        Shop
                        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2">
                          <path d="M7 17 17 7M7 7h10v10" />
                        </svg>
                      </a>
                    )}
                  </div>
                </div>
              )
            })}
          </div>

          {/* Legend */}
          <div className="mt-2.5 flex gap-3.5 text-[11px] text-mcb-ink-mute">
            <div className="flex items-center gap-1.5">
              <span className="inline-block h-[5px] w-[5px] rounded-full bg-mcb-pos" />
              Cashback
            </div>
            <div className="flex items-center gap-1.5">
              <span className="inline-block h-[5px] w-[5px] rounded-full" style={{ background: 'oklch(0.6 0.12 260)' }} />
              Points — converted to CAD at published rate
            </div>
          </div>
        </div>

        {/* Sidebar */}
        <div className="flex flex-col gap-4 max-lg:hidden">
          <AdSlot placement="store_inline" className="min-h-[600px]" label="Advertisement" size="300 × 600" />

          {/* Stack teaser */}
          <div className="rounded-xl border border-mcb-line bg-mcb-surface p-5">
            <div className="mb-2 text-[10px] font-semibold uppercase tracking-[1px] text-mcb-accent-ink">
              Coming soon
            </div>
            <h3 className="font-serif text-[20px] text-mcb-ink" style={{ letterSpacing: -0.4, lineHeight: 1.2 }}>
              Stack with your card.
            </h3>
            <p className="mt-2 text-[13px] leading-relaxed text-mcb-ink-soft">
              Add 2–4% more on top of portal cashback. We&apos;ll suggest the best credit card for each store.
            </p>
            <div className="mt-3 rounded-[7px] border border-mcb-line bg-mcb-bg px-3 py-2 text-center text-[12px] font-medium text-mcb-ink">
              Join the waitlist &rarr;
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

/* ─── 30-Day History ─────────────────────────────────────── */

function buildSparklinePath(points: StoreHistoryPoint[], width: number, height: number): string {
  if (points.length === 0) return ''
  const values = points.map(p => p.effective_cash_percentage)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  const pad = { t: 20, r: 20, b: 24, l: 32 }
  const plotW = width - pad.l - pad.r
  const plotH = height - pad.t - pad.b
  const step = points.length > 1 ? plotW / (points.length - 1) : 0

  return points
    .map((p, i) => {
      const x = pad.l + i * step
      const y = pad.t + ((max - p.effective_cash_percentage) / range) * plotH
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`
    })
    .join(' ')
}

function buildAreaPath(linePath: string, points: StoreHistoryPoint[], width: number, height: number): string {
  if (!linePath) return ''
  const pad = { t: 20, r: 20, b: 24, l: 32 }
  const plotH = height - pad.t - pad.b
  const plotW = width - pad.l - pad.r
  const step = points.length > 1 ? plotW / (points.length - 1) : 0
  const lastX = pad.l + (points.length - 1) * step
  return `${linePath} L${lastX.toFixed(1)},${(pad.t + plotH).toFixed(1)} L${pad.l},${(pad.t + plotH).toFixed(1)} Z`
}

function HistorySection({ history }: { history: StoreRateHistory }) {
  if (history.points.length === 0) return null

  const chartW = 820
  const chartH = 180
  const linePath = buildSparklinePath(history.points, chartW, chartH)
  const areaPath = buildAreaPath(linePath, history.points, chartW, chartH)
  const latestPoints = history.points.slice().reverse().slice(0, 10)

  // Y-axis labels
  const values = history.points.map(p => p.effective_cash_percentage)
  const maxVal = Math.max(...values)
  const ySteps = [0, Math.round(maxVal / 3), Math.round((maxVal * 2) / 3), Math.round(maxVal)]

  return (
    <section className="mt-10 px-5 pb-10 md:px-10">
      <div className="mx-auto max-w-[1200px]">
        <div className="mb-5 flex items-baseline justify-between">
          <div>
            <h2 className="font-serif text-[28px] text-mcb-ink" style={{ letterSpacing: -0.6 }}>
              30-day best-rate history
            </h2>
            <p className="mt-1 text-[13px] text-mcb-ink-soft">
              Daily snapshot of the highest effective rate across all portals.
            </p>
          </div>
          <span className="text-[11px] uppercase tracking-wider text-mcb-ink-mute">Last 30 days</span>
        </div>

        <div className="grid grid-cols-1 gap-8 lg:grid-cols-[1fr_360px]">
          {/* Chart */}
          <div className="rounded-xl border border-mcb-line bg-mcb-surface p-5">
            <svg viewBox={`0 0 ${chartW} ${chartH}`} className="h-auto w-full" role="img" aria-label="30-day best rate trend">
              <defs>
                <linearGradient id="historyAreaFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="var(--color-mcb-accent)" stopOpacity="0.2" />
                  <stop offset="100%" stopColor="var(--color-mcb-accent)" stopOpacity="0" />
                </linearGradient>
              </defs>
              {/* Y gridlines */}
              {ySteps.map(v => {
                const y = 20 + (maxVal > 0 ? ((maxVal - v) / maxVal) * (chartH - 44) : 0)
                return (
                  <g key={v}>
                    <line x1={32} y1={y} x2={chartW - 20} y2={y} stroke="var(--color-mcb-line-soft)" strokeWidth="1" />
                    <text x={24} y={y + 3} fontFamily="var(--font-sans)" fontSize="9" fill="var(--color-mcb-ink-mute)" textAnchor="end">
                      {v}%
                    </text>
                  </g>
                )
              })}
              {/* Area + line */}
              <path d={areaPath} fill="url(#historyAreaFill)" />
              <path d={linePath} fill="none" stroke="var(--color-mcb-accent)" strokeWidth="1.8" strokeLinejoin="round" />
              {/* Current point */}
              {history.points.length > 0 && (() => {
                const last = history.points.length - 1
                const vals = history.points.map(p => p.effective_cash_percentage)
                const mn = Math.min(...vals)
                const mx = Math.max(...vals)
                const rng = mx - mn || 1
                const step = history.points.length > 1 ? (chartW - 52) / (history.points.length - 1) : 0
                const cx = 32 + last * step
                const cy = 20 + ((mx - history.points[last].effective_cash_percentage) / rng) * (chartH - 44)
                return (
                  <g>
                    <circle cx={cx} cy={cy} r="5" fill="var(--color-mcb-surface)" stroke="var(--color-mcb-accent)" strokeWidth="2" />
                    <circle cx={cx} cy={cy} r="2" fill="var(--color-mcb-accent)" />
                  </g>
                )
              })()}
              {/* X ticks */}
              {[0, Math.floor(history.points.length / 4), Math.floor(history.points.length / 2), Math.floor(history.points.length * 3 / 4), history.points.length - 1].map(idx => {
                if (idx >= history.points.length) return null
                const step = history.points.length > 1 ? (chartW - 52) / (history.points.length - 1) : 0
                return (
                  <text key={idx} x={32 + idx * step} y={chartH - 6} fontFamily="var(--font-sans)" fontSize="9" fill="var(--color-mcb-ink-mute)" textAnchor="middle">
                    {history.points[idx].label}
                  </text>
                )
              })}
            </svg>
          </div>

          {/* Stat tiles */}
          <div className="grid grid-cols-2 gap-2.5">
            {[
              {
                l: 'Current',
                v: history.current ? `${history.current.effective_cash_percentage.toFixed(2)}%` : '—',
                sub: history.current ? `via ${history.current.source_name}` : '',
                c: 'text-mcb-accent-ink',
              },
              {
                l: '30-day high',
                v: history.high != null ? `${history.high.toFixed(2)}%` : '—',
                sub: '',
                c: 'text-mcb-pos',
              },
              {
                l: '30-day low',
                v: history.low != null ? `${history.low.toFixed(2)}%` : '—',
                sub: '',
                c: 'text-mcb-neg',
              },
              {
                l: '30-day delta',
                v: formatSignedPercentage(history.delta),
                sub: history.delta != null && history.delta > 0 ? 'trending up' : history.delta != null && history.delta < 0 ? 'trending down' : '',
                c: history.delta != null && history.delta >= 0 ? 'text-mcb-pos' : 'text-mcb-neg',
              },
            ].map(s => (
              <div key={s.l} className="rounded-[10px] border border-mcb-line bg-mcb-surface px-4 py-4">
                <div className="text-[10px] uppercase tracking-wider text-mcb-ink-mute">{s.l}</div>
                <div className="tabular-nums mt-1.5 font-serif text-[30px] text-mcb-ink" style={{ letterSpacing: -0.7 }}>
                  {s.v}
                </div>
                {s.sub && <div className={`mt-0.5 text-[11px] ${s.c}`}>{s.sub}</div>}
              </div>
            ))}
          </div>
        </div>

        {/* Daily log */}
        <div className="mt-6">
          <div className="mb-3 text-[11px] uppercase tracking-wider text-mcb-ink-mute">Daily log</div>
          <div className="overflow-hidden rounded-[10px] border border-mcb-line bg-mcb-surface">
            <div className="grid grid-cols-[120px_1fr_1fr_120px] border-b border-mcb-line bg-mcb-bg px-4 py-2.5 text-[10px] uppercase tracking-wider text-mcb-ink-mute">
              <div>Day</div>
              <div>Best offer</div>
              <div>Portal</div>
              <div className="text-right">Effective</div>
            </div>
            {latestPoints.map((point, i) => (
              <div
                key={point.date}
                className={`grid grid-cols-[120px_1fr_1fr_120px] items-center px-4 py-2.5 text-[13px] ${
                  i < latestPoints.length - 1 ? 'border-b border-mcb-line-soft' : ''
                }`}
              >
                <div className="tabular-nums text-mcb-ink-soft">{point.label}</div>
                <div className="text-mcb-ink">{point.rate_display ?? `${point.effective_cash_percentage.toFixed(2)}%`}</div>
                <div className="flex items-center gap-1.5 text-mcb-ink-soft">
                  <PortalDot name={point.source_name} />
                  {point.source_name}
                </div>
                <div className="tabular-nums text-right font-serif text-[16px] text-mcb-ink" style={{ letterSpacing: -0.2 }}>
                  {point.effective_cash_percentage.toFixed(2)}
                  <span className="text-[11px] text-mcb-ink-mute">%</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}

/* ─── Report footnote ────────────────────────────────────── */

function Footnote({ retailerName, slug }: { retailerName: string; slug: string }) {
  const params = new URLSearchParams({
    subject: `Issue with ${retailerName} on MaxCashBack`,
    body: `Store: ${retailerName}\nPage: https://maxcashback.ca/store/${slug}\n\nIssue details:\n`,
  })

  return (
    <div className="mt-8 flex flex-col justify-between gap-2 border-t border-mcb-line bg-mcb-bg px-5 py-6 text-[12px] text-mcb-ink-mute sm:flex-row md:px-10">
      <div>
        Spot a stale rate, duplicate store, or broken portal link?{' '}
        <a href={`mailto:help@maxcashback.ca?${params.toString()}`} className="text-mcb-accent-ink">
          Report an issue &rarr;
        </a>
      </div>
      <div>Rates scraped daily · always verify before purchasing.</div>
    </div>
  )
}

/* ─── Main Content ───────────────────────────────────────── */

async function StoreContent({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params
  const [retailer, rates, history] = await Promise.all([
    getRetailerBySlug(slug),
    getStoreRates(slug),
    getStoreRateHistory(slug),
  ])

  if (!retailer) notFound()

  after(async () => {
    try {
      const supabaseAdmin = getSupabaseAdmin()
      await supabaseAdmin.rpc('increment_store_visit', { p_retailer_id: retailer.id })
    } catch (err) {
      console.error('[store-visit] Failed to track visit:', err)
    }
  })

  const portalCount = rates.length

  return (
    <>
      {/* Store header */}
      <div className="border-b border-mcb-line bg-mcb-surface px-5 pb-7 pt-5 md:px-10">
        <div className="mx-auto max-w-[1200px]">
          <Breadcrumb category={retailer.category} name={retailer.name} />

          <div className="mt-5 flex flex-col gap-5 sm:flex-row sm:items-center">
            <StoreLogo name={retailer.name} size={72} />
            <div className="min-w-0 flex-1">
              <h1 className="font-serif text-mcb-ink" style={{ fontSize: 'clamp(28px, 4vw, 44px)', letterSpacing: -1.2, lineHeight: 1 }}>
                {retailer.name}
              </h1>
              <div className="mt-2.5 flex flex-wrap items-center gap-3 text-[13px] text-mcb-ink-soft">
                {retailer.category && <span>{retailer.category}</span>}
                {retailer.category && <span className="text-mcb-ink-mute">·</span>}
                <span className="flex items-center gap-1.5">
                  <span className="h-1.5 w-1.5 rounded-full bg-mcb-pos" />
                  {portalCount} portals tracked
                </span>
                {rates[0] && (
                  <>
                    <span className="text-mcb-ink-mute">·</span>
                    <span>Updated {timeAgo(rates[0].last_seen_at)}</span>
                  </>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      {rates.length === 0 ? (
        <div className="mx-auto max-w-[1200px] px-5 py-16 text-center md:px-10">
          <p className="text-mcb-ink-soft">No cashback rates found for this store yet.</p>
        </div>
      ) : (
        <>
          <ComparisonTable rates={rates} />
          <HistorySection history={history} />
        </>
      )}

      <Footnote retailerName={retailer.name} slug={slug} />
    </>
  )
}

/* ─── Skeleton ───────────────────────────────────────────── */

function StoreContentSkeleton() {
  return (
    <div className="animate-pulse">
      <div className="border-b border-mcb-line bg-mcb-surface px-5 pb-7 pt-5 md:px-10">
        <div className="mx-auto max-w-[1200px]">
          <div className="h-3 w-32 rounded bg-mcb-bg" />
          <div className="mt-5 flex items-center gap-5">
            <div className="h-[72px] w-[72px] rounded-[14px] bg-mcb-bg" />
            <div className="space-y-3">
              <div className="h-10 w-64 rounded bg-mcb-bg" />
              <div className="h-3 w-48 rounded bg-mcb-bg" />
            </div>
          </div>
        </div>
      </div>
      <div className="mx-5 mt-7 h-48 rounded-2xl bg-mcb-accent-soft md:mx-10" />
      <div className="mx-5 mt-10 h-96 rounded-xl bg-mcb-bg md:mx-10" />
    </div>
  )
}

/* ─── Page ───────────────────────────────────────────────── */

export default function StorePage({ params }: Props) {
  return (
    <Suspense fallback={<StoreContentSkeleton />}>
      <StoreContent params={params} />
    </Suspense>
  )
}
