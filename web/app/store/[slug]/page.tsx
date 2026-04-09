import { Suspense } from 'react'
import { after } from 'next/server'
import { notFound } from 'next/navigation'
import Link from 'next/link'
import type { Metadata } from 'next'
import AdSlot from '@/components/AdSlot'
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

function formatEffectiveCash(rate: CashbackRate): string | null {
  if (rate.effective_cash_percentage == null) return null
  if (rate.rate_type === 'percentage') return null
  return `≈ ${rate.effective_cash_percentage.toFixed(2)}%`
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

function buildSparklinePath(points: StoreHistoryPoint[], width: number, height: number): string {
  if (points.length === 0) return ''

  const values = points.map((point) => point.effective_cash_percentage)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  const horizontalStep = points.length > 1 ? (width - 24) / (points.length - 1) : 0

  return points
    .map((point, index) => {
      const x = 12 + index * horizontalStep
      const y = 12 + ((max - point.effective_cash_percentage) / range) * (height - 24)
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`
    })
    .join(' ')
}

function HistoryCard({ history }: { history: StoreRateHistory }) {
  if (history.points.length === 0) return null

  const path = buildSparklinePath(history.points, 640, 180)
  const latestPoints = history.points.slice(-7).reverse()

  return (
    <section className="rounded-lg border border-gray-200 bg-white p-5 shadow-sm">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 className="text-lg font-semibold text-gray-950">30-day best-rate history</h2>
          <p className="mt-1 text-sm text-gray-500">
            Daily snapshot of the best effective cashback rate across all tracked portals.
          </p>
        </div>
        <div className="text-sm text-gray-500">
          {history.points[0]?.label} to {history.points.at(-1)?.label}
        </div>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-4">
        <div className="rounded-lg bg-gray-50 px-4 py-3">
          <div className="text-xs uppercase tracking-wide text-gray-400">Current</div>
          <div className="mt-1 text-xl font-semibold text-gray-950">
            {history.current?.effective_cash_percentage.toFixed(2)}%
          </div>
        </div>
        <div className="rounded-lg bg-gray-50 px-4 py-3">
          <div className="text-xs uppercase tracking-wide text-gray-400">30-day high</div>
          <div className="mt-1 text-xl font-semibold text-gray-950">
            {history.high?.toFixed(2)}%
          </div>
        </div>
        <div className="rounded-lg bg-gray-50 px-4 py-3">
          <div className="text-xs uppercase tracking-wide text-gray-400">30-day low</div>
          <div className="mt-1 text-xl font-semibold text-gray-950">
            {history.low?.toFixed(2)}%
          </div>
        </div>
        <div className="rounded-lg bg-gray-50 px-4 py-3">
          <div className="text-xs uppercase tracking-wide text-gray-400">30-day delta</div>
          <div className="mt-1 text-xl font-semibold text-gray-950">
            {formatSignedPercentage(history.delta)}
          </div>
        </div>
      </div>

      <div className="mt-5 rounded-lg border border-gray-200 bg-gradient-to-b from-gray-50 to-white p-3">
        <svg
          viewBox="0 0 640 180"
          className="h-40 w-full"
          role="img"
          aria-label="30-day best rate trend"
        >
          <path
            d={path}
            fill="none"
            stroke="currentColor"
            strokeWidth="3"
            className="text-blue-600"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          {history.points.length > 0 && (
            <circle
              cx={history.points.length > 1 ? 628 : 12}
              cy={
                12 +
                ((Math.max(...history.points.map((point) => point.effective_cash_percentage)) -
                  history.points[history.points.length - 1].effective_cash_percentage) /
                  (Math.max(...history.points.map((point) => point.effective_cash_percentage)) -
                    Math.min(...history.points.map((point) => point.effective_cash_percentage)) ||
                    1)) *
                  (180 - 24)
              }
              r="4"
              className="fill-blue-600"
            />
          )}
        </svg>
      </div>

      <div className="mt-5 overflow-hidden rounded-lg border border-gray-200">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-xs uppercase tracking-wide text-gray-500">
            <tr>
              <th className="px-4 py-3 text-left">Day</th>
              <th className="px-4 py-3 text-left">Best rate</th>
              <th className="px-4 py-3 text-left">Portal</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {latestPoints.map((point) => (
              <tr key={point.date}>
                <td className="px-4 py-3 text-gray-600">{point.label}</td>
                <td className="px-4 py-3 font-medium text-gray-950">
                  {point.rate_display ?? `${point.effective_cash_percentage.toFixed(2)}%`}
                </td>
                <td className="px-4 py-3 text-gray-600">{point.source_name}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

async function StoreContent({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params
  const [retailer, rates, history] = await Promise.all([
    getRetailerBySlug(slug),
    getStoreRates(slug),
    getStoreRateHistory(slug),
  ])

  if (!retailer) notFound()

  // Track store visit server-side after the response is sent
  after(async () => {
    try {
      const supabaseAdmin = getSupabaseAdmin()
      await supabaseAdmin.rpc('increment_store_visit', {
        p_retailer_id: retailer.id,
      })
    } catch (err) {
      console.error('[store-visit] Failed to track visit:', err)
    }
  })

  const bestRate = rates[0] ?? null

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">{retailer.name}</h1>
        {retailer.category && (
          <p className="mt-1 text-sm text-gray-500">{retailer.category}</p>
        )}
      </div>

      {rates.length === 0 ? (
        <div className="rounded-lg border border-dashed border-gray-300 p-8 text-center text-gray-500">
          No cashback rates found for this store yet.
        </div>
      ) : (
        <>
          {bestRate && (
            <div className="rounded-lg border border-green-200 bg-green-50 px-5 py-4">
              <p className="text-sm font-medium text-green-700">Best rate</p>
              <p className="mt-1 text-2xl font-bold text-green-800">
                {formatRate(bestRate)}{' '}
                <span className="text-base font-normal text-green-700">
                  via {bestRate.source_name}
                </span>
              </p>
            </div>
          )}

          <AdSlot placement="store_inline" />

          <div className="overflow-hidden rounded-lg border border-gray-200 bg-white">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-xs uppercase tracking-wide text-gray-500">
                <tr>
                  <th className="px-4 py-3 text-left">Portal</th>
                  <th className="px-4 py-3 text-left">Rate</th>
                  <th className="px-4 py-3 text-left">Type</th>
                  <th className="px-4 py-3 text-right">Updated</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {rates.map((rate) => (
                  <tr key={rate.source_slug} className="hover:bg-gray-50">
                    <td className="px-4 py-3 font-medium">
                      <a
                        href={rate.source_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-blue-600 hover:underline"
                      >
                        {rate.source_name}
                      </a>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <span
                          className={`inline-block rounded px-2 py-0.5 text-sm font-semibold ${
                            rate.effective_cash_percentage != null
                              ? 'bg-green-100 text-green-800'
                              : 'bg-gray-100 text-gray-700'
                          }`}
                        >
                          {formatRate(rate)}
                        </span>
                        {formatEffectiveCash(rate) && (
                          <span className="text-xs text-gray-400">
                            {formatEffectiveCash(rate)}
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-3 text-gray-500 capitalize">
                      {rate.rate_type === 'points_per_dollar' ? 'Points' : rate.rate_type}
                    </td>
                    <td className="px-4 py-3 text-right text-gray-400">
                      {timeAgo(rate.last_seen_at)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <HistoryCard history={history} />
        </>
      )}
    </div>
  )
}

function StoreContentSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-9 w-48 rounded bg-gray-200" />
      <div className="h-20 rounded-lg bg-gray-100" />
      <div className="h-48 rounded-lg bg-gray-100" />
    </div>
  )
}

export default function StorePage({ params }: Props) {
  return (
    <div className="space-y-6">
      <div>
        <Link href="/" className="text-sm text-blue-600 hover:underline">
          ← Back to search
        </Link>
      </div>

      <Suspense fallback={<StoreContentSkeleton />}>
        <StoreContent params={params} />
      </Suspense>
    </div>
  )
}
