import { Suspense } from 'react'
import { notFound } from 'next/navigation'
import Link from 'next/link'
import type { Metadata } from 'next'
import AdSlot from '@/components/AdSlot'
import { getRetailerBySlug, getStoreRates } from '@/lib/db'
import type { CashbackRate } from '@/lib/db'

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

async function StoreContent({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params
  const [retailer, rates] = await Promise.all([
    getRetailerBySlug(slug),
    getStoreRates(slug),
  ])

  if (!retailer) notFound()

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
                      {timeAgo(rate.scraped_at)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
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
