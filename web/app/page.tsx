import Link from 'next/link'
import { Suspense } from 'react'
import AdSlot from '@/components/AdSlot'
import SearchBox from '@/components/SearchBox'
import {
  getFeaturedRetailers,
  getHighestEarnRates,
  type CashbackRate,
} from '@/lib/db'

function SearchSection() {
  return <SearchBox />
}

async function FeaturedRetailersSection() {
  const retailers = await getFeaturedRetailers()

  return (
    <section className="w-full rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="flex items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-gray-950">
            Most viewed stores
          </h2>
          <p className="mt-1 text-sm text-gray-500">
            Top {retailers.length} stores by visitor traffic.
          </p>
        </div>
      </div>

      <div className="mt-5 overflow-hidden rounded-xl border border-gray-200">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-left text-xs uppercase tracking-wide text-gray-500">
            <tr>
              <th className="w-10 px-4 py-3 text-center">#</th>
              <th className="px-4 py-3">Store</th>
              <th className="px-4 py-3 text-right">Best Rate</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100 bg-white">
            {retailers.map((retailer, index) => (
              <tr key={retailer.id} className="hover:bg-gray-50">
                <td className="px-4 py-2.5 text-center text-xs font-medium text-gray-400">
                  {index + 1}
                </td>
                <td className="px-4 py-2.5">
                  <Link
                    href={`/store/${retailer.slug}`}
                    className="font-medium text-blue-600 hover:underline"
                  >
                    {retailer.name}
                  </Link>
                </td>
                <td className="px-4 py-2.5 text-right">
                  {retailer.best_rate ? (
                    <span className="font-medium text-green-700">
                      {retailer.best_rate}
                    </span>
                  ) : (
                    <span className="text-gray-400">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

function formatRate(rate: CashbackRate): string {
  const prefix = rate.is_up_to ? 'Up to ' : ''
  if (rate.rate_type === 'percentage') return `${prefix}${rate.rate_value}%`
  if (rate.rate_type === 'points_per_dollar') return `${prefix}${rate.rate_value} pts/$`
  return `${prefix}$${rate.rate_value}`
}

async function HighestEarnRatesSection() {
  const rates = await getHighestEarnRates()

  return (
    <section className="w-full rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="flex items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-gray-950">
            Highest earn rates
          </h2>
          <p className="mt-1 text-sm text-gray-500">
            Top 30 current offers sorted by effective cash rate.
          </p>
        </div>
        <span className="rounded-full bg-green-50 px-3 py-1 text-xs font-medium text-green-700">
          Live from current rates
        </span>
      </div>

      <div className="mt-5 overflow-hidden rounded-xl border border-gray-200">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-left text-xs uppercase tracking-wide text-gray-500">
            <tr>
              <th className="px-4 py-3">Store</th>
              <th className="px-4 py-3">Portal</th>
              <th className="px-4 py-3">Offer</th>
              <th className="px-4 py-3 text-right">Effective cash</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100 bg-white">
            {rates.map((rate) => (
              <tr key={`${rate.retailer_slug}-${rate.source_slug}`} className="hover:bg-gray-50">
                <td className="px-4 py-3">
                  <Link
                    href={`/store/${rate.retailer_slug}`}
                    className="font-medium text-gray-900 hover:text-blue-600"
                  >
                    {rate.retailer_name}
                  </Link>
                </td>
                <td className="px-4 py-3 text-gray-600">{rate.source_name}</td>
                <td className="px-4 py-3">
                  <span className="inline-flex rounded-full bg-blue-50 px-2.5 py-1 font-medium text-blue-700">
                    {formatRate(rate)}
                  </span>
                </td>
                <td className="px-4 py-3 text-right font-semibold text-green-700">
                  {rate.effective_cash_percentage?.toFixed(2)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

function SearchSkeleton() {
  return (
    <div className="w-full max-w-xl animate-pulse rounded-lg border border-gray-200 bg-gray-100 py-3 px-4 text-transparent">
      Loading stores…
    </div>
  )
}

function SectionSkeleton({ height }: { height: string }) {
  return (
    <div className={`w-full animate-pulse rounded-2xl border border-gray-200 bg-white ${height}`} />
  )
}

export default function HomePage() {
  return (
    <div className="space-y-10 py-10">
      <section className="rounded-3xl border border-gray-200 bg-gradient-to-br from-white via-blue-50 to-emerald-50 px-6 py-10 shadow-sm sm:px-8">
        <div className="mx-auto flex max-w-3xl flex-col items-center gap-8 text-center">
          <div>
            <div className="inline-flex rounded-full border border-blue-200 bg-white/80 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-blue-700">
              Daily cashback snapshots
            </div>
            <h1 className="mt-4 text-4xl font-bold tracking-tight text-gray-950 sm:text-5xl">
              Find the best Canadian cashback rate
            </h1>
            <p className="mt-3 text-lg text-gray-600">
              Search stores instantly, browse popular merchants, and scan the strongest earn rates across every portal we track.
            </p>
          </div>

          <Suspense fallback={<SearchSkeleton />}>
            <SearchSection />
          </Suspense>
        </div>
      </section>

      <AdSlot placement="homepage_top" />

      <div className="grid gap-8 xl:grid-cols-[1fr_1.4fr]">
        <Suspense fallback={<SectionSkeleton height="h-96" />}>
          <FeaturedRetailersSection />
        </Suspense>

        <Suspense fallback={<SectionSkeleton height="h-[32rem]" />}>
          <HighestEarnRatesSection />
        </Suspense>
      </div>

      <AdSlot placement="homepage_inline" />
    </div>
  )
}
