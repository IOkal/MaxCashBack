import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'About Us',
  description:
    'Learn how MaxCashBack compares Canadian cashback and rewards portal rates across major providers.',
  alternates: {
    canonical: 'https://maxcashback.ca/about',
  },
  openGraph: {
    title: 'About MaxCashBack',
    description:
      'Learn how MaxCashBack compares Canadian cashback and rewards portal rates across major providers.',
    url: 'https://maxcashback.ca/about',
    siteName: 'MaxCashBack',
    type: 'website',
    locale: 'en_CA',
  },
}

export default function AboutPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-8">
      <div className="space-y-3">
        <h1 className="text-3xl font-bold tracking-tight text-gray-950">About MaxCashBack</h1>
        <p className="text-base leading-7 text-gray-600">
          MaxCashBack helps Canadian shoppers compare cashback and rewards rates across the major
          portals we track, so it is easier to find the best available return before making a
          purchase.
        </p>
      </div>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">What we track</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          We currently compare rates from Rakuten, Great Canadian Rebates, and Aeroplan eStore.
          Rates are scraped daily and shown side by side on retailer pages so you can quickly see
          which portal is strongest at that moment.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">How to use the site</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          Search for a retailer, compare the listed portal offers, and click through to the portal
          you want to use. Historical snapshots may also be shown when available to help you
          understand how rates have changed recently.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Important disclaimer</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          Cashback and rewards rates can change at any time, and portal terms may vary by product,
          category, or promotion. Always verify the final rate and the portal&apos;s terms before
          purchasing.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">How MaxCashBack makes money</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          MaxCashBack may earn money through advertising and affiliate relationships. That does not
          change our goal of showing accurate, easy-to-compare cashback information for Canadian
          users.
        </p>
      </section>
    </div>
  )
}
