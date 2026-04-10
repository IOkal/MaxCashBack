import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'Disclaimer',
  description:
    'Read the MaxCashBack disclaimer about cashback accuracy, portal terms, and shopping decisions.',
  alternates: {
    canonical: 'https://maxcashback.ca/disclaimer',
  },
  openGraph: {
    title: 'MaxCashBack Disclaimer',
    description:
      'Read the MaxCashBack disclaimer about cashback accuracy, portal terms, and shopping decisions.',
    url: 'https://maxcashback.ca/disclaimer',
    siteName: 'MaxCashBack',
    type: 'website',
    locale: 'en_CA',
  },
}

export default function DisclaimerPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-8">
      <div className="space-y-3">
        <h1 className="text-3xl font-bold tracking-tight text-gray-950">Disclaimer</h1>
        <p className="text-base leading-7 text-gray-600">
          MaxCashBack is an informational comparison site. We work to keep cashback and rewards
          data accurate, but portal rates and shopping terms can change at any time.
        </p>
      </div>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Rates and availability</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          Cashback, fixed payouts, and points-per-dollar rates are scraped from third-party portals
          and may change without notice. Rates shown on MaxCashBack may lag behind a portal&apos;s
          real-time updates.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Always verify before purchasing</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          Before making a purchase, always confirm the final rate, exclusions, and portal terms on
          the destination cashback site. Product categories, promotions, coupon use, and card
          offers may all affect eligibility.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">No financial or legal advice</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          Content on MaxCashBack is provided for general information only and should not be treated
          as financial, legal, tax, or shopping advice.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Questions or corrections</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          If you notice inaccurate data or a broken retailer page, contact us at{' '}
          <a
            href="mailto:help@maxcashback.ca"
            className="font-medium text-blue-600 hover:underline"
          >
            help@maxcashback.ca
          </a>
          .
        </p>
      </section>
    </div>
  )
}
