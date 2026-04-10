import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'Privacy Policy',
  description:
    'Read the MaxCashBack privacy policy, including information about analytics, advertising, and how we use site data.',
  alternates: {
    canonical: 'https://maxcashback.ca/privacy',
  },
  openGraph: {
    title: 'MaxCashBack Privacy Policy',
    description:
      'Read the MaxCashBack privacy policy, including information about analytics, advertising, and how we use site data.',
    url: 'https://maxcashback.ca/privacy',
    siteName: 'MaxCashBack',
    type: 'website',
    locale: 'en_CA',
  },
}

export default function PrivacyPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-8">
      <div className="space-y-3">
        <h1 className="text-3xl font-bold tracking-tight text-gray-950">Privacy Policy</h1>
        <p className="text-base leading-7 text-gray-600">
          This page explains what information MaxCashBack collects, how it is used, and what role
          advertising and analytics may play on the site.
        </p>
      </div>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Information we collect</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          MaxCashBack does not require users to create an account to browse cashback data. We may
          collect basic usage information such as page views, retailer popularity, approximate
          device or browser information, and other standard analytics data used to understand how
          the site is used.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Analytics</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          We use analytics tools to measure traffic and understand which pages and retailers are
          most useful to visitors. This helps us improve site quality, track usage trends, and
          prioritize data cleanup.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Advertising</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          MaxCashBack may display advertising, including Google AdSense ads. Advertising providers
          may use cookies or similar technologies to serve ads, measure performance, and improve ad
          relevance where permitted by law and platform policy.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">External links</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          The site links to third-party cashback portals and merchant pages. Once you leave
          MaxCashBack, their own privacy policies and terms apply.
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Contact</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          If you have questions about this policy, contact us at{' '}
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
