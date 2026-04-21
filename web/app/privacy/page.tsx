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
    <div className="mx-auto max-w-3xl space-y-8 px-5 py-12 md:px-10">
      <div className="space-y-3">
        <h1 className="font-serif text-3xl text-mcb-ink" style={{ letterSpacing: -0.8 }}>Privacy Policy</h1>
        <p className="text-base leading-7 text-mcb-ink-soft">
          This page explains what information MaxCashBack collects, how it is used, and what role
          advertising and analytics may play on the site.
        </p>
      </div>

      <section className="rounded-xl border border-mcb-line bg-mcb-surface p-6">
        <h2 className="text-lg font-semibold text-mcb-ink">Information we collect</h2>
        <p className="mt-3 text-sm leading-7 text-mcb-ink-soft">
          MaxCashBack does not require users to create an account to browse cashback data. We may
          collect basic usage information such as page views, retailer popularity, approximate
          device or browser information, and other standard analytics data used to understand how
          the site is used.
        </p>
      </section>

      <section className="rounded-xl border border-mcb-line bg-mcb-surface p-6">
        <h2 className="text-lg font-semibold text-mcb-ink">Analytics</h2>
        <p className="mt-3 text-sm leading-7 text-mcb-ink-soft">
          We use analytics tools to measure traffic and understand which pages and retailers are
          most useful to visitors. This helps us improve site quality, track usage trends, and
          prioritize data cleanup.
        </p>
      </section>

      <section className="rounded-xl border border-mcb-line bg-mcb-surface p-6">
        <h2 className="text-lg font-semibold text-mcb-ink">Advertising</h2>
        <p className="mt-3 text-sm leading-7 text-mcb-ink-soft">
          MaxCashBack may display advertising, including Google AdSense ads. Advertising providers
          may use cookies or similar technologies to serve ads, measure performance, and improve ad
          relevance where permitted by law and platform policy.
        </p>
      </section>

      <section className="rounded-xl border border-mcb-line bg-mcb-surface p-6">
        <h2 className="text-lg font-semibold text-mcb-ink">External links</h2>
        <p className="mt-3 text-sm leading-7 text-mcb-ink-soft">
          The site links to third-party cashback portals and merchant pages. Once you leave
          MaxCashBack, their own privacy policies and terms apply.
        </p>
      </section>

      <section className="rounded-xl border border-mcb-line bg-mcb-surface p-6">
        <h2 className="text-lg font-semibold text-mcb-ink">Contact</h2>
        <p className="mt-3 text-sm leading-7 text-mcb-ink-soft">
          If you have questions about this policy, contact us at{' '}
          <a
            href="mailto:help@maxcashback.ca"
            className="font-medium text-mcb-accent-ink hover:underline"
          >
            help@maxcashback.ca
          </a>
          .
        </p>
      </section>
    </div>
  )
}
