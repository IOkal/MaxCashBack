import type { Metadata } from 'next'

const contactEmail = 'help@maxcashback.ca'

export const metadata: Metadata = {
  title: 'Contact Us',
  description: 'Contact MaxCashBack about data issues, retailer merges, or general site feedback.',
  alternates: {
    canonical: 'https://maxcashback.ca/contact',
  },
  openGraph: {
    title: 'Contact MaxCashBack',
    description:
      'Contact MaxCashBack about data issues, retailer merges, or general site feedback.',
    url: 'https://maxcashback.ca/contact',
    siteName: 'MaxCashBack',
    type: 'website',
    locale: 'en_CA',
  },
}

export default function ContactPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-8 px-5 py-12 md:px-10">
      <div className="space-y-3">
        <h1 className="font-serif text-3xl text-mcb-ink" style={{ letterSpacing: -0.8 }}>Contact Us</h1>
        <p className="text-base leading-7 text-mcb-ink-soft">
          If you spot incorrect cashback data, a broken retailer merge, or anything else on the
          site that looks wrong, send us an email.
        </p>
      </div>

      <section className="rounded-xl border border-mcb-line bg-mcb-surface p-6">
        <h2 className="text-lg font-semibold text-mcb-ink">Email</h2>
        <p className="mt-3 text-sm leading-7 text-mcb-ink-soft">
          Reach us at{' '}
          <a
            href={`mailto:${contactEmail}`}
            className="font-medium text-mcb-accent-ink hover:underline"
          >
            {contactEmail}
          </a>
          .
        </p>
      </section>

      <section className="rounded-xl border border-mcb-line bg-mcb-surface p-6">
        <h2 className="text-lg font-semibold text-mcb-ink">What to contact us about</h2>
        <ul className="mt-3 list-disc space-y-2 pl-5 text-sm leading-7 text-mcb-ink-soft">
          <li>Incorrect cashback or rewards rates</li>
          <li>Duplicate or broken retailer pages</li>
          <li>General questions, support, or feedback about the site</li>
        </ul>
      </section>
    </div>
  )
}
