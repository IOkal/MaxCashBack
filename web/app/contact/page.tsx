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
    <div className="mx-auto max-w-3xl space-y-8">
      <div className="space-y-3">
        <h1 className="text-3xl font-bold tracking-tight text-gray-950">Contact Us</h1>
        <p className="text-base leading-7 text-gray-600">
          If you spot incorrect cashback data, a broken retailer merge, or anything else on the
          site that looks wrong, send us an email.
        </p>
      </div>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">Email</h2>
        <p className="mt-3 text-sm leading-7 text-gray-600">
          Reach us at{' '}
          <a
            href={`mailto:${contactEmail}`}
            className="font-medium text-blue-600 hover:underline"
          >
            {contactEmail}
          </a>
          .
        </p>
      </section>

      <section className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="text-lg font-semibold text-gray-950">What to contact us about</h2>
        <ul className="mt-3 list-disc space-y-2 pl-5 text-sm leading-7 text-gray-600">
          <li>Incorrect cashback or rewards rates</li>
          <li>Duplicate or broken retailer pages</li>
          <li>General questions, support, or feedback about the site</li>
        </ul>
      </section>
    </div>
  )
}
