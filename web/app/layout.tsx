import { Suspense } from 'react'
import type { Metadata } from 'next'
import Link from 'next/link'
import Script from 'next/script'
import { Geist } from 'next/font/google'
import { adsEnabled, adsenseClient } from '@/lib/ads'
import GoogleAnalytics from '@/components/GoogleAnalytics'
import './globals.css'

const geist = Geist({ subsets: ['latin'] })
const googleSiteVerification = process.env.GOOGLE_SITE_VERIFICATION?.trim()

export const metadata: Metadata = {
  title: {
    default: 'MaxCashBack — Canadian Cashback Comparison',
    template: '%s | MaxCashBack',
  },
  description:
    'Compare cashback rates across Rakuten, Great Canadian Rebates, Aeroplan eStore and more. Find the best Canadian cashback portal for every store.',
  metadataBase: new URL('https://maxcashback.ca'),
  openGraph: {
    title: 'MaxCashBack — Canadian Cashback Comparison',
    description:
      'Compare cashback rates across Rakuten, Great Canadian Rebates, Aeroplan eStore and more. Find the best Canadian cashback portal for every store.',
    url: 'https://maxcashback.ca',
    siteName: 'MaxCashBack',
    type: 'website',
    locale: 'en_CA',
  },
  twitter: {
    card: 'summary',
    title: 'MaxCashBack — Canadian Cashback Comparison',
    description:
      'Compare cashback rates across Rakuten, GCR, Aeroplan eStore and more.',
  },
  alternates: {
    canonical: 'https://maxcashback.ca',
  },
  verification: googleSiteVerification
    ? { google: googleSiteVerification }
    : undefined,
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" className={geist.className}>
      <body className="min-h-screen bg-gray-50 text-gray-900 antialiased">
        {adsEnabled && adsenseClient && (
          <Script
            id="adsense-script"
            async
            strategy="afterInteractive"
            src={`https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=${adsenseClient}`}
            crossOrigin="anonymous"
          />
        )}
        <header className="border-b bg-white">
          <div className="mx-auto flex max-w-4xl items-center gap-3 px-4 py-3">
            <Link href="/" className="text-xl font-bold tracking-tight text-blue-600">
              MaxCashBack
            </Link>
            <span className="text-xs text-gray-400">Canadian cashback comparison</span>
          </div>
        </header>
        <main className="mx-auto max-w-4xl px-4 py-8">{children}</main>
        <footer className="mt-auto border-t bg-white py-6 text-center text-xs text-gray-400">
          Rates scraped daily. Always verify before purchasing.
        </footer>
        <Suspense fallback={null}>
          <GoogleAnalytics />
        </Suspense>
      </body>
    </html>
  )
}
