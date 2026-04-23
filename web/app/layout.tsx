import { Suspense } from 'react'
import type { Metadata } from 'next'
import Link from 'next/link'
import Script from 'next/script'
import { Inter, Instrument_Serif } from 'next/font/google'
import { adsEnabled, adsenseClient } from '@/lib/ads'
import GoogleAnalytics from '@/components/GoogleAnalytics'
import NavSearch from '@/components/NavSearch'
import './globals.css'

const inter = Inter({
  subsets: ['latin'],
  variable: '--font-inter',
  display: 'swap',
})

const instrumentSerif = Instrument_Serif({
  weight: '400',
  subsets: ['latin'],
  style: ['normal', 'italic'],
  variable: '--font-instrument-serif',
  display: 'swap',
})

const googleSiteVerification = process.env.GOOGLE_SITE_VERIFICATION?.trim()
const currentYear = new Date().getFullYear()

export const metadata: Metadata = {
  title: {
    default: 'MaxCashBack — Canadian Cashback Comparison',
    template: '%s | MaxCashBack',
  },
  description:
    'Compare cashback rates across Rakuten, Great Canadian Rebates, Aeroplan eStore, TopCashback Canada and more. Find the best Canadian cashback portal for every store.',
  metadataBase: new URL('https://maxcashback.ca'),
  openGraph: {
    title: 'MaxCashBack — Canadian Cashback Comparison',
    description:
      'Compare cashback rates across Rakuten, Great Canadian Rebates, Aeroplan eStore, TopCashback Canada and more. Find the best Canadian cashback portal for every store.',
    url: 'https://maxcashback.ca',
    siteName: 'MaxCashBack',
    type: 'website',
    locale: 'en_CA',
  },
  twitter: {
    card: 'summary',
    title: 'MaxCashBack — Canadian Cashback Comparison',
    description:
      'Compare cashback rates across Rakuten, GCR, Aeroplan eStore, TopCashback Canada and more.',
  },
  alternates: {
    canonical: 'https://maxcashback.ca',
  },
  verification: googleSiteVerification
    ? { google: googleSiteVerification }
    : undefined,
}

function NavLinks() {
  return (
    <nav className="hidden items-center gap-7 text-[14px] text-mcb-ink-soft md:flex" style={{ marginLeft: 48 }}>
      <Link href="/" className="font-medium text-mcb-ink">Browse</Link>
      <Link href="/#top-rates" className="hover:text-mcb-ink">Top rates</Link>
      <Link href="/#popular" className="hover:text-mcb-ink">Popular</Link>
      <Link href="/#portals" className="hover:text-mcb-ink">Portals</Link>
    </nav>
  )
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" className={`${inter.variable} ${instrumentSerif.variable}`}>
      <body className="min-h-screen antialiased">
        {adsEnabled && adsenseClient && (
          <Script
            id="adsense-script"
            async
            strategy="afterInteractive"
            src={`https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=${adsenseClient}`}
            crossOrigin="anonymous"
          />
        )}

        {/* Nav */}
        <header className="sticky top-0 z-10 flex h-16 items-center border-b border-mcb-line bg-mcb-surface px-5 md:px-10">
          {/* Logo */}
          <Link href="/" className="flex items-center gap-2.5">
            <div className="flex h-7 w-7 items-center justify-center rounded-[7px] bg-mcb-ink font-serif text-lg italic text-mcb-bg">
              m
            </div>
            <span className="text-[15px] font-semibold tracking-tight text-mcb-ink" style={{ letterSpacing: -0.3 }}>
              MaxCashBack
            </span>
            <span className="ml-1.5 rounded border border-mcb-line px-1.5 py-0.5 text-[10px] text-mcb-ink-mute">
              CA
            </span>
          </Link>

          {/* Desktop nav links */}
          <NavLinks />

          {/* Nav search (visible on store pages) */}
          <div className="ml-8 hidden flex-1 lg:block" style={{ maxWidth: 520 }}>
            <Suspense fallback={null}>
              <NavSearch />
            </Suspense>
          </div>

          {/* Right side */}
          <div className="ml-auto flex items-center gap-3.5">
            <span className="hidden text-[13px] text-mcb-ink-soft sm:block">Sign in</span>
            <span className="rounded-lg bg-mcb-ink px-3.5 py-2 text-[13px] font-medium text-mcb-bg">
              Get alerts
            </span>
          </div>
        </header>

        <main>{children}</main>

        {/* Footer */}
        <footer className="bg-mcb-ink text-mcb-bg">
          <div className="mx-auto max-w-[1200px] px-10 pb-9 pt-14">
            <div className="grid grid-cols-1 gap-12 sm:grid-cols-2 lg:grid-cols-[2fr_1fr_1fr_1fr]">
              {/* Brand */}
              <div>
                <div className="mb-3.5 flex items-center gap-2.5">
                  <div className="flex h-7 w-7 items-center justify-center rounded-[7px] bg-mcb-bg font-serif text-lg italic text-mcb-ink">
                    m
                  </div>
                  <span className="text-[15px] font-semibold">MaxCashBack</span>
                </div>
                <p className="max-w-[320px] text-[13px] leading-relaxed opacity-60">
                  Independent cashback comparison. Not affiliated with any portal. Rates scraped daily — always verify before purchasing.
                </p>
              </div>

              {/* Product */}
              <div>
                <div className="mb-3 text-[11px] uppercase tracking-wider opacity-50">Product</div>
                <div className="space-y-2">
                  <Link href="/" className="block text-[13px] opacity-85 hover:opacity-100">Browse stores</Link>
                  <Link href="/#top-rates" className="block text-[13px] opacity-85 hover:opacity-100">Top rates</Link>
                  <Link href="/#popular" className="block text-[13px] opacity-85 hover:opacity-100">Popular</Link>
                  <Link href="/#portals" className="block text-[13px] opacity-85 hover:opacity-100">Portals</Link>
                </div>
              </div>

              {/* Portals */}
              <div>
                <div className="mb-3 text-[11px] uppercase tracking-wider opacity-50">Portals</div>
                <div className="space-y-2">
                  <a href="https://www.rakuten.ca" target="_blank" rel="noopener noreferrer" className="block text-[13px] opacity-85 hover:opacity-100">Rakuten.ca</a>
                  <a href="https://www.greatcanadianrebates.ca" target="_blank" rel="noopener noreferrer" className="block text-[13px] opacity-85 hover:opacity-100">Great Canadian Rebates</a>
                  <a href="https://www.swagbucks.com" target="_blank" rel="noopener noreferrer" className="block text-[13px] opacity-85 hover:opacity-100">Swagbucks</a>
                  <a href="https://www.aircanada.com/ca/en/aco/home/aeroplan/estore.html" target="_blank" rel="noopener noreferrer" className="block text-[13px] opacity-85 hover:opacity-100">Aeroplan eStore</a>
                </div>
              </div>

              {/* Company */}
              <div>
                <div className="mb-3 text-[11px] uppercase tracking-wider opacity-50">Company</div>
                <div className="space-y-2">
                  <Link href="/about" className="block text-[13px] opacity-85 hover:opacity-100">About</Link>
                  <Link href="/contact" className="block text-[13px] opacity-85 hover:opacity-100">Contact</Link>
                  <Link href="/privacy" className="block text-[13px] opacity-85 hover:opacity-100">Privacy</Link>
                  <Link href="/disclaimer" className="block text-[13px] opacity-85 hover:opacity-100">Disclaimer</Link>
                </div>
              </div>
            </div>

            <div className="mt-12 flex justify-between border-t border-white/10 pt-6 text-[12px] opacity-50">
              <div>&copy; {currentYear} MaxCashBack</div>
            </div>
          </div>
        </footer>

        <Suspense fallback={null}>
          <GoogleAnalytics />
        </Suspense>
      </body>
    </html>
  )
}
