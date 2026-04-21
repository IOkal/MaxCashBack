'use client'

import Link from 'next/link'

export default function StoreError({
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <div className="mx-auto max-w-[1200px] px-5 py-16 md:px-10">
      <Link href="/" className="text-[13px] text-mcb-accent-ink hover:underline">
        &larr; Back to search
      </Link>
      <div className="mt-8 flex flex-col items-center gap-4 text-center">
        <h2 className="font-serif text-2xl text-mcb-ink">Couldn&apos;t load store data</h2>
        <p className="text-mcb-ink-soft">
          This is likely a temporary issue. The rates you&apos;d see here are at most 12 hours old.
        </p>
        <button
          onClick={reset}
          className="rounded-lg bg-mcb-ink px-4 py-2 text-sm font-medium text-mcb-bg hover:opacity-90"
        >
          Try again
        </button>
      </div>
    </div>
  )
}
