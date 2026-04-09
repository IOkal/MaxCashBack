'use client'

import Link from 'next/link'

export default function StoreError({
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <div className="space-y-6">
      <div>
        <Link href="/" className="text-sm text-blue-600 hover:underline">
          ← Back to search
        </Link>
      </div>
      <div className="flex flex-col items-center gap-4 py-16 text-center">
        <h2 className="text-2xl font-bold">Couldn&apos;t load store data</h2>
        <p className="text-gray-500">
          This is likely a temporary issue. The rates you&apos;d see here are at most 12 hours old.
        </p>
        <button
          onClick={reset}
          className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
        >
          Try again
        </button>
      </div>
    </div>
  )
}
