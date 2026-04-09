'use client'

export default function GlobalError({
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <div className="flex flex-col items-center gap-4 py-20 text-center">
      <h2 className="text-2xl font-bold">Something went wrong</h2>
      <p className="text-gray-500">
        We&apos;re having trouble loading data right now. Rates may be temporarily unavailable.
      </p>
      <button
        onClick={reset}
        className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
      >
        Try again
      </button>
    </div>
  )
}
