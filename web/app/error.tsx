'use client'

export default function GlobalError({
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <div className="flex flex-col items-center gap-4 px-5 py-20 text-center">
      <h2 className="font-serif text-2xl text-mcb-ink">Something went wrong</h2>
      <p className="text-mcb-ink-soft">
        We&apos;re having trouble loading data right now. Rates may be temporarily unavailable.
      </p>
      <button
        onClick={reset}
        className="rounded-lg bg-mcb-ink px-4 py-2 text-sm font-medium text-mcb-bg hover:opacity-90"
      >
        Try again
      </button>
    </div>
  )
}
