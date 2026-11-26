'use client'

import { useState, type FormEvent } from 'react'

type Props = {
  feature?: string
}

export default function WaitlistSignup({ feature = 'card_stacking' }: Props) {
  const [email, setEmail] = useState('')
  const [status, setStatus] = useState<'idle' | 'loading' | 'done' | 'error'>('idle')

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    if (status === 'loading' || status === 'done') return

    setStatus('loading')
    try {
      const res = await fetch('/api/waitlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, feature }),
      })
      if (!res.ok) throw new Error('request failed')
      setStatus('done')
    } catch {
      setStatus('error')
    }
  }

  if (status === 'done') {
    return (
      <div className="mt-3 rounded-[7px] border border-mcb-line bg-mcb-bg px-3 py-2 text-center text-[12px] font-medium text-mcb-ink">
        You&apos;re on the list — we&apos;ll email you when it&apos;s ready.
      </div>
    )
  }

  return (
    <form onSubmit={handleSubmit} className="mt-3 flex flex-col gap-2">
      <input
        type="email"
        required
        value={email}
        onChange={e => setEmail(e.target.value)}
        placeholder="you@email.com"
        className="w-full rounded-[7px] border border-mcb-line bg-mcb-bg px-3 py-2 text-[12px] text-mcb-ink placeholder:text-mcb-ink-soft focus:outline-none focus:ring-1 focus:ring-mcb-accent-ink"
      />
      <button
        type="submit"
        disabled={status === 'loading'}
        className="rounded-[7px] border border-mcb-line bg-mcb-bg px-3 py-2 text-center text-[12px] font-medium text-mcb-ink disabled:opacity-60"
      >
        {status === 'loading' ? 'Joining…' : 'Join the waitlist →'}
      </button>
      {status === 'error' && (
        <p className="text-[11px] text-red-600">Something went wrong — try again.</p>
      )}
    </form>
  )
}
