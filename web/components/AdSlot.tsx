'use client'

import { useEffect, useRef } from 'react'
import { adsenseClient, getAdSlot, isAdPlacementEnabled, type AdPlacement } from '@/lib/ads'

declare global {
  interface Window {
    adsbygoogle?: unknown[]
  }
}

type AdSlotProps = {
  placement: AdPlacement
  className?: string
}

export default function AdSlot({ placement, className = '' }: AdSlotProps) {
  const pushedRef = useRef(false)
  const slot = getAdSlot(placement)
  const enabled = isAdPlacementEnabled(placement)

  useEffect(() => {
    if (!enabled || pushedRef.current || typeof window === 'undefined') return

    try {
      window.adsbygoogle = window.adsbygoogle || []
      window.adsbygoogle.push({})
      pushedRef.current = true
    } catch (error) {
      console.error(`Failed to initialize AdSense slot for ${placement}`, error)
    }
  }, [enabled, placement])

  if (!enabled) return null

  return (
    <aside
      className={`rounded-2xl border border-gray-200 bg-white p-4 shadow-sm ${className}`.trim()}
      aria-label="Advertisement"
    >
      <p className="mb-3 text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-400">
        Advertisement
      </p>
      <div className="min-h-[140px]">
        <ins
          className="adsbygoogle block"
          style={{ display: 'block' }}
          data-ad-client={adsenseClient}
          data-ad-slot={slot}
          data-ad-format="auto"
          data-full-width-responsive="true"
        />
      </div>
    </aside>
  )
}
