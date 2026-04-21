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
  label?: string
  size?: string
}

export default function AdSlot({
  placement,
  className = '',
  label = 'Advertisement',
  size,
}: AdSlotProps) {
  const pushedRef = useRef(false)
  const slot = getAdSlot(placement)
  const enabled = isAdPlacementEnabled(placement)

  useEffect(() => {
    if (!enabled || pushedRef.current || typeof window === 'undefined') return

    // Defer push until the ad container is visible and has a non-zero width
    const timer = window.setTimeout(() => {
      try {
        window.adsbygoogle = window.adsbygoogle || []
        window.adsbygoogle.push({})
        pushedRef.current = true
      } catch {
        // AdSense may throw if the slot was already filled or has no width — safe to ignore
      }
    }, 100)

    return () => window.clearTimeout(timer)
  }, [enabled, placement])

  if (!enabled) return null

  return (
    <aside
      className={`flex flex-col items-center justify-center gap-1.5 rounded-[10px] border border-dashed border-mcb-line bg-mcb-ad-bg ${className}`.trim()}
      aria-label="Advertisement"
      style={{ minHeight: 120 }}
    >
      <p className="text-[10px] uppercase tracking-[1.2px] text-mcb-ink-mute">
        {label}
      </p>
      {size && (
        <p className="text-[13px] font-medium text-mcb-ink-soft">{size}</p>
      )}
      <div className="min-h-[90px]">
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
