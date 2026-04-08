export type AdPlacement = 'homepage_top' | 'homepage_inline' | 'store_inline'

const TRUE_VALUES = new Set(['1', 'true', 'yes', 'on'])

export const adsEnabled = TRUE_VALUES.has(
  process.env.NEXT_PUBLIC_ENABLE_ADS?.toLowerCase() ?? ''
)

export const adsenseClient = process.env.NEXT_PUBLIC_ADSENSE_CLIENT?.trim() ?? ''

const slotMap: Record<AdPlacement, string> = {
  homepage_top: process.env.NEXT_PUBLIC_ADSENSE_SLOT_HOMEPAGE_TOP?.trim() ?? '',
  homepage_inline: process.env.NEXT_PUBLIC_ADSENSE_SLOT_HOMEPAGE_INLINE?.trim() ?? '',
  store_inline: process.env.NEXT_PUBLIC_ADSENSE_SLOT_STORE_INLINE?.trim() ?? '',
}

export function getAdSlot(placement: AdPlacement): string {
  return slotMap[placement]
}

export function isAdPlacementEnabled(placement: AdPlacement): boolean {
  return adsEnabled && Boolean(adsenseClient) && Boolean(getAdSlot(placement))
}
