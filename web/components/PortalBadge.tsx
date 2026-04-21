const PORTAL_COLORS: Record<string, string> = {
  'Rakuten.ca': '#BF0000',
  'Great Canadian Rebates': '#006B3C',
  'Swagbucks': '#0A7AA6',
  'Aeroplan eStore': '#D3273E',
  'TopCashback': '#E8344C',
  'Drop': '#6B4FBB',
  'Air Miles Shops': '#0066A4',
}

const PORTAL_SHORTS: Record<string, string> = {
  'Rakuten.ca': 'RK',
  'Great Canadian Rebates': 'GCR',
  'Swagbucks': 'SB',
  'Aeroplan eStore': 'AE',
  'TopCashback': 'TCB',
  'Air Miles Shops': 'AM',
  'Drop': 'DR',
}

export function getPortalColor(name: string): string {
  return PORTAL_COLORS[name] ?? '#888'
}

type PortalBadgeProps = {
  name: string
  size?: number
}

export default function PortalBadge({ name, size = 32 }: PortalBadgeProps) {
  const color = getPortalColor(name)
  const short = PORTAL_SHORTS[name] ?? name.slice(0, 2).toUpperCase()

  return (
    <div
      className="flex shrink-0 items-center justify-center font-sans font-bold"
      style={{
        width: size,
        height: size,
        borderRadius: 7,
        background: color,
        color: 'white',
        fontSize: size * 0.35,
        letterSpacing: 0.3,
      }}
    >
      {short}
    </div>
  )
}

export function PortalDot({ name }: { name: string }) {
  return (
    <span
      className="inline-block rounded-full align-middle"
      style={{
        width: 5,
        height: 5,
        background: getPortalColor(name),
        marginRight: 6,
      }}
    />
  )
}
