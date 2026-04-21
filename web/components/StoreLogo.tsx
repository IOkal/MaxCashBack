export function hashNameToHue(name: string): number {
  let h = 0
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) % 360
  return h
}

type StoreLogoProps = {
  name: string
  size?: number
}

export default function StoreLogo({ name, size = 34 }: StoreLogoProps) {
  const h = hashNameToHue(name)
  return (
    <div
      className="flex shrink-0 items-center justify-center font-sans font-semibold"
      style={{
        width: size,
        height: size,
        borderRadius: size > 40 ? 14 : 8,
        background: `oklch(0.94 0.04 ${h})`,
        color: `oklch(0.38 0.08 ${h})`,
        fontSize: size * 0.44,
        letterSpacing: -0.2,
      }}
    >
      {name[0]}
    </div>
  )
}
