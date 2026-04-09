import { adsenseClient } from '@/lib/ads'

const GOOGLE_CERT_AUTHORITY_ID = 'f08c47fec0942fa0'

function getAdsTxtContent(): string {
  if (!adsenseClient) return ''

  const sellerId = adsenseClient.replace(/^ca-/, '')
  return `google.com, ${sellerId}, DIRECT, ${GOOGLE_CERT_AUTHORITY_ID}\n`
}

export function GET() {
  const body = getAdsTxtContent()

  return new Response(body, {
    status: body ? 200 : 404,
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
      'Cache-Control': 'public, max-age=3600',
    },
  })
}
