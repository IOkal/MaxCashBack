import { type NextRequest, NextResponse } from 'next/server'
import { getSupabaseAdmin } from '@/lib/supabase-admin'

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/

export async function POST(request: NextRequest) {
  try {
    const { email, feature } = await request.json()

    if (typeof email !== 'string' || !EMAIL_RE.test(email.trim())) {
      return NextResponse.json({ error: 'Invalid email' }, { status: 400 })
    }

    const supabaseAdmin = getSupabaseAdmin()
    const { error } = await supabaseAdmin.from('waitlist_signups').insert({
      email: email.trim().toLowerCase(),
      feature: typeof feature === 'string' && feature.trim() ? feature.trim() : 'card_stacking',
    })

    // 23505 = unique_violation — already on the list, treat as success
    if (error && error.code !== '23505') {
      console.error('[waitlist] insert error:', error.message)
      return NextResponse.json({ error: 'Failed' }, { status: 500 })
    }

    return NextResponse.json({ ok: true })
  } catch {
    return NextResponse.json({ error: 'Bad request' }, { status: 400 })
  }
}
