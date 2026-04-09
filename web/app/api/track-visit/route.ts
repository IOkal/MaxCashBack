import { type NextRequest, NextResponse } from 'next/server'
import { getSupabaseAdmin } from '@/lib/supabase-admin'

export async function POST(request: NextRequest) {
  try {
    const { retailer_id } = await request.json()

    if (typeof retailer_id !== 'number' || retailer_id <= 0) {
      return NextResponse.json({ error: 'Invalid retailer_id' }, { status: 400 })
    }

    const supabaseAdmin = getSupabaseAdmin()
    const { error } = await supabaseAdmin.rpc('increment_store_visit', {
      p_retailer_id: retailer_id,
    })

    if (error) {
      console.error('[track-visit] RPC error:', error.message)
      return NextResponse.json({ error: 'Failed' }, { status: 500 })
    }

    return NextResponse.json({ ok: true })
  } catch {
    return NextResponse.json({ error: 'Bad request' }, { status: 400 })
  }
}
