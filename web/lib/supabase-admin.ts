import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY

// This client bypasses RLS — only use server-side.
// It's lazy-initialized so the frontend build doesn't fail when the key isn't set.
export function getSupabaseAdmin() {
  if (!supabaseUrl || !supabaseServiceKey) {
    throw new Error('Missing SUPABASE_SERVICE_ROLE_KEY env var')
  }
  return createClient(supabaseUrl, supabaseServiceKey)
}
