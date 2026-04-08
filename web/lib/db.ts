import { supabase } from './supabase'

export type Retailer = {
  id: number
  name: string
  slug: string
  category: string | null
}

export type CashbackRate = {
  retailer_id: number
  retailer_slug: string
  retailer_name: string
  category: string | null
  source_slug: string
  source_name: string
  source_url: string
  rate_value: number
  rate_type: string
  rate_display: string | null
  is_up_to: boolean
  scraped_at: string
  effective_cash_percentage: number | null
}

export async function getAllRetailers(): Promise<Retailer[]> {
  const { data, error } = await supabase
    .from('retailers')
    .select('id, name, slug, category')
    .order('name')

  if (error) throw error
  return data ?? []
}

export async function getFeaturedRetailers(limit = 24): Promise<Retailer[]> {
  const { data, error } = await supabase
    .from('retailers')
    .select('id, name, slug, category')
    .order('name')
    .limit(limit)

  if (error) throw error
  return data ?? []
}

export async function getRetailerBySlug(slug: string): Promise<Retailer | null> {
  const { data, error } = await supabase
    .from('retailers')
    .select('id, name, slug, category')
    .eq('slug', slug)
    .maybeSingle()

  if (error) throw error
  return data
}

export async function getStoreRates(slug: string): Promise<CashbackRate[]> {
  const { data, error } = await supabase
    .from('current_rates')
    .select('*')
    .eq('retailer_slug', slug)
    .order('effective_cash_percentage', { ascending: false, nullsFirst: false })

  if (error) throw error
  return data ?? []
}

export async function getHighestEarnRates(limit = 50): Promise<CashbackRate[]> {
  const { data, error } = await supabase
    .from('current_rates')
    .select('*')
    .not('effective_cash_percentage', 'is', null)
    .order('effective_cash_percentage', { ascending: false, nullsFirst: false })
    .order('retailer_name')
    .limit(limit)

  if (error) throw error
  return data ?? []
}
