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
  last_seen_at: string
  effective_cash_percentage: number | null
}

export type HistoricalRate = {
  retailer_id: number
  retailer_slug: string
  retailer_name: string
  source_slug: string
  source_name: string
  rate_value: number
  rate_type: string
  rate_display: string | null
  is_up_to: boolean
  scraped_at: string
  last_seen_at: string
  effective_cash_percentage: number | null
}

export type StoreHistoryPoint = {
  date: string
  label: string
  effective_cash_percentage: number
  source_name: string
  source_slug: string
  rate_display: string | null
}

export type StoreRateHistory = {
  points: StoreHistoryPoint[]
  high: number | null
  low: number | null
  delta: number | null
  current: StoreHistoryPoint | null
}

function startOfUtcDay(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()))
}

function addUtcDays(date: Date, days: number): Date {
  return new Date(date.getTime() + days * 86_400_000)
}

function formatDayLabel(date: Date): string {
  return new Intl.DateTimeFormat('en-CA', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  }).format(date)
}

function buildStoreRateHistory(rows: HistoricalRate[], days = 30): StoreRateHistory {
  const comparableRows = rows
    .filter((row) => row.effective_cash_percentage != null)
    .sort(
      (a, b) => new Date(a.scraped_at).getTime() - new Date(b.scraped_at).getTime()
    )

  if (comparableRows.length === 0) {
    return { points: [], high: null, low: null, delta: null, current: null }
  }

  const rowsBySource = new Map<string, HistoricalRate[]>()
  for (const row of comparableRows) {
    const existing = rowsBySource.get(row.source_slug) ?? []
    existing.push(row)
    rowsBySource.set(row.source_slug, existing)
  }

  const today = startOfUtcDay(new Date())
  const start = addUtcDays(today, -(days - 1))
  const points: StoreHistoryPoint[] = []

  for (let index = 0; index < days; index += 1) {
    const day = addUtcDays(start, index)
    const nextDay = addUtcDays(day, 1)
    const cutoff = nextDay.getTime()
    const candidates: HistoricalRate[] = []

    for (const sourceRows of rowsBySource.values()) {
      let latest: HistoricalRate | null = null
      for (const row of sourceRows) {
        if (new Date(row.scraped_at).getTime() < cutoff) {
          latest = row
        } else {
          break
        }
      }
      if (latest) candidates.push(latest)
    }

    if (candidates.length === 0) continue

    candidates.sort((a, b) => {
      const effectiveDiff =
        (b.effective_cash_percentage ?? -Infinity) -
        (a.effective_cash_percentage ?? -Infinity)
      if (effectiveDiff !== 0) return effectiveDiff
      return new Date(b.scraped_at).getTime() - new Date(a.scraped_at).getTime()
    })

    const best = candidates[0]
    points.push({
      date: day.toISOString().slice(0, 10),
      label: formatDayLabel(day),
      effective_cash_percentage: best.effective_cash_percentage ?? 0,
      source_name: best.source_name,
      source_slug: best.source_slug,
      rate_display: best.rate_display,
    })
  }

  if (points.length === 0) {
    return { points: [], high: null, low: null, delta: null, current: null }
  }

  const values = points.map((point) => point.effective_cash_percentage)
  const current = points.at(-1) ?? null

  return {
    points,
    high: Math.max(...values),
    low: Math.min(...values),
    delta:
      points.length > 1
        ? points[points.length - 1].effective_cash_percentage -
          points[0].effective_cash_percentage
        : 0,
    current,
  }
}

export async function getAllRetailers(): Promise<Retailer[]> {
  const { data, error } = await supabase
    .from('retailers')
    .select('id, name, slug, category')
    .order('name')

  if (error) {
    console.error('[db] getAllRetailers failed:', error.message)
    return []
  }
  return data ?? []
}

export type PopularRetailer = Retailer & {
  best_rate: string | null
}

export async function getFeaturedRetailers(limit = 30): Promise<PopularRetailer[]> {
  const { data: retailers, error } = await supabase
    .from('popular_retailers')
    .select('id, name, slug, category')
    .limit(limit)

  if (error || !retailers?.length) {
    console.error('[db] getFeaturedRetailers failed:', error?.message)
    return []
  }

  // Fetch best rate per retailer in one query
  const slugs = retailers.map((r) => r.slug)
  const { data: rates } = await supabase
    .from('current_rates')
    .select('retailer_slug, rate_display, effective_cash_percentage')
    .in('retailer_slug', slugs)
    .order('effective_cash_percentage', { ascending: false, nullsFirst: false })

  // Group: keep only the best rate per retailer
  const bestBySlug = new Map<string, string>()
  for (const rate of rates ?? []) {
    if (!bestBySlug.has(rate.retailer_slug)) {
      bestBySlug.set(rate.retailer_slug, rate.rate_display ?? `${rate.effective_cash_percentage}%`)
    }
  }

  return retailers.map((r) => ({
    ...r,
    best_rate: bestBySlug.get(r.slug) ?? null,
  }))
}

export async function getRetailerBySlug(slug: string): Promise<Retailer | null> {
  const { data, error } = await supabase
    .from('retailers')
    .select('id, name, slug, category')
    .eq('slug', slug)
    .maybeSingle()

  if (error) {
    console.error('[db] getRetailerBySlug failed:', error.message)
    return null
  }
  return data
}

export async function getStoreRates(slug: string): Promise<CashbackRate[]> {
  const { data, error } = await supabase
    .from('current_rates')
    .select('*')
    .eq('retailer_slug', slug)
    .order('effective_cash_percentage', { ascending: false, nullsFirst: false })

  if (error) {
    console.error('[db] getStoreRates failed:', error.message)
    return []
  }
  return data ?? []
}

export async function getStoreRateHistory(
  slug: string,
  days = 30
): Promise<StoreRateHistory> {
  const { data, error } = await supabase
    .from('historical_rates')
    .select(
      [
        'retailer_id',
        'retailer_slug',
        'retailer_name',
        'source_slug',
        'source_name',
        'rate_value',
        'rate_type',
        'rate_display',
        'is_up_to',
        'scraped_at',
        'last_seen_at',
        'effective_cash_percentage',
      ].join(',')
    )
    .eq('retailer_slug', slug)
    .order('scraped_at', { ascending: true })

  if (error) {
    console.error('[db] getStoreRateHistory failed:', error.message)
    return { points: [], high: null, low: null, delta: null, current: null }
  }

  return buildStoreRateHistory(((data ?? []) as unknown) as HistoricalRate[], days)
}

export async function getHighestEarnRates(limit = 30): Promise<CashbackRate[]> {
  const { data, error } = await supabase
    .from('current_rates')
    .select('*')
    .not('effective_cash_percentage', 'is', null)
    .order('effective_cash_percentage', { ascending: false, nullsFirst: false })
    .order('retailer_name')
    .limit(limit)

  if (error) {
    console.error('[db] getHighestEarnRates failed:', error.message)
    return []
  }
  return data ?? []
}
