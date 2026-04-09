import { NextResponse } from 'next/server'
import { searchRetailers } from '@/lib/db'

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url)
  const query = searchParams.get('q') ?? ''

  const retailers = await searchRetailers(query)
  return NextResponse.json({ retailers })
}
