import type { MetadataRoute } from 'next'
import { getAllRetailers } from '@/lib/db'

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const retailers = await getAllRetailers()

  const storeUrls: MetadataRoute.Sitemap = retailers.map((r) => ({
    url: `https://maxcashback.ca/store/${r.slug}`,
    changeFrequency: 'daily',
    priority: 0.7,
  }))

  return [
    {
      url: 'https://maxcashback.ca',
      changeFrequency: 'daily',
      priority: 1,
    },
    {
      url: 'https://maxcashback.ca/about',
      changeFrequency: 'monthly',
      priority: 0.5,
    },
    {
      url: 'https://maxcashback.ca/contact',
      changeFrequency: 'monthly',
      priority: 0.5,
    },
    ...storeUrls,
  ]
}
