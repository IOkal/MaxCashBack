'use client'

import { useState, useRef, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import type { Retailer } from '@/lib/db'

export default function SearchBox() {
  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const [activeIndex, setActiveIndex] = useState(-1)
  const [matches, setMatches] = useState<Retailer[]>([])
  const [loading, setLoading] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLUListElement>(null)
  const router = useRouter()

  function handleSelect(retailer: Retailer) {
    setQuery('')
    setMatches([])
    setOpen(false)
    router.push(`/store/${retailer.slug}`)
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (!open || matches.length === 0) return

    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActiveIndex((i) => Math.min(i + 1, matches.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActiveIndex((i) => Math.max(i - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      if (activeIndex >= 0) {
        handleSelect(matches[activeIndex])
      } else if (matches.length > 0) {
        handleSelect(matches[0])
      }
    } else if (e.key === 'Escape') {
      setOpen(false)
    }
  }

  useEffect(() => {
    const trimmed = query.trim()

    if (!trimmed) {
      setMatches([])
      setLoading(false)
      return
    }

    const controller = new AbortController()
    const timeoutId = window.setTimeout(async () => {
      setLoading(true)

      try {
        const response = await fetch(
          `/api/retailers/search?q=${encodeURIComponent(trimmed)}`,
          {
            signal: controller.signal,
            cache: 'no-store',
          }
        )

        if (!response.ok) {
          throw new Error(`Search request failed with ${response.status}`)
        }

        const payload = (await response.json()) as { retailers?: Retailer[] }
        if (!controller.signal.aborted) {
          setMatches(payload.retailers ?? [])
        }
      } catch (error) {
        if (!controller.signal.aborted) {
          console.error('[SearchBox] retailer search failed:', error)
          setMatches([])
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false)
        }
      }
    }, 150)

    return () => {
      controller.abort()
      window.clearTimeout(timeoutId)
    }
  }, [query])

  // Scroll active item into view
  useEffect(() => {
    if (activeIndex >= 0 && listRef.current) {
      const item = listRef.current.children[activeIndex] as HTMLElement
      item?.scrollIntoView({ block: 'nearest' })
    }
  }, [activeIndex])

  return (
    <div className="relative w-full max-w-xl">
      <input
        ref={inputRef}
        type="search"
        value={query}
        onChange={(e) => {
          const nextQuery = e.target.value
          setQuery(nextQuery)
          setMatches([])
          setActiveIndex(-1)
          setOpen(true)
          setLoading(nextQuery.trim().length > 0)
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        onKeyDown={handleKeyDown}
        placeholder="Search for a store… e.g. Amazon, Nike, Best Buy"
        className="w-full rounded-lg border border-gray-300 px-4 py-3 text-base shadow-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-200"
        autoComplete="off"
        spellCheck={false}
      />

      {open && matches.length > 0 && (
        <ul
          ref={listRef}
          className="absolute z-10 mt-1 w-full overflow-auto rounded-lg border border-gray-200 bg-white shadow-lg max-h-72"
          role="listbox"
        >
          {matches.map((r, i) => (
            <li
              key={r.id}
              role="option"
              aria-selected={i === activeIndex}
              onMouseDown={() => handleSelect(r)}
              onMouseEnter={() => setActiveIndex(i)}
              className={`cursor-pointer px-4 py-2.5 text-sm ${
                i === activeIndex ? 'bg-blue-50 text-blue-700' : 'text-gray-800 hover:bg-gray-50'
              }`}
            >
              {r.name}
              {r.category && (
                <span className="ml-2 text-xs text-gray-400">{r.category}</span>
              )}
            </li>
          ))}
        </ul>
      )}

      {open && loading && query.trim().length > 0 && matches.length === 0 && (
        <div className="absolute z-10 mt-1 w-full rounded-lg border border-gray-200 bg-white px-4 py-3 text-sm text-gray-500 shadow-lg">
          Searching stores…
        </div>
      )}

      {open && !loading && query.trim().length > 0 && matches.length === 0 && (
        <div className="absolute z-10 mt-1 w-full rounded-lg border border-gray-200 bg-white px-4 py-3 text-sm text-gray-500 shadow-lg">
          No stores found for &ldquo;{query}&rdquo;
        </div>
      )}
    </div>
  )
}
