'use client'

import { useState, useRef, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import type { Retailer } from '@/lib/db'

export default function SearchBox({ retailers }: { retailers: Retailer[] }) {
  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const [activeIndex, setActiveIndex] = useState(-1)
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLUListElement>(null)
  const router = useRouter()

  const matches = query.trim().length === 0
    ? []
    : retailers
        .filter((r) => r.name.toLowerCase().includes(query.toLowerCase()))
        .slice(0, 8)

  function handleSelect(retailer: Retailer) {
    setQuery('')
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
      if (activeIndex >= 0) handleSelect(matches[activeIndex])
    } else if (e.key === 'Escape') {
      setOpen(false)
    }
  }

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
          setQuery(e.target.value)
          setActiveIndex(-1)
          setOpen(true)
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

      {open && query.trim().length > 0 && matches.length === 0 && (
        <div className="absolute z-10 mt-1 w-full rounded-lg border border-gray-200 bg-white px-4 py-3 text-sm text-gray-500 shadow-lg">
          No stores found for &ldquo;{query}&rdquo;
        </div>
      )}
    </div>
  )
}
