'use client'

import { useState, useRef, useEffect, useCallback } from 'react'
import { useRouter, usePathname } from 'next/navigation'
import StoreLogo from './StoreLogo'

type SearchResult = {
  id: number
  name: string
  slug: string
  category: string | null
}

function scoreStore(name: string, qLower: string): number {
  const lower = name.toLowerCase()
  if (lower === qLower) return 1000
  if (lower.startsWith(qLower)) return 500 - lower.length
  const words = lower.split(/[\s.'\-]+/)
  for (const w of words) if (w.startsWith(qLower)) return 300 - lower.length
  const idx = lower.indexOf(qLower)
  if (idx >= 0) return 100 - idx - lower.length * 0.1
  return -1
}

function highlightMatch(text: string, query: string) {
  if (!query) return <span>{text}</span>
  const i = text.toLowerCase().indexOf(query.toLowerCase())
  if (i < 0) return <span>{text}</span>
  return (
    <span>
      {text.slice(0, i)}
      <b className="font-semibold text-mcb-ink">{text.slice(i, i + query.length)}</b>
      {text.slice(i + query.length)}
    </span>
  )
}

export default function NavSearch() {
  const pathname = usePathname()
  const isHomepage = pathname === '/'

  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const [activeIndex, setActiveIndex] = useState(0)
  const [matches, setMatches] = useState<SearchResult[]>([])
  const [loading, setLoading] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const wrapperRef = useRef<HTMLDivElement>(null)
  const router = useRouter()

  // Cmd+K shortcut
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        inputRef.current?.focus()
      }
    }
    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [])

  // Click outside to close
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  // Search API
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
          { signal: controller.signal, cache: 'no-store' }
        )
        if (!response.ok) throw new Error(`${response.status}`)
        const payload = (await response.json()) as { retailers?: SearchResult[] }
        if (!controller.signal.aborted) {
          const results = (payload.retailers ?? [])
            .map(r => ({ ...r, _score: scoreStore(r.name, trimmed.toLowerCase()) }))
            .sort((a, b) => b._score - a._score)
            .slice(0, 8)
          setMatches(results)
          setActiveIndex(0)
        }
      } catch {
        if (!controller.signal.aborted) setMatches([])
      } finally {
        if (!controller.signal.aborted) setLoading(false)
      }
    }, 150)

    return () => {
      controller.abort()
      window.clearTimeout(timeoutId)
    }
  }, [query])

  const handleSelect = useCallback((result: SearchResult) => {
    setQuery('')
    setMatches([])
    setOpen(false)
    router.push(`/store/${result.slug}`)
  }, [router])

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActiveIndex(i => Math.min(i + 1, matches.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActiveIndex(i => Math.max(i - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      if (matches[activeIndex]) handleSelect(matches[activeIndex])
    } else if (e.key === 'Escape') {
      setOpen(false)
      inputRef.current?.blur()
    }
  }

  const hasQuery = query.trim().length > 0
  const dropdownOpen = open && (hasQuery || document.activeElement === inputRef.current)
  const showNoResults = hasQuery && !loading && matches.length === 0

  // On homepage, hide the nav search (the hero has its own)
  if (isHomepage) return null

  return (
    <div ref={wrapperRef} className="relative w-full">
      {/* Input */}
      <div
        className={`flex h-[38px] items-center gap-2.5 bg-mcb-bg px-3.5 ${
          dropdownOpen
            ? 'rounded-t-[10px] border-x-[1.5px] border-t-[1.5px] border-mcb-ink border-b-transparent'
            : 'rounded-[10px] border-[1.5px] border-mcb-line'
        }`}
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="text-mcb-ink-mute">
          <circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" />
        </svg>
        <input
          ref={inputRef}
          type="search"
          value={query}
          onChange={e => { setQuery(e.target.value); setOpen(true); setActiveIndex(0) }}
          onFocus={() => setOpen(true)}
          onKeyDown={handleKeyDown}
          placeholder="Search stores..."
          className="flex-1 bg-transparent text-[13px] text-mcb-ink outline-none placeholder:text-mcb-ink-mute"
          autoComplete="off"
          spellCheck={false}
        />
        <kbd className="rounded border border-mcb-line bg-mcb-surface px-1.5 py-0.5 text-[10px] text-mcb-ink-mute">
          ⌘K
        </kbd>
      </div>

      {/* Dropdown */}
      {dropdownOpen && (
        <div className="absolute left-0 right-0 top-[38px] z-20 overflow-hidden rounded-b-[10px] border-x-[1.5px] border-b-[1.5px] border-mcb-ink bg-mcb-surface shadow-[0_16px_36px_rgba(0,0,0,0.08)]">
          {!hasQuery && (
            <div className="px-4 py-4 text-center text-[12px] text-mcb-ink-mute">
              Start typing to search stores.
            </div>
          )}

          {showNoResults && (
            <div className="px-4 py-4">
              <div className="text-[13px] font-medium text-mcb-ink">
                No stores found for &ldquo;{query}&rdquo;
              </div>
              <div className="mt-1 text-[12px] text-mcb-ink-soft">
                Check your spelling, or{' '}
                <span className="border-b border-mcb-accent text-mcb-accent-ink">
                  request this store &rarr;
                </span>
              </div>
            </div>
          )}

          {matches.map((r, i) => (
            <div
              key={r.id}
              onMouseDown={() => handleSelect(r)}
              onMouseEnter={() => setActiveIndex(i)}
              className={`flex cursor-pointer items-center gap-2.5 border-b border-mcb-line-soft px-3.5 py-2 last:border-b-0 ${
                i === activeIndex ? 'bg-mcb-bg' : 'bg-mcb-surface'
              }`}
            >
              <StoreLogo name={r.name} size={24} />
              <div className="flex-1 truncate text-[14px] text-mcb-ink-soft">
                {highlightMatch(r.name, query)}
              </div>
              {i === activeIndex && (
                <kbd className="rounded border border-mcb-line px-1 py-0.5 text-[10px] text-mcb-ink-mute">↵</kbd>
              )}
            </div>
          ))}

          {matches.length > 0 && (
            <div className="flex justify-between border-t border-mcb-line-soft bg-mcb-bg px-3.5 py-2 text-[11px] text-mcb-ink-mute">
              <span>{matches.length} {matches.length === 1 ? 'match' : 'matches'}</span>
              <span>↑↓ navigate · ↵ open · esc close</span>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
