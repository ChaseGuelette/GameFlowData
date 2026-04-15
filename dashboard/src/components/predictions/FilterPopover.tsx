'use client'

import { useEffect, useRef, useState } from 'react'
import { BookFilterDropdown } from '@/components/predictions/BookFilterDropdown'
import { DirectionFilter, type DirectionFilterValue } from '@/components/shared/DirectionFilter'
import { US_STATES } from '@/lib/sportsbook-availability'

interface FilterPopoverProps {
  userState: string
  onStateChange: (state: string) => void
  excludedBooks: Set<string>
  onBooksChange: (excluded: Set<string>) => void
  showLive: boolean
  onShowLiveChange: (live: boolean) => void
  directionFilter: DirectionFilterValue
  onDirectionChange: (dir: DirectionFilterValue) => void
}

export function FilterPopover({
  userState,
  onStateChange,
  excludedBooks,
  onBooksChange,
  showLive,
  onShowLiveChange,
  directionFilter,
  onDirectionChange,
}: FilterPopoverProps) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  const activeCount = [
    userState !== '',
    excludedBooks.size > 0,
    showLive,
    directionFilter !== 'both',
  ].filter(Boolean).length

  // Close on outside click
  useEffect(() => {
    if (!open) return
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [open])

  // Close on Escape
  useEffect(() => {
    if (!open) return
    function handleKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('keydown', handleKey)
    return () => document.removeEventListener('keydown', handleKey)
  }, [open])

  const handleReset = () => {
    onStateChange('')
    onBooksChange(new Set())
    onShowLiveChange(false)
    onDirectionChange('both')
  }

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(prev => !prev)}
        className="flex items-center gap-2 px-3 py-2 text-sm font-medium rounded-lg border border-slate-700 bg-slate-800 text-slate-300 hover:text-slate-100 transition-colors whitespace-nowrap"
      >
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-4 h-4">
          <path fillRule="evenodd" d="M11.49 3.17c-.38-1.56-2.6-1.56-2.98 0a1.532 1.532 0 0 1-2.286.948c-1.372-.836-2.942.734-2.106 2.106.54.886.061 2.042-.947 2.287-1.561.379-1.561 2.6 0 2.978a1.532 1.532 0 0 1 .947 2.287c-.836 1.372.734 2.942 2.106 2.106a1.532 1.532 0 0 1 2.287.947c.379 1.561 2.6 1.561 2.978 0a1.533 1.533 0 0 1 2.287-.947c1.372.836 2.942-.734 2.106-2.106a1.533 1.533 0 0 1 .947-2.287c1.561-.379 1.561-2.6 0-2.978a1.532 1.532 0 0 1-.947-2.287c.836-1.372-.734-2.942-2.106-2.106a1.532 1.532 0 0 1-2.287-.947zM10 13a3 3 0 1 0 0-6 3 3 0 0 0 0 6z" clipRule="evenodd" />
        </svg>
        Filters
        {activeCount > 0 && (
          <span className="bg-blue-600 text-white text-xs font-bold rounded-full w-5 h-5 flex items-center justify-center">
            {activeCount}
          </span>
        )}
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-1 z-50 bg-slate-800 border border-slate-700 rounded-lg shadow-xl p-4 w-72 sm:w-80">
          {/* State */}
          <div className="flex items-center justify-between mb-3">
            <span className="text-xs font-medium text-slate-400 uppercase tracking-wide">State</span>
            <div className="relative">
              <select
                value={userState}
                onChange={(e) => onStateChange(e.target.value)}
                className="appearance-none bg-slate-700 border border-slate-600 rounded-lg pl-3 pr-7 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500 cursor-pointer"
              >
                {US_STATES.map((s) => (
                  <option key={s.value} value={s.value}>
                    {s.value ? s.value : 'All States'}
                  </option>
                ))}
              </select>
              <div className="pointer-events-none absolute inset-y-0 right-1.5 flex items-center">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-3.5 h-3.5 text-slate-400">
                  <path fillRule="evenodd" d="M5.22 8.22a.75.75 0 0 1 1.06 0L10 11.94l3.72-3.72a.75.75 0 1 1 1.06 1.06l-4.25 4.25a.75.75 0 0 1-1.06 0L5.22 9.28a.75.75 0 0 1 0-1.06Z" clipRule="evenodd" />
                </svg>
              </div>
            </div>
          </div>

          {/* Sportsbooks */}
          <div className="flex items-center justify-between mb-3">
            <span className="text-xs font-medium text-slate-400 uppercase tracking-wide">Sportsbooks</span>
            <BookFilterDropdown
              excludedBooks={excludedBooks}
              onChange={onBooksChange}
              userState={userState}
            />
          </div>

          {/* Game Status */}
          <div className="flex items-center justify-between mb-3">
            <span className="text-xs font-medium text-slate-400 uppercase tracking-wide">Game Status</span>
            <div className="flex bg-slate-700 rounded-lg p-0.5">
              <button
                onClick={() => onShowLiveChange(false)}
                className={`px-2.5 py-1 text-xs font-medium rounded-md transition-colors ${
                  !showLive
                    ? 'bg-slate-600 text-slate-100'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                Pre-Game
              </button>
              <button
                onClick={() => onShowLiveChange(true)}
                className={`px-2.5 py-1 text-xs font-medium rounded-md transition-colors ${
                  showLive
                    ? 'bg-orange-600 text-white'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                + Live
              </button>
            </div>
          </div>

          {/* Direction */}
          <div className="flex items-center justify-between mb-4">
            <span className="text-xs font-medium text-slate-400 uppercase tracking-wide">Direction</span>
            <DirectionFilter activeDirection={directionFilter} onDirectionChange={onDirectionChange} />
          </div>

          {/* Reset */}
          <button
            onClick={handleReset}
            className="w-full py-1.5 text-xs font-medium text-slate-400 hover:text-slate-200 border border-slate-700 hover:border-slate-600 rounded-lg transition-colors"
          >
            Reset Filters
          </button>
        </div>
      )}
    </div>
  )
}
