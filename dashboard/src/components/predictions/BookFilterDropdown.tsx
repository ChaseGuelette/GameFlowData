'use client'

import { useEffect, useRef, useState } from 'react'
import { SPORTSBOOK_OPTIONS, STATE_SPORTSBOOKS } from '@/lib/sportsbook-availability'

interface BookFilterDropdownProps {
  excludedBooks: Set<string>
  onChange: (excluded: Set<string>) => void
  userState: string
}

export function BookFilterDropdown({ excludedBooks, onChange, userState }: BookFilterDropdownProps) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  // Books available for this state (all non-empty options, filtered by state)
  const allowedBooks = SPORTSBOOK_OPTIONS.filter(
    opt => opt.value && (!userState || STATE_SPORTSBOOKS[userState]?.includes(opt.value))
  )

  const allChecked = excludedBooks.size === 0
  const checkedCount = allowedBooks.length - excludedBooks.size

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

  const toggleBook = (value: string) => {
    const next = new Set(excludedBooks)
    if (next.has(value)) {
      next.delete(value)
    } else {
      next.add(value)
    }
    onChange(next)
  }

  const selectAll = () => onChange(new Set())

  const clearAll = () => onChange(new Set(allowedBooks.map(b => b.value)))

  const label = allChecked ? 'All Books' : `Books (${checkedCount})`

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(prev => !prev)}
        className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500 flex items-center gap-1.5 whitespace-nowrap"
      >
        {label}
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 20 20"
          fill="currentColor"
          className={`w-4 h-4 text-slate-400 transition-transform ${open ? 'rotate-180' : ''}`}
        >
          <path fillRule="evenodd" d="M5.22 8.22a.75.75 0 0 1 1.06 0L10 11.94l3.72-3.72a.75.75 0 1 1 1.06 1.06l-4.25 4.25a.75.75 0 0 1-1.06 0L5.22 9.28a.75.75 0 0 1 0-1.06Z" clipRule="evenodd" />
        </svg>
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-1 z-50 bg-slate-800 border border-slate-700 rounded-lg shadow-xl py-1 min-w-[180px] max-h-72 overflow-y-auto">
          {/* Select All / Clear All */}
          <div className="flex items-center justify-between px-3 py-1.5 border-b border-slate-700">
            <button onClick={selectAll} className="text-xs text-blue-400 hover:text-blue-300">Select All</button>
            <button onClick={clearAll} className="text-xs text-slate-400 hover:text-slate-300">Clear All</button>
          </div>

          {allowedBooks.map(opt => {
            const checked = !excludedBooks.has(opt.value)
            return (
              <label
                key={opt.value}
                className="flex items-center gap-2 px-3 py-1.5 hover:bg-slate-700/50 cursor-pointer text-sm text-slate-200"
              >
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => toggleBook(opt.value)}
                  className="rounded border-slate-600 bg-slate-700 text-blue-500 focus:ring-blue-500 focus:ring-offset-0 h-3.5 w-3.5"
                />
                {opt.label}
              </label>
            )
          })}
        </div>
      )}
    </div>
  )
}
