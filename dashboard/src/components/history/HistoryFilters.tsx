'use client'

import { cn } from '@/lib/utils'
import { type BetStatus } from '@/types/predictions'

export type StatusFilter = 'all' | BetStatus

interface HistoryFiltersProps {
  activeFilter: StatusFilter
  onFilterChange: (filter: StatusFilter) => void
}

const FILTERS: { value: StatusFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'pending', label: 'Pending' },
  { value: 'won', label: 'Won' },
  { value: 'lost', label: 'Lost' },
  { value: 'push', label: 'Push' },
]

export function HistoryFilters({ activeFilter, onFilterChange }: HistoryFiltersProps) {
  return (
    <div className="flex items-center gap-2 overflow-x-auto scrollbar-hide">
      {FILTERS.map((filter) => (
        <button
          key={filter.value}
          onClick={() => onFilterChange(filter.value)}
          className={cn(
            'px-2.5 py-1.5 sm:px-4 sm:py-2 rounded-lg text-sm font-medium transition-colors whitespace-nowrap',
            activeFilter === filter.value
              ? 'bg-blue-600 text-white'
              : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
          )}
        >
          {filter.label}
        </button>
      ))}
    </div>
  )
}
