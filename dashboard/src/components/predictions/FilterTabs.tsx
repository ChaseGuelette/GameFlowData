'use client'

import { cn } from '@/lib/utils'
import { type StatType, STAT_LABELS } from '@/types/predictions'

type FilterOption = 'all' | 'combos' | StatType

export const COMBO_STATS = new Set<StatType>(['pra', 'pr', 'pa', 'ra'])

interface FilterTabsProps {
  activeFilter: FilterOption
  onFilterChange: (filter: FilterOption) => void
  className?: string
}

const filters: { value: FilterOption; label: string }[] = [
  { value: 'all', label: 'All Props' },
  { value: 'pts', label: STAT_LABELS.pts },
  { value: 'reb', label: STAT_LABELS.reb },
  { value: 'ast', label: STAT_LABELS.ast },
  { value: 'combos', label: 'Combos' },
]

export function FilterTabs({ activeFilter, onFilterChange, className }: FilterTabsProps) {
  return (
    <div className={cn('flex items-center space-x-1 bg-slate-800 p-1 rounded-lg', className)}>
      {filters.map((filter) => (
        <button
          key={filter.value}
          onClick={() => onFilterChange(filter.value)}
          className={cn(
            'px-4 py-2 text-sm font-medium rounded-md transition-colors',
            activeFilter === filter.value
              ? 'bg-blue-600 text-white'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
          )}
        >
          {filter.label}
        </button>
      ))}
    </div>
  )
}

export type { FilterOption }
