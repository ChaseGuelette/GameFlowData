'use client'

import { cn } from '@/lib/utils'
import type { FilterTabDef } from '@/lib/sport-config'

type FilterOption = string

interface FilterTabsProps {
  activeFilter: FilterOption
  onFilterChange: (filter: FilterOption) => void
  tabs: FilterTabDef[]
  className?: string
}

export function FilterTabs({ activeFilter, onFilterChange, tabs, className }: FilterTabsProps) {
  return (
    <div className={cn('flex items-center space-x-1 bg-slate-800 p-1 rounded-lg overflow-x-auto scrollbar-hide', className)}>
      {tabs.map((tab) => (
        <button
          key={tab.value}
          onClick={() => onFilterChange(tab.value)}
          className={cn(
            'px-2.5 py-1.5 sm:px-4 sm:py-2 text-sm font-medium rounded-md transition-colors whitespace-nowrap',
            activeFilter === tab.value
              ? 'bg-blue-600 text-white'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
          )}
        >
          {tab.label}
        </button>
      ))}
    </div>
  )
}

export type { FilterOption }
