'use client'

import { cn } from '@/lib/utils'
import type { StatsMainTab } from '@/types/stats'

const tabs: { value: StatsMainTab; label: string }[] = [
  { value: 'players', label: 'Players' },
  { value: 'teams', label: 'Teams' },
  { value: 'defense', label: 'Defense vs Position' },
]

interface StatTabsProps {
  active: StatsMainTab
  onChange: (tab: StatsMainTab) => void
}

export function StatTabs({ active, onChange }: StatTabsProps) {
  return (
    <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg">
      {tabs.map((tab) => (
        <button
          key={tab.value}
          onClick={() => onChange(tab.value)}
          className={cn(
            'px-4 py-2 text-sm font-medium rounded-md transition-colors',
            active === tab.value
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
