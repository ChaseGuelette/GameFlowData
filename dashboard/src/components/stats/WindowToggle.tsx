'use client'

import { cn } from '@/lib/utils'
import type { WindowSuffix } from '@/types/stats'

const NBA_WINDOWS: { value: WindowSuffix; label: string }[] = [
  { value: 'l5', label: 'L5' },
  { value: 'l15', label: 'L15' },
  { value: 'szn', label: 'SZN' },
]

interface WindowToggleProps {
  active: WindowSuffix
  onChange: (w: WindowSuffix) => void
  options?: { value: WindowSuffix; label: string }[]
}

export function WindowToggle({ active, onChange, options = NBA_WINDOWS }: WindowToggleProps) {
  return (
    <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg">
      {options.map((w) => (
        <button
          key={w.value}
          onClick={() => onChange(w.value)}
          className={cn(
            'px-3 py-1.5 text-xs font-medium rounded-md transition-colors',
            active === w.value
              ? 'bg-blue-600 text-white'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
          )}
        >
          {w.label}
        </button>
      ))}
    </div>
  )
}
