'use client'

import { cn } from '@/lib/utils'
import type { PlayTypeGrouping } from '@/types/stats'

const options: { value: PlayTypeGrouping; label: string }[] = [
  { value: 'Offensive', label: 'Offense' },
  { value: 'Defensive', label: 'Defense' },
]

interface OffDefToggleProps {
  active: PlayTypeGrouping
  onChange: (g: PlayTypeGrouping) => void
}

export function OffDefToggle({ active, onChange }: OffDefToggleProps) {
  return (
    <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg">
      {options.map((o) => (
        <button
          key={o.value}
          onClick={() => onChange(o.value)}
          className={cn(
            'px-3 py-1.5 text-xs font-medium rounded-md transition-colors',
            active === o.value
              ? 'bg-blue-600 text-white'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
}
