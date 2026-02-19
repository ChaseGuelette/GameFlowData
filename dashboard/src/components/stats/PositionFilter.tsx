'use client'

import { cn } from '@/lib/utils'
import type { PositionGroup } from '@/types/stats'

type PositionOption = 'all' | PositionGroup

const positions: { value: PositionOption; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'G', label: 'Guards' },
  { value: 'W', label: 'Wings' },
  { value: 'B', label: 'Bigs' },
]

interface PositionFilterProps {
  active: PositionOption
  onChange: (pos: PositionOption) => void
}

export function PositionFilter({ active, onChange }: PositionFilterProps) {
  return (
    <div className="flex items-center space-x-1">
      {positions.map((pos) => (
        <button
          key={pos.value}
          onClick={() => onChange(pos.value)}
          className={cn(
            'px-3 py-1.5 text-xs font-medium rounded-full transition-colors',
            active === pos.value
              ? 'bg-slate-600 text-white'
              : 'text-slate-400 hover:text-slate-200 bg-slate-800 hover:bg-slate-700'
          )}
        >
          {pos.label}
        </button>
      ))}
    </div>
  )
}

export type { PositionOption }
