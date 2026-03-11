'use client'

import { cn } from '@/lib/utils'

export type DirectionFilterValue = 'both' | 'over' | 'under'

interface DirectionFilterProps {
  activeDirection: DirectionFilterValue
  onDirectionChange: (direction: DirectionFilterValue) => void
  className?: string
}

const DIRECTIONS: { value: DirectionFilterValue; label: string }[] = [
  { value: 'both', label: 'Both' },
  { value: 'over', label: 'Over' },
  { value: 'under', label: 'Under' },
]

export function DirectionFilter({ activeDirection, onDirectionChange, className }: DirectionFilterProps) {
  return (
    <div className={cn('flex items-center gap-2', className)}>
      {DIRECTIONS.map((dir) => (
        <button
          key={dir.value}
          onClick={() => onDirectionChange(dir.value)}
          className={cn(
            'px-4 py-2 rounded-lg text-sm font-medium transition-colors',
            activeDirection === dir.value
              ? 'bg-blue-600 text-white'
              : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
          )}
        >
          {dir.label}
        </button>
      ))}
    </div>
  )
}
