'use client'

import { cn } from '@/lib/utils'

export type BetSource = 'model' | 'all'

interface BetSourceFilterProps {
  activeSource: BetSource
  onSourceChange: (source: BetSource) => void
  className?: string
}

const SOURCES: { value: BetSource; label: string; description: string }[] = [
  { value: 'model', label: 'Model Picks', description: 'Edge ≥9%' },
  { value: 'all', label: 'All Bets', description: 'All placed bets' },
]

export function BetSourceFilter({ activeSource, onSourceChange, className }: BetSourceFilterProps) {
  return (
    <div className={cn('flex items-center gap-2', className)}>
      {SOURCES.map((source) => (
        <button
          key={source.value}
          onClick={() => onSourceChange(source.value)}
          className={cn(
            'px-4 py-2 rounded-lg text-sm font-medium transition-colors',
            activeSource === source.value
              ? 'bg-emerald-600 text-white'
              : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
          )}
          title={source.description}
        >
          {source.label}
        </button>
      ))}
    </div>
  )
}

// Model picks threshold - bets with edge >= 9%
export const MODEL_PICKS_EDGE_THRESHOLD = 0.09
