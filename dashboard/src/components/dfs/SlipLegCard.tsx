'use client'

import { Badge } from '@/components/shared/Badge'
import type { SlipBuilderLeg } from '@/types/dfs-entries'
import { formatProb } from '@/lib/utils'

interface SlipLegCardProps {
  leg: SlipBuilderLeg
  onRemove: (key: string) => void
}

export function SlipLegCard({ leg, onRemove }: SlipLegCardProps) {
  return (
    <div className="flex items-center gap-2 bg-slate-700/50 rounded-lg px-3 py-2">
      <div className="flex-1 min-w-0">
        <div className="text-sm font-medium text-slate-100 truncate">
          {leg.player_name}
        </div>
        <div className="flex items-center gap-1.5 mt-0.5">
          <Badge stat={leg.stat_type} />
          <span className="text-slate-300 text-xs font-medium">{leg.line}</span>
          <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
            leg.direction === 'over'
              ? 'bg-green-500/20 text-green-400'
              : 'bg-red-500/20 text-red-400'
          }`}>
            {leg.direction === 'over' ? 'O' : 'U'}
          </span>
          {leg.model_prob !== null && (
            <span className="text-slate-400 text-xs">{formatProb(leg.model_prob)}</span>
          )}
        </div>
      </div>
      <button
        onClick={() => onRemove(leg.key)}
        className="text-slate-500 hover:text-red-400 transition-colors p-1 shrink-0"
        aria-label={`Remove ${leg.player_name}`}
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  )
}
