'use client'

import { useState } from 'react'
import { Badge } from '@/components/shared/Badge'
import { USER_DFS_SLIP_TYPES, type UserDfsEntryWithLegs, type EntryStatus } from '@/types/dfs-entries'
import { cn, formatProb } from '@/lib/utils'

interface DfsEntryCardProps {
  entry: UserDfsEntryWithLegs
  onRemove?: (id: number) => void
}

const statusColors: Record<EntryStatus, string> = {
  pending: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/50',
  won: 'bg-green-500/20 text-green-400 border-green-500/50',
  lost: 'bg-red-500/20 text-red-400 border-red-500/50',
  push: 'bg-slate-500/20 text-slate-400 border-slate-500/50',
  cancelled: 'bg-slate-500/20 text-slate-500 border-slate-500/30',
}

export function DfsEntryCard({ entry, onRemove }: DfsEntryCardProps) {
  const [expanded, setExpanded] = useState(false)
  const config = USER_DFS_SLIP_TYPES[entry.slip_type as keyof typeof USER_DFS_SLIP_TYPES]

  return (
    <div className="bg-slate-800 rounded-lg border border-slate-700">
      {/* Header row */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-3 p-4 text-left"
      >
        {/* Slip type badge */}
        <span className="px-2 py-1 rounded text-xs font-medium bg-blue-500/20 text-blue-400 border border-blue-500/50 whitespace-nowrap">
          {config?.label ?? entry.slip_type}
        </span>

        {/* Status badge */}
        <span className={cn(
          'px-2 py-1 rounded text-xs font-medium border whitespace-nowrap',
          statusColors[entry.status],
        )}>
          {entry.status.toUpperCase()}
        </span>

        {/* Date */}
        <span className="text-slate-400 text-xs whitespace-nowrap">
          {new Date(entry.entry_date + 'T00:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
        </span>

        {/* Stake */}
        <span className="text-slate-300 text-sm">${Number(entry.stake).toFixed(2)}</span>

        {/* PnL */}
        <span className={cn(
          'text-sm font-medium ml-auto',
          entry.pnl > 0 ? 'text-green-400' : entry.pnl < 0 ? 'text-red-400' : 'text-slate-400',
        )}>
          {entry.status === 'pending'
            ? `${entry.payout_multiplier}x`
            : `${entry.pnl >= 0 ? '+' : ''}$${Number(entry.pnl).toFixed(2)}`
          }
        </span>

        {/* Remove button for pending entries */}
        {entry.status === 'pending' && onRemove && (
          <button
            onClick={(e) => { e.stopPropagation(); onRemove(entry.id) }}
            className="text-slate-500 hover:text-red-400 transition-colors p-1"
            aria-label="Remove entry"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}

        {/* Expand indicator */}
        <svg
          className={cn('w-4 h-4 text-slate-500 transition-transform', expanded && 'rotate-180')}
          fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* Expanded legs */}
      {expanded && entry.legs.length > 0 && (
        <div className="border-t border-slate-700 px-4 py-3 space-y-2">
          {entry.legs.map(leg => (
            <div key={leg.id} className="flex items-center gap-2 text-sm">
              <span className={cn(
                'w-1.5 h-1.5 rounded-full shrink-0',
                leg.status === 'won' ? 'bg-green-400'
                  : leg.status === 'lost' ? 'bg-red-400'
                  : leg.status === 'push' ? 'bg-slate-400'
                  : 'bg-yellow-400',
              )} />
              <span className="text-slate-200 font-medium truncate">{leg.player_name}</span>
              <Badge stat={leg.stat_type} />
              <span className="text-slate-300">{Number(leg.line)}</span>
              <span className={cn(
                'text-xs font-medium px-1.5 py-0.5 rounded',
                leg.direction === 'over' ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400',
              )}>
                {leg.direction === 'over' ? 'O' : 'U'}
              </span>
              {leg.model_prob !== null && (
                <span className="text-slate-500 text-xs">{formatProb(leg.model_prob)}</span>
              )}
              {leg.actual_value !== null && (
                <span className="text-slate-400 text-xs ml-auto">
                  Actual: <span className="text-slate-200">{Number(leg.actual_value)}</span>
                </span>
              )}
            </div>
          ))}

          {/* Entry metadata */}
          <div className="flex items-center gap-3 pt-1 text-xs text-slate-500 border-t border-slate-700/50 mt-2">
            <span>Prob: {(Number(entry.combined_prob) * 100).toFixed(1)}%</span>
            <span>Kelly: {(Number(entry.kelly_fraction_used) * 100).toFixed(0)}%</span>
            <span>Mode: {entry.edge_mode}</span>
          </div>
        </div>
      )}
    </div>
  )
}
