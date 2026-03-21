'use client'

import { useState } from 'react'
import { type PaperBet, STAT_COLORS, STAT_LABELS } from '@/types/predictions'
import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { BetContextDetail } from '@/components/history/BetContextDetail'
import { cn } from '@/lib/utils'

interface BetCardProps {
  bet: PaperBet
  onRemove?: (id: number) => void
}

const STATUS_STYLES = {
  won: 'bg-green-500/20 text-green-400 border-green-500/50',
  lost: 'bg-red-500/20 text-red-400 border-red-500/50',
  push: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/50',
  pending: 'bg-slate-500/20 text-slate-400 border-slate-500/50',
  cancelled: 'bg-slate-500/20 text-slate-400 border-slate-500/50',
}

const STATUS_LABELS = {
  won: 'Won',
  lost: 'Lost',
  push: 'Push',
  pending: 'Pending',
  cancelled: 'Cancelled',
}

export function BetCard({ bet, onRemove }: BetCardProps) {
  const [expanded, setExpanded] = useState(false)
  const hasContext = !!bet.bet_context
  const statusStyle = STATUS_STYLES[bet.status]
  const pnlColor = bet.pnl && bet.pnl > 0 ? 'text-green-400' : bet.pnl && bet.pnl < 0 ? 'text-red-400' : 'text-slate-400'

  const formatPnl = (pnl: number | null) => {
    if (pnl === null) return '-'
    const sign = pnl >= 0 ? '+' : ''
    return `${sign}$${Math.abs(pnl).toFixed(2)}`
  }

  const formatOdds = (odds: number) => {
    return odds >= 0 ? `+${odds}` : `${odds}`
  }

  return (
    <div
      className={cn(
        'bg-slate-800 rounded-lg border p-4 transition-colors',
        bet.status === 'won' ? 'border-green-500/30' :
        bet.status === 'lost' ? 'border-red-500/30' :
        'border-slate-700',
        hasContext && 'cursor-pointer'
      )}
      onClick={hasContext ? () => setExpanded(prev => !prev) : undefined}
    >
      {/* Header: Player + Date */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-3">
          <PlayerAvatar playerId={bet.player_id} playerName={bet.player_name} size="sm" />
          <div>
            <div className="font-medium text-slate-50">{bet.player_name}</div>
            <div className="text-xs text-slate-400">
              {bet.team_abbrev && bet.opponent_abbrev
                ? `${bet.team_abbrev} vs ${bet.opponent_abbrev} • ${bet.game_date}`
                : bet.game_date}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {/* Inline confidence stars on collapsed view */}
          {bet.user_confidence && !expanded && (
            <div className="flex gap-0.5">
              {[1, 2, 3, 4, 5].map((star) => (
                <svg
                  key={star}
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                  className={`w-3.5 h-3.5 ${
                    star <= bet.user_confidence! ? 'text-yellow-400' : 'text-slate-600'
                  }`}
                >
                  <path fillRule="evenodd" d="M10.868 2.884c-.321-.772-1.415-.772-1.736 0l-1.83 4.401-4.753.381c-.833.067-1.171 1.107-.536 1.651l3.62 3.102-1.106 4.637c-.194.813.691 1.456 1.405 1.02L10 15.591l4.069 2.485c.713.436 1.598-.207 1.404-1.02l-1.106-4.637 3.62-3.102c.635-.544.297-1.584-.536-1.65l-4.752-.382-1.831-4.401Z" clipRule="evenodd" />
                </svg>
              ))}
            </div>
          )}
          <span className={cn('text-xs px-2 py-1 rounded border', statusStyle)}>
            {STATUS_LABELS[bet.status]}
          </span>
          {bet.status === 'pending' && onRemove && (
            <button
              onClick={(e) => { e.stopPropagation(); onRemove(bet.id) }}
              className="text-slate-500 hover:text-red-400 transition-colors p-1"
              title="Remove bet"
            >
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-4 h-4">
                <path fillRule="evenodd" d="M10 18a8 8 0 1 0 0-16 8 8 0 0 0 0 16ZM8.28 7.22a.75.75 0 0 0-1.06 1.06L8.94 10l-1.72 1.72a.75.75 0 1 0 1.06 1.06L10 11.06l1.72 1.72a.75.75 0 1 0 1.06-1.06L11.06 10l1.72-1.72a.75.75 0 0 0-1.06-1.06L10 8.94 8.28 7.22Z" clipRule="evenodd" />
              </svg>
            </button>
          )}
        </div>
      </div>

      {/* Bet Details */}
      <div className="flex items-center gap-2 mb-3">
        <span className={cn('text-xs px-2 py-0.5 rounded text-white', STAT_COLORS[bet.stat_type])}>
          {STAT_LABELS[bet.stat_type]}
        </span>
        <span className="text-slate-300 text-sm">
          {bet.bet_direction === 'over' ? 'Over' : 'Under'} {bet.line}
        </span>
        <span className="text-slate-500 text-sm">
          ({formatOdds(bet.odds_at_bet)})
        </span>
        {bet.bookmaker && (
          <span className="text-xs px-2 py-0.5 rounded bg-slate-700 text-slate-300">
            {bet.bookmaker}
          </span>
        )}
      </div>

      {/* Result Row */}
      <div className="flex items-center justify-between text-sm">
        <div className="text-slate-400">
          {bet.actual_value !== null ? (
            <span>
              Actual: <span className="text-slate-200">{bet.actual_value}</span>
            </span>
          ) : (
            <span>Awaiting result</span>
          )}
        </div>
        <div className="flex items-center gap-4">
          <span className="text-slate-400">
            Stake: <span className="text-slate-200">${bet.stake.toFixed(2)}</span>
          </span>
          <span className={cn('font-medium', pnlColor)}>
            {formatPnl(bet.pnl)}
          </span>
        </div>
      </div>

      {/* Expand indicator */}
      {hasContext && (
        <div className="flex justify-center mt-2">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 20 20"
            fill="currentColor"
            className={cn(
              'w-4 h-4 text-slate-500 transition-transform',
              expanded && 'rotate-180'
            )}
          >
            <path fillRule="evenodd" d="M5.22 8.22a.75.75 0 0 1 1.06 0L10 11.94l3.72-3.72a.75.75 0 1 1 1.06 1.06l-4.25 4.25a.75.75 0 0 1-1.06 0L5.22 9.28a.75.75 0 0 1 0-1.06Z" clipRule="evenodd" />
          </svg>
        </div>
      )}

      {/* Expanded Context Detail */}
      {expanded && bet.bet_context && (
        <BetContextDetail
          context={bet.bet_context}
          userConfidence={bet.user_confidence}
          line={bet.line}
        />
      )}
    </div>
  )
}
