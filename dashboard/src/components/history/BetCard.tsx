'use client'

import { type PaperBet, STAT_COLORS, STAT_LABELS } from '@/types/predictions'
import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { cn } from '@/lib/utils'

interface BetCardProps {
  bet: PaperBet
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

export function BetCard({ bet }: BetCardProps) {
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
    <div className={cn(
      'bg-slate-800 rounded-lg border p-4 transition-colors',
      bet.status === 'won' ? 'border-green-500/30' :
      bet.status === 'lost' ? 'border-red-500/30' :
      'border-slate-700'
    )}>
      {/* Header: Player + Date */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-3">
          <PlayerAvatar playerId={bet.player_id} size="sm" />
          <div>
            <div className="font-medium text-slate-50">{bet.player_name}</div>
            <div className="text-xs text-slate-400">{bet.game_date}</div>
          </div>
        </div>
        <span className={cn('text-xs px-2 py-1 rounded border', statusStyle)}>
          {STATUS_LABELS[bet.status]}
        </span>
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
    </div>
  )
}
