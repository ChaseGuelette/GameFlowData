'use client'

import { type PaperBet, type StatType, STAT_LABELS, STAT_COLORS } from '@/types/predictions'
import { cn } from '@/lib/utils'

interface HistorySummaryProps {
  bets: PaperBet[]
  statTypes?: StatType[]
}

export function HistorySummary({ bets, statTypes = ['pts', 'reb', 'ast'] }: HistorySummaryProps) {
  const pendingBets = bets.filter(b => b.status === 'pending')
  const resolvedBets = bets.filter(b => b.status !== 'pending' && b.status !== 'cancelled')
  const wins = resolvedBets.filter(b => b.status === 'won').length
  const losses = resolvedBets.filter(b => b.status === 'lost').length
  const pushes = resolvedBets.filter(b => b.status === 'push').length
  const totalPnl = resolvedBets.reduce((sum, b) => sum + (b.pnl || 0), 0)
  const winRate = resolvedBets.length > 0 ? (wins / (wins + losses)) * 100 : 0

  const stats = [
    { label: 'Total Bets', value: resolvedBets.length.toString() },
    ...(pendingBets.length > 0 ? [{ label: 'Pending', value: pendingBets.length.toString(), color: 'text-blue-400' }] : []),
    { label: 'Wins', value: wins.toString(), color: 'text-green-400' },
    { label: 'Losses', value: losses.toString(), color: 'text-red-400' },
    { label: 'Pushes', value: pushes.toString(), color: 'text-yellow-400' },
    { label: 'Win Rate', value: `${winRate.toFixed(1)}%` },
    {
      label: 'Total P&L',
      value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`,
      color: totalPnl >= 0 ? 'text-green-400' : 'text-red-400'
    },
  ]

  // Per-stat win rates
  const perStatData = statTypes
    .map(stat => {
      const statBets = resolvedBets.filter(b => b.stat_type === stat)
      if (statBets.length === 0) return null
      const statWins = statBets.filter(b => b.status === 'won').length
      const statLosses = statBets.filter(b => b.status === 'lost').length
      const rate = statWins + statLosses > 0 ? (statWins / (statWins + statLosses)) * 100 : 0
      return { stat, wins: statWins, losses: statLosses, total: statBets.length, rate }
    })
    .filter(Boolean) as { stat: StatType; wins: number; losses: number; total: number; rate: number }[]

  // Per-direction breakdown (over/under)
  const perDirectionData = (['over', 'under'] as const)
    .map(dir => {
      const dirBets = resolvedBets.filter(b => b.bet_direction === dir)
      if (dirBets.length === 0) return null
      const dirWins = dirBets.filter(b => b.status === 'won').length
      const dirLosses = dirBets.filter(b => b.status === 'lost').length
      const rate = dirWins + dirLosses > 0 ? (dirWins / (dirWins + dirLosses)) * 100 : 0
      const pnl = dirBets.reduce((sum, b) => sum + (b.pnl || 0), 0)
      return { direction: dir, wins: dirWins, losses: dirLosses, total: dirBets.length, rate, pnl }
    })
    .filter(Boolean) as { direction: 'over' | 'under'; wins: number; losses: number; total: number; rate: number; pnl: number }[]

  return (
    <div className="space-y-4 mb-6">
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
        {stats.map((stat) => (
          <div key={stat.label} className="bg-slate-800 rounded-lg border border-slate-700 p-4">
            <div className="text-xs text-slate-400 mb-1">{stat.label}</div>
            <div className={cn('text-xl font-semibold', stat.color || 'text-slate-50')}>
              {stat.value}
            </div>
          </div>
        ))}
      </div>
      {perStatData.length > 0 && (
        <div className="grid grid-cols-3 gap-4">
          {perStatData.map(({ stat, wins: w, losses: l, rate }) => (
            <div key={stat} className="bg-slate-800 rounded-lg border border-slate-700 p-4 flex items-center gap-3">
              <span className={cn('text-xs px-2 py-0.5 rounded text-white', STAT_COLORS[stat])}>
                {STAT_LABELS[stat]}
              </span>
              <div className="flex-1">
                <div className="text-lg font-semibold text-slate-50">{rate.toFixed(1)}%</div>
                <div className="text-xs text-slate-400">{w}W - {l}L</div>
              </div>
            </div>
          ))}
        </div>
      )}
      {perDirectionData.length > 0 && (
        <div className="grid grid-cols-2 gap-4">
          {perDirectionData.map(({ direction, wins: w, losses: l, rate, pnl }) => (
            <div key={direction} className="bg-slate-800 rounded-lg border border-slate-700 p-4 flex items-center gap-3">
              <span className={cn(
                'text-xs px-2 py-0.5 rounded text-white',
                direction === 'over' ? 'bg-emerald-600' : 'bg-orange-600'
              )}>
                {direction === 'over' ? 'Over' : 'Under'}
              </span>
              <div className="flex-1">
                <div className="text-lg font-semibold text-slate-50">{rate.toFixed(1)}%</div>
                <div className="text-xs text-slate-400">{w}W - {l}L</div>
              </div>
              <div className={cn('text-sm font-medium', pnl >= 0 ? 'text-green-400' : 'text-red-400')}>
                {pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
