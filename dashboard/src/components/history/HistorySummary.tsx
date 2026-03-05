'use client'

import { type PaperBet } from '@/types/predictions'
import { cn } from '@/lib/utils'

interface HistorySummaryProps {
  bets: PaperBet[]
}

export function HistorySummary({ bets }: HistorySummaryProps) {
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

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
      {stats.map((stat) => (
        <div key={stat.label} className="bg-slate-800 rounded-lg border border-slate-700 p-4">
          <div className="text-xs text-slate-400 mb-1">{stat.label}</div>
          <div className={cn('text-xl font-semibold', stat.color || 'text-slate-50')}>
            {stat.value}
          </div>
        </div>
      ))}
    </div>
  )
}
