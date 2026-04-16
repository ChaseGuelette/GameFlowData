'use client'

import { useState } from 'react'
import { cn } from '@/lib/utils'
import { type DailyPerformance, type PaperBet, STAT_LABELS } from '@/types/predictions'

interface DailyBreakdownProps {
  daily: DailyPerformance[]
  bets: PaperBet[]
}

export function DailyBreakdown({ daily, bets }: DailyBreakdownProps) {
  const [expandedDate, setExpandedDate] = useState<string | null>(null)

  // Group bets by date
  const betsByDate = new Map<string, PaperBet[]>()
  for (const bet of bets) {
    const arr = betsByDate.get(bet.game_date) ?? []
    arr.push(bet)
    betsByDate.set(bet.game_date, arr)
  }

  // Sort daily descending
  const sorted = [...daily].sort((a, b) => b.game_date.localeCompare(a.game_date))

  if (sorted.length === 0) {
    return (
      <div className="p-6 text-center text-slate-500 text-sm">
        No daily data for this month.
      </div>
    )
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-xs text-slate-400 border-b border-slate-700">
            <th className="px-4 py-2 font-medium w-8"></th>
            <th className="px-2 py-2 font-medium">Date</th>
            <th className="px-2 py-2 font-medium text-right">Bets</th>
            <th className="px-2 py-2 font-medium text-right">W-L</th>
            <th className="px-2 py-2 font-medium text-right">P&L</th>
            <th className="px-2 py-2 font-medium text-right">Cumulative</th>
            <th className="px-2 py-2 font-medium text-right">ROI</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => {
            const isExpanded = expandedDate === row.game_date
            const dayBets = betsByDate.get(row.game_date) ?? []
            const date = new Date(row.game_date + 'T12:00:00')
            const dateLabel = date.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' })

            return (
              <>
                <tr
                  key={row.game_date}
                  onClick={() => setExpandedDate(isExpanded ? null : row.game_date)}
                  className={cn(
                    'border-b border-slate-700/50 cursor-pointer transition-colors',
                    row.total_pnl >= 0 ? 'hover:bg-green-900/10' : 'hover:bg-red-900/10'
                  )}
                >
                  <td className="px-4 py-2.5 text-slate-500">
                    {dayBets.length > 0 && (
                      <svg
                        className={cn('h-3 w-3 transition-transform', isExpanded && 'rotate-90')}
                        fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                      >
                        <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                      </svg>
                    )}
                  </td>
                  <td className="px-2 py-2.5 text-slate-300">{dateLabel}</td>
                  <td className="px-2 py-2.5 text-right text-slate-400">{row.total_bets}</td>
                  <td className="px-2 py-2.5 text-right whitespace-nowrap">
                    <span className="text-green-400">{row.bets_won}</span>
                    <span className="text-slate-500">-</span>
                    <span className="text-red-400">{row.bets_lost}</span>
                  </td>
                  <td className={cn(
                    'px-2 py-2.5 text-right font-medium whitespace-nowrap',
                    row.total_pnl >= 0 ? 'text-green-400' : 'text-red-400'
                  )}>
                    {row.total_pnl >= 0 ? '+' : ''}${row.total_pnl.toFixed(2)}
                  </td>
                  <td className={cn(
                    'px-2 py-2.5 text-right text-slate-300 whitespace-nowrap',
                  )}>
                    {row.cumulative_pnl >= 0 ? '+' : ''}${row.cumulative_pnl.toFixed(2)}
                  </td>
                  <td className={cn(
                    'px-2 py-2.5 text-right whitespace-nowrap',
                    row.roi_pct >= 0 ? 'text-green-400' : 'text-red-400'
                  )}>
                    {row.roi_pct >= 0 ? '+' : ''}{row.roi_pct.toFixed(1)}%
                  </td>
                </tr>

                {/* Bet list for this day */}
                {isExpanded && dayBets.length > 0 && (
                  <tr key={`${row.game_date}-bets`} className="bg-slate-900/50 border-b border-slate-700/30">
                    <td colSpan={7} className="px-6 py-3">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="text-slate-500 text-left">
                            <th className="pb-1 font-medium">Player</th>
                            <th className="pb-1 font-medium">Stat</th>
                            <th className="pb-1 font-medium text-right">Line</th>
                            <th className="pb-1 font-medium text-center">Side</th>
                            <th className="pb-1 font-medium text-right">Odds</th>
                            <th className="pb-1 font-medium text-right">Stake</th>
                            <th className="pb-1 font-medium text-right">Result</th>
                            <th className="pb-1 font-medium text-right">P&L</th>
                          </tr>
                        </thead>
                        <tbody>
                          {dayBets.map((bet) => (
                            <tr key={bet.id} className="border-t border-slate-700/30">
                              <td className="py-1.5 pr-2 text-slate-300 font-medium">{bet.player_name}</td>
                              <td className="py-1.5 pr-2 text-slate-400">
                                {STAT_LABELS[bet.stat_type] ?? bet.stat_type}
                              </td>
                              <td className="py-1.5 pr-2 text-right text-slate-400">{bet.line}</td>
                              <td className="py-1.5 pr-2 text-center">
                                <span className={cn(
                                  'px-1.5 py-0.5 rounded text-xs font-medium',
                                  bet.bet_direction === 'over'
                                    ? 'bg-blue-900/50 text-blue-300'
                                    : 'bg-purple-900/50 text-purple-300'
                                )}>
                                  {bet.bet_direction.toUpperCase()}
                                </span>
                              </td>
                              <td className="py-1.5 pr-2 text-right text-slate-400">
                                {bet.odds_at_bet > 0 ? '+' : ''}{bet.odds_at_bet}
                              </td>
                              <td className="py-1.5 pr-2 text-right text-slate-400">
                                ${bet.stake.toFixed(2)}
                              </td>
                              <td className="py-1.5 pr-2 text-center">
                                <span className={cn(
                                  'px-1.5 py-0.5 rounded text-xs font-medium',
                                  bet.status === 'won'  && 'bg-green-900/50 text-green-300',
                                  bet.status === 'lost' && 'bg-red-900/50 text-red-300',
                                  bet.status === 'push' && 'bg-slate-700 text-slate-300',
                                )}>
                                  {bet.status.toUpperCase()}
                                </span>
                              </td>
                              <td className={cn(
                                'py-1.5 text-right font-medium',
                                (bet.pnl ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'
                              )}>
                                {(bet.pnl ?? 0) >= 0 ? '+' : ''}${(bet.pnl ?? 0).toFixed(2)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </td>
                  </tr>
                )}
              </>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
