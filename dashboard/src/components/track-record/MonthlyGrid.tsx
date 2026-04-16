'use client'

import { useState } from 'react'
import { cn } from '@/lib/utils'
import { type MonthlyAggregate } from '@/lib/hooks/useTrackRecordData'
import { DailyBreakdown } from './DailyBreakdown'
import { type DailyPerformance, type PaperBet } from '@/types/predictions'

interface MonthlyGridProps {
  months: MonthlyAggregate[]
  dailyLog: DailyPerformance[]
  bets: PaperBet[]
}

export function MonthlyGrid({ months, dailyLog, bets }: MonthlyGridProps) {
  const [expandedMonth, setExpandedMonth] = useState<string | null>(null)

  if (months.length === 0) {
    return (
      <div className="bg-slate-800 rounded-lg border border-slate-700 p-8 text-center text-slate-500">
        No betting data yet. Import a CSV or add bets manually to get started.
      </div>
    )
  }

  // Group daily log and bets by month for drilldown
  const dailyByMonth = new Map<string, DailyPerformance[]>()
  for (const row of dailyLog) {
    const m = row.game_date.slice(0, 7)
    const arr = dailyByMonth.get(m) ?? []
    arr.push(row)
    dailyByMonth.set(m, arr)
  }

  const betsByMonth = new Map<string, PaperBet[]>()
  for (const bet of bets) {
    const m = bet.game_date.slice(0, 7)
    const arr = betsByMonth.get(m) ?? []
    arr.push(bet)
    betsByMonth.set(m, arr)
  }

  return (
    <div className="space-y-3">
      {months.map((month) => {
        const isExpanded = expandedMonth === month.month
        const winRate = (month.wins + month.losses) > 0
          ? (month.wins / (month.wins + month.losses)) * 100
          : 0

        return (
          <div key={month.month} className="bg-slate-800 rounded-lg border border-slate-700 overflow-hidden">
            {/* Month card header — clickable */}
            <button
              onClick={() => setExpandedMonth(isExpanded ? null : month.month)}
              className="w-full text-left p-4 sm:p-5 hover:bg-slate-700/40 transition-colors"
            >
              <div className="flex flex-wrap items-center gap-x-6 gap-y-2">
                {/* Month name + expand indicator */}
                <div className="flex items-center gap-2 min-w-[140px]">
                  <svg
                    className={cn('h-4 w-4 text-slate-400 transition-transform', isExpanded && 'rotate-90')}
                    fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                  </svg>
                  <span className="text-base font-semibold text-slate-50">{month.monthLabel}</span>
                </div>

                {/* Record */}
                <div className="text-sm text-slate-400">
                  <span className="text-green-400 font-medium">{month.wins}W</span>
                  <span className="text-slate-500 mx-0.5">-</span>
                  <span className="text-red-400 font-medium">{month.losses}L</span>
                  {month.pushes > 0 && (
                    <>
                      <span className="text-slate-500 mx-0.5">-</span>
                      <span className="text-slate-400 font-medium">{month.pushes}P</span>
                    </>
                  )}
                </div>

                {/* P&L */}
                <div className={cn(
                  'text-sm font-bold',
                  month.total_pnl >= 0 ? 'text-green-400' : 'text-red-400'
                )}>
                  {month.total_pnl >= 0 ? '+' : ''}${month.total_pnl.toFixed(2)}
                </div>

                {/* ROI */}
                <div className={cn(
                  'text-sm font-medium',
                  month.roi_pct >= 0 ? 'text-green-400' : 'text-red-400'
                )}>
                  {month.roi_pct >= 0 ? '+' : ''}{month.roi_pct.toFixed(1)}% ROI
                </div>

                {/* Win rate */}
                <div className="text-sm text-slate-400">
                  {winRate.toFixed(1)}% Win Rate
                </div>

                {/* Bets count */}
                <div className="text-xs text-slate-500">
                  {month.total_bets} bets
                </div>
              </div>
            </button>

            {/* Expanded drilldown */}
            {isExpanded && (
              <div className="border-t border-slate-700">
                <DailyBreakdown
                  daily={dailyByMonth.get(month.month) ?? []}
                  bets={betsByMonth.get(month.month) ?? []}
                />
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}
