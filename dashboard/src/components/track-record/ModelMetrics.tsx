'use client'

import { useMemo } from 'react'
import { type PaperBet, type DailyPerformance } from '@/types/predictions'

interface ModelMetricsProps {
  bets: PaperBet[]
  daily: DailyPerformance[]
}

interface EdgeBucket {
  label: string
  bets: number
  wins: number
  winRate: number
  pnl: number
}

export function ModelMetrics({ bets, daily }: ModelMetricsProps) {
  const metrics = useMemo(() => {
    const resolved = bets.filter(b => ['won', 'lost', 'push'].includes(b.status))
    if (resolved.length === 0) return null

    // Edge accuracy buckets (only for bets with edge data)
    const withEdge = resolved.filter(b => b.edge > 0)
    const bucket10 = withEdge.filter(b => b.edge >= 0.10)
    const bucket8  = withEdge.filter(b => b.edge >= 0.08 && b.edge < 0.10)
    const bucket5  = withEdge.filter(b => b.edge >= 0.05 && b.edge < 0.08)

    const toBucket = (arr: PaperBet[], label: string): EdgeBucket => {
      const wins = arr.filter(b => b.status === 'won').length
      const pnl  = arr.reduce((s, b) => s + (b.pnl ?? 0), 0)
      return {
        label,
        bets: arr.length,
        wins,
        winRate: arr.length > 0 ? (wins / arr.length) * 100 : 0,
        pnl,
      }
    }

    const edgeBuckets = [
      toBucket(bucket10, '10%+ edge'),
      toBucket(bucket8,  '8-10% edge'),
      toBucket(bucket5,  '5-8% edge'),
    ].filter(b => b.bets > 0)

    // Streak analysis
    let currentStreak = 0
    let currentStreakType: 'W' | 'L' | null = null
    let longestWin = 0
    let longestLoss = 0
    let curWin = 0
    let curLoss = 0

    for (const bet of resolved) {
      if (bet.status === 'won') {
        curWin++
        curLoss = 0
        if (currentStreakType === 'W') currentStreak++
        else { currentStreak = 1; currentStreakType = 'W' }
      } else if (bet.status === 'lost') {
        curLoss++
        curWin = 0
        if (currentStreakType === 'L') currentStreak++
        else { currentStreak = 1; currentStreakType = 'L' }
      } else {
        // push resets streak
        currentStreak = 0
        currentStreakType = null
        curWin = 0
        curLoss = 0
      }
      longestWin  = Math.max(longestWin, curWin)
      longestLoss = Math.max(longestLoss, curLoss)
    }

    // Profitable days
    const profitableDays = daily.filter(d => d.total_pnl > 0).length
    const totalDays = daily.length

    return {
      edgeBuckets,
      currentStreak,
      currentStreakType,
      longestWin,
      longestLoss,
      profitableDays,
      totalDays,
    }
  }, [bets, daily])

  if (!metrics) {
    return null
  }

  return (
    <div className="bg-slate-800 rounded-lg border border-slate-700 p-5 space-y-5">
      <h3 className="text-base font-semibold text-slate-50">Model Performance Metrics</h3>

      {/* Edge accuracy */}
      {metrics.edgeBuckets.length > 0 && (
        <div>
          <div className="text-xs text-slate-400 mb-2 font-medium uppercase tracking-wide">
            Edge Accuracy
          </div>
          <div className="space-y-2">
            {metrics.edgeBuckets.map(bucket => (
              <div key={bucket.label} className="flex items-center gap-3">
                <div className="text-xs text-slate-400 w-24 shrink-0">{bucket.label}</div>
                <div className="flex-1 bg-slate-700 rounded-full h-1.5 overflow-hidden">
                  <div
                    className="h-full bg-blue-500 rounded-full"
                    style={{ width: `${Math.min(bucket.winRate, 100)}%` }}
                  />
                </div>
                <div className="text-xs text-slate-300 w-20 text-right shrink-0">
                  {bucket.winRate.toFixed(1)}% ({bucket.bets} bets)
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Streaks + day stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="bg-slate-900 rounded-lg p-3">
          <div className="text-xs text-slate-500 mb-1">Current Streak</div>
          <div className={`text-lg font-bold ${metrics.currentStreakType === 'W' ? 'text-green-400' : metrics.currentStreakType === 'L' ? 'text-red-400' : 'text-slate-400'}`}>
            {metrics.currentStreak > 0
              ? `${metrics.currentStreak}${metrics.currentStreakType}`
              : '—'}
          </div>
        </div>
        <div className="bg-slate-900 rounded-lg p-3">
          <div className="text-xs text-slate-500 mb-1">Best Win Streak</div>
          <div className="text-lg font-bold text-green-400">{metrics.longestWin}W</div>
        </div>
        <div className="bg-slate-900 rounded-lg p-3">
          <div className="text-xs text-slate-500 mb-1">Worst Lose Streak</div>
          <div className="text-lg font-bold text-red-400">{metrics.longestLoss}L</div>
        </div>
        <div className="bg-slate-900 rounded-lg p-3">
          <div className="text-xs text-slate-500 mb-1">Profitable Days</div>
          <div className="text-lg font-bold text-slate-50">
            {metrics.profitableDays}
            <span className="text-sm text-slate-500 font-normal">/{metrics.totalDays}</span>
          </div>
        </div>
      </div>
    </div>
  )
}
