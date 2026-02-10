'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { Navbar } from '@/components/layout/Navbar'
import { KPICard } from '@/components/performance/KPICard'
import { BankrollChart } from '@/components/performance/BankrollChart'
import { StatBreakdown } from '@/components/performance/StatBreakdown'
import { type DailyPerformance, type StatPerformance, type PaperBet, type StatType } from '@/types/predictions'

export default function PerformancePage() {
  const [dailyData, setDailyData] = useState<DailyPerformance[]>([])
  const [statData, setStatData] = useState<StatPerformance[]>([])
  const [loading, setLoading] = useState(true)
  const [currentBankroll, setCurrentBankroll] = useState<number>(0)

  useEffect(() => {
    async function fetchData() {
      const supabase = createClient()

      // Fetch daily performance log
      const { data: logData, error: logError } = await supabase
        .from('paper_trading_daily_log')
        .select('*')
        .order('game_date', { ascending: true })

      if (!logError && logData) {
        setDailyData(logData as DailyPerformance[])
        if (logData.length > 0) {
          setCurrentBankroll(logData[logData.length - 1].bankroll_after)
        }
      }

      // Fetch all resolved bets for stat breakdown
      const { data: betsData, error: betsError } = await supabase
        .from('paper_bets')
        .select('stat_type, status, pnl, stake')
        .in('status', ['won', 'lost', 'push'])

      if (!betsError && betsData) {
        // Aggregate by stat type
        const statMap = new Map<StatType, { wins: number; losses: number; pnl: number; staked: number }>()

        for (const bet of betsData as PaperBet[]) {
          const stat = bet.stat_type
          if (!statMap.has(stat)) {
            statMap.set(stat, { wins: 0, losses: 0, pnl: 0, staked: 0 })
          }
          const entry = statMap.get(stat)!
          if (bet.status === 'won') entry.wins++
          if (bet.status === 'lost') entry.losses++
          entry.pnl += bet.pnl || 0
          entry.staked += bet.stake || 0
        }

        const statPerformance: StatPerformance[] = []
        for (const [stat, data] of statMap) {
          const totalBets = data.wins + data.losses
          statPerformance.push({
            stat,
            total_bets: totalBets,
            wins: data.wins,
            losses: data.losses,
            win_rate: totalBets > 0 ? (data.wins / totalBets) * 100 : 0,
            total_pnl: data.pnl,
            roi: data.staked > 0 ? (data.pnl / data.staked) * 100 : 0,
          })
        }

        // Sort by total bets descending
        statPerformance.sort((a, b) => b.total_bets - a.total_bets)
        setStatData(statPerformance)
      }

      setLoading(false)
    }

    fetchData()
  }, [])

  // Calculate aggregate KPIs
  const totalPnl = dailyData.reduce((sum, d) => sum + d.total_pnl, 0)
  const totalStaked = dailyData.reduce((sum, d) => sum + d.total_staked, 0)
  const totalWins = dailyData.reduce((sum, d) => sum + d.bets_won, 0)
  const totalLosses = dailyData.reduce((sum, d) => sum + d.bets_lost, 0)
  const overallRoi = totalStaked > 0 ? (totalPnl / totalStaked) * 100 : 0
  const winRate = (totalWins + totalLosses) > 0 ? (totalWins / (totalWins + totalLosses)) * 100 : 0

  return (
    <div className="min-h-screen flex flex-col bg-slate-900">
      <Navbar bankroll={currentBankroll} />

      <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-slate-50">Performance</h1>
          <p className="text-slate-400">Track your betting performance over time</p>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-16">
            <div className="text-slate-400">Loading performance data...</div>
          </div>
        ) : (
          <div className="space-y-6">
            {/* KPI Cards */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <KPICard
                label="Current Bankroll"
                value={`$${currentBankroll.toLocaleString()}`}
              />
              <KPICard
                label="Total P&L"
                value={`${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`}
                trend={totalPnl >= 0 ? 'up' : 'down'}
              />
              <KPICard
                label="Overall ROI"
                value={`${overallRoi >= 0 ? '+' : ''}${overallRoi.toFixed(1)}%`}
                trend={overallRoi >= 0 ? 'up' : 'down'}
              />
              <KPICard
                label="Win Rate"
                value={`${winRate.toFixed(1)}%`}
                subValue={`${totalWins}W - ${totalLosses}L`}
              />
            </div>

            {/* Bankroll Chart */}
            <BankrollChart data={dailyData} />

            {/* Stat Breakdown */}
            <StatBreakdown stats={statData} />
          </div>
        )}
      </main>
    </div>
  )
}
