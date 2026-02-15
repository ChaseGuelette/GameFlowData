'use client'

import { useEffect, useState, useMemo } from 'react'
import { createClient } from '@/lib/supabase/client'
import { Navbar } from '@/components/layout/Navbar'
import { KPICard } from '@/components/performance/KPICard'
import { BankrollChart } from '@/components/performance/BankrollChart'
import { StatBreakdown } from '@/components/performance/StatBreakdown'
import { BetSourceFilter, type BetSource, MODEL_PICKS_EDGE_THRESHOLD } from '@/components/shared/BetSourceFilter'
import { type DailyPerformance, type StatPerformance, type PaperBet, type StatType } from '@/types/predictions'

export default function PerformancePage() {
  const [dailyData, setDailyData] = useState<DailyPerformance[]>([])
  const [allBets, setAllBets] = useState<PaperBet[]>([])
  const [loading, setLoading] = useState(true)
  const [currentBankroll, setCurrentBankroll] = useState<number>(0)
  const [betSource, setBetSource] = useState<BetSource>('model') // Default to Model Picks

  useEffect(() => {
    async function fetchData() {
      const supabase = createClient()

      // Fetch daily performance log (for bankroll chart)
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

      // Fetch all resolved bets with edge for filtering
      const { data: betsData, error: betsError } = await supabase
        .from('paper_bets')
        .select('*')
        .in('status', ['won', 'lost', 'push'])
        .order('game_date', { ascending: true })

      if (!betsError && betsData) {
        setAllBets(betsData as PaperBet[])
      }

      setLoading(false)
    }

    fetchData()
  }, [])

  // Filter bets based on source (Model Picks vs All)
  const filteredBets = useMemo(() => {
    if (betSource === 'model') {
      return allBets.filter(b => b.edge >= MODEL_PICKS_EDGE_THRESHOLD)
    }
    return allBets
  }, [allBets, betSource])

  // Calculate aggregate KPIs from filtered bets
  const { totalPnl, totalStaked, totalWins, totalLosses, overallRoi, winRate } = useMemo(() => {
    const wins = filteredBets.filter(b => b.status === 'won').length
    const losses = filteredBets.filter(b => b.status === 'lost').length
    const pnl = filteredBets.reduce((sum, b) => sum + (b.pnl || 0), 0)
    const staked = filteredBets.reduce((sum, b) => sum + (b.stake || 0), 0)
    const roi = staked > 0 ? (pnl / staked) * 100 : 0
    const rate = (wins + losses) > 0 ? (wins / (wins + losses)) * 100 : 0

    return {
      totalPnl: pnl,
      totalStaked: staked,
      totalWins: wins,
      totalLosses: losses,
      overallRoi: roi,
      winRate: rate
    }
  }, [filteredBets])

  // Calculate stat breakdown from filtered bets
  const statData = useMemo(() => {
    const statMap = new Map<StatType, { wins: number; losses: number; pnl: number; staked: number }>()

    for (const bet of filteredBets) {
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
    return statPerformance
  }, [filteredBets])

  // Calculate simulated daily performance for Model Picks bankroll chart
  const chartData = useMemo(() => {
    if (betSource === 'all') {
      return dailyData
    }

    // For Model Picks, simulate bankroll progression
    const INITIAL_BANKROLL = 1000
    const dailyPnlMap = new Map<string, number>()

    for (const bet of filteredBets) {
      const date = bet.game_date
      dailyPnlMap.set(date, (dailyPnlMap.get(date) || 0) + (bet.pnl || 0))
    }

    // Get sorted unique dates
    const dates = Array.from(dailyPnlMap.keys()).sort()
    let cumulativePnl = 0

    return dates.map(date => {
      const dayPnl = dailyPnlMap.get(date) || 0
      cumulativePnl += dayPnl
      return {
        game_date: date,
        total_pnl: dayPnl,
        cumulative_pnl: cumulativePnl,
        bankroll_after: INITIAL_BANKROLL + cumulativePnl,
        total_bets: 0, // Not used in chart
        bets_won: 0,
        bets_lost: 0,
        total_staked: 0,
        roi_pct: 0
      } as DailyPerformance
    })
  }, [dailyData, filteredBets, betSource])

  // Calculate display bankroll based on filter
  const displayBankroll = betSource === 'model' && chartData.length > 0
    ? chartData[chartData.length - 1].bankroll_after
    : currentBankroll

  return (
    <div className="min-h-screen flex flex-col bg-slate-900">
      <Navbar bankroll={currentBankroll} />

      <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold text-slate-50">Performance</h1>
            <p className="text-slate-400">
              {betSource === 'model' ? 'Model Picks only (Edge ≥9%)' : 'All bets'}
            </p>
          </div>
          <BetSourceFilter activeSource={betSource} onSourceChange={setBetSource} />
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
                label={betSource === 'model' ? 'Model P&L Bankroll' : 'Current Bankroll'}
                value={`$${displayBankroll.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
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
            <BankrollChart data={chartData} />

            {/* Stat Breakdown */}
            <StatBreakdown stats={statData} />
          </div>
        )}
      </main>
    </div>
  )
}
