'use client'

import { useEffect, useState, useMemo } from 'react'
import { createClient } from '@/lib/supabase/client'
import { KPICard } from '@/components/performance/KPICard'
import { BankrollChart } from '@/components/performance/BankrollChart'
import { StatBreakdown } from '@/components/performance/StatBreakdown'
import { BetSourceFilter, type BetSource } from '@/components/shared/BetSourceFilter'
import { cn } from '@/lib/utils'
import { type DailyPerformance, type StatPerformance, type PaperBet, type StatType } from '@/types/predictions'

type PerformanceTab = 'props' | 'dfs'

// Extended type to include is_recommended from joined daily_predictions
interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
}

// DFS entry from dfs_paper_entries table
interface DfsEntry {
  id: number
  entry_date: string
  slip_type: string
  platform: string
  num_legs: number
  stake: number
  status: string
  legs_won: number
  legs_lost: number
  legs_push: number
  legs_cancelled: number
  payout_multiplier: number
  pnl: number
  avg_edge: number
  min_edge: number
}

// DFS daily log from dfs_paper_daily_log table
interface DfsDailyLog {
  entry_date: string
  entries_placed: number
  entries_won: number
  entries_lost: number
  entries_partial: number
  total_staked: number
  total_pnl: number
  roi_pct: number
  cumulative_pnl: number
  bankroll_after: number
}

const SLIP_TYPE_LABELS: Record<string, string> = {
  ud_3_standard: 'UD 3-Pick',
  ud_5_standard: 'UD 5-Pick',
  pp_5_flex: 'PP 5-Flex',
  pp_6_flex: 'PP 6-Flex',
}

export default function PerformancePage() {
  const [dailyData, setDailyData] = useState<DailyPerformance[]>([])
  const [allBets, setAllBets] = useState<PaperBetWithRecommended[]>([])
  const [loading, setLoading] = useState(true)
  const [currentBankroll, setCurrentBankroll] = useState<number>(0)
  const [betSource, setBetSource] = useState<BetSource>('model') // Default to Model Picks
  const [activeTab, setActiveTab] = useState<PerformanceTab>('props')

  // DFS state
  const [dfsEntries, setDfsEntries] = useState<DfsEntry[]>([])
  const [dfsDailyData, setDfsDailyData] = useState<DfsDailyLog[]>([])
  const [dfsLoading, setDfsLoading] = useState(false)

  // Fetch props data
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

      // Fetch all resolved bets
      const { data: betsData, error: betsError } = await supabase
        .from('paper_bets')
        .select('*')
        .in('status', ['won', 'lost', 'push'])
        .order('game_date', { ascending: true })

      if (!betsError && betsData) {
        // Get unique prediction IDs to fetch is_recommended status
        const predictionIds = [...new Set(betsData.map(b => b.prediction_id).filter(Boolean))]

        // Fetch is_recommended for these predictions
        const { data: predictionsData } = await supabase
          .from('daily_predictions')
          .select('id, is_recommended')
          .in('id', predictionIds)

        // Create a map for quick lookup
        const recommendedMap = new Map<number, boolean>()
        if (predictionsData) {
          for (const p of predictionsData) {
            recommendedMap.set(p.id, p.is_recommended ?? false)
          }
        }

        // Merge is_recommended into bets
        const enrichedBets = betsData.map(bet => ({
          ...bet,
          is_recommended: recommendedMap.get(bet.prediction_id) ?? false,
        })) as PaperBetWithRecommended[]

        setAllBets(enrichedBets)
      }

      setLoading(false)
    }

    fetchData()
  }, [])

  // Fetch DFS data when tab switches to DFS
  useEffect(() => {
    if (activeTab !== 'dfs') return
    if (dfsEntries.length > 0) return // Already loaded

    async function fetchDfsData() {
      setDfsLoading(true)
      const supabase = createClient()

      const [entriesRes, dailyRes] = await Promise.all([
        supabase
          .from('dfs_paper_entries')
          .select('*')
          .order('entry_date', { ascending: true }),
        supabase
          .from('dfs_paper_daily_log')
          .select('*')
          .order('entry_date', { ascending: true }),
      ])

      if (!entriesRes.error && entriesRes.data) {
        setDfsEntries(entriesRes.data as DfsEntry[])
      }
      if (!dailyRes.error && dailyRes.data) {
        setDfsDailyData(dailyRes.data as DfsDailyLog[])
      }

      setDfsLoading(false)
    }

    fetchDfsData()
  }, [activeTab, dfsEntries.length])

  // Filter bets based on source (Model Picks = is_recommended from daily_predictions)
  const filteredBets = useMemo(() => {
    if (betSource === 'model') {
      return allBets.filter(b => b.is_recommended === true)
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

  // DFS KPIs from resolved entries
  const dfsKpis = useMemo(() => {
    const resolved = dfsEntries.filter(e => ['won', 'lost', 'partial'].includes(e.status))
    const wins = resolved.filter(e => e.status === 'won').length
    const losses = resolved.filter(e => e.status === 'lost').length
    const partials = resolved.filter(e => e.status === 'partial').length
    const pnl = resolved.reduce((sum, e) => sum + (e.pnl || 0), 0)
    const staked = resolved.reduce((sum, e) => sum + (e.stake || 0), 0)
    const roi = staked > 0 ? (pnl / staked) * 100 : 0
    const total = wins + losses + partials
    const winRate = total > 0 ? (wins / total) * 100 : 0
    const bankroll = dfsDailyData.length > 0
      ? dfsDailyData[dfsDailyData.length - 1].bankroll_after
      : 500

    return { wins, losses, partials, pnl, staked, roi, winRate, bankroll, totalResolved: total }
  }, [dfsEntries, dfsDailyData])

  // DFS chart data adapted to BankrollChart format
  const dfsChartData = useMemo(() => {
    return dfsDailyData.map(d => ({
      game_date: d.entry_date,
      total_pnl: d.total_pnl,
      cumulative_pnl: d.cumulative_pnl,
      bankroll_after: d.bankroll_after,
      total_bets: d.entries_placed,
      bets_won: d.entries_won,
      bets_lost: d.entries_lost,
      total_staked: d.total_staked,
      roi_pct: d.roi_pct,
    } as DailyPerformance))
  }, [dfsDailyData])

  // DFS slip type breakdown
  const dfsSlipBreakdown = useMemo(() => {
    const slipMap = new Map<string, { entries: number; wins: number; losses: number; partials: number; pnl: number; staked: number; totalEdge: number }>()

    for (const entry of dfsEntries) {
      if (!['won', 'lost', 'partial'].includes(entry.status)) continue
      const key = entry.slip_type
      if (!slipMap.has(key)) {
        slipMap.set(key, { entries: 0, wins: 0, losses: 0, partials: 0, pnl: 0, staked: 0, totalEdge: 0 })
      }
      const s = slipMap.get(key)!
      s.entries++
      if (entry.status === 'won') s.wins++
      if (entry.status === 'lost') s.losses++
      if (entry.status === 'partial') s.partials++
      s.pnl += entry.pnl || 0
      s.staked += entry.stake || 0
      s.totalEdge += entry.avg_edge || 0
    }

    return Array.from(slipMap.entries()).map(([slipType, data]) => ({
      slipType,
      label: SLIP_TYPE_LABELS[slipType] || slipType,
      ...data,
      roi: data.staked > 0 ? (data.pnl / data.staked) * 100 : 0,
      avgEdge: data.entries > 0 ? (data.totalEdge / data.entries) * 100 : 0,
    })).sort((a, b) => b.entries - a.entries)
  }, [dfsEntries])

  return (
    <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-50">Performance</h1>
          <p className="text-slate-400">
            {activeTab === 'props'
              ? (betSource === 'model' ? 'Model Picks only (BL Edge ≥9%)' : 'All bets')
              : 'DFS Paper Trading (Market Edge)'}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Tab Toggle */}
          <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg border border-slate-700">
            <button
              onClick={() => setActiveTab('props')}
              className={cn(
                'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                activeTab === 'props'
                  ? 'bg-slate-700 text-slate-100'
                  : 'text-slate-400 hover:text-slate-200'
              )}
            >
              Props
            </button>
            <button
              onClick={() => setActiveTab('dfs')}
              className={cn(
                'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                activeTab === 'dfs'
                  ? 'bg-indigo-600 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              )}
            >
              DFS
            </button>
          </div>
          {/* Bet Source Filter (props only) */}
          {activeTab === 'props' && (
            <BetSourceFilter activeSource={betSource} onSourceChange={setBetSource} />
          )}
        </div>
      </div>

      {/* Props Tab */}
      {activeTab === 'props' && (
        <>
          {loading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading performance data...</div>
            </div>
          ) : (
            <div className="space-y-6">
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
              <BankrollChart data={chartData} />
              <StatBreakdown stats={statData} />
            </div>
          )}
        </>
      )}

      {/* DFS Tab */}
      {activeTab === 'dfs' && (
        <>
          {dfsLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading DFS performance data...</div>
            </div>
          ) : (
            <div className="space-y-6">
              {/* DFS KPI Cards */}
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                <KPICard
                  label="DFS Bankroll"
                  value={`$${dfsKpis.bankroll.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
                />
                <KPICard
                  label="Total P&L"
                  value={`${dfsKpis.pnl >= 0 ? '+' : ''}$${dfsKpis.pnl.toFixed(2)}`}
                  trend={dfsKpis.pnl >= 0 ? 'up' : 'down'}
                />
                <KPICard
                  label="Overall ROI"
                  value={`${dfsKpis.roi >= 0 ? '+' : ''}${dfsKpis.roi.toFixed(1)}%`}
                  trend={dfsKpis.roi >= 0 ? 'up' : 'down'}
                />
                <KPICard
                  label="Win Rate"
                  value={`${dfsKpis.winRate.toFixed(1)}%`}
                  subValue={`${dfsKpis.wins}W - ${dfsKpis.losses}L - ${dfsKpis.partials}P`}
                />
              </div>

              {/* DFS Bankroll Chart */}
              <BankrollChart data={dfsChartData} />

              {/* DFS Slip Type Breakdown */}
              <div className="bg-slate-800 rounded-lg border border-slate-700 p-6">
                <h3 className="text-lg font-medium text-slate-50 mb-4">Performance by Slip Type</h3>
                {dfsSlipBreakdown.length === 0 ? (
                  <div className="text-center py-8 text-slate-500">
                    No resolved DFS entries yet
                  </div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="text-left text-xs text-slate-400 border-b border-slate-700">
                          <th className="pb-3 font-medium">Slip Type</th>
                          <th className="pb-3 font-medium text-right">Entries</th>
                          <th className="pb-3 font-medium text-right">W-L-P</th>
                          <th className="pb-3 font-medium text-right">Avg Edge</th>
                          <th className="pb-3 font-medium text-right">P&L</th>
                          <th className="pb-3 font-medium text-right">ROI</th>
                        </tr>
                      </thead>
                      <tbody className="text-sm">
                        {dfsSlipBreakdown.map((slip) => (
                          <tr key={slip.slipType} className="border-b border-slate-700/50">
                            <td className="py-3">
                              <span className="inline-block px-2 py-0.5 rounded text-xs text-white bg-indigo-600">
                                {slip.label}
                              </span>
                            </td>
                            <td className="py-3 text-right text-slate-300">{slip.entries}</td>
                            <td className="py-3 text-right text-slate-300">
                              <span className="text-green-400">{slip.wins}</span>
                              <span className="text-slate-500">-</span>
                              <span className="text-red-400">{slip.losses}</span>
                              <span className="text-slate-500">-</span>
                              <span className="text-yellow-400">{slip.partials}</span>
                            </td>
                            <td className="py-3 text-right text-slate-300">
                              {slip.avgEdge.toFixed(1)}%
                            </td>
                            <td className={cn(
                              'py-3 text-right font-medium',
                              slip.pnl >= 0 ? 'text-green-400' : 'text-red-400'
                            )}>
                              {slip.pnl >= 0 ? '+' : ''}${slip.pnl.toFixed(2)}
                            </td>
                            <td className={cn(
                              'py-3 text-right font-medium',
                              slip.roi >= 0 ? 'text-green-400' : 'text-red-400'
                            )}>
                              {slip.roi >= 0 ? '+' : ''}{slip.roi.toFixed(1)}%
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
          )}
        </>
      )}
    </main>
  )
}
