'use client'

import { useState, useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { KPICard } from '@/components/performance/KPICard'
import { BankrollChart } from '@/components/performance/BankrollChart'
import { StatBreakdown } from '@/components/performance/StatBreakdown'
import { MonthlyGrid } from '@/components/track-record/MonthlyGrid'
import { ModelMetrics } from '@/components/track-record/ModelMetrics'
import { CsvUpload } from '@/components/track-record/CsvUpload'
import { ManualBetForm } from '@/components/track-record/ManualBetForm'
import { useTrackRecordData, type TrackRecordSource } from '@/lib/hooks/useTrackRecordData'
import { BetSourceFilter, type BetSource } from '@/components/shared/BetSourceFilter'
import { cn } from '@/lib/utils'
import { type DailyPerformance, type StatPerformance, type PaperBet, type StatType } from '@/types/predictions'
import { useUserPreferences } from '@/lib/hooks/useUserPreferences'
import { useSport } from '@/contexts/SportContext'
import {
  usePropsPerformance,
  useDfsPerformance,
  useMyBetsPerformance,
  type DfsEntry,
} from '@/lib/hooks/usePerformanceData'

type PerformanceTab = 'props' | 'dfs' | 'my_bets' | 'record'

interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
}

const SLIP_TYPE_LABELS: Record<string, string> = {
  ud_3_standard: 'UD 3-Pick',
  ud_5_standard: 'UD 5-Pick',
  pp_5_flex: 'PP 5-Flex',
  pp_6_flex: 'PP 6-Flex',
}

const TRACK_SOURCES: { value: TrackRecordSource; label: string }[] = [
  { value: 'my_bets',  label: 'My Bets' },
  { value: 'paper',    label: 'Paper Trading' },
  { value: 'combined', label: 'Combined' },
]

function Modal({ title, children, onClose }: { title: string; children: React.ReactNode; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-slate-800 border border-slate-700 rounded-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto shadow-2xl">
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700">
          <h2 className="text-base font-semibold text-slate-50">{title}</h2>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-200 transition-colors">
            <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div className="p-5">{children}</div>
      </div>
    </div>
  )
}

export default function PerformancePage() {
  const { config } = useSport()
  const { prefs } = useUserPreferences()
  const [betSource, setBetSource] = useState<BetSource>('model')
  const [activeTab, setActiveTab] = useState<PerformanceTab>('props')
  const queryClient = useQueryClient()
  const [trackSource, setTrackSource] = useState<TrackRecordSource>('my_bets')
  const [showCsvModal, setShowCsvModal] = useState(false)
  const [showBetForm, setShowBetForm] = useState(false)

  // React Query hooks
  const propsQuery = usePropsPerformance()
  const dfsQuery = useDfsPerformance()
  const myBetsQuery = useMyBetsPerformance()
  const trackQuery = useTrackRecordData(trackSource)

  const dailyData = propsQuery.data?.dailyData ?? []
  const allBets = (propsQuery.data?.allBets ?? []) as PaperBetWithRecommended[]
  const currentBankroll = propsQuery.data?.currentBankroll ?? 0

  const dfsEntries = dfsQuery.data?.dfsEntries ?? []
  const dfsDailyData = dfsQuery.data?.dfsDailyData ?? []

  const myBets = myBetsQuery.data ?? []

  // Filter bets based on source (Model Picks = is_recommended from daily_predictions)
  const filteredBets = useMemo(() => {
    if (betSource === 'model') {
      return allBets.filter(b => b.is_recommended === true)
    }
    return allBets
  }, [allBets, betSource])

  // Calculate aggregate KPIs from filtered bets
  const { totalPnl, totalWins, totalLosses, overallRoi, winRate } = useMemo(() => {
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

    statPerformance.sort((a, b) => b.total_bets - a.total_bets)
    return statPerformance
  }, [filteredBets])

  // Calculate simulated daily performance for Model Picks bankroll chart
  const chartData = useMemo(() => {
    if (betSource === 'all') {
      return dailyData
    }

    const dailyPnlMap = new Map<string, number>()

    for (const bet of filteredBets) {
      const date = bet.game_date
      dailyPnlMap.set(date, (dailyPnlMap.get(date) || 0) + (bet.pnl || 0))
    }

    const dates = Array.from(dailyPnlMap.keys()).sort()
    const dailyPnls = dates.map(date => dailyPnlMap.get(date) || 0)
    const cumulativePnls = dailyPnls.reduce<number[]>((acc, pnl) => {
      acc.push((acc.length > 0 ? acc[acc.length - 1] : 0) + pnl)
      return acc
    }, [])

    return dates.map((date, i) => ({
      game_date: date,
      total_pnl: dailyPnls[i],
      cumulative_pnl: cumulativePnls[i],
      bankroll_after: prefs.initialBankroll + cumulativePnls[i],
      total_bets: 0,
      bets_won: 0,
      bets_lost: 0,
      total_staked: 0,
      roi_pct: 0
    } as DailyPerformance))
  }, [dailyData, filteredBets, betSource, prefs.initialBankroll])

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

  // My Bets KPIs
  const myBetsKpis = useMemo(() => {
    const wins = myBets.filter(b => b.status === 'won').length
    const losses = myBets.filter(b => b.status === 'lost').length
    const pnl = myBets.reduce((sum, b) => sum + (b.pnl || 0), 0)
    const staked = myBets.reduce((sum, b) => sum + (b.stake || 0), 0)
    const roi = staked > 0 ? (pnl / staked) * 100 : 0
    const total = wins + losses
    const winRate = total > 0 ? (wins / total) * 100 : 0
    return { wins, losses, pnl, staked, roi, winRate }
  }, [myBets])

  // My Bets bankroll chart data
  const myBetsChartData = useMemo(() => {
    const dailyPnlMap = new Map<string, number>()

    for (const bet of myBets) {
      const date = bet.game_date
      dailyPnlMap.set(date, (dailyPnlMap.get(date) || 0) + (bet.pnl || 0))
    }

    const dates = Array.from(dailyPnlMap.keys()).sort()
    const dailyPnls = dates.map(date => dailyPnlMap.get(date) || 0)
    const cumulativePnls = dailyPnls.reduce<number[]>((acc, pnl) => {
      acc.push((acc.length > 0 ? acc[acc.length - 1] : 0) + pnl)
      return acc
    }, [])

    return dates.map((date, i) => ({
      game_date: date,
      total_pnl: dailyPnls[i],
      cumulative_pnl: cumulativePnls[i],
      bankroll_after: prefs.initialBankroll + cumulativePnls[i],
      total_bets: 0,
      bets_won: 0,
      bets_lost: 0,
      total_staked: 0,
      roi_pct: 0,
    } as DailyPerformance))
  }, [myBets, prefs.initialBankroll])

  // My Bets stat breakdown
  const myBetsStatData = useMemo(() => {
    const statMap = new Map<StatType, { wins: number; losses: number; pnl: number; staked: number }>()

    for (const bet of myBets) {
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

    statPerformance.sort((a, b) => b.total_bets - a.total_bets)
    return statPerformance
  }, [myBets])

  return (
    <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-50">Performance</h1>
          <p className="text-slate-400">
            {activeTab === 'props'
              ? (betSource === 'model' ? 'Model Picks only (BL Edge ≥9%)' : 'All bets')
              : activeTab === 'dfs'
                ? 'DFS Paper Trading (Market Edge)'
                : activeTab === 'record'
                  ? 'Your verified betting history and performance'
                  : 'Your personal bet performance'}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Tab Toggle */}
          <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg border border-slate-700">
            <button
              onClick={() => setActiveTab('my_bets')}
              className={cn(
                'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                activeTab === 'my_bets'
                  ? 'bg-green-600 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              )}
            >
              My Bets
            </button>
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
            {config.features.dfs && (
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
            )}
            <button
              onClick={() => setActiveTab('record')}
              className={cn(
                'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                activeTab === 'record'
                  ? 'bg-blue-600 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              )}
            >
              Record
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
          {propsQuery.isLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading performance data...</div>
            </div>
          ) : (
            <div className="space-y-6">
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
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
          {dfsQuery.isLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading DFS performance data...</div>
            </div>
          ) : (
            <div className="space-y-6">
              {/* DFS KPI Cards */}
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
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
      {/* My Bets Tab */}
      {activeTab === 'my_bets' && (
        <>
          {myBetsQuery.isLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading your bet performance...</div>
            </div>
          ) : myBets.length === 0 ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-center">
                <p className="text-slate-400 text-lg">No resolved bets yet</p>
                <p className="text-slate-500 text-sm mt-2">
                  Tap the checkmark on prop cards to track bets. Results appear after games finish.
                </p>
              </div>
            </div>
          ) : (
            <div className="space-y-6">
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <KPICard
                  label="My Bankroll"
                  value={`$${(prefs.initialBankroll + myBetsKpis.pnl).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
                />
                <KPICard
                  label="Total P&L"
                  value={`${myBetsKpis.pnl >= 0 ? '+' : ''}$${myBetsKpis.pnl.toFixed(2)}`}
                  trend={myBetsKpis.pnl >= 0 ? 'up' : 'down'}
                />
                <KPICard
                  label="Overall ROI"
                  value={`${myBetsKpis.roi >= 0 ? '+' : ''}${myBetsKpis.roi.toFixed(1)}%`}
                  trend={myBetsKpis.roi >= 0 ? 'up' : 'down'}
                />
                <KPICard
                  label="Win Rate"
                  value={`${myBetsKpis.winRate.toFixed(1)}%`}
                  subValue={`${myBetsKpis.wins}W - ${myBetsKpis.losses}L`}
                />
              </div>
              <BankrollChart data={myBetsChartData} />
              <StatBreakdown stats={myBetsStatData} />
            </div>
          )}
        </>
      )}

      {/* Record Tab */}
      {activeTab === 'record' && (
        <>
          <div className="flex flex-wrap items-center justify-between gap-4 mb-6">
            <div className="flex items-center gap-1 bg-slate-900 rounded-lg p-1 w-fit">
              {TRACK_SOURCES.map(s => (
                <button
                  key={s.value}
                  onClick={() => setTrackSource(s.value)}
                  className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                    trackSource === s.value
                      ? 'bg-blue-600 text-white'
                      : 'text-slate-400 hover:text-slate-200'
                  }`}
                >
                  {s.label}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setShowCsvModal(true)}
                className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium bg-slate-700 hover:bg-slate-600 text-slate-200 transition-colors"
              >
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
                </svg>
                Import CSV
              </button>
              <button
                onClick={() => setShowBetForm(true)}
                className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium bg-blue-600 hover:bg-blue-700 text-white transition-colors"
              >
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m8-8H4" />
                </svg>
                Add Bet
              </button>
            </div>
          </div>

          {trackQuery.isLoading && (
            <div className="text-center py-16">
              <div className="animate-spin h-8 w-8 border-2 border-blue-500 border-t-transparent rounded-full mx-auto mb-3" />
              <div className="text-sm text-slate-400">Loading track record...</div>
            </div>
          )}
          {trackQuery.error && (
            <div className="bg-red-900/30 border border-red-700 rounded-lg px-5 py-4 text-sm text-red-300">
              Failed to load data: {trackQuery.error instanceof Error ? trackQuery.error.message : 'Unknown error'}
            </div>
          )}
          {trackQuery.data && !trackQuery.isLoading && (() => {
            const td = trackQuery.data
            const kpis = td.kpis
            const formatPnl = (v: number) => `${v >= 0 ? '+' : ''}$${Math.abs(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
            const formatRoi = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`
            return (
              <div className="space-y-6">
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                  <KPICard label="Total P&L" value={formatPnl(kpis.totalPnl)} trend={kpis.totalPnl >= 0 ? 'up' : 'down'} />
                  <KPICard label="ROI" value={formatRoi(kpis.roi)} trend={kpis.roi >= 0 ? 'up' : 'down'} />
                  <KPICard
                    label="Win Rate"
                    value={kpis.wins + kpis.losses > 0 ? `${((kpis.wins / (kpis.wins + kpis.losses)) * 100).toFixed(1)}%` : '—'}
                    subValue={`${kpis.wins}W-${kpis.losses}L${kpis.pushes > 0 ? `-${kpis.pushes}P` : ''}`}
                    trend={kpis.wins > kpis.losses ? 'up' : kpis.wins < kpis.losses ? 'down' : 'neutral'}
                  />
                  <KPICard
                    label="Profitable Months"
                    value={`${kpis.profitableMonths} of ${kpis.totalMonths}`}
                    trend={kpis.profitableMonths > kpis.totalMonths / 2 ? 'up' : 'neutral'}
                  />
                </div>
                <BankrollChart data={td.dailyLog} />
                <div>
                  <h2 className="text-lg font-semibold text-slate-50 mb-3">Monthly Summary</h2>
                  <MonthlyGrid months={td.monthlyAggregates} dailyLog={td.dailyLog} bets={td.bets} />
                </div>
                <StatBreakdown stats={td.statBreakdown} />
                {(trackSource === 'paper' || trackSource === 'combined') && td.bets.length > 0 && (
                  <ModelMetrics bets={td.bets} daily={td.dailyLog} />
                )}
                <div className="text-xs text-slate-600 text-center pb-4 max-w-2xl mx-auto">
                  Results shown are based on paper trading and/or personal bet tracking. Past performance does not
                  guarantee future results. Always bet responsibly.
                </div>
              </div>
            )
          })()}

          {showCsvModal && (
            <Modal title="Import Bets from CSV" onClose={() => setShowCsvModal(false)}>
              <CsvUpload
                onSuccess={() => { setShowCsvModal(false); queryClient.invalidateQueries({ queryKey: ['trackRecord'] }) }}
                onCancel={() => setShowCsvModal(false)}
              />
            </Modal>
          )}
          {showBetForm && (
            <Modal title="Add Bet Manually" onClose={() => setShowBetForm(false)}>
              <ManualBetForm
                onSuccess={() => { setShowBetForm(false); queryClient.invalidateQueries({ queryKey: ['trackRecord'] }) }}
                onCancel={() => setShowBetForm(false)}
              />
            </Modal>
          )}
        </>
      )}
    </main>
  )
}
