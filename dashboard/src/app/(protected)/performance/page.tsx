'use client'

import { useEffect, useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { KPICard } from '@/components/performance/KPICard'
import { BankrollChart } from '@/components/performance/BankrollChart'
import { StatBreakdown } from '@/components/performance/StatBreakdown'
import { MonthlyGrid } from '@/components/track-record/MonthlyGrid'
import { ModelMetrics } from '@/components/track-record/ModelMetrics'
import { CsvUpload } from '@/components/track-record/CsvUpload'
import { ManualBetForm } from '@/components/track-record/ManualBetForm'
import { HistoryPanel } from '@/components/performance/HistoryPanel'
import { DateRangeFilter, getDefaultEndDate, getDefaultStartDate } from '@/components/shared/DateRangeFilter'
import { useTrackRecordData, type TrackRecordSource } from '@/lib/hooks/useTrackRecordData'
import { BetSourceFilter, type BetSource } from '@/components/shared/BetSourceFilter'
import { cn } from '@/lib/utils'
import { type DailyPerformance, type PaperBet, type StatPerformance, type StatType } from '@/types/predictions'
import { useUserPreferences } from '@/lib/hooks/useUserPreferences'
import { usePropsPerformance, useMyBetsPerformance } from '@/lib/hooks/usePerformanceData'

type PerformanceTab = 'props' | 'record' | 'history'

interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
}

const TRACK_SOURCES: { value: TrackRecordSource; label: string }[] = [
  { value: 'my_bets', label: 'My Bets' },
  { value: 'paper', label: 'Paper Trading' },
  { value: 'combined', label: 'Combined' },
]

function Modal({ title, children, onClose }: { title: string; children: React.ReactNode; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-slate-800 border border-slate-700 rounded-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto shadow-2xl">
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700">
          <h2 className="text-base font-semibold text-slate-50">{title}</h2>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-200 transition-colors">&times;</button>
        </div>
        <div className="p-5">{children}</div>
      </div>
    </div>
  )
}

function buildChartData(bets: PaperBet[], initialBankroll: number): DailyPerformance[] {
  const dailyPnl = new Map<string, number>()
  for (const bet of bets) dailyPnl.set(bet.game_date, (dailyPnl.get(bet.game_date) ?? 0) + (bet.pnl ?? 0))
  let cumulative = 0
  return [...dailyPnl.keys()].sort().map(gameDate => {
    const pnl = dailyPnl.get(gameDate) ?? 0
    cumulative += pnl
    return {
      game_date: gameDate,
      total_pnl: pnl,
      cumulative_pnl: cumulative,
      bankroll_after: initialBankroll + cumulative,
      total_bets: 0,
      bets_won: 0,
      bets_lost: 0,
      total_staked: 0,
      roi_pct: 0,
    } as DailyPerformance
  })
}

function buildStatData(bets: PaperBet[]): StatPerformance[] {
  const stats = new Map<StatType, { wins: number; losses: number; pnl: number; staked: number }>()
  for (const bet of bets) {
    const row = stats.get(bet.stat_type) ?? { wins: 0, losses: 0, pnl: 0, staked: 0 }
    if (bet.status === 'won') row.wins++
    if (bet.status === 'lost') row.losses++
    row.pnl += bet.pnl ?? 0
    row.staked += bet.stake ?? 0
    stats.set(bet.stat_type, row)
  }
  return [...stats.entries()].map(([stat, row]) => {
    const total = row.wins + row.losses
    return {
      stat,
      total_bets: total,
      wins: row.wins,
      losses: row.losses,
      win_rate: total > 0 ? (row.wins / total) * 100 : 0,
      total_pnl: row.pnl,
      roi: row.staked > 0 ? (row.pnl / row.staked) * 100 : 0,
    }
  }).sort((a, b) => b.total_bets - a.total_bets)
}

export default function PerformancePage() {
  const { prefs } = useUserPreferences()
  const queryClient = useQueryClient()
  const [betSource, setBetSource] = useState<BetSource>('my_bets')
  const [activeTab, setActiveTab] = useState<PerformanceTab>('props')
  const [trackSource, setTrackSource] = useState<TrackRecordSource>('my_bets')
  const [showCsvModal, setShowCsvModal] = useState(false)
  const [showBetForm, setShowBetForm] = useState(false)
  const [startDate, setStartDate] = useState(getDefaultStartDate)
  const [endDate, setEndDate] = useState(getDefaultEndDate)
  const validRange = Boolean(startDate && endDate && startDate <= endDate)

  useEffect(() => {
    const requestedTab = new URLSearchParams(window.location.search).get('tab')
    if (requestedTab === 'history') setActiveTab('history')
    if (requestedTab === 'my_bets' || requestedTab === 'dfs') {
      setActiveTab('props')
      setBetSource('my_bets')
    }
  }, [])

  const propsQuery = usePropsPerformance(activeTab === 'props' && betSource === 'model')
  const myBetsQuery = useMyBetsPerformance(startDate, endDate, activeTab === 'props' && betSource === 'my_bets')
  const trackQuery = useTrackRecordData(trackSource, activeTab === 'record')

  const modelBets = ((propsQuery.data?.allBets ?? []) as PaperBetWithRecommended[]).filter(bet => bet.is_recommended === true)
  const personalBets = validRange ? (myBetsQuery.data ?? []) : []
  const displayedBets = betSource === 'model' ? modelBets : personalBets
  const activeQuery = betSource === 'model' ? propsQuery : myBetsQuery
  const canShowAnalytics = betSource === 'model' || validRange

  const kpis = useMemo(() => {
    const wins = displayedBets.filter(bet => bet.status === 'won').length
    const losses = displayedBets.filter(bet => bet.status === 'lost').length
    const pnl = displayedBets.reduce((sum, bet) => sum + (bet.pnl ?? 0), 0)
    const staked = displayedBets.reduce((sum, bet) => sum + (bet.stake ?? 0), 0)
    return {
      wins,
      losses,
      pnl,
      roi: staked > 0 ? (pnl / staked) * 100 : 0,
      winRate: wins + losses > 0 ? (wins / (wins + losses)) * 100 : 0,
    }
  }, [displayedBets])

  const chartData = useMemo(() => buildChartData(displayedBets, prefs.initialBankroll), [displayedBets, prefs.initialBankroll])
  const statData = useMemo(() => buildStatData(displayedBets), [displayedBets])
  const displayBankroll = prefs.initialBankroll + kpis.pnl

  return (
    <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-50">Performance</h1>
          <p className="text-slate-400">
            {activeTab === 'props'
              ? betSource === 'model' ? 'Automatic model picks' : 'Your saved real and paper bets'
              : activeTab === 'record' ? 'Your verified betting history and performance' : 'Personal bets and model results'}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg border border-slate-700">
            {(['props', 'record', 'history'] as PerformanceTab[]).map(tab => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={cn(
                  'px-3 py-1.5 rounded text-sm font-medium transition-colors capitalize',
                  activeTab === tab ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-slate-200'
                )}
              >
                {tab}
              </button>
            ))}
          </div>
          {activeTab === 'props' && <BetSourceFilter activeSource={betSource} onSourceChange={setBetSource} />}
        </div>
      </div>

      {activeTab === 'props' && (
        <div className="space-y-6">
          {betSource === 'my_bets' && (
            <>
              <DateRangeFilter startDate={startDate} endDate={endDate} onStartDateChange={setStartDate} onEndDateChange={setEndDate} />
              <p className="text-xs text-slate-500">Includes personally saved real bets and paper trades.</p>
              {!validRange && (
                <div className="bg-red-900/30 border border-red-700 rounded-lg px-5 py-4 text-sm text-red-300">
                  Start date must be on or before end date.
                </div>
              )}
            </>
          )}
          {canShowAnalytics && activeQuery.isLoading ? (
            <div className="text-center py-16 text-slate-400">Loading performance data...</div>
          ) : canShowAnalytics && activeQuery.error ? (
            <div className="bg-red-900/30 border border-red-700 rounded-lg px-5 py-4 text-sm text-red-300">
              Failed to load performance: {activeQuery.error instanceof Error ? activeQuery.error.message : 'Unknown error'}
            </div>
          ) : canShowAnalytics && displayedBets.length === 0 ? (
            <div className="text-center py-16">
              <p className="text-slate-400 text-lg">No resolved bets in this date range</p>
              <p className="text-slate-500 text-sm mt-2">Pending bets remain available in History.</p>
            </div>
          ) : canShowAnalytics ? (
            <>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <KPICard label={betSource === 'model' ? 'Model P&L Bankroll' : 'My Bankroll'} value={`$${displayBankroll.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`} />
                <KPICard label="Total P&L" value={`${kpis.pnl >= 0 ? '+' : ''}$${kpis.pnl.toFixed(2)}`} trend={kpis.pnl >= 0 ? 'up' : 'down'} />
                <KPICard label="Overall ROI" value={`${kpis.roi >= 0 ? '+' : ''}${kpis.roi.toFixed(1)}%`} trend={kpis.roi >= 0 ? 'up' : 'down'} />
                <KPICard label="Win Rate" value={`${kpis.winRate.toFixed(1)}%`} subValue={`${kpis.wins}W - ${kpis.losses}L`} />
              </div>
              <BankrollChart data={chartData} />
              <StatBreakdown stats={statData} />
            </>
          ) : null}
        </div>
      )}

      {activeTab === 'record' && (
        <>
          <div className="flex flex-wrap items-center justify-between gap-4 mb-6">
            <div className="flex items-center gap-1 bg-slate-900 rounded-lg p-1 w-fit">
              {TRACK_SOURCES.map(source => (
                <button key={source.value} onClick={() => setTrackSource(source.value)} className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${trackSource === source.value ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}>
                  {source.label}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-2">
              <button onClick={() => setShowCsvModal(true)} className="px-3 py-2 rounded-lg text-sm font-medium bg-slate-700 hover:bg-slate-600 text-slate-200 transition-colors">Import CSV</button>
              <button onClick={() => setShowBetForm(true)} className="px-3 py-2 rounded-lg text-sm font-medium bg-blue-600 hover:bg-blue-700 text-white transition-colors">Add Bet</button>
            </div>
          </div>
          {trackQuery.isLoading && <div className="text-center py-16 text-sm text-slate-400">Loading track record...</div>}
          {trackQuery.error && <div className="bg-red-900/30 border border-red-700 rounded-lg px-5 py-4 text-sm text-red-300">Failed to load data: {trackQuery.error instanceof Error ? trackQuery.error.message : 'Unknown error'}</div>}
          {trackQuery.data && !trackQuery.isLoading && (() => {
            const data = trackQuery.data
            const kpis = data.kpis
            return (
              <div className="space-y-6">
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                  <KPICard label="Total P&L" value={`${kpis.totalPnl >= 0 ? '+' : ''}$${Math.abs(kpis.totalPnl).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`} trend={kpis.totalPnl >= 0 ? 'up' : 'down'} />
                  <KPICard label="ROI" value={`${kpis.roi >= 0 ? '+' : ''}${kpis.roi.toFixed(1)}%`} trend={kpis.roi >= 0 ? 'up' : 'down'} />
                  <KPICard label="Win Rate" value={kpis.wins + kpis.losses > 0 ? `${((kpis.wins / (kpis.wins + kpis.losses)) * 100).toFixed(1)}%` : '—'} subValue={`${kpis.wins}W-${kpis.losses}L${kpis.pushes > 0 ? `-${kpis.pushes}P` : ''}`} />
                  <KPICard label="Profitable Months" value={`${kpis.profitableMonths} of ${kpis.totalMonths}`} />
                </div>
                <BankrollChart data={data.dailyLog} />
                <div><h2 className="text-lg font-semibold text-slate-50 mb-3">Monthly Summary</h2><MonthlyGrid months={data.monthlyAggregates} dailyLog={data.dailyLog} bets={data.bets} /></div>
                <StatBreakdown stats={data.statBreakdown} />
                {(trackSource === 'paper' || trackSource === 'combined') && data.bets.length > 0 && <ModelMetrics bets={data.bets} daily={data.dailyLog} />}
              </div>
            )
          })()}
          {showCsvModal && <Modal title="Import Bets from CSV" onClose={() => setShowCsvModal(false)}><CsvUpload onSuccess={() => { setShowCsvModal(false); queryClient.invalidateQueries({ queryKey: ['trackRecord'] }) }} onCancel={() => setShowCsvModal(false)} /></Modal>}
          {showBetForm && <Modal title="Add Bet Manually" onClose={() => setShowBetForm(false)}><ManualBetForm onSuccess={() => { setShowBetForm(false); queryClient.invalidateQueries({ queryKey: ['trackRecord'] }) }} onCancel={() => setShowBetForm(false)} /></Modal>}
        </>
      )}

      {activeTab === 'history' && <HistoryPanel />}
    </main>
  )
}