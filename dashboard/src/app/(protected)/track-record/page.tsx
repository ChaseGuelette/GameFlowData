'use client'

import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { KPICard } from '@/components/performance/KPICard'
import { BankrollChart } from '@/components/performance/BankrollChart'
import { StatBreakdown } from '@/components/performance/StatBreakdown'
import { MonthlyGrid } from '@/components/track-record/MonthlyGrid'
import { ModelMetrics } from '@/components/track-record/ModelMetrics'
import { CsvUpload } from '@/components/track-record/CsvUpload'
import { ManualBetForm } from '@/components/track-record/ManualBetForm'
import { useTrackRecordData, type TrackRecordSource } from '@/lib/hooks/useTrackRecordData'

// ── Modal wrapper ─────────────────────────────────────────────────────────────

function Modal({ title, children, onClose }: { title: string; children: React.ReactNode; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-slate-800 border border-slate-700 rounded-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto shadow-2xl">
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700">
          <h2 className="text-base font-semibold text-slate-50">{title}</h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-200 transition-colors"
          >
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

// ── Source toggle ─────────────────────────────────────────────────────────────

const SOURCES: { value: TrackRecordSource; label: string }[] = [
  { value: 'my_bets',  label: 'My Bets' },
  { value: 'paper',    label: 'Paper Trading' },
  { value: 'combined', label: 'Combined' },
]

// ── Page ──────────────────────────────────────────────────────────────────────

export default function TrackRecordPage() {
  const [source, setSource] = useState<TrackRecordSource>('my_bets')
  const [showCsvModal, setShowCsvModal] = useState(false)
  const [showBetForm, setShowBetForm] = useState(false)
  const queryClient = useQueryClient()

  const { data, isLoading, error } = useTrackRecordData(source)

  const handleImportSuccess = () => {
    setShowCsvModal(false)
    queryClient.invalidateQueries({ queryKey: ['trackRecord'] })
  }

  const handleBetSuccess = () => {
    setShowBetForm(false)
    queryClient.invalidateQueries({ queryKey: ['trackRecord'] })
  }

  const kpis = data?.kpis
  const formatPnl = (v: number) => `${v >= 0 ? '+' : ''}$${Math.abs(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
  const formatRoi = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`

  return (
    <>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-6">

        {/* ── Header ────────────────────────────────────────────────── */}
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-slate-50">Track Record</h1>
            <p className="text-sm text-slate-400 mt-0.5">Your verified betting history and performance</p>
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            {/* Import CSV button */}
            <button
              onClick={() => setShowCsvModal(true)}
              className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium bg-slate-700 hover:bg-slate-600 text-slate-200 transition-colors"
            >
              <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
              </svg>
              Import CSV
            </button>

            {/* Add Bet button */}
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

        {/* ── Source toggle ──────────────────────────────────────────── */}
        <div className="flex items-center gap-1 bg-slate-900 rounded-lg p-1 w-fit">
          {SOURCES.map(s => (
            <button
              key={s.value}
              onClick={() => setSource(s.value)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                source === s.value
                  ? 'bg-blue-600 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>

        {/* ── Loading / Error ───────────────────────────────────────── */}
        {isLoading && (
          <div className="text-center py-16">
            <div className="animate-spin h-8 w-8 border-2 border-blue-500 border-t-transparent rounded-full mx-auto mb-3" />
            <div className="text-sm text-slate-400">Loading track record...</div>
          </div>
        )}

        {error && (
          <div className="bg-red-900/30 border border-red-700 rounded-lg px-5 py-4 text-sm text-red-300">
            Failed to load data: {error instanceof Error ? error.message : 'Unknown error'}
          </div>
        )}

        {data && !isLoading && (
          <>
            {/* ── KPI Banner ──────────────────────────────────────────── */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <KPICard
                label="Total P&L"
                value={formatPnl(kpis!.totalPnl)}
                trend={kpis!.totalPnl >= 0 ? 'up' : 'down'}
              />
              <KPICard
                label="ROI"
                value={formatRoi(kpis!.roi)}
                trend={kpis!.roi >= 0 ? 'up' : 'down'}
              />
              <KPICard
                label="Win Rate"
                value={kpis!.wins + kpis!.losses > 0
                  ? `${((kpis!.wins / (kpis!.wins + kpis!.losses)) * 100).toFixed(1)}%`
                  : '—'
                }
                subValue={`${kpis!.wins}W-${kpis!.losses}L${kpis!.pushes > 0 ? `-${kpis!.pushes}P` : ''}`}
                trend={kpis!.wins > kpis!.losses ? 'up' : kpis!.wins < kpis!.losses ? 'down' : 'neutral'}
              />
              <KPICard
                label="Profitable Months"
                value={`${kpis!.profitableMonths} of ${kpis!.totalMonths}`}
                trend={kpis!.profitableMonths > kpis!.totalMonths / 2 ? 'up' : 'neutral'}
              />
            </div>

            {/* ── Bankroll Chart ──────────────────────────────────────── */}
            <BankrollChart data={data.dailyLog} />

            {/* ── Monthly Grid ────────────────────────────────────────── */}
            <div>
              <h2 className="text-lg font-semibold text-slate-50 mb-3">Monthly Summary</h2>
              <MonthlyGrid
                months={data.monthlyAggregates}
                dailyLog={data.dailyLog}
                bets={data.bets}
              />
            </div>

            {/* ── Stat Breakdown ──────────────────────────────────────── */}
            <StatBreakdown stats={data.statBreakdown} />

            {/* ── Model Metrics (paper / combined only) ───────────────── */}
            {(source === 'paper' || source === 'combined') && data.bets.length > 0 && (
              <ModelMetrics bets={data.bets} daily={data.dailyLog} />
            )}

            {/* ── Disclaimer ──────────────────────────────────────────── */}
            <div className="text-xs text-slate-600 text-center pb-4 max-w-2xl mx-auto">
              Results shown are based on paper trading and/or personal bet tracking. Past performance does not
              guarantee future results. Always bet responsibly.
            </div>
          </>
        )}
      </div>

      {/* ── Modals ─────────────────────────────────────────────────── */}
      {showCsvModal && (
        <Modal title="Import Bets from CSV" onClose={() => setShowCsvModal(false)}>
          <CsvUpload
            onSuccess={handleImportSuccess}
            onCancel={() => setShowCsvModal(false)}
          />
        </Modal>
      )}

      {showBetForm && (
        <Modal title="Add Bet Manually" onClose={() => setShowBetForm(false)}>
          <ManualBetForm
            onSuccess={handleBetSuccess}
            onCancel={() => setShowBetForm(false)}
          />
        </Modal>
      )}
    </>
  )
}
