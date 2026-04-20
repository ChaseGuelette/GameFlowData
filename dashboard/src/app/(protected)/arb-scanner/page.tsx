'use client'

import { useState } from 'react'
import { useAdmin } from '@/lib/hooks/useAdmin'
import { useArbSummary, useArbBets, useArbDailyLogs, useMatchQueue } from '@/lib/hooks/useArbScanner'
import { ArbSummaryCards } from '@/components/arb-scanner/ArbSummaryCards'
import { ArbBetsTable } from '@/components/arb-scanner/ArbBetsTable'
import { ArbDailyLogTable } from '@/components/arb-scanner/ArbDailyLogTable'
import { MatchQueueTable } from '@/components/arb-scanner/MatchQueueTable'
import type { ArbDateRange, ArbTab } from '@/types/arb-scanner'

const DATE_RANGES: { label: string; value: ArbDateRange }[] = [
  { label: 'Today', value: 'today' },
  { label: '7d', value: '7d' },
  { label: '30d', value: '30d' },
  { label: 'All', value: 'all' },
]

export default function ArbScannerPage() {
  const { isAdmin } = useAdmin()
  const [tab, setTab] = useState<ArbTab>('bets')
  const [dateRange, setDateRange] = useState<ArbDateRange>('7d')

  const { data: summary, isLoading: summaryLoading } = useArbSummary()
  const { data: bets = [], isLoading: betsLoading } = useArbBets(dateRange)
  const { data: dailyLogs = [], isLoading: logsLoading } = useArbDailyLogs(dateRange)
  const { links: queueLinks, loading: queueLoading, decide } = useMatchQueue()

  if (!isAdmin) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="text-slate-400 text-sm">Access denied</div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Arb Scanner</h1>
        <p className="text-sm text-slate-400 mt-1">Kalshi-Polymarket Paper Trading</p>
      </div>

      {/* Summary Cards */}
      {summaryLoading ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="rounded-lg border border-slate-700 bg-slate-800 p-4 h-20 animate-pulse" />
          ))}
        </div>
      ) : (
        summary && <ArbSummaryCards summary={summary} />
      )}

      {/* Tab Toggle + Date Range */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex bg-slate-900 rounded-lg p-0.5">
          {(['bets', 'daily-log', 'queue'] as ArbTab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-4 py-1.5 text-sm font-medium rounded-md transition-colors ${
                tab === t
                  ? 'bg-blue-600 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              {t === 'bets' ? 'Paper Bets' : t === 'daily-log' ? 'Daily Log' : (
                <span className="flex items-center gap-1.5">
                  Queue
                  {queueLinks.length > 0 && (
                    <span className="text-xs bg-yellow-500/20 text-yellow-400 px-1.5 py-0.5 rounded-full font-normal">
                      {queueLinks.length}
                    </span>
                  )}
                </span>
              )}
            </button>
          ))}
        </div>
        <div className="flex bg-slate-900 rounded-lg p-0.5">
          {DATE_RANGES.map((r) => (
            <button
              key={r.value}
              onClick={() => setDateRange(r.value)}
              className={`px-3 py-1 text-xs font-medium rounded-md transition-colors ${
                dateRange === r.value
                  ? 'bg-slate-700 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {/* Tab Content */}
      {tab === 'bets' ? (
        <ArbBetsTable bets={bets} loading={betsLoading} />
      ) : tab === 'daily-log' ? (
        <ArbDailyLogTable logs={dailyLogs} loading={logsLoading} />
      ) : (
        <MatchQueueTable links={queueLinks} loading={queueLoading} decide={decide} />
      )}
    </div>
  )
}
