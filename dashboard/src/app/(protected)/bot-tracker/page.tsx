'use client'

import { useState } from 'react'
import { useBotSummary, useBotOrders, useBotDailyLogs } from '@/lib/hooks/useBotTracker'
import { TradeApprovalPanel } from '@/components/bot-tracker/TradeApprovalPanel'
import { CircuitBreakerCard } from '@/components/bot-tracker/CircuitBreakerCard'
import { BotSummaryCards } from '@/components/bot-tracker/BotSummaryCards'
import { BotOrdersTable } from '@/components/bot-tracker/BotOrdersTable'
import { DailyPnlTable } from '@/components/bot-tracker/DailyPnlTable'
import { PriceBucketTable } from '@/components/bot-tracker/PriceBucketTable'
import type { DateRange, BotTab } from '@/types/bot-tracker'

const DATE_RANGES: { label: string; value: DateRange }[] = [
  { label: 'Today', value: 'today' },
  { label: '7d', value: '7d' },
  { label: '30d', value: '30d' },
  { label: 'All', value: 'all' },
]

export default function BotTrackerPage() {
  const [tab, setTab] = useState<BotTab>('live')
  const [dateRange, setDateRange] = useState<DateRange>('7d')

  const { data: summary, isLoading: summaryLoading } = useBotSummary()
  const { data: orders = [], isLoading: ordersLoading } = useBotOrders(tab, dateRange)
  const { data: dailyLogs = [], isLoading: logsLoading } = useBotDailyLogs(tab, dateRange)

  const stats = summary ? (tab === 'live' ? summary.live : summary.paper) : null

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-slate-100">Bot Tracker</h1>

      {/* Trade Approval Queue (shows only when trades pending) */}
      {tab === 'live' && <TradeApprovalPanel />}

      {/* Circuit Breaker */}
      {summary && <CircuitBreakerCard config={summary.config} />}

      {/* Summary Cards */}
      {summaryLoading ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="rounded-lg border border-slate-700 bg-slate-800 p-4 h-20 animate-pulse" />
          ))}
        </div>
      ) : (
        stats && summary && <BotSummaryCards stats={stats} config={summary.config} />
      )}

      {/* Tab Toggle + Date Range */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex bg-slate-900 rounded-lg p-0.5">
          {(['live', 'paper'] as BotTab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-4 py-1.5 text-sm font-medium rounded-md transition-colors ${
                tab === t
                  ? 'bg-blue-600 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              {t === 'live' ? 'Live Orders' : 'Paper Bets'}
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

      {/* Orders Table */}
      <BotOrdersTable orders={orders} tab={tab} loading={ordersLoading} />

      {/* Price Bucket Win Rate */}
      {!ordersLoading && <PriceBucketTable orders={orders} tab={tab} />}

      {/* Daily P&L Table */}
      <DailyPnlTable logs={dailyLogs} loading={logsLoading} />
    </div>
  )
}
