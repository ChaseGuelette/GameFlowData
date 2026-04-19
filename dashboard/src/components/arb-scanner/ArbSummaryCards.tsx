'use client'

import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'
import type { ArbSummary } from '@/types/arb-scanner'

interface ArbSummaryCardsProps {
  summary: ArbSummary
}

function formatDollars(dollars: number): string {
  const sign = dollars >= 0 ? '+' : ''
  return `${sign}$${Math.abs(dollars).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

export function ArbSummaryCards({ summary }: ArbSummaryCardsProps) {
  const supabase = createClient()

  const { data: todayOpps } = useQuery({
    queryKey: ['arb-opps-today'],
    queryFn: async () => {
      const since = new Date()
      since.setHours(since.getHours() - 24)
      const { data } = await supabase
        .from('arb_opportunities')
        .select('arb_type')
        .gte('detected_at', since.toISOString())
      return data ?? []
    },
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  })

  const pureCount = todayOpps?.filter((o) => o.arb_type === 'pure').length ?? 0
  const softCount = todayOpps?.filter((o) => o.arb_type === 'soft').length ?? 0
  const winRatePct = summary.win_rate * 100

  const cards = [
    {
      label: 'Total P&L',
      value: formatDollars(summary.total_pnl),
      color: summary.total_pnl >= 0 ? 'text-green-400' : 'text-red-400',
      sub: null,
    },
    {
      label: 'Win Rate',
      value: summary.total_resolved > 0 ? `${winRatePct.toFixed(1)}%` : '—',
      color: winRatePct >= 80 ? 'text-green-400' : winRatePct >= 60 ? 'text-yellow-400' : 'text-red-400',
      sub: summary.total_resolved > 0 ? `${summary.total_resolved} resolved` : null,
    },
    {
      label: 'Active Bets',
      value: `${summary.active_bets}`,
      color: 'text-slate-200',
      sub: `${summary.total_resolved} resolved`,
    },
    {
      label: 'Detected (24h)',
      value: todayOpps !== undefined ? `${pureCount + softCount}` : '…',
      color: 'text-slate-200',
      sub: todayOpps !== undefined ? `${pureCount} pure / ${softCount} soft` : null,
    },
  ]

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      {cards.map((card) => (
        <div
          key={card.label}
          className="rounded-lg border border-slate-700 bg-slate-800 p-4"
        >
          <div className="text-xs text-slate-400 mb-1">{card.label}</div>
          <div className={`text-xl font-semibold ${card.color}`}>
            {card.value}
          </div>
          {card.sub && (
            <div className="text-xs text-slate-500 mt-1">{card.sub}</div>
          )}
        </div>
      ))}
    </div>
  )
}
