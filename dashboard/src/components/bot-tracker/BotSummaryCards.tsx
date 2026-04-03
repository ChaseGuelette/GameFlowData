'use client'

import type { KalshiTradingStats, KalshiConfig } from '@/types/bot-tracker'

interface BotSummaryCardsProps {
  stats: KalshiTradingStats
  config: KalshiConfig
}

function formatDollars(cents: number): string {
  const dollars = cents / 100
  const sign = dollars >= 0 ? '+' : ''
  return `${sign}$${Math.abs(dollars).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

export function BotSummaryCards({ stats, config }: BotSummaryCardsProps) {
  const resolved = stats.trades_won + stats.trades_lost
  const winRate = resolved > 0 ? (stats.trades_won / resolved) * 100 : 0
  const pnlDollars = stats.total_pnl / 100
  const balance = config.starting_bankroll + pnlDollars

  const cards = [
    {
      label: 'Total P&L',
      value: formatDollars(stats.total_pnl),
      color: stats.total_pnl >= 0 ? 'text-green-400' : 'text-red-400',
    },
    {
      label: 'Win Rate',
      value: resolved > 0 ? `${winRate.toFixed(1)}%` : '—',
      color: winRate >= 55 ? 'text-green-400' : winRate >= 45 ? 'text-yellow-400' : 'text-red-400',
    },
    {
      label: 'Trades',
      value: `${stats.total_trades}`,
      sub: `${stats.trades_pending} pending`,
      color: 'text-slate-200',
    },
    {
      label: 'Balance',
      value: `$${balance.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      color: 'text-slate-200',
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
