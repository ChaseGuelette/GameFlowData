'use client'

import { useMemo } from 'react'
import type { KalshiLiveOrder, KalshiPaperBet, BotTab } from '@/types/bot-tracker'

interface PriceBucketTableProps {
  orders: (KalshiLiveOrder | KalshiPaperBet)[]
  tab: BotTab
}

interface BucketStats {
  label: string
  minPrice: number
  maxPrice: number
  bets: number
  won: number
  lost: number
  winRate: number | null
  breakEven: number   // midpoint of bucket = approximate break-even needed
  edge: number | null // winRate - breakEven (pp above/below break-even)
  pnl: number
  cost: number
  roi: number | null
}

const BUCKETS = [
  { label: '0–10¢',  min: 0,  max: 10,  mid: 5  },
  { label: '10–20¢', min: 10, max: 20,  mid: 15 },
  { label: '20–30¢', min: 20, max: 30,  mid: 25 },
  { label: '30–40¢', min: 30, max: 40,  mid: 35 },
  { label: '40–50¢', min: 40, max: 50,  mid: 45 },
  { label: '50¢+',   min: 50, max: 100, mid: 55 },
]

function getEntryPrice(order: KalshiLiveOrder | KalshiPaperBet, tab: BotTab): number | null {
  if (tab === 'live') {
    return (order as KalshiLiveOrder).fill_price
  }
  const p = (order as KalshiPaperBet).price
  if (p == null) return null
  return order.side === 'yes' ? p : 100 - p
}

function getCost(order: KalshiLiveOrder | KalshiPaperBet, tab: BotTab): number {
  if (tab === 'live') {
    return ((order as KalshiLiveOrder).total_cost ?? 0)
  }
  const p = (order as KalshiPaperBet).price ?? 0
  const entryPrice = order.side === 'yes' ? p : 100 - p
  return (entryPrice * order.contracts) / 100
}

export function PriceBucketTable({ orders, tab }: PriceBucketTableProps) {
  const bucketStats = useMemo<BucketStats[]>(() => {
    // Only resolved bets
    const resolved = orders.filter(
      (o) => (o.status === 'won' || o.status === 'lost') && !o.status.startsWith('overflow')
    )

    return BUCKETS.map(({ label, min, max, mid }) => {
      const inBucket = resolved.filter((o) => {
        const ep = getEntryPrice(o, tab)
        return ep != null && ep >= min && ep < max
      })

      const won = inBucket.filter((o) => o.status === 'won').length
      const lost = inBucket.filter((o) => o.status === 'lost').length
      const total = won + lost
      const winRate = total > 0 ? (won / total) * 100 : null
      const pnl = inBucket.reduce((sum, o) => sum + (Number(o.pnl) || 0), 0)
      const cost = inBucket.reduce((sum, o) => sum + getCost(o, tab), 0)
      const roi = cost > 0 ? (pnl / cost) * 100 : null
      const edge = winRate != null ? winRate - mid : null

      return { label, minPrice: min, maxPrice: max, bets: inBucket.length, won, lost, winRate, breakEven: mid, edge, pnl, cost, roi }
    }).filter((b) => b.bets > 0)
  }, [orders, tab])

  if (bucketStats.length === 0) return null

  return (
    <div className="rounded-lg border border-slate-700 bg-slate-800 overflow-hidden">
      <div className="px-4 py-3 border-b border-slate-700">
        <h3 className="text-sm font-semibold text-slate-200">Win Rate by Entry Price</h3>
        <p className="text-xs text-slate-500 mt-0.5">
          Each bucket's break-even win rate equals its midpoint price. Green = beating break-even.
        </p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-slate-400 bg-slate-900/50">
            <tr>
              <th className="px-4 py-2">Entry Price</th>
              <th className="px-4 py-2 text-right">Bets</th>
              <th className="px-4 py-2 text-right">Won</th>
              <th className="px-4 py-2 text-right">Win Rate</th>
              <th className="px-4 py-2 text-right">Break-even</th>
              <th className="px-4 py-2 text-right">Edge vs BE</th>
              <th className="px-4 py-2 text-right">P&L</th>
              <th className="px-4 py-2 text-right">ROI</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {bucketStats.map((b) => {
              const beating = b.edge != null && b.edge > 0
              const tinysample = b.bets < 10
              return (
                <tr key={b.label} className="hover:bg-slate-700/30">
                  <td className="px-4 py-2 font-medium text-slate-200">{b.label}</td>
                  <td className="px-4 py-2 text-right text-slate-300">
                    {b.bets}
                    {tinysample && (
                      <span className="ml-1 text-xs text-yellow-500" title="Small sample — results unreliable">⚠</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-right text-slate-300">{b.won}/{b.lost + b.won}</td>
                  <td className={`px-4 py-2 text-right font-medium ${beating ? 'text-green-400' : 'text-red-400'}`}>
                    {b.winRate != null ? `${b.winRate.toFixed(1)}%` : '—'}
                  </td>
                  <td className="px-4 py-2 text-right text-slate-400">~{b.breakEven}%</td>
                  <td className={`px-4 py-2 text-right font-medium ${
                    b.edge == null ? 'text-slate-500'
                    : b.edge > 0 ? 'text-green-400'
                    : 'text-red-400'
                  }`}>
                    {b.edge != null ? `${b.edge > 0 ? '+' : ''}${b.edge.toFixed(1)}pp` : '—'}
                  </td>
                  <td className={`px-4 py-2 text-right font-medium ${b.pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {b.pnl >= 0 ? '+' : ''}${Math.abs(b.pnl).toFixed(2)}
                  </td>
                  <td className={`px-4 py-2 text-right ${b.roi == null ? 'text-slate-500' : b.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {b.roi != null ? `${b.roi >= 0 ? '+' : ''}${b.roi.toFixed(0)}%` : '—'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
