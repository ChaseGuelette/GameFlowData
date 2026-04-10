'use client'

import { useState, useMemo } from 'react'
import type { KalshiLiveOrder, KalshiPaperBet, BotTab } from '@/types/bot-tracker'
import { KALSHI_STAT_LABELS } from '@/types/bot-tracker'

type SortField = 'placed_at' | 'player_name' | 'stat_type' | 'edge' | 'pnl' | 'status'
type SortDir = 'asc' | 'desc'

interface BotOrdersTableProps {
  orders: (KalshiLiveOrder | KalshiPaperBet)[]
  tab: BotTab
  loading: boolean
}

function statusBadge(status: string | null) {
  const s = status ?? 'pending'
  const colors: Record<string, string> = {
    won: 'bg-green-500/20 text-green-400',
    lost: 'bg-red-500/20 text-red-400',
    pending: 'bg-yellow-500/20 text-yellow-400',
    cancelled: 'bg-slate-500/20 text-slate-400',
    filled: 'bg-blue-500/20 text-blue-400',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${colors[s] ?? colors.pending}`}>
      {s}
    </span>
  )
}

function formatPnl(dollars: number | null): string {
  if (dollars == null) return '—'
  const sign = dollars >= 0 ? '+' : ''
  return `${sign}$${Math.abs(dollars).toFixed(2)}`
}

export function BotOrdersTable({ orders, tab, loading }: BotOrdersTableProps) {
  const [sortField, setSortField] = useState<SortField>('placed_at')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [statFilter, setStatFilter] = useState<string>('all')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const [showOverflow, setShowOverflow] = useState<boolean>(false)

  const statTypes = useMemo(() => {
    const types = new Set(orders.map((o) => o.stat_type))
    return Array.from(types).sort()
  }, [orders])

  const sortedOrders = useMemo(() => {
    let filtered = orders
    if (!showOverflow) {
      filtered = filtered.filter((o) => !(o.status ?? '').startsWith('overflow'))
    }
    if (statFilter !== 'all') {
      filtered = filtered.filter((o) => o.stat_type === statFilter)
    }
    if (statusFilter !== 'all') {
      filtered = filtered.filter((o) => (o.status ?? 'pending') === statusFilter)
    }
    return [...filtered].sort((a, b) => {
      let aVal: string | number | null = null
      let bVal: string | number | null = null
      switch (sortField) {
        case 'placed_at':
          aVal = a.placed_at ?? ''
          bVal = b.placed_at ?? ''
          break
        case 'player_name':
          aVal = a.player_name ?? ''
          bVal = b.player_name ?? ''
          break
        case 'stat_type':
          aVal = a.stat_type
          bVal = b.stat_type
          break
        case 'edge':
          aVal = a.fee_adjusted_edge ?? 0
          bVal = b.fee_adjusted_edge ?? 0
          break
        case 'pnl':
          aVal = a.pnl ?? 0
          bVal = b.pnl ?? 0
          break
        case 'status':
          aVal = a.status ?? ''
          bVal = b.status ?? ''
          break
      }
      if (aVal == null && bVal == null) return 0
      if (aVal == null) return 1
      if (bVal == null) return -1
      if (aVal < bVal) return sortDir === 'asc' ? -1 : 1
      if (aVal > bVal) return sortDir === 'asc' ? 1 : -1
      return 0
    })
  }, [orders, sortField, sortDir, statFilter, statusFilter, showOverflow])

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortField(field)
      setSortDir('desc')
    }
  }

  const sortIcon = (field: SortField) => {
    if (sortField !== field) return ''
    return sortDir === 'asc' ? ' ▲' : ' ▼'
  }

  const isLive = tab === 'live'

  if (loading) {
    return (
      <div className="rounded-lg border border-slate-700 bg-slate-800 p-8 text-center text-slate-400">
        Loading orders...
      </div>
    )
  }

  return (
    <div className="rounded-lg border border-slate-700 bg-slate-800 overflow-hidden">
      {/* Filters */}
      <div className="flex flex-wrap gap-3 p-3 border-b border-slate-700">
        <select
          value={statFilter}
          onChange={(e) => setStatFilter(e.target.value)}
          className="bg-slate-900 border border-slate-600 rounded px-2 py-1 text-xs text-slate-300"
        >
          <option value="all">All Stats</option>
          {statTypes.map((s) => (
            <option key={s} value={s}>
              {KALSHI_STAT_LABELS[s] ?? s}
            </option>
          ))}
        </select>
        <select
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          className="bg-slate-900 border border-slate-600 rounded px-2 py-1 text-xs text-slate-300"
        >
          <option value="all">All Status</option>
          <option value="won">Won</option>
          <option value="lost">Lost</option>
          <option value="pending">Pending</option>
          <option value="cancelled">Cancelled</option>
          {showOverflow && (
            <>
              <option value="overflow_won">Overflow Won</option>
              <option value="overflow_lost">Overflow Lost</option>
              <option value="overflow_cancelled">Overflow Cancelled</option>
            </>
          )}
        </select>
        <label className="flex items-center gap-1.5 text-xs text-slate-400 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={showOverflow}
            onChange={(e) => {
              setShowOverflow(e.target.checked)
              if (!e.target.checked && statusFilter.startsWith('overflow')) {
                setStatusFilter('all')
              }
            }}
            className="accent-blue-500"
          />
          Show overflow
        </label>
        <span className="text-xs text-slate-500 self-center ml-auto">
          {sortedOrders.length} order{sortedOrders.length !== 1 ? 's' : ''}
        </span>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-slate-400 bg-slate-900/50">
            <tr>
              <th className="px-3 py-2">Date</th>
              <th className="px-3 py-2">Sport</th>
              <th
                className="px-3 py-2 cursor-pointer hover:text-slate-200"
                onClick={() => toggleSort('player_name')}
              >
                Player{sortIcon('player_name')}
              </th>
              <th
                className="px-3 py-2 cursor-pointer hover:text-slate-200"
                onClick={() => toggleSort('stat_type')}
              >
                Stat{sortIcon('stat_type')}
              </th>
              <th className="px-3 py-2">Line</th>
              <th className="px-3 py-2">Side</th>
              <th className="px-3 py-2">Contracts</th>
              <th className="px-3 py-2">{isLive ? 'Fill' : 'Price'}</th>
              <th
                className="px-3 py-2 cursor-pointer hover:text-slate-200"
                onClick={() => toggleSort('edge')}
              >
                Edge{sortIcon('edge')}
              </th>
              <th
                className="px-3 py-2 cursor-pointer hover:text-slate-200"
                onClick={() => toggleSort('status')}
              >
                Status{sortIcon('status')}
              </th>
              <th
                className="px-3 py-2 cursor-pointer hover:text-slate-200 text-right"
                onClick={() => toggleSort('pnl')}
              >
                P&L{sortIcon('pnl')}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {sortedOrders.length === 0 ? (
              <tr>
                <td colSpan={11} className="px-3 py-8 text-center text-slate-500">
                  No orders found
                </td>
              </tr>
            ) : (
              sortedOrders.map((order) => {
                // For live orders fill_price is already the side-adjusted fill.
                // For paper bets, `price` stores the YES/market price; the actual
                // cost per contract depends on side: YES pays price¢, NO pays (100-price)¢.
                const rawPrice = isLive
                  ? (order as KalshiLiveOrder).fill_price
                  : (order as KalshiPaperBet).price
                const entryPrice = isLive
                  ? rawPrice
                  : rawPrice != null
                    ? order.side === 'yes' ? rawPrice : 100 - rawPrice
                    : null
                const isOverflow = (order.status ?? '').startsWith('overflow')
                return (
                  <tr key={order.id} className={`hover:bg-slate-700/30 ${isOverflow ? 'opacity-40' : ''}`}>
                    <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                      {order.game_date}
                    </td>
                    <td className="px-3 py-2">
                      <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
                        order.sport === 'nba'
                          ? 'bg-orange-500/20 text-orange-400'
                          : 'bg-blue-500/20 text-blue-400'
                      }`}>
                        {order.sport.toUpperCase()}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-slate-200 font-medium">
                      {order.player_name ?? '—'}
                    </td>
                    <td className="px-3 py-2 text-slate-300">
                      {KALSHI_STAT_LABELS[order.stat_type] ?? order.stat_type}
                    </td>
                    <td className="px-3 py-2 text-slate-300">{Number(order.line)}</td>
                    <td className="px-3 py-2">
                      <span
                        className={`text-xs font-medium ${
                          order.side === 'yes' ? 'text-green-400' : 'text-red-400'
                        }`}
                      >
                        {order.side.toUpperCase()}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-slate-300">{order.contracts}</td>
                    <td className="px-3 py-2 text-slate-300">
                      {entryPrice != null ? `${entryPrice}¢` : '—'}
                    </td>
                    <td className="px-3 py-2 text-slate-300">
                      {order.fee_adjusted_edge != null
                        ? `${(Number(order.fee_adjusted_edge) * 100).toFixed(1)}%`
                        : '—'}
                    </td>
                    <td className="px-3 py-2">{statusBadge(order.status)}</td>
                    <td
                      className={`px-3 py-2 text-right font-medium ${
                        (order.pnl ?? 0) > 0
                          ? 'text-green-400'
                          : (order.pnl ?? 0) < 0
                            ? 'text-red-400'
                            : 'text-slate-400'
                      }`}
                    >
                      {formatPnl(order.pnl != null ? Number(order.pnl) : null)}
                    </td>
                  </tr>
                )
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
