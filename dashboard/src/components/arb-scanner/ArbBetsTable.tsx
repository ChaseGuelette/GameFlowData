'use client'

import { useState, useMemo } from 'react'
import type { ArbPaperBet } from '@/types/arb-scanner'

type SortField = 'placed_at' | 'net_margin' | 'pnl' | 'status'
type SortDir = 'asc' | 'desc'

interface ArbBetsTableProps {
  bets: ArbPaperBet[]
  loading: boolean
}

function statusBadge(status: ArbPaperBet['status']) {
  const colors: Record<string, string> = {
    placed: 'bg-blue-500/20 text-blue-400',
    resolved_profit: 'bg-green-500/20 text-green-400',
    resolved_loss: 'bg-red-500/20 text-red-400',
    cancelled: 'bg-slate-500/20 text-slate-400',
  }
  const labels: Record<string, string> = {
    placed: 'placed',
    resolved_profit: 'profit',
    resolved_loss: 'loss',
    cancelled: 'cancelled',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${colors[status] ?? colors.placed}`}>
      {labels[status] ?? status}
    </span>
  )
}

function formatPnl(dollars: number | null): string {
  if (dollars == null) return '—'
  const sign = dollars >= 0 ? '+' : ''
  return `${sign}$${Math.abs(dollars).toFixed(2)}`
}

function formatDate(iso: string): string {
  const d = new Date(iso)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

function getGameLabel(bet: ArbPaperBet): string {
  if (bet.team1 && bet.team2) return `${bet.team1} vs ${bet.team2}`
  if (bet.description) return bet.description
  return bet.kalshi_ticker
}

export function ArbBetsTable({ bets, loading }: ArbBetsTableProps) {
  const [sortField, setSortField] = useState<SortField>('placed_at')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const sortedBets = useMemo(() => {
    return [...bets].sort((a, b) => {
      let aVal: string | number | null = null
      let bVal: string | number | null = null
      switch (sortField) {
        case 'placed_at':
          aVal = a.placed_at ?? ''
          bVal = b.placed_at ?? ''
          break
        case 'net_margin':
          aVal = a.net_margin ?? 0
          bVal = b.net_margin ?? 0
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
  }, [bets, sortField, sortDir])

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

  if (loading) {
    return (
      <div className="rounded-lg border border-slate-700 bg-slate-800 p-8 text-center text-slate-400">
        Loading bets...
      </div>
    )
  }

  return (
    <div className="rounded-lg border border-slate-700 bg-slate-800 overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-700">
        <h3 className="text-sm font-semibold text-slate-200">Arb Paper Bets</h3>
        <span className="text-xs text-slate-500">
          {sortedBets.length} bet{sortedBets.length !== 1 ? 's' : ''}
        </span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-slate-400 bg-slate-900/50">
            <tr>
              <th
                className="px-3 py-2 cursor-pointer hover:text-slate-200"
                onClick={() => toggleSort('placed_at')}
              >
                Date{sortIcon('placed_at')}
              </th>
              <th className="px-3 py-2">Game</th>
              <th className="px-3 py-2">Market</th>
              <th className="px-3 py-2">Kalshi</th>
              <th className="px-3 py-2">Poly</th>
              <th
                className="px-3 py-2 cursor-pointer hover:text-slate-200"
                onClick={() => toggleSort('net_margin')}
              >
                Margin{sortIcon('net_margin')}
              </th>
              <th className="px-3 py-2 text-right">Contracts</th>
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
            {sortedBets.length === 0 ? (
              <tr>
                <td colSpan={9} className="px-3 py-8 text-center text-slate-500">
                  No bets found
                </td>
              </tr>
            ) : (
              sortedBets.map((bet) => (
                <tr key={bet.id} className="hover:bg-slate-700/30">
                  <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                    {formatDate(bet.placed_at)}
                  </td>
                  <td className="px-3 py-2 text-slate-200 max-w-[180px] truncate" title={getGameLabel(bet)}>
                    {getGameLabel(bet)}
                  </td>
                  <td className="px-3 py-2">
                    <span className="px-1.5 py-0.5 rounded text-xs bg-slate-700 text-slate-300">
                      {bet.market_type}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                    {bet.kalshi_side.toUpperCase()} @ {(bet.kalshi_price * 100).toFixed(0)}¢
                  </td>
                  <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                    {bet.poly_side.toUpperCase()} @ {(bet.poly_price * 100).toFixed(0)}¢
                  </td>
                  <td className="px-3 py-2 text-green-400 font-medium whitespace-nowrap">
                    +{(bet.net_margin * 100).toFixed(1)}¢
                  </td>
                  <td className="px-3 py-2 text-right text-slate-300">
                    {bet.kalshi_contracts}
                  </td>
                  <td className="px-3 py-2">
                    {statusBadge(bet.status)}
                  </td>
                  <td
                    className={`px-3 py-2 text-right font-medium ${
                      (bet.pnl ?? 0) > 0
                        ? 'text-green-400'
                        : (bet.pnl ?? 0) < 0
                          ? 'text-red-400'
                          : 'text-slate-400'
                    }`}
                  >
                    {formatPnl(bet.pnl != null ? Number(bet.pnl) : null)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
