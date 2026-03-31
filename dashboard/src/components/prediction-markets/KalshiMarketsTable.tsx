'use client'

import { useState, useMemo } from 'react'
import type { KalshiMarket } from '@/types/kalshi'
import { KALSHI_STAT_LABELS } from '@/types/kalshi'
import { KalshiCountdown } from './KalshiCountdown'
import { KalshiMarketDetail } from './KalshiMarketDetail'

type SortKey = 'edge' | 'volume' | 'close_time' | 'line' | 'spread'

interface KalshiMarketsTableProps {
  markets: KalshiMarket[]
}

export function KalshiMarketsTable({ markets }: KalshiMarketsTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>('edge')
  const [sortAsc, setSortAsc] = useState(false)
  const [statFilter, setStatFilter] = useState<string>('all')
  const [minEdge, setMinEdge] = useState<number>(0)
  const [minVolume, setMinVolume] = useState<number>(0)
  const [selectedMarket, setSelectedMarket] = useState<KalshiMarket | null>(null)

  // Get unique stat types for filter dropdown
  const statTypes = useMemo(() => {
    const types = new Set(markets.map((m) => m.stat_type))
    return ['all', ...Array.from(types).sort()]
  }, [markets])

  // Filter and sort
  const displayMarkets = useMemo(() => {
    let filtered = markets

    if (statFilter !== 'all') {
      filtered = filtered.filter((m) => m.stat_type === statFilter)
    }

    if (minEdge > 0) {
      filtered = filtered.filter(
        (m) => m.maker_fee_adjusted_edge != null && Math.abs(m.maker_fee_adjusted_edge) >= minEdge / 100
      )
    }

    if (minVolume > 0) {
      filtered = filtered.filter((m) => m.volume >= minVolume)
    }

    const sorted = [...filtered].sort((a, b) => {
      let aVal: number, bVal: number
      switch (sortKey) {
        case 'edge':
          aVal = a.maker_fee_adjusted_edge ?? -999
          bVal = b.maker_fee_adjusted_edge ?? -999
          break
        case 'volume':
          aVal = a.volume
          bVal = b.volume
          break
        case 'close_time':
          aVal = a.close_time ? new Date(a.close_time).getTime() : Infinity
          bVal = b.close_time ? new Date(b.close_time).getTime() : Infinity
          break
        case 'line':
          aVal = a.line
          bVal = b.line
          break
        case 'spread':
          aVal = a.bid_ask_spread
          bVal = b.bid_ask_spread
          break
        default:
          aVal = 0
          bVal = 0
      }
      return sortAsc ? aVal - bVal : bVal - aVal
    })

    return sorted
  }, [markets, statFilter, minEdge, minVolume, sortKey, sortAsc])

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortAsc(!sortAsc)
    } else {
      setSortKey(key)
      setSortAsc(false)
    }
  }

  const sortIcon = (key: SortKey) => {
    if (sortKey !== key) return null
    return sortAsc ? ' \u25B2' : ' \u25BC'
  }

  const edgeColor = (edge: number | null) => {
    if (edge == null) return 'text-slate-500'
    if (edge >= 0.05) return 'text-green-400 font-semibold'
    if (edge > 0) return 'text-green-300'
    if (edge > -0.03) return 'text-slate-400'
    return 'text-red-400'
  }

  return (
    <div>
      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-4">
        <select
          value={statFilter}
          onChange={(e) => setStatFilter(e.target.value)}
          className="bg-slate-700 border border-slate-600 rounded-md px-3 py-1.5 text-sm text-slate-200"
        >
          {statTypes.map((s) => (
            <option key={s} value={s}>
              {s === 'all' ? 'All Stats' : (KALSHI_STAT_LABELS[s] || s)}
            </option>
          ))}
        </select>

        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-400">Min Edge %</label>
          <input
            type="number"
            min={0}
            step={1}
            value={minEdge}
            onChange={(e) => setMinEdge(Number(e.target.value))}
            className="bg-slate-700 border border-slate-600 rounded-md px-2 py-1.5 text-sm text-slate-200 w-16"
          />
        </div>

        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-400">Min Volume</label>
          <input
            type="number"
            min={0}
            step={100}
            value={minVolume}
            onChange={(e) => setMinVolume(Number(e.target.value))}
            className="bg-slate-700 border border-slate-600 rounded-md px-2 py-1.5 text-sm text-slate-200 w-20"
          />
        </div>

        <div className="ml-auto text-sm text-slate-400">
          {displayMarkets.length} market{displayMarkets.length !== 1 ? 's' : ''}
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-700 text-slate-400">
              <th className="text-left py-2 px-2">Player</th>
              <th className="text-left py-2 px-2">Stat</th>
              <th className="text-right py-2 px-2 cursor-pointer hover:text-slate-200" onClick={() => handleSort('line')}>
                Line{sortIcon('line')}
              </th>
              <th className="text-right py-2 px-2">YES</th>
              <th className="text-right py-2 px-2">NO</th>
              <th className="text-right py-2 px-2 cursor-pointer hover:text-slate-200" onClick={() => handleSort('spread')}>
                Spread{sortIcon('spread')}
              </th>
              <th className="text-right py-2 px-2 cursor-pointer hover:text-slate-200" onClick={() => handleSort('volume')}>
                Volume{sortIcon('volume')}
              </th>
              <th className="text-right py-2 px-2">Model</th>
              <th className="text-right py-2 px-2 cursor-pointer hover:text-slate-200" onClick={() => handleSort('edge')}>
                Edge{sortIcon('edge')}
              </th>
              <th className="text-right py-2 px-2 cursor-pointer hover:text-slate-200" onClick={() => handleSort('close_time')}>
                Closes{sortIcon('close_time')}
              </th>
            </tr>
          </thead>
          <tbody>
            {displayMarkets.length === 0 ? (
              <tr>
                <td colSpan={10} className="text-center py-8 text-slate-500">
                  No markets match your filters
                </td>
              </tr>
            ) : (
              displayMarkets.map((market) => (
                <tr
                  key={market.ticker}
                  className="border-b border-slate-700/50 hover:bg-slate-700/30 cursor-pointer transition-colors"
                  onClick={() => setSelectedMarket(market)}
                >
                  <td className="py-2 px-2 text-slate-200">{market.player_name}</td>
                  <td className="py-2 px-2 text-slate-400">
                    {KALSHI_STAT_LABELS[market.stat_type] || market.stat_type}
                  </td>
                  <td className="py-2 px-2 text-right text-slate-200 font-mono">{market.line}</td>
                  <td className="py-2 px-2 text-right text-green-400 font-mono">{market.yes_price}c</td>
                  <td className="py-2 px-2 text-right font-mono">
                    <span className="text-red-400">{market.no_price}c</span>
                    <span className="ml-1 bg-violet-600/20 text-violet-300 text-[10px] px-1 rounded">API</span>
                  </td>
                  <td className="py-2 px-2 text-right text-slate-400 font-mono">{market.bid_ask_spread}c</td>
                  <td className="py-2 px-2 text-right text-slate-300 font-mono">{market.volume.toLocaleString()}</td>
                  <td className="py-2 px-2 text-right text-blue-400 font-mono">
                    {market.model_prob != null ? `${(market.model_prob * 100).toFixed(0)}%` : '--'}
                  </td>
                  <td className={`py-2 px-2 text-right font-mono ${edgeColor(market.maker_fee_adjusted_edge)}`}>
                    {market.maker_fee_adjusted_edge != null
                      ? `${(market.maker_fee_adjusted_edge * 100).toFixed(1)}%`
                      : '--'}
                  </td>
                  <td className="py-2 px-2 text-right">
                    <KalshiCountdown closeTime={market.close_time} />
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Detail Modal */}
      {selectedMarket && (
        <KalshiMarketDetail
          market={selectedMarket}
          onClose={() => setSelectedMarket(null)}
        />
      )}
    </div>
  )
}
