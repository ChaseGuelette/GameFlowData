'use client'

import { useState } from 'react'
import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { Badge } from '@/components/shared/Badge'
import { DFS_PLATFORM_NAMES, DFS_SLIP_TYPES } from '@/types/dfs'
import type { DfsPlatformLine, DfsComparison, EdgeMode, MarketEdgePlatformLine, CombinedEdgePlatformLine } from '@/types/dfs'
import { formatProb } from '@/lib/utils'
import { formatBookmaker } from '@/lib/dfs-utils'

// Row shapes per mode
export interface ModelDfsRow {
  comparison: DfsComparison
  platform: DfsPlatformLine
}

export interface MarketDfsRow {
  comparison: DfsComparison
  platform: MarketEdgePlatformLine
}

export interface CombinedDfsRow {
  comparison: DfsComparison
  platform: CombinedEdgePlatformLine
}

export type DfsRow = ModelDfsRow | MarketDfsRow | CombinedDfsRow

type SortKey = 'player' | 'stat' | 'platform' | 'sharp_line' | 'dfs_line' | 'diff' | 'direction' | 'model_prob' | 'market_prob' | 'books' | 'break_even' | 'edge'

interface DfsTableProps {
  rows: DfsRow[]
  slipType: string
  edgeMode: EdgeMode
}

export function DfsTable({ rows, slipType, edgeMode }: DfsTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>('edge')
  const [sortAsc, setSortAsc] = useState(false)

  const breakEven = DFS_SLIP_TYPES[slipType]?.breakEven ?? 0.55

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortAsc(!sortAsc)
    } else {
      setSortKey(key)
      setSortAsc(false)
    }
  }

  const sortedRows = [...rows].sort((a, b) => {
    const dir = sortAsc ? 1 : -1
    switch (sortKey) {
      case 'player':
        return dir * a.comparison.player_name.localeCompare(b.comparison.player_name)
      case 'stat':
        return dir * a.comparison.stat.localeCompare(b.comparison.stat)
      case 'platform':
        return dir * a.platform.bookmaker.localeCompare(b.platform.bookmaker)
      case 'sharp_line': {
        const aVal = edgeMode === 'model' ? a.comparison.sharp_line : ((a.platform as MarketEdgePlatformLine).sharp_line ?? 0)
        const bVal = edgeMode === 'model' ? b.comparison.sharp_line : ((b.platform as MarketEdgePlatformLine).sharp_line ?? 0)
        return dir * (aVal - bVal)
      }
      case 'dfs_line':
        return dir * (a.platform.line - b.platform.line)
      case 'diff': {
        const aDiff = edgeMode === 'model'
          ? (a.platform as DfsPlatformLine).line_diff
          : ((a.platform as MarketEdgePlatformLine).line_diff ?? 0)
        const bDiff = edgeMode === 'model'
          ? (b.platform as DfsPlatformLine).line_diff
          : ((b.platform as MarketEdgePlatformLine).line_diff ?? 0)
        return dir * (aDiff - bDiff)
      }
      case 'direction':
        return dir * a.platform.best_direction.localeCompare(b.platform.best_direction)
      case 'model_prob': {
        const aProb = edgeMode === 'model'
          ? (a.platform as DfsPlatformLine).best_prob
          : edgeMode === 'combined'
            ? (a.platform as CombinedEdgePlatformLine).model_prob
            : 0
        const bProb = edgeMode === 'model'
          ? (b.platform as DfsPlatformLine).best_prob
          : edgeMode === 'combined'
            ? (b.platform as CombinedEdgePlatformLine).model_prob
            : 0
        return dir * (aProb - bProb)
      }
      case 'market_prob': {
        const aProb = (a.platform as MarketEdgePlatformLine | CombinedEdgePlatformLine).market_prob ?? 0
        const bProb = (b.platform as MarketEdgePlatformLine | CombinedEdgePlatformLine).market_prob ?? 0
        return dir * (aProb - bProb)
      }
      case 'books': {
        const aBooks = (a.platform as MarketEdgePlatformLine | CombinedEdgePlatformLine).market_books_count ?? 0
        const bBooks = (b.platform as MarketEdgePlatformLine | CombinedEdgePlatformLine).market_books_count ?? 0
        return dir * (aBooks - bBooks)
      }
      case 'break_even':
        return 0
      case 'edge': {
        const aEdge = (a.platform.ev_by_slip[slipType] ?? 0)
        const bEdge = (b.platform.ev_by_slip[slipType] ?? 0)
        return dir * (aEdge - bEdge)
      }
      default:
        return 0
    }
  })

  const SortHeader = ({ label, sortKeyValue, className }: { label: string; sortKeyValue: SortKey; className?: string }) => (
    <th
      className={`py-3 px-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-200 select-none ${className ?? ''}`}
      onClick={() => handleSort(sortKeyValue)}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {sortKey === sortKeyValue && (
          <span className="text-blue-400">{sortAsc ? '\u25B2' : '\u25BC'}</span>
        )}
      </span>
    </th>
  )

  if (rows.length === 0) {
    const emptyMessages: Record<EdgeMode, { title: string; subtitle: string }> = {
      model: {
        title: 'No DFS lines available',
        subtitle: 'DFS data will appear after the next scrape with DFS platforms enabled',
      },
      market: {
        title: 'No sportsbook lines available yet',
        subtitle: 'Sportsbook data will appear after the next scrape',
      },
      combined: {
        title: 'No picks where both model and market agree',
        subtitle: 'Combined mode shows only picks with positive edge from both signals',
      },
    }
    const msg = emptyMessages[edgeMode]
    return (
      <div className="flex items-center justify-center py-16">
        <div className="text-center">
          <p className="text-slate-400 text-lg">{msg.title}</p>
          <p className="text-slate-500 text-sm mt-2">{msg.subtitle}</p>
        </div>
      </div>
    )
  }

  const renderEdgeBadge = (edge: number) => {
    const edgePositive = edge > 0
    const edgeTier = edge >= 0.05 ? 'high' : edge >= 0.02 ? 'medium' : 'low'
    return (
      <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${
        edgeTier === 'high'
          ? 'bg-green-500/20 text-green-400 border-green-500/50'
          : edgeTier === 'medium'
            ? 'bg-yellow-500/20 text-yellow-400 border-yellow-500/50'
            : edgePositive
              ? 'bg-green-500/10 text-green-400/70 border-green-500/30'
              : 'bg-slate-500/20 text-slate-400 border-slate-500/50'
      }`}>
        {edge >= 0 ? '+' : ''}{(edge * 100).toFixed(1)}%
      </span>
    )
  }

  const renderDirection = (direction: 'over' | 'under') => (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
      direction === 'over'
        ? 'bg-green-500/20 text-green-400'
        : 'bg-red-500/20 text-red-400'
    }`}>
      {direction === 'over' ? 'OVER' : 'UNDER'}
    </span>
  )

  const renderPlayerCell = (row: DfsRow) => (
    <td className="py-3 px-3">
      <div className="flex items-center gap-2">
        <PlayerAvatar
          playerId={row.comparison.player_id}
          playerName={row.comparison.player_name}
          size="sm"
        />
        <div>
          <div className="text-slate-100 font-medium text-sm">
            {row.comparison.player_name}
          </div>
          <div className="text-slate-500 text-xs">
            {row.comparison.team_abbrev} vs {row.comparison.opponent_abbrev}
          </div>
        </div>
      </div>
    </td>
  )

  // --- MODEL EDGE MODE ---
  if (edgeMode === 'model') {
    return (
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b border-slate-700">
            <tr>
              <SortHeader label="Player" sortKeyValue="player" />
              <SortHeader label="Stat" sortKeyValue="stat" />
              <SortHeader label="Platform" sortKeyValue="platform" />
              <SortHeader label="Sharp" sortKeyValue="sharp_line" className="text-center" />
              <SortHeader label="DFS" sortKeyValue="dfs_line" className="text-center" />
              <SortHeader label="Diff" sortKeyValue="diff" className="text-center" />
              <SortHeader label="Pick" sortKeyValue="direction" className="text-center" />
              <SortHeader label="Model %" sortKeyValue="model_prob" className="text-center" />
              <SortHeader label="B/E" sortKeyValue="break_even" className="text-center" />
              <SortHeader label="Edge" sortKeyValue="edge" className="text-center" />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {sortedRows.map((row, i) => {
              const pl = row.platform as DfsPlatformLine
              const edge = pl.ev_by_slip[slipType] ?? 0
              return (
                <tr key={`${row.comparison.player_id}-${row.comparison.stat}-${pl.bookmaker}-${i}`} className="hover:bg-slate-700/30 transition-colors">
                  {renderPlayerCell(row)}
                  <td className="py-3 px-3"><Badge stat={row.comparison.stat} /></td>
                  <td className="py-3 px-3 text-slate-300 text-sm">{DFS_PLATFORM_NAMES[pl.bookmaker] ?? pl.bookmaker}</td>
                  <td className="py-3 px-3 text-center text-slate-400">{row.comparison.sharp_line}</td>
                  <td className="py-3 px-3 text-center text-slate-200 font-medium">{pl.line}</td>
                  <td className="py-3 px-3 text-center">
                    <span className={pl.line_diff > 0 ? 'text-green-400' : pl.line_diff < 0 ? 'text-red-400' : 'text-slate-400'}>
                      {pl.line_diff > 0 ? '+' : ''}{pl.line_diff.toFixed(1)}
                    </span>
                  </td>
                  <td className="py-3 px-3 text-center">{renderDirection(pl.best_direction)}</td>
                  <td className="py-3 px-3 text-center text-slate-200">{formatProb(pl.best_prob)}</td>
                  <td className="py-3 px-3 text-center text-slate-400">{formatProb(breakEven)}</td>
                  <td className="py-3 px-3 text-center">{renderEdgeBadge(edge)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    )
  }

  // --- MARKET EDGE MODE ---
  if (edgeMode === 'market') {
    return (
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b border-slate-700">
            <tr>
              <SortHeader label="Player" sortKeyValue="player" />
              <SortHeader label="Stat" sortKeyValue="stat" />
              <SortHeader label="Platform" sortKeyValue="platform" />
              <SortHeader label="DFS Line" sortKeyValue="dfs_line" className="text-center" />
              <SortHeader label="Pick" sortKeyValue="direction" className="text-center" />
              <SortHeader label="Market %" sortKeyValue="market_prob" className="text-center" />
              <SortHeader label="Books" sortKeyValue="books" className="text-center" />
              <SortHeader label="Sharp Line" sortKeyValue="sharp_line" className="text-center" />
              <SortHeader label="Line Diff" sortKeyValue="diff" className="text-center" />
              <SortHeader label="B/E" sortKeyValue="break_even" className="text-center" />
              <SortHeader label="Edge" sortKeyValue="edge" className="text-center" />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {sortedRows.map((row, i) => {
              const pl = row.platform as MarketEdgePlatformLine
              const edge = pl.ev_by_slip[slipType] ?? 0
              const hasMarketProb = pl.market_prob !== null
              return (
                <tr key={`${row.comparison.player_id}-${row.comparison.stat}-${pl.bookmaker}-${i}`} className="hover:bg-slate-700/30 transition-colors">
                  {renderPlayerCell(row)}
                  <td className="py-3 px-3"><Badge stat={row.comparison.stat} /></td>
                  <td className="py-3 px-3 text-slate-300 text-sm">{DFS_PLATFORM_NAMES[pl.bookmaker] ?? pl.bookmaker}</td>
                  <td className="py-3 px-3 text-center text-slate-200 font-medium">{pl.line}</td>
                  <td className="py-3 px-3 text-center">{renderDirection(pl.best_direction)}</td>
                  <td className="py-3 px-3 text-center text-slate-200">
                    {hasMarketProb ? formatProb(pl.market_prob!) : <span className="text-slate-500">--</span>}
                  </td>
                  <td className="py-3 px-3 text-center text-slate-300">{pl.market_books_count || '--'}</td>
                  <td className="py-3 px-3 text-center text-slate-400">
                    {pl.sharp_line !== null ? pl.sharp_line : '--'}
                    {pl.sharp_book && <span className="text-slate-500 text-xs ml-1">({formatBookmaker(pl.sharp_book)})</span>}
                  </td>
                  <td className="py-3 px-3 text-center">
                    {pl.line_diff !== null ? (
                      <span className={pl.line_diff > 0 ? 'text-green-400' : pl.line_diff < 0 ? 'text-red-400' : 'text-slate-400'}>
                        {pl.line_diff > 0 ? '+' : ''}{pl.line_diff.toFixed(1)}
                      </span>
                    ) : <span className="text-slate-500">--</span>}
                  </td>
                  <td className="py-3 px-3 text-center text-slate-400">{formatProb(breakEven)}</td>
                  <td className="py-3 px-3 text-center">
                    {hasMarketProb ? renderEdgeBadge(edge) : <span className="text-slate-500">--</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    )
  }

  // --- COMBINED EDGE MODE ---
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="border-b border-slate-700">
          <tr>
            <SortHeader label="Player" sortKeyValue="player" />
            <SortHeader label="Stat" sortKeyValue="stat" />
            <SortHeader label="Platform" sortKeyValue="platform" />
            <SortHeader label="DFS Line" sortKeyValue="dfs_line" className="text-center" />
            <SortHeader label="Pick" sortKeyValue="direction" className="text-center" />
            <SortHeader label="Model %" sortKeyValue="model_prob" className="text-center" />
            <SortHeader label="Market %" sortKeyValue="market_prob" className="text-center" />
            <SortHeader label="Books" sortKeyValue="books" className="text-center" />
            <SortHeader label="B/E" sortKeyValue="break_even" className="text-center" />
            <SortHeader label="Edge" sortKeyValue="edge" className="text-center" />
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-700/50">
          {sortedRows.map((row, i) => {
            const pl = row.platform as CombinedEdgePlatformLine
            const edge = pl.ev_by_slip[slipType] ?? 0
            return (
              <tr key={`${row.comparison.player_id}-${row.comparison.stat}-${pl.bookmaker}-${i}`} className="hover:bg-slate-700/30 transition-colors">
                {renderPlayerCell(row)}
                <td className="py-3 px-3"><Badge stat={row.comparison.stat} /></td>
                <td className="py-3 px-3 text-slate-300 text-sm">{DFS_PLATFORM_NAMES[pl.bookmaker] ?? pl.bookmaker}</td>
                <td className="py-3 px-3 text-center text-slate-200 font-medium">{pl.line}</td>
                <td className="py-3 px-3 text-center">{renderDirection(pl.best_direction)}</td>
                <td className="py-3 px-3 text-center text-slate-200">{formatProb(pl.model_prob)}</td>
                <td className="py-3 px-3 text-center text-slate-200">
                  {pl.market_prob !== null ? formatProb(pl.market_prob) : <span className="text-slate-500">--</span>}
                </td>
                <td className="py-3 px-3 text-center text-slate-300">{pl.market_books_count || '--'}</td>
                <td className="py-3 px-3 text-center text-slate-400">{formatProb(breakEven)}</td>
                <td className="py-3 px-3 text-center">{renderEdgeBadge(edge)}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
