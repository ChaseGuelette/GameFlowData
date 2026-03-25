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

function SortHeader({ label, sortKeyValue, className, sortKey, sortAsc, onSort }: {
  label: string; sortKeyValue: SortKey; className?: string;
  sortKey: SortKey; sortAsc: boolean; onSort: (key: SortKey) => void;
}) {
  return (
    <th
      className={`py-3 px-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-200 select-none ${className ?? ''}`}
      onClick={() => onSort(sortKeyValue)}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {sortKey === sortKeyValue && (
          <span className="text-blue-400">{sortAsc ? '\u25B2' : '\u25BC'}</span>
        )}
      </span>
    </th>
  )
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

  // ─── Mobile Card Renderer ────────────────────────────────────────
  const renderMobileCard = (row: DfsRow, i: number) => {
    const edge = row.platform.ev_by_slip[slipType] ?? 0
    const pl = row.platform

    // Get prob based on edge mode
    let probLabel: string
    let probValue: string
    if (edgeMode === 'model') {
      probLabel = 'Model'
      probValue = formatProb((pl as DfsPlatformLine).best_prob)
    } else if (edgeMode === 'market') {
      probLabel = 'Market'
      const mp = (pl as MarketEdgePlatformLine).market_prob
      probValue = mp !== null ? formatProb(mp) : '--'
    } else {
      probLabel = 'Mdl/Mkt'
      const cp = pl as CombinedEdgePlatformLine
      const mktStr = cp.market_prob !== null ? formatProb(cp.market_prob) : '--'
      probValue = `${formatProb(cp.model_prob)} / ${mktStr}`
    }

    return (
      <div
        key={`${row.comparison.player_id}-${row.comparison.stat}-${pl.bookmaker}-${i}`}
        className="bg-slate-800 rounded-lg border border-slate-700 p-3"
      >
        {/* Top row: player + edge */}
        <div className="flex items-center justify-between gap-2 mb-2">
          <div className="flex items-center gap-2 min-w-0">
            <PlayerAvatar
              playerId={row.comparison.player_id}
              playerName={row.comparison.player_name}
              size="sm"
            />
            <div className="min-w-0">
              <div className="text-slate-100 font-medium text-sm truncate">
                {row.comparison.player_name}
              </div>
              <div className="text-slate-500 text-xs">
                {row.comparison.team_abbrev} vs {row.comparison.opponent_abbrev}
              </div>
            </div>
          </div>
          {renderEdgeBadge(edge)}
        </div>

        {/* Bottom row: stat, platform, line, direction, prob */}
        <div className="flex items-center gap-2 flex-wrap">
          <Badge stat={row.comparison.stat} />
          <span className="text-slate-400 text-xs">{DFS_PLATFORM_NAMES[pl.bookmaker] ?? pl.bookmaker}</span>
          <span className="text-slate-200 text-sm font-medium">{pl.line}</span>
          {renderDirection(pl.best_direction)}
          <span className="text-slate-400 text-xs ml-auto">
            {probLabel}: <span className="text-slate-200">{probValue}</span>
          </span>
        </div>
      </div>
    )
  }

  // ─── Mobile card list (visible < sm) ─────────────────────────────
  const mobileCards = (
    <div className="block sm:hidden space-y-2">
      {sortedRows.map((row, i) => renderMobileCard(row, i))}
    </div>
  )

  // --- MODEL EDGE MODE ---
  if (edgeMode === 'model') {
    return (
      <>
        {mobileCards}
        <div className="hidden sm:block overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="border-b border-slate-700">
              <tr>
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Player" sortKeyValue="player" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Stat" sortKeyValue="stat" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Platform" sortKeyValue="platform" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Sharp" sortKeyValue="sharp_line" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="DFS" sortKeyValue="dfs_line" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Diff" sortKeyValue="diff" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Pick" sortKeyValue="direction" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Model %" sortKeyValue="model_prob" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="B/E" sortKeyValue="break_even" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Edge" sortKeyValue="edge" className="text-center" />
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
      </>
    )
  }

  // --- MARKET EDGE MODE ---
  if (edgeMode === 'market') {
    return (
      <>
        {mobileCards}
        <div className="hidden sm:block overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="border-b border-slate-700">
              <tr>
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Player" sortKeyValue="player" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Stat" sortKeyValue="stat" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Platform" sortKeyValue="platform" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="DFS Line" sortKeyValue="dfs_line" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Pick" sortKeyValue="direction" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Market %" sortKeyValue="market_prob" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Books" sortKeyValue="books" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Sharp Line" sortKeyValue="sharp_line" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Line Diff" sortKeyValue="diff" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="B/E" sortKeyValue="break_even" className="text-center" />
                <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Edge" sortKeyValue="edge" className="text-center" />
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
      </>
    )
  }

  // --- COMBINED EDGE MODE ---
  return (
    <>
      {mobileCards}
      <div className="hidden sm:block overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b border-slate-700">
            <tr>
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Player" sortKeyValue="player" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Stat" sortKeyValue="stat" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Platform" sortKeyValue="platform" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="DFS Line" sortKeyValue="dfs_line" className="text-center" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Pick" sortKeyValue="direction" className="text-center" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Model %" sortKeyValue="model_prob" className="text-center" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Market %" sortKeyValue="market_prob" className="text-center" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Books" sortKeyValue="books" className="text-center" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="B/E" sortKeyValue="break_even" className="text-center" />
              <SortHeader sortKey={sortKey} sortAsc={sortAsc} onSort={handleSort} label="Edge" sortKeyValue="edge" className="text-center" />
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
    </>
  )
}
