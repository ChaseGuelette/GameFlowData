'use client'

import { useState } from 'react'
import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { Badge } from '@/components/shared/Badge'
import { DFS_PLATFORM_NAMES, DFS_SLIP_TYPES } from '@/types/dfs'
import type { DfsPlatformLine, DfsComparison } from '@/types/dfs'
import { formatProb } from '@/lib/utils'

interface DfsRow {
  comparison: DfsComparison
  platform: DfsPlatformLine
}

type SortKey = 'player' | 'stat' | 'platform' | 'sharp_line' | 'dfs_line' | 'diff' | 'direction' | 'model_prob' | 'break_even' | 'edge'

interface DfsTableProps {
  rows: DfsRow[]
  slipType: string
}

export function DfsTable({ rows, slipType }: DfsTableProps) {
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
      case 'sharp_line':
        return dir * (a.comparison.sharp_line - b.comparison.sharp_line)
      case 'dfs_line':
        return dir * (a.platform.line - b.platform.line)
      case 'diff':
        return dir * (a.platform.line_diff - b.platform.line_diff)
      case 'direction':
        return dir * a.platform.best_direction.localeCompare(b.platform.best_direction)
      case 'model_prob':
        return dir * (a.platform.best_prob - b.platform.best_prob)
      case 'break_even':
        return 0 // Same for all rows
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
    return (
      <div className="flex items-center justify-center py-16">
        <div className="text-center">
          <p className="text-slate-400 text-lg">No DFS lines available</p>
          <p className="text-slate-500 text-sm mt-2">
            DFS data will appear after the next scrape with DFS platforms enabled
          </p>
        </div>
      </div>
    )
  }

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
            const edge = row.platform.ev_by_slip[slipType] ?? 0
            const edgePositive = edge > 0
            const edgeTier = edge >= 0.05 ? 'high' : edge >= 0.02 ? 'medium' : 'low'

            return (
              <tr
                key={`${row.comparison.player_id}-${row.comparison.stat}-${row.platform.bookmaker}-${i}`}
                className="hover:bg-slate-700/30 transition-colors"
              >
                {/* Player */}
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

                {/* Stat */}
                <td className="py-3 px-3">
                  <Badge stat={row.comparison.stat} />
                </td>

                {/* Platform */}
                <td className="py-3 px-3 text-slate-300 text-sm">
                  {DFS_PLATFORM_NAMES[row.platform.bookmaker] ?? row.platform.bookmaker}
                </td>

                {/* Sharp Line */}
                <td className="py-3 px-3 text-center text-slate-400">
                  {row.comparison.sharp_line}
                </td>

                {/* DFS Line */}
                <td className="py-3 px-3 text-center text-slate-200 font-medium">
                  {row.platform.line}
                </td>

                {/* Diff */}
                <td className="py-3 px-3 text-center">
                  <span className={
                    row.platform.line_diff > 0
                      ? 'text-green-400'
                      : row.platform.line_diff < 0
                        ? 'text-red-400'
                        : 'text-slate-400'
                  }>
                    {row.platform.line_diff > 0 ? '+' : ''}{row.platform.line_diff.toFixed(1)}
                  </span>
                </td>

                {/* Direction */}
                <td className="py-3 px-3 text-center">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                    row.platform.best_direction === 'over'
                      ? 'bg-green-500/20 text-green-400'
                      : 'bg-red-500/20 text-red-400'
                  }`}>
                    {row.platform.best_direction === 'over' ? 'OVER' : 'UNDER'}
                  </span>
                </td>

                {/* Model Prob */}
                <td className="py-3 px-3 text-center text-slate-200">
                  {formatProb(row.platform.best_prob)}
                </td>

                {/* Break-Even */}
                <td className="py-3 px-3 text-center text-slate-400">
                  {formatProb(breakEven)}
                </td>

                {/* Edge */}
                <td className="py-3 px-3 text-center">
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
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export type { DfsRow }
