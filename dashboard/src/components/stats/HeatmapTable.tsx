'use client'

import { useMemo, useCallback } from 'react'
import { cn } from '@/lib/utils'
import type { ColumnDef, StatRow, SortState, WindowSuffix } from '@/types/stats'

// ─── Value Formatting ───────────────────────────────────────────────
function formatValue(v: number | string | null | undefined, format: ColumnDef['format']): string {
  if (v == null || v === '') return '—'
  const n = typeof v === 'string' ? parseFloat(v) : v
  if (!Number.isFinite(n)) return '—'
  switch (format) {
    case 'int':
      return Math.round(n).toString()
    case 'dec1':
      return n.toFixed(1)
    case 'dec2':
      return n.toFixed(2)
    case 'dec3':
      return n.toFixed(3)
    case 'pct1':
      return (n * 100).toFixed(1) + '%'
    case 'rawPct1':
      return n.toFixed(1) + '%'
    case 'plusMinus1':
      return (n >= 0 ? '+' : '') + n.toFixed(1)
  }
}

// ─── Percentile Heatmap Color ───────────────────────────────────────
function getHeatmapClass(percentile: number, inverted: boolean): string {
  const p = inverted ? 1 - percentile : percentile
  if (p >= 0.90) return 'bg-blue-600/60 text-white font-medium'
  if (p >= 0.75) return 'bg-blue-700/50 text-blue-100'
  if (p >= 0.50) return 'bg-blue-800/40 text-slate-200'
  if (p >= 0.25) return 'bg-blue-900/20 text-slate-300'
  return 'text-slate-400'
}

// ─── Resolve column key to actual DB column name ────────────────────
function resolveDbColumn(col: ColumnDef, window: WindowSuffix): string {
  if (col.windowless) return col.dbColumn
  return col.dbColumn.replace('{window}', window)
}

// ─── Props ──────────────────────────────────────────────────────────
interface HeatmapTableProps {
  rows: StatRow[]
  columns: ColumnDef[]
  window: WindowSuffix
  sort: SortState
  onSort: (sort: SortState) => void
  nameLabel?: string
  showTeam?: boolean
  showPosition?: boolean
}

export function HeatmapTable({
  rows,
  columns,
  window,
  sort,
  onSort,
  nameLabel = 'Name',
  showTeam = false,
  showPosition = false,
}: HeatmapTableProps) {
  // Resolve column keys to actual DB column names for current window
  const resolvedColumns = useMemo(
    () => columns.map((col) => ({ ...col, resolved: resolveDbColumn(col, window) })),
    [columns, window]
  )

  // Sort rows
  const sortedRows = useMemo(() => {
    const col = resolvedColumns.find((c) => c.key === sort.column)
    if (!col) return rows
    const dbCol = col.resolved
    return [...rows].sort((a, b) => {
      const av = a[dbCol]
      const bv = b[dbCol]
      // nulls last
      if (av == null && bv == null) return 0
      if (av == null) return 1
      if (bv == null) return -1
      const numA = typeof av === 'string' ? parseFloat(av) : (av as number)
      const numB = typeof bv === 'string' ? parseFloat(bv) : (bv as number)
      if (!Number.isFinite(numA) && !Number.isFinite(numB)) return 0
      if (!Number.isFinite(numA)) return 1
      if (!Number.isFinite(numB)) return -1
      return sort.direction === 'asc' ? numA - numB : numB - numA
    })
  }, [rows, sort, resolvedColumns])

  // Compute percentiles per column
  const percentiles = useMemo(() => {
    const result: Record<string, Map<string | number, number>> = {}
    for (const col of resolvedColumns) {
      const dbCol = col.resolved
      const values: { id: string | number; val: number }[] = []
      for (const row of rows) {
        const raw = row[dbCol]
        if (raw == null) continue
        const n = typeof raw === 'string' ? parseFloat(raw) : (raw as number)
        if (Number.isFinite(n)) values.push({ id: row.id, val: n })
      }
      values.sort((a, b) => a.val - b.val)
      const map = new Map<string | number, number>()
      const len = values.length
      for (let i = 0; i < len; i++) {
        // Handle ties by averaging the rank
        map.set(values[i].id, len > 1 ? i / (len - 1) : 0.5)
      }
      result[col.key] = map
    }
    return result
  }, [rows, resolvedColumns])

  const handleSort = useCallback(
    (colKey: string) => {
      onSort({
        column: colKey,
        direction: sort.column === colKey && sort.direction === 'desc' ? 'asc' : 'desc',
      })
    },
    [sort, onSort]
  )

  const sortArrow = (colKey: string) => {
    if (sort.column !== colKey) return null
    return sort.direction === 'desc' ? ' ▼' : ' ▲'
  }

  return (
    <div className="relative max-h-[calc(100vh-280px)] overflow-auto rounded-lg border border-slate-700">
      <table className="w-full text-[10px] sm:text-xs border-collapse">
        <thead className="sticky top-0 z-20 bg-slate-800">
          <tr>
            {/* Sticky name column header */}
            <th className="sticky left-0 z-30 bg-slate-800 px-2 sm:px-3 py-2 text-left text-slate-300 font-medium border-b border-r border-slate-700 min-w-[100px] sm:min-w-[160px]">
              {nameLabel}
            </th>
            {showPosition && (
              <th className="sticky left-[100px] sm:left-[160px] z-30 bg-slate-800 px-1.5 sm:px-2 py-2 text-center text-slate-300 font-medium border-b border-r border-slate-700 min-w-[32px] sm:min-w-[40px]">
                Pos
              </th>
            )}
            {showTeam && (
              <th
                className={cn(
                  'sticky z-30 bg-slate-800 px-1.5 sm:px-2 py-2 text-center text-slate-300 font-medium border-b border-r border-slate-700 min-w-[40px] sm:min-w-[48px]',
                  showPosition ? 'left-[132px] sm:left-[200px]' : 'left-[100px] sm:left-[160px]'
                )}
              >
                Team
              </th>
            )}
            {resolvedColumns.map((col) => (
              <th
                key={col.key}
                onClick={() => handleSort(col.key)}
                className="px-2 py-2 text-center text-slate-300 font-medium border-b border-slate-700 cursor-pointer hover:bg-slate-700 whitespace-nowrap select-none"
                title={col.tooltip}
              >
                {col.label}
                {sortArrow(col.key)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row) => (
            <tr key={row.id} className="hover:bg-slate-800/50 border-b border-slate-800">
              {/* Sticky name cell */}
              <td className="sticky left-0 z-10 bg-slate-900 px-2 sm:px-3 py-1.5 text-slate-200 font-medium border-r border-slate-800 whitespace-nowrap">
                {row.name}
              </td>
              {showPosition && (
                <td className="sticky left-[100px] sm:left-[160px] z-10 bg-slate-900 px-1.5 sm:px-2 py-1.5 text-center text-slate-400 border-r border-slate-800">
                  {row.position || '—'}
                </td>
              )}
              {showTeam && (
                <td
                  className={cn(
                    'sticky z-10 bg-slate-900 px-1.5 sm:px-2 py-1.5 text-center text-slate-400 border-r border-slate-800',
                    showPosition ? 'left-[132px] sm:left-[200px]' : 'left-[100px] sm:left-[160px]'
                  )}
                >
                  {row.teamAbbrev || '—'}
                </td>
              )}
              {resolvedColumns.map((col) => {
                const dbCol = col.resolved
                const raw = row[dbCol]
                const pMap = percentiles[col.key]
                const p = pMap?.get(row.id) ?? 0.5
                return (
                  <td
                    key={col.key}
                    className={cn(
                      'px-1.5 sm:px-2 py-1.5 text-center tabular-nums whitespace-nowrap',
                      raw != null ? getHeatmapClass(p, !!col.invertHeatmap) : 'text-slate-600'
                    )}
                  >
                    {formatValue(raw, col.format)}
                  </td>
                )
              })}
            </tr>
          ))}
          {sortedRows.length === 0 && (
            <tr>
              <td
                colSpan={resolvedColumns.length + 1 + (showTeam ? 1 : 0) + (showPosition ? 1 : 0)}
                className="px-4 py-12 text-center text-slate-500"
              >
                No data available
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

// ─── Heatmap Color Legend ──────────────────────────────────────────
const legendTiers = [
  { label: '90th+', className: 'bg-blue-600/60 text-white' },
  { label: '75th+', className: 'bg-blue-700/50 text-blue-100' },
  { label: '50th+', className: 'bg-blue-800/40 text-slate-200' },
  { label: '25th+', className: 'bg-blue-900/20 text-slate-300' },
  { label: '<25th', className: 'bg-transparent text-slate-400' },
]

export function HeatmapLegend() {
  return (
    <div className="flex items-center gap-2 text-xs text-slate-400 overflow-x-auto scrollbar-hide">
      <span className="whitespace-nowrap">Percentile:</span>
      {legendTiers.map((tier) => (
        <span
          key={tier.label}
          className={cn('px-2 py-0.5 rounded whitespace-nowrap', tier.className)}
        >
          {tier.label}
        </span>
      ))}
    </div>
  )
}
