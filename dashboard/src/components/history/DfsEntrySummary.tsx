'use client'

import { useMemo } from 'react'
import { USER_DFS_SLIP_TYPES, type UserDfsEntryWithLegs } from '@/types/dfs-entries'

interface DfsEntrySummaryProps {
  entries: UserDfsEntryWithLegs[]
}

export function DfsEntrySummary({ entries }: DfsEntrySummaryProps) {
  const stats = useMemo(() => {
    const resolved = entries.filter(e => e.status === 'won' || e.status === 'lost')
    const wins = resolved.filter(e => e.status === 'won').length
    const losses = resolved.filter(e => e.status === 'lost').length
    const pending = entries.filter(e => e.status === 'pending').length
    const totalPnl = entries.reduce((sum, e) => sum + Number(e.pnl), 0)
    const totalStake = resolved.reduce((sum, e) => sum + Number(e.stake), 0)
    const winRate = resolved.length > 0 ? (wins / resolved.length) * 100 : 0
    const roi = totalStake > 0 ? (totalPnl / totalStake) * 100 : 0

    // Per-slip-type breakdown
    const byType: Record<string, { entries: number; wins: number; pnl: number }> = {}
    for (const entry of entries) {
      if (!byType[entry.slip_type]) byType[entry.slip_type] = { entries: 0, wins: 0, pnl: 0 }
      byType[entry.slip_type].entries++
      if (entry.status === 'won') byType[entry.slip_type].wins++
      byType[entry.slip_type].pnl += Number(entry.pnl)
    }

    return { total: entries.length, wins, losses, pending, totalPnl, winRate, roi, byType }
  }, [entries])

  if (entries.length === 0) return null

  return (
    <div className="mb-6">
      {/* Top-level KPIs */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
        <div className="bg-slate-800 rounded-lg border border-slate-700 p-3">
          <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">Entries</div>
          <div className="text-xl font-bold text-slate-100">{stats.total}</div>
          {stats.pending > 0 && (
            <div className="text-xs text-yellow-400 mt-0.5">{stats.pending} pending</div>
          )}
        </div>
        <div className="bg-slate-800 rounded-lg border border-slate-700 p-3">
          <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">Win Rate</div>
          <div className="text-xl font-bold text-slate-100">
            {stats.winRate.toFixed(1)}%
          </div>
          <div className="text-xs text-slate-500 mt-0.5">{stats.wins}W – {stats.losses}L</div>
        </div>
        <div className="bg-slate-800 rounded-lg border border-slate-700 p-3">
          <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">P&L</div>
          <div className={`text-xl font-bold ${stats.totalPnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
            {stats.totalPnl >= 0 ? '+' : ''}${stats.totalPnl.toFixed(2)}
          </div>
        </div>
        <div className="bg-slate-800 rounded-lg border border-slate-700 p-3">
          <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">ROI</div>
          <div className={`text-xl font-bold ${stats.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
            {stats.roi >= 0 ? '+' : ''}{stats.roi.toFixed(1)}%
          </div>
        </div>
      </div>

      {/* Per-slip-type breakdown */}
      {Object.keys(stats.byType).length > 1 && (
        <div className="flex flex-wrap gap-3">
          {Object.entries(stats.byType).map(([type, data]) => {
            const config = USER_DFS_SLIP_TYPES[type as keyof typeof USER_DFS_SLIP_TYPES]
            return (
              <div key={type} className="bg-slate-800/50 rounded border border-slate-700 px-3 py-2 text-xs">
                <span className="text-slate-300 font-medium">{config?.label ?? type}</span>
                <span className="text-slate-500 ml-2">{data.entries} entries</span>
                <span className="text-slate-500 ml-1">{data.wins}W</span>
                <span className={`ml-2 font-medium ${data.pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {data.pnl >= 0 ? '+' : ''}${data.pnl.toFixed(2)}
                </span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
