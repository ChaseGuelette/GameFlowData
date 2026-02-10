'use client'

import { type StatPerformance, STAT_COLORS, STAT_LABELS } from '@/types/predictions'
import { cn } from '@/lib/utils'

interface StatBreakdownProps {
  stats: StatPerformance[]
}

export function StatBreakdown({ stats }: StatBreakdownProps) {
  if (stats.length === 0) {
    return (
      <div className="bg-slate-800 rounded-lg border border-slate-700 p-6">
        <h3 className="text-lg font-medium text-slate-50 mb-4">Performance by Stat</h3>
        <div className="text-center py-8 text-slate-500">
          No betting data available
        </div>
      </div>
    )
  }

  return (
    <div className="bg-slate-800 rounded-lg border border-slate-700 p-6">
      <h3 className="text-lg font-medium text-slate-50 mb-4">Performance by Stat</h3>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="text-left text-xs text-slate-400 border-b border-slate-700">
              <th className="pb-3 font-medium">Stat</th>
              <th className="pb-3 font-medium text-right">Bets</th>
              <th className="pb-3 font-medium text-right">W-L</th>
              <th className="pb-3 font-medium text-right">Win %</th>
              <th className="pb-3 font-medium text-right">P&L</th>
              <th className="pb-3 font-medium text-right">ROI</th>
            </tr>
          </thead>
          <tbody className="text-sm">
            {stats.map((stat) => (
              <tr key={stat.stat} className="border-b border-slate-700/50">
                <td className="py-3">
                  <span className={cn(
                    'inline-block px-2 py-0.5 rounded text-xs text-white',
                    STAT_COLORS[stat.stat]
                  )}>
                    {STAT_LABELS[stat.stat]}
                  </span>
                </td>
                <td className="py-3 text-right text-slate-300">{stat.total_bets}</td>
                <td className="py-3 text-right text-slate-300">
                  <span className="text-green-400">{stat.wins}</span>
                  <span className="text-slate-500">-</span>
                  <span className="text-red-400">{stat.losses}</span>
                </td>
                <td className="py-3 text-right text-slate-300">
                  {stat.win_rate.toFixed(1)}%
                </td>
                <td className={cn(
                  'py-3 text-right font-medium',
                  stat.total_pnl >= 0 ? 'text-green-400' : 'text-red-400'
                )}>
                  {stat.total_pnl >= 0 ? '+' : ''}${stat.total_pnl.toFixed(2)}
                </td>
                <td className={cn(
                  'py-3 text-right font-medium',
                  stat.roi >= 0 ? 'text-green-400' : 'text-red-400'
                )}>
                  {stat.roi >= 0 ? '+' : ''}{stat.roi.toFixed(1)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
