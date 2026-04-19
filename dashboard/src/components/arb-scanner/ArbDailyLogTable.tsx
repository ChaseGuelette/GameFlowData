'use client'

import type { ArbDailyLog } from '@/types/arb-scanner'

interface ArbDailyLogTableProps {
  logs: ArbDailyLog[]
  loading: boolean
}

function formatDollars(dollars: number): string {
  const sign = dollars >= 0 ? '+' : ''
  return `${sign}$${Math.abs(dollars).toFixed(2)}`
}

export function ArbDailyLogTable({ logs, loading }: ArbDailyLogTableProps) {
  if (loading) {
    return (
      <div className="rounded-lg border border-slate-700 bg-slate-800 p-8 text-center text-slate-400">
        Loading daily logs...
      </div>
    )
  }

  return (
    <div className="rounded-lg border border-slate-700 bg-slate-800 overflow-hidden">
      <div className="px-4 py-3 border-b border-slate-700">
        <h3 className="text-sm font-semibold text-slate-200">Daily Arb P&L Log</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-slate-400 bg-slate-900/50">
            <tr>
              <th className="px-3 py-2">Date</th>
              <th className="px-3 py-2">Sport</th>
              <th className="px-3 py-2 text-right">Placed</th>
              <th className="px-3 py-2 text-right">Resolved</th>
              <th className="px-3 py-2 text-right">Profit</th>
              <th className="px-3 py-2 text-right">Loss</th>
              <th className="px-3 py-2 text-right">Cancelled</th>
              <th className="px-3 py-2 text-right">Daily P&L</th>
              <th className="px-3 py-2 text-right">Cumulative</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {logs.length === 0 ? (
              <tr>
                <td colSpan={9} className="px-3 py-8 text-center text-slate-500">
                  No daily logs found
                </td>
              </tr>
            ) : (
              logs.map((log) => (
                <tr
                  key={log.id}
                  className={`hover:bg-slate-700/30 ${log.total_pnl > 0 ? 'bg-green-500/5' : ''}`}
                >
                  <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                    {log.log_date}
                  </td>
                  <td className="px-3 py-2">
                    <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
                      log.sport === 'nba'
                        ? 'bg-orange-500/20 text-orange-400'
                        : log.sport === 'mlb'
                          ? 'bg-blue-500/20 text-blue-400'
                          : 'bg-slate-500/20 text-slate-400'
                    }`}>
                      {log.sport.toUpperCase()}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right text-slate-300">
                    {log.arbs_placed}
                  </td>
                  <td className="px-3 py-2 text-right text-slate-300">
                    {log.arbs_resolved}
                  </td>
                  <td className="px-3 py-2 text-right text-green-400">
                    {log.arbs_profit}
                  </td>
                  <td className="px-3 py-2 text-right text-red-400">
                    {log.arbs_loss}
                  </td>
                  <td className="px-3 py-2 text-right text-slate-400">
                    {log.arbs_cancelled}
                  </td>
                  <td
                    className={`px-3 py-2 text-right font-medium ${
                      Number(log.total_pnl) >= 0 ? 'text-green-400' : 'text-red-400'
                    }`}
                  >
                    {formatDollars(Number(log.total_pnl))}
                  </td>
                  <td
                    className={`px-3 py-2 text-right font-medium ${
                      Number(log.cumulative_pnl) >= 0 ? 'text-green-400' : 'text-red-400'
                    }`}
                  >
                    {formatDollars(Number(log.cumulative_pnl))}
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
