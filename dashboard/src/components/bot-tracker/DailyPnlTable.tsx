'use client'

import type { KalshiLiveDailyLog, KalshiPaperDailyLog } from '@/types/bot-tracker'

interface DailyPnlTableProps {
  logs: (KalshiLiveDailyLog | KalshiPaperDailyLog)[]
  loading: boolean
}

function formatDollars(cents: number): string {
  const dollars = cents / 100
  const sign = dollars >= 0 ? '+' : ''
  return `${sign}$${Math.abs(dollars).toFixed(2)}`
}

export function DailyPnlTable({ logs, loading }: DailyPnlTableProps) {
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
        <h3 className="text-sm font-semibold text-slate-200">Daily P&L Log</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-slate-400 bg-slate-900/50">
            <tr>
              <th className="px-3 py-2">Date</th>
              <th className="px-3 py-2 text-right">Trades</th>
              <th className="px-3 py-2 text-right">Won</th>
              <th className="px-3 py-2 text-right">Lost</th>
              <th className="px-3 py-2 text-right">Daily P&L</th>
              <th className="px-3 py-2 text-right">Cumulative</th>
              <th className="px-3 py-2 text-right">ROI</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {logs.length === 0 ? (
              <tr>
                <td colSpan={7} className="px-3 py-8 text-center text-slate-500">
                  No daily logs found
                </td>
              </tr>
            ) : (
              logs.map((log) => (
                <tr key={log.game_date} className="hover:bg-slate-700/30">
                  <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                    {log.game_date}
                  </td>
                  <td className="px-3 py-2 text-right text-slate-300">
                    {log.total_trades}
                  </td>
                  <td className="px-3 py-2 text-right text-green-400">
                    {log.trades_won}
                  </td>
                  <td className="px-3 py-2 text-right text-red-400">
                    {log.trades_lost}
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
                  <td
                    className={`px-3 py-2 text-right ${
                      Number(log.roi_pct) >= 0 ? 'text-green-400' : 'text-red-400'
                    }`}
                  >
                    {Number(log.roi_pct).toFixed(1)}%
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
