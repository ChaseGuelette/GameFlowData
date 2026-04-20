'use client'

import { useState, useEffect } from 'react'
import { useTradeQueue, useTradeApproval } from '@/lib/hooks/useTradeQueue'
import { KALSHI_STAT_LABELS } from '@/types/bot-tracker'
import type { KalshiTradeQueueItem } from '@/types/bot-tracker'

function TimeRemaining({ expiresAt }: { expiresAt: string }) {
  const [remaining, setRemaining] = useState('')

  useEffect(() => {
    function update() {
      const diff = new Date(expiresAt).getTime() - Date.now()
      if (diff <= 0) {
        setRemaining('Expired')
        return
      }
      const mins = Math.floor(diff / 60000)
      const secs = Math.floor((diff % 60000) / 1000)
      setRemaining(`${mins}:${secs.toString().padStart(2, '0')}`)
    }
    update()
    const interval = setInterval(update, 1000)
    return () => clearInterval(interval)
  }, [expiresAt])

  const isLow = remaining !== 'Expired' && parseInt(remaining) < 5
  return (
    <span className={`text-xs font-mono ${remaining === 'Expired' ? 'text-red-400' : isLow ? 'text-yellow-400' : 'text-slate-400'}`}>
      {remaining}
    </span>
  )
}

function TradeRow({
  trade,
  selected,
  onToggle,
}: {
  trade: KalshiTradeQueueItem
  selected: boolean
  onToggle: (id: number) => void
}) {
  return (
    <tr className="hover:bg-slate-700/30">
      <td className="px-3 py-2">
        <input
          type="checkbox"
          checked={selected}
          onChange={() => onToggle(trade.id)}
          className="accent-blue-500"
        />
      </td>
      <td className="px-3 py-2">
        <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
          trade.sport === 'nba' ? 'bg-orange-500/20 text-orange-400' : 'bg-blue-500/20 text-blue-400'
        }`}>
          {trade.sport.toUpperCase()}
        </span>
      </td>
      <td className="px-3 py-2 text-slate-200 font-medium">{trade.player_name ?? '—'}</td>
      <td className="px-3 py-2 text-slate-300">{KALSHI_STAT_LABELS[trade.stat_type] ?? trade.stat_type}</td>
      <td className="px-3 py-2 text-slate-300">{Number(trade.line)}</td>
      <td className="px-3 py-2">
        <span className={`text-xs font-medium ${trade.side === 'yes' ? 'text-green-400' : 'text-red-400'}`}>
          {trade.side.toUpperCase()}
        </span>
      </td>
      <td className="px-3 py-2 text-slate-300">{trade.contracts}</td>
      <td className="px-3 py-2 text-slate-300">${Number(trade.expected_cost).toFixed(2)}</td>
      <td className="px-3 py-2 text-slate-300">
        {trade.fee_adjusted_edge != null ? `${(Number(trade.fee_adjusted_edge) * 100).toFixed(1)}%` : '—'}
      </td>
      <td className="px-3 py-2">
        <TimeRemaining expiresAt={trade.expires_at} />
      </td>
    </tr>
  )
}

export function TradeApprovalPanel() {
  const { data: trades = [], isLoading } = useTradeQueue()
  const { approve, reject, approveAll } = useTradeApproval()
  const [selected, setSelected] = useState<Set<number>>(new Set())

  // Reset selection when trades change
  useEffect(() => {
    setSelected(new Set())
  }, [trades])

  if (isLoading || trades.length === 0) return null

  const totalExposure = trades.reduce((sum, t) => sum + Number(t.expected_cost), 0)
  const allSelected = selected.size === trades.length && trades.length > 0

  const toggleOne = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const toggleAll = () => {
    if (allSelected) setSelected(new Set())
    else setSelected(new Set(trades.map((t) => t.id)))
  }

  const handleApprove = () => {
    if (selected.size === trades.length) {
      approveAll.mutate()
    } else {
      approve.mutate(Array.from(selected))
    }
  }

  const handleReject = () => {
    reject.mutate(Array.from(selected))
  }

  const isPending = approve.isPending || reject.isPending || approveAll.isPending

  return (
    <div className="rounded-lg border-2 border-yellow-500/50 bg-slate-800 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-slate-700 bg-yellow-500/5">
        <div>
          <h3 className="text-sm font-semibold text-yellow-400">
            Pending Trade Approval
          </h3>
          <p className="text-xs text-slate-400 mt-0.5">
            {trades.length} trade{trades.length !== 1 ? 's' : ''} pending
            {' | '}Total exposure: ${totalExposure.toFixed(2)}
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleReject}
            disabled={selected.size === 0 || isPending}
            className="px-3 py-1.5 text-xs font-medium rounded bg-red-500/20 text-red-400 hover:bg-red-500/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            Reject ({selected.size})
          </button>
          <button
            onClick={handleApprove}
            disabled={selected.size === 0 || isPending}
            className="px-3 py-1.5 text-xs font-medium rounded bg-green-500/20 text-green-400 hover:bg-green-500/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {selected.size === trades.length ? 'Approve All' : `Approve (${selected.size})`}
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-slate-400 bg-slate-900/50">
            <tr>
              <th className="px-3 py-2">
                <input
                  type="checkbox"
                  checked={allSelected}
                  onChange={toggleAll}
                  className="accent-blue-500"
                />
              </th>
              <th className="px-3 py-2">Sport</th>
              <th className="px-3 py-2">Player</th>
              <th className="px-3 py-2">Stat</th>
              <th className="px-3 py-2">Line</th>
              <th className="px-3 py-2">Side</th>
              <th className="px-3 py-2">Contracts</th>
              <th className="px-3 py-2">Cost</th>
              <th className="px-3 py-2">Edge</th>
              <th className="px-3 py-2">Expires</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {trades.map((trade) => (
              <TradeRow
                key={trade.id}
                trade={trade}
                selected={selected.has(trade.id)}
                onToggle={toggleOne}
              />
            ))}
          </tbody>
        </table>
      </div>

      {/* Status messages */}
      {(approve.isSuccess || approveAll.isSuccess) && (
        <div className="px-4 py-2 bg-green-500/10 text-green-400 text-xs">
          Trades approved and queued for execution.
        </div>
      )}
      {reject.isSuccess && (
        <div className="px-4 py-2 bg-red-500/10 text-red-400 text-xs">
          Trades rejected.
        </div>
      )}
      {(approve.isError || reject.isError || approveAll.isError) && (
        <div className="px-4 py-2 bg-red-500/10 text-red-400 text-xs">
          Error: {(approve.error ?? reject.error ?? approveAll.error)?.message}
        </div>
      )}
    </div>
  )
}
