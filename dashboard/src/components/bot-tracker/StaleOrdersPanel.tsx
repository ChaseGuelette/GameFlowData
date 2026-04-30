'use client'

import { useState, useEffect, useCallback } from 'react'

interface StaleOrder {
  id: number
  kalshi_order_id: string
  game_date: string
  ticker: string
  sport: string | null
  player_name: string | null
  stat_type: string | null
  line: number | null
  side: string | null
  contracts: number | null
  expected_cost: number | null
  game_start_time: string | null
  detected_at: string
  status: string
}

function minutesAgo(isoString: string | null): string {
  if (!isoString) return '?'
  const mins = Math.floor((Date.now() - new Date(isoString).getTime()) / 60000)
  if (mins < 1) return '<1'
  return String(mins)
}

export function StaleOrdersPanel() {
  const [orders, setOrders] = useState<StaleOrder[]>([])
  const [loading, setLoading] = useState(true)
  const [acting, setActing] = useState(false)
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set())

  const fetchOrders = useCallback(async () => {
    try {
      const res = await fetch('/api/kalshi/cancel-queue')
      const data = await res.json()
      const newOrders: StaleOrder[] = data.orders ?? []
      setOrders(newOrders)
      // Prune selection to only IDs still present
      const validIds = new Set(newOrders.map((o) => o.id))
      setSelectedIds((prev) => new Set([...prev].filter((id) => validIds.has(id))))
    } catch (e) {
      console.error('Failed to fetch cancel queue:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchOrders()
    const interval = setInterval(fetchOrders, 30_000)
    return () => clearInterval(interval)
  }, [fetchOrders])

  const act = useCallback(async (
    action: 'approve' | 'reject' | 'approve_all',
    ids?: number[]
  ) => {
    setActing(true)
    try {
      await fetch('/api/kalshi/cancel-approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, order_ids: ids }),
      })
      await fetchOrders()
    } finally {
      setActing(false)
    }
  }, [fetchOrders])

  const toggleSelect = (id: number) => {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const toggleAll = () => {
    if (selectedIds.size === orders.length) {
      setSelectedIds(new Set())
    } else {
      setSelectedIds(new Set(orders.map((o) => o.id)))
    }
  }

  if (loading) {
    return (
      <div className="rounded-lg border border-yellow-500/20 bg-yellow-950/10 p-4">
        <p className="text-sm text-gray-400">Loading stale orders...</p>
      </div>
    )
  }

  if (orders.length === 0) {
    return null
  }

  const allSelected = selectedIds.size === orders.length
  const someSelected = selectedIds.size > 0 && !allSelected

  return (
    <div className="rounded-lg border border-yellow-500/30 bg-yellow-950/10 p-4">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={allSelected}
            ref={(el) => { if (el) el.indeterminate = someSelected }}
            onChange={toggleAll}
            className="h-3.5 w-3.5 cursor-pointer accent-yellow-400"
          />
          <h3 className="text-sm font-semibold text-yellow-400">
            ⚠️ Stale Orders — Pending Cancellation
            <span className="ml-2 rounded-full bg-yellow-500/20 px-2 py-0.5 text-xs text-yellow-300">
              {orders.length}
            </span>
          </h3>
        </div>
        <div className="flex gap-2">
          {selectedIds.size > 0 && (
            <button
              onClick={() => act('approve', [...selectedIds])}
              disabled={acting}
              className="text-xs rounded bg-red-700 px-3 py-1 text-white hover:bg-red-600 disabled:opacity-50 transition-colors"
            >
              Cancel Selected ({selectedIds.size})
            </button>
          )}
          <button
            onClick={() => act('approve_all')}
            disabled={acting}
            className="text-xs rounded bg-red-900 px-3 py-1 text-white hover:bg-red-800 disabled:opacity-50 transition-colors"
          >
            Cancel All
          </button>
        </div>
      </div>

      <div className="space-y-2">
        {orders.map((order) => (
          <div
            key={order.id}
            className={`flex items-center justify-between rounded border px-3 py-2 transition-colors ${
              selectedIds.has(order.id)
                ? 'border-yellow-400/40 bg-yellow-900/20'
                : 'border-yellow-500/20 bg-yellow-900/10'
            }`}
          >
            <div className="flex items-center gap-3">
              <input
                type="checkbox"
                checked={selectedIds.has(order.id)}
                onChange={() => toggleSelect(order.id)}
                className="h-3.5 w-3.5 cursor-pointer accent-yellow-400 shrink-0"
              />
              <div className="space-y-0.5 text-xs">
                <div className="font-medium text-white">
                  {order.player_name ?? order.ticker}
                </div>
                <div className="text-gray-400">
                  {[
                    order.stat_type,
                    order.line != null ? `@ ${order.line}` : null,
                    order.side,
                    order.contracts != null ? `${order.contracts} contracts` : null,
                    order.expected_cost != null ? `$${order.expected_cost.toFixed(2)}` : null,
                  ].filter(Boolean).join(' · ')}
                </div>
                <div className="text-yellow-400/80">
                  Game started {minutesAgo(order.game_start_time)} min ago
                </div>
              </div>
            </div>
            <div className="flex gap-2 ml-4 shrink-0">
              <button
                onClick={() => act('approve', [order.id])}
                disabled={acting}
                className="rounded bg-red-700 px-3 py-1 text-xs text-white hover:bg-red-600 disabled:opacity-50 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={() => act('reject', [order.id])}
                disabled={acting}
                className="rounded bg-gray-700 px-3 py-1 text-xs text-gray-200 hover:bg-gray-600 disabled:opacity-50 transition-colors"
              >
                Keep
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
