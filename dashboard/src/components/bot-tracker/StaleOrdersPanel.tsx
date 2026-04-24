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

  const fetchOrders = useCallback(async () => {
    try {
      const res = await fetch('/api/kalshi/cancel-queue')
      const data = await res.json()
      setOrders(data.orders ?? [])
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

  return (
    <div className="rounded-lg border border-yellow-500/30 bg-yellow-950/10 p-4">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold text-yellow-400">
          ⚠️ Stale Orders — Pending Cancellation
          <span className="ml-2 rounded-full bg-yellow-500/20 px-2 py-0.5 text-xs text-yellow-300">
            {orders.length}
          </span>
        </h3>
        <button
          onClick={() => act('approve_all')}
          disabled={acting}
          className="text-xs rounded bg-red-700 px-3 py-1 text-white hover:bg-red-600 disabled:opacity-50 transition-colors"
        >
          Cancel All
        </button>
      </div>

      <div className="space-y-2">
        {orders.map((order) => (
          <div
            key={order.id}
            className="flex items-center justify-between rounded border border-yellow-500/20 bg-yellow-900/10 px-3 py-2"
          >
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
            <div className="flex gap-2 ml-4 shrink-0">
              <button
                onClick={() => act('approve', [order.id])}
                disabled={acting}
                className="rounded bg-red-700 px-3 py-1 text-xs text-white hover:bg-red-600 disabled:opacity-50 transition-colors"
              >
                Cancel Order
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
