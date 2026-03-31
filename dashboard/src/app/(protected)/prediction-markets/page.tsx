'use client'

import { useEffect, useState, useMemo } from 'react'
import { createClient } from '@/lib/supabase/client'
import { useSport } from '@/contexts/SportContext'
import { KalshiMarketsTable } from '@/components/prediction-markets/KalshiMarketsTable'
import type { KalshiMarket } from '@/types/kalshi'

export default function PredictionMarketsPage() {
  const supabase = createClient()
  const { sport, config } = useSport()
  const [markets, setMarkets] = useState<KalshiMarket[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function fetchMarkets() {
      setLoading(true)
      setError(null)

      try {
        const today = new Date().toISOString().split('T')[0]

        const { data, error: rpcError } = await supabase
          .rpc('get_kalshi_edges', { p_date: today, p_sport: sport })

        if (rpcError) {
          // If RPC doesn't exist yet or subscription check fails, fall back to direct query
          if (rpcError.message?.includes('subscription')) {
            setError('Active subscription required to view prediction markets.')
          } else {
            // Fallback: direct table query (public read RLS)
            const { data: fallbackData, error: fallbackError } = await supabase
              .from('kalshi_markets')
              .select('*')
              .eq('sport', sport)
              .gte('snapshot_time', `${today}T00:00:00`)
              .eq('market_status', 'open')
              .order('snapshot_time', { ascending: false })

            if (fallbackError) {
              setError('Failed to load markets.')
            } else {
              // Deduplicate by ticker (keep latest snapshot)
              const seen = new Set<string>()
              const deduped = (fallbackData || []).filter((m: KalshiMarket) => {
                if (seen.has(m.ticker)) return false
                seen.add(m.ticker)
                return true
              })
              setMarkets(deduped)
            }
          }
        } else {
          setMarkets(data || [])
        }
      } catch {
        setError('Failed to load prediction markets.')
      } finally {
        setLoading(false)
      }
    }

    fetchMarkets()

    // Refresh every 2 minutes
    const interval = setInterval(fetchMarkets, 120_000)
    return () => clearInterval(interval)
  }, [sport, supabase])

  // Summary stats
  const summary = useMemo(() => {
    const total = markets.length
    const withEdge = markets.filter(
      (m) => m.maker_fee_adjusted_edge != null && m.maker_fee_adjusted_edge > 0
    ).length
    const bestEdge = markets.reduce((max, m) => {
      const e = m.maker_fee_adjusted_edge ?? -Infinity
      return e > max ? e : max
    }, -Infinity)
    const totalVolume = markets.reduce((sum, m) => sum + m.volume, 0)

    return { total, withEdge, bestEdge: bestEdge > -Infinity ? bestEdge : null, totalVolume }
  }, [markets])

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-slate-100">Prediction Markets</h1>
        <p className="text-sm text-slate-400 mt-1">
          Kalshi {sport.toUpperCase()} player prop contracts with model edge overlay
        </p>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
          <div className="text-xs text-slate-400">Total Markets</div>
          <div className="text-2xl font-bold text-slate-100 mt-1">
            {loading ? '--' : summary.total}
          </div>
        </div>
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
          <div className="text-xs text-slate-400">Markets with Edge</div>
          <div className="text-2xl font-bold text-green-400 mt-1">
            {loading ? '--' : summary.withEdge}
          </div>
        </div>
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
          <div className="text-xs text-slate-400">Best Edge Today</div>
          <div className="text-2xl font-bold text-green-400 mt-1">
            {loading || !summary.bestEdge ? '--' : `${(summary.bestEdge * 100).toFixed(1)}%`}
          </div>
        </div>
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
          <div className="text-xs text-slate-400">Total Volume</div>
          <div className="text-2xl font-bold text-slate-100 mt-1">
            {loading ? '--' : summary.totalVolume.toLocaleString()}
          </div>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-red-900/20 border border-red-700/50 rounded-lg p-4 mb-6 text-red-300 text-sm">
          {error}
        </div>
      )}

      {/* Loading State */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
          <span className="ml-3 text-slate-400">Loading markets...</span>
        </div>
      )}

      {/* Empty State */}
      {!loading && !error && markets.length === 0 && (
        <div className="text-center py-12 bg-slate-800 border border-slate-700 rounded-lg">
          <div className="text-4xl mb-3">📊</div>
          <h3 className="text-lg font-medium text-slate-300 mb-1">No Markets Available</h3>
          <p className="text-sm text-slate-500">
            {sport.toUpperCase()} prediction markets will appear here once Kalshi data is scraped.
            Markets refresh every 10 minutes during game hours (11 AM - 11 PM ET).
          </p>
        </div>
      )}

      {/* Markets Table */}
      {!loading && !error && markets.length > 0 && (
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
          <KalshiMarketsTable markets={markets} />
        </div>
      )}

      {/* Info Footer */}
      <div className="mt-4 text-xs text-slate-500 space-y-1">
        <p>
          Kalshi is a CFTC-regulated prediction market. YES/NO contracts are priced in cents (1-99).
          NO (under) contracts are only available via the Kalshi API.
        </p>
        <p>
          Edge = Model Probability - Kalshi Implied - Fees. Maker fees are ~75% cheaper than taker fees.
        </p>
      </div>
    </div>
  )
}
