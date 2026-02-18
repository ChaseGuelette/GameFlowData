'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { HistoryFilters, type StatusFilter } from '@/components/history/HistoryFilters'
import { HistorySummary } from '@/components/history/HistorySummary'
import { BetList } from '@/components/history/BetList'
import { BetSourceFilter, type BetSource } from '@/components/shared/BetSourceFilter'
import { type PaperBet } from '@/types/predictions'

// Extended type to include is_recommended and bookmaker from joined daily_predictions
interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
  bookmaker?: string
}

export default function HistoryPage() {
  const [bets, setBets] = useState<PaperBetWithRecommended[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<StatusFilter>('all')
  const [betSource, setBetSource] = useState<BetSource>('model') // Default to Model Picks

  useEffect(() => {
    async function fetchData() {
      const supabase = createClient()

      // Fetch all resolved bets (last 30 days)
      const thirtyDaysAgo = new Date()
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30)
      const startDate = thirtyDaysAgo.toISOString().split('T')[0]

      const { data: betsData, error: betsError } = await supabase
        .from('paper_bets')
        .select('*')
        .gte('game_date', startDate)
        .order('game_date', { ascending: false })

      if (!betsError && betsData) {
        // Get unique prediction IDs to fetch is_recommended status
        const predictionIds = [...new Set(betsData.map(b => b.prediction_id).filter(Boolean))]

        // Fetch is_recommended and bookmaker for these predictions
        const { data: predictionsData } = await supabase
          .from('daily_predictions')
          .select('id, is_recommended, bookmaker')
          .in('id', predictionIds)

        // Create maps for quick lookup (use string keys for consistent bigint handling)
        const recommendedMap = new Map<string, boolean>()
        const bookmakerMap = new Map<string, string>()
        if (predictionsData) {
          for (const p of predictionsData) {
            const key = String(p.id)
            recommendedMap.set(key, p.is_recommended ?? false)
            if (p.bookmaker) {
              bookmakerMap.set(key, p.bookmaker)
            }
          }
        }

        // Merge is_recommended and bookmaker into bets
        const enrichedBets = betsData.map(bet => {
          const key = String(bet.prediction_id)
          return {
            ...bet,
            is_recommended: recommendedMap.get(key) ?? false,
            bookmaker: bookmakerMap.get(key),
          }
        }) as PaperBetWithRecommended[]

        setBets(enrichedBets)
      }

      setLoading(false)
    }

    fetchData()
  }, [])

  // Filter bets by source (Model Picks = is_recommended from daily_predictions)
  const sourcedBets = betSource === 'model'
    ? bets.filter(b => b.is_recommended === true)
    : bets

  // Filter bets by status
  const filteredBets = filter === 'all'
    ? sourcedBets.filter(b => b.status !== 'pending' && b.status !== 'cancelled')
    : sourcedBets.filter(b => b.status === filter)

  return (
    <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="flex flex-col gap-4 mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-slate-50">Bet History</h1>
            <p className="text-slate-400">Last 30 days</p>
          </div>
          <BetSourceFilter activeSource={betSource} onSourceChange={setBetSource} />
        </div>
        <div className="flex justify-end">
          <HistoryFilters activeFilter={filter} onFilterChange={setFilter} />
        </div>
      </div>

      {/* Content */}
      {loading ? (
        <div className="flex items-center justify-center py-16">
          <div className="text-slate-400">Loading history...</div>
        </div>
      ) : (
        <>
          <HistorySummary bets={sourcedBets} />
          <BetList bets={filteredBets} />
        </>
      )}
    </main>
  )
}
