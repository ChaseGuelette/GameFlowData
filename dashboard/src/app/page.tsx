'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { Navbar } from '@/components/layout/Navbar'
import { FilterTabs, type FilterOption } from '@/components/predictions/FilterTabs'
import { PropGrid } from '@/components/predictions/PropGrid'
import { AnalysisModal } from '@/components/analysis/AnalysisModal'
import { type Prediction } from '@/types/predictions'
import { getToday, formatDate } from '@/lib/utils'

export default function HomePage() {
  const [predictions, setPredictions] = useState<Prediction[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<FilterOption>('all')
  const [selectedPrediction, setSelectedPrediction] = useState<Prediction | null>(null)
  const [bankroll, setBankroll] = useState<number | undefined>(undefined)

  useEffect(() => {
    async function fetchData() {
      const supabase = createClient()
      const today = getToday()

      // Fetch predictions for today
      const { data: predictionsData, error: predictionsError } = await supabase
        .from('daily_predictions')
        .select('*')
        .eq('prediction_date', today)
        .or('over_edge.gte.0.03,under_edge.gte.0.03')
        .order('over_edge', { ascending: false })

      if (!predictionsError && predictionsData) {
        // Enrich with player names from players table
        const playerIds = [...new Set(predictionsData.map(p => p.player_id))]

        if (playerIds.length > 0) {
          const { data: playersData } = await supabase
            .from('players')
            .select('player_id, full_name')
            .in('player_id', playerIds)

          const playerMap = new Map(
            playersData?.map(p => [p.player_id, p.full_name]) || []
          )

          const enrichedPredictions = predictionsData.map(p => ({
            ...p,
            player_name: playerMap.get(p.player_id) || `Player ${p.player_id}`,
          }))

          setPredictions(enrichedPredictions)
        } else {
          setPredictions(predictionsData)
        }
      }

      // Fetch latest bankroll from paper trading log
      const { data: logData } = await supabase
        .from('paper_trading_daily_log')
        .select('bankroll')
        .order('game_date', { ascending: false })
        .limit(1)
        .single()

      if (logData?.bankroll) {
        setBankroll(logData.bankroll)
      }

      setLoading(false)
    }

    fetchData()
  }, [])

  // Filter predictions by stat type
  const filteredPredictions = filter === 'all'
    ? predictions
    : predictions.filter(p => p.stat === filter)

  // Sort by max edge
  const sortedPredictions = [...filteredPredictions].sort((a, b) => {
    const aEdge = Math.max(a.over_edge, a.under_edge)
    const bEdge = Math.max(b.over_edge, b.under_edge)
    return bEdge - aEdge
  })

  return (
    <div className="min-h-screen flex flex-col">
      <Navbar bankroll={bankroll} />

      <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold text-slate-50">Today&apos;s Props</h1>
            <p className="text-slate-400">{formatDate(new Date())}</p>
          </div>
          <FilterTabs activeFilter={filter} onFilterChange={setFilter} />
        </div>

        {/* Content */}
        {loading ? (
          <div className="flex items-center justify-center py-16">
            <div className="text-slate-400">Loading predictions...</div>
          </div>
        ) : (
          <PropGrid
            predictions={sortedPredictions}
            onAnalyze={setSelectedPrediction}
          />
        )}
      </main>

      {/* Analysis Modal */}
      {selectedPrediction && (
        <AnalysisModal
          prediction={selectedPrediction}
          onClose={() => setSelectedPrediction(null)}
        />
      )}
    </div>
  )
}
