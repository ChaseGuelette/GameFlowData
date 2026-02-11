'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { Navbar } from '@/components/layout/Navbar'
import { FilterTabs, type FilterOption } from '@/components/predictions/FilterTabs'
import { PropGrid } from '@/components/predictions/PropGrid'
import { AnalysisModal } from '@/components/analysis/AnalysisModal'
import { type Prediction } from '@/types/predictions'
import { getToday, formatDate } from '@/lib/utils'

// NBA team ID to abbreviation map
const TEAM_ABBREV: Record<number, string> = {
  1610612737: 'ATL', 1610612738: 'BOS', 1610612751: 'BKN',
  1610612766: 'CHA', 1610612741: 'CHI', 1610612739: 'CLE',
  1610612742: 'DAL', 1610612743: 'DEN', 1610612765: 'DET',
  1610612744: 'GSW', 1610612745: 'HOU', 1610612754: 'IND',
  1610612746: 'LAC', 1610612747: 'LAL', 1610612763: 'MEM',
  1610612748: 'MIA', 1610612749: 'MIL', 1610612750: 'MIN',
  1610612740: 'NOP', 1610612752: 'NYK', 1610612760: 'OKC',
  1610612753: 'ORL', 1610612755: 'PHI', 1610612756: 'PHX',
  1610612757: 'POR', 1610612758: 'SAC', 1610612759: 'SAS',
  1610612761: 'TOR', 1610612762: 'UTA', 1610612764: 'WAS',
}

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
      // Filter out NaN edges by checking line is not null (NaN edges have null lines)
      const { data: predictionsData, error: predictionsError } = await supabase
        .from('daily_predictions')
        .select('*')
        .eq('prediction_date', today)
        .not('line', 'is', null)
        .or('over_edge.gte.0.03,under_edge.gte.0.03')
        .order('over_edge', { ascending: false })

      if (!predictionsError && predictionsData) {
        // Map DB columns to frontend expected names and add team abbrevs
        // Note: player_name already exists in daily_predictions table
        const mappedPredictions = predictionsData
          .filter(p => {
            // Filter out any remaining NaN values
            const overEdge = p.over_edge
            const underEdge = p.under_edge
            return Number.isFinite(overEdge) || Number.isFinite(underEdge)
          })
          .map(p => ({
            ...p,
            // Map column names from DB to frontend expectations
            prop_line: p.line,
            model_prob_over: p.over_prob,
            model_prob_under: p.under_prob,
            implied_prob_over: p.implied_over,
            implied_prob_under: p.implied_under,
            q10: p.pred_q10,
            q25: p.pred_q25,
            q50: p.pred_q50,
            q75: p.pred_q75,
            q90: p.pred_q90,
            // Add team abbreviations
            team_abbrev: TEAM_ABBREV[p.team_id] || 'UNK',
            opponent_abbrev: TEAM_ABBREV[p.opponent_id] || 'UNK',
          }))

        setPredictions(mappedPredictions)
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
