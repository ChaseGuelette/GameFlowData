'use client'

import { useEffect, useState, useCallback } from 'react'
import { createClient } from '@/lib/supabase/client'
import { Navbar } from '@/components/layout/Navbar'
import { FilterTabs, type FilterOption } from '@/components/predictions/FilterTabs'
import { PropGrid } from '@/components/predictions/PropGrid'
import { PlayOfTheDay } from '@/components/predictions/PlayOfTheDay'
import { AnalysisModal } from '@/components/analysis/AnalysisModal'
import { type Prediction } from '@/types/predictions'
import { getToday, formatDate, calculateBLConfidence, blendProbability } from '@/lib/utils'

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
  const [teamFilter, setTeamFilter] = useState<string>('all')
  const [selectedPrediction, setSelectedPrediction] = useState<Prediction | null>(null)
  const [bankroll, setBankroll] = useState<number | undefined>(undefined)
  const [selectedDate, setSelectedDate] = useState<string>(getToday())
  const [availableDates, setAvailableDates] = useState<string[]>([])
  const [edgeThreshold, setEdgeThreshold] = useState<number>(0.03)  // Default 3%
  const [blTau, setBlTau] = useState<number | null>(null)  // null = no BL blending
  const [showModelPicks, setShowModelPicks] = useState<boolean>(false)  // Model Picks toggle

  // Fetch predictions for a specific date
  const fetchPredictions = useCallback(async (date: string) => {
    setLoading(true)
    const supabase = createClient()

    console.log('Fetching predictions for date:', date)
    // Fetch all predictions with prop lines (filtering done client-side based on user selection)
    const { data: predictionsData, error: predictionsError } = await supabase
      .from('daily_predictions')
      .select('*')
      .eq('prediction_date', date)
      .not('line', 'is', null)
      .order('over_edge', { ascending: false })

    console.log('Predictions result:', {
      count: predictionsData?.length || 0,
      error: predictionsError?.message || null,
      date
    })

    if (!predictionsError && predictionsData) {
      // Map DB columns to frontend expected names and add team abbrevs
      const mappedPredictions = predictionsData
        .filter(p => {
          const overEdge = p.over_edge
          const underEdge = p.under_edge
          return Number.isFinite(overEdge) || Number.isFinite(underEdge)
        })
        .map(p => ({
          ...p,
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
          pred_mean: p.pred_mean,
          pred_std: p.pred_std,
          team_abbrev: TEAM_ABBREV[p.team_id] || 'UNK',
          opponent_abbrev: TEAM_ABBREV[p.opponent_id] || 'UNK',
        }))

      setPredictions(mappedPredictions)
    } else {
      setPredictions([])
    }

    setLoading(false)
  }, [])

  // Initial load: fetch available dates and bankroll
  useEffect(() => {
    async function fetchInitialData() {
      const supabase = createClient()

      // Fetch available dates using RPC function (efficient distinct query)
      const { data: datesData, error: datesError } = await supabase
        .rpc('get_prediction_dates', { days_back: 30 })

      if (!datesError && datesData) {
        const uniqueDates = datesData.map((d: { prediction_date: string }) => d.prediction_date)
        setAvailableDates(uniqueDates)

        // If today has no predictions, default to most recent date
        const today = getToday()
        if (uniqueDates.length > 0 && !uniqueDates.includes(today)) {
          setSelectedDate(uniqueDates[0])
        }
      } else if (datesError) {
        console.error('Error fetching dates:', datesError)
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
    }

    fetchInitialData()
  }, [])

  // Fetch predictions when selectedDate changes
  useEffect(() => {
    if (selectedDate) {
      fetchPredictions(selectedDate)
    }
  }, [selectedDate, fetchPredictions])

  // Get unique matchups from predictions for dropdown (e.g., "LAL vs SAS")
  const availableMatchups = [...new Set(predictions.map(p => {
    const teams = [p.team_abbrev || 'UNK', p.opponent_abbrev || 'UNK'].sort()
    return `${teams[0]} vs ${teams[1]}`
  }))].sort()

  // Filter predictions by stat type, matchup, edge threshold, and Model Picks toggle
  const filteredPredictions = predictions.filter(p => {
    // Model Picks filter (uses server-computed is_recommended flag)
    if (showModelPicks && !p.is_recommended) return false

    // Stat type filter
    if (filter !== 'all' && p.stat !== filter) return false

    // Team/matchup filter
    if (teamFilter !== 'all') {
      const matchupTeams = teamFilter.split(' vs ')
      if (!matchupTeams.includes(p.team_abbrev || '') && !matchupTeams.includes(p.opponent_abbrev || '')) {
        return false
      }
    }

    // When viewing Model Picks, use BL edge; otherwise use raw edge with optional client-side BL
    let effectiveOverEdge = p.over_edge
    let effectiveUnderEdge = p.under_edge

    if (showModelPicks && p.bl_over_edge != null && p.bl_under_edge != null) {
      // Use server-computed BL edges for Model Picks
      effectiveOverEdge = p.bl_over_edge
      effectiveUnderEdge = p.bl_under_edge
    } else if (blTau !== null && p.pred_mean && p.pred_std) {
      // Client-side BL blending for custom exploration
      const confidence = calculateBLConfidence(p.pred_mean, p.pred_std, p.prop_line)
      const blendedOver = blendProbability(p.model_prob_over, p.implied_prob_over, blTau, confidence)
      const blendedUnder = blendProbability(p.model_prob_under, p.implied_prob_under, blTau, confidence)
      effectiveOverEdge = blendedOver - p.implied_prob_over
      effectiveUnderEdge = blendedUnder - p.implied_prob_under
    }

    // Skip edge threshold for Model Picks (is_recommended already guarantees BL edge >= 9%)
    if (showModelPicks) return true

    // At least one direction must meet edge threshold
    return Math.max(effectiveOverEdge, effectiveUnderEdge) >= edgeThreshold
  })

  // Sort by max edge (use BL edges when viewing Model Picks)
  const sortedPredictions = [...filteredPredictions].sort((a, b) => {
    let aEdge: number, bEdge: number

    if (showModelPicks && a.bl_over_edge != null && a.bl_under_edge != null) {
      aEdge = Math.max(a.bl_over_edge, a.bl_under_edge)
    } else {
      aEdge = Math.max(a.over_edge, a.under_edge)
    }

    if (showModelPicks && b.bl_over_edge != null && b.bl_under_edge != null) {
      bEdge = Math.max(b.bl_over_edge, b.bl_under_edge)
    } else {
      bEdge = Math.max(b.over_edge, b.under_edge)
    }

    return bEdge - aEdge
  })

  return (
    <div className="min-h-screen flex flex-col">
      <Navbar bankroll={bankroll} />

      <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
          <div>
            <h1 className="text-2xl font-bold text-slate-50">
              {showModelPicks
                ? "Model Picks"
                : selectedDate === getToday()
                  ? "Today's Props"
                  : "Historical Props"
              }
            </h1>
            <p className="text-slate-400">
              {formatDate(selectedDate)} • {sortedPredictions.length} {showModelPicks ? 'recommended' : 'picks'}
              {showModelPicks && <span className="text-green-400 ml-2">BL Edge ≥9%</span>}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            {/* Model Picks Toggle */}
            <div className="flex bg-slate-800 rounded-lg p-0.5 border border-slate-700">
              <button
                onClick={() => setShowModelPicks(false)}
                className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                  !showModelPicks
                    ? 'bg-slate-700 text-slate-100'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                All Bets
              </button>
              <button
                onClick={() => setShowModelPicks(true)}
                className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                  showModelPicks
                    ? 'bg-green-600 text-white'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                Model Picks
              </button>
            </div>
            {/* Date Selector */}
            <select
              value={selectedDate}
              onChange={(e) => setSelectedDate(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
            >
              {availableDates.map((date) => (
                <option key={date} value={date}>
                  {date === getToday() ? `${formatDate(date)} (Today)` : formatDate(date)}
                </option>
              ))}
            </select>
            {/* Matchup Filter */}
            <select
              value={teamFilter}
              onChange={(e) => setTeamFilter(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
            >
              <option value="all">All Games</option>
              {availableMatchups.map((matchup) => (
                <option key={matchup} value={matchup}>{matchup}</option>
              ))}
            </select>
            {/* Edge Threshold Filter */}
            <select
              value={edgeThreshold}
              onChange={(e) => setEdgeThreshold(Number(e.target.value))}
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
            >
              <option value={0}>Edge: All</option>
              <option value={0.03}>Edge: ≥3%</option>
              <option value={0.05}>Edge: ≥5% (Rec)</option>
              <option value={0.07}>Edge: ≥7%</option>
              <option value={0.10}>Edge: ≥10%</option>
              <option value={0.15}>Edge: ≥15%</option>
              <option value={0.20}>Edge: ≥20%</option>
            </select>
            {/* BL Tau Filter */}
            <select
              value={blTau === null ? 'none' : blTau}
              onChange={(e) => setBlTau(e.target.value === 'none' ? null : Number(e.target.value))}
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
            >
              <option value="none">BL: Off</option>
              <option value={0.03}>BL: τ=0.03</option>
              <option value={0.05}>BL: τ=0.05</option>
              <option value={0.10}>BL: τ=0.10 (Rec)</option>
              <option value={0.15}>BL: τ=0.15</option>
              <option value={0.25}>BL: τ=0.25</option>
            </select>
            <FilterTabs activeFilter={filter} onFilterChange={setFilter} />
          </div>
        </div>

        {/* Play of the Day */}
        {!loading && sortedPredictions.length > 0 && (
          <PlayOfTheDay
            prediction={sortedPredictions[0]}
            onAnalyze={setSelectedPrediction}
          />
        )}

        {/* Content */}
        {loading ? (
          <div className="flex items-center justify-center py-16">
            <div className="flex flex-col items-center gap-3">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
              <div className="text-slate-400">Loading predictions...</div>
            </div>
          </div>
        ) : sortedPredictions.length === 0 ? (
          <div className="flex items-center justify-center py-16">
            <div className="text-center">
              <p className="text-slate-400 text-lg">No predictions available for {formatDate(selectedDate)}</p>
              <p className="text-slate-500 text-sm mt-2">Try selecting a different date</p>
            </div>
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
