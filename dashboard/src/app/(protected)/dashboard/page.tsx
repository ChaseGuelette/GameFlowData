'use client'

import { useEffect, useState, useCallback } from 'react'
import { createClient } from '@/lib/supabase/client'
import { FilterTabs, type FilterOption } from '@/components/predictions/FilterTabs'
import { PropGrid } from '@/components/predictions/PropGrid'
import { PlayOfTheDay } from '@/components/predictions/PlayOfTheDay'
import { AnalysisModal } from '@/components/analysis/AnalysisModal'
import { SlateModal } from '@/components/predictions/SlateModal'
import { TonightsGames, type GameInfo } from '@/components/predictions/TonightsGames'
import { type Prediction } from '@/types/predictions'
import { getToday, formatDate, calculateBLConfidence, blendProbability } from '@/lib/utils'
import { TEAM_ABBREV } from '@/lib/constants'
import { US_STATES } from '@/lib/sportsbook-availability'

export default function DashboardPage() {
  const [predictions, setPredictions] = useState<Prediction[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<FilterOption>('all')
  const [teamFilter, setTeamFilter] = useState<string>('all')
  const [selectedPrediction, setSelectedPrediction] = useState<Prediction | null>(null)
  const [selectedDate, setSelectedDate] = useState<string>(getToday())
  const [availableDates, setAvailableDates] = useState<string[]>([])
  const [edgeThreshold, setEdgeThreshold] = useState<number>(0.03)  // Default 3%
  const [blTau, setBlTau] = useState<number | null>(null)  // null = no BL blending
  const [showModelPicks, setShowModelPicks] = useState<boolean>(false)  // Model Picks toggle
  const [userState, setUserState] = useState<string>(() => {
    if (typeof window === 'undefined') return ''
    return localStorage.getItem('user_state') || ''
  })

  // Taken bets state (persisted per date in localStorage)
  const [takenBets, setTakenBets] = useState<Set<string>>(() => {
    if (typeof window === 'undefined') return new Set()
    try {
      const stored = localStorage.getItem(`taken_bets_${selectedDate}`)
      return stored ? new Set(JSON.parse(stored)) : new Set()
    } catch { return new Set() }
  })

  // Slate builder state
  const [slateMode, setSlateMode] = useState<boolean>(false)
  const [selectedPicks, setSelectedPicks] = useState<Set<string>>(new Set())
  const [slateImageUrl, setSlateImageUrl] = useState<string | null>(null)
  const [slateLoading, setSlateLoading] = useState<boolean>(false)
  const [slateError, setSlateError] = useState<string | null>(null)

  const MAX_SLATE_PICKS = 5

  // Optimal model config (from backtest sweep)
  const MODEL_PICKS_EDGE = 0.09
  const MODEL_PICKS_TAU = 0.50

  // Handle Model Picks toggle - auto-set optimal config
  const handleModelPicksToggle = (enabled: boolean) => {
    setShowModelPicks(enabled)
    if (enabled) {
      // Set optimal config when enabling Model Picks
      setEdgeThreshold(MODEL_PICKS_EDGE)
      setBlTau(MODEL_PICKS_TAU)
    } else {
      // Reset to defaults when disabling
      setEdgeThreshold(0.03)
      setBlTau(null)
    }
  }

  // Reload taken bets when date changes
  useEffect(() => {
    try {
      const stored = localStorage.getItem(`taken_bets_${selectedDate}`)
      setTakenBets(stored ? new Set(JSON.parse(stored)) : new Set())
    } catch { setTakenBets(new Set()) }
  }, [selectedDate])

  // Toggle taken bet
  const handleToggleTaken = (prediction: Prediction) => {
    const key = `${prediction.player_id}-${prediction.stat}`
    setTakenBets(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      localStorage.setItem(`taken_bets_${selectedDate}`, JSON.stringify([...next]))
      return next
    })
  }

  // Toggle pick selection for slate
  const handleToggleSelect = (prediction: Prediction) => {
    const key = `${prediction.player_id}-${prediction.stat}`
    setSelectedPicks(prev => {
      const next = new Set(prev)
      if (next.has(key)) {
        next.delete(key)
      } else {
        if (next.size >= MAX_SLATE_PICKS) return prev // Don't add beyond max
        next.add(key)
      }
      return next
    })
  }

  // Generate slate image
  const handleGenerateSlate = async () => {
    if (selectedPicks.size === 0) return
    setSlateLoading(true)

    // Build pick data from selected predictions
    const picks = sortedPredictions
      .filter(p => selectedPicks.has(`${p.player_id}-${p.stat}`))
      .map(p => {
        const overEdge = Number.isFinite(p.over_edge) ? p.over_edge : 0
        const underEdge = Number.isFinite(p.under_edge) ? p.under_edge : 0
        const isOver = overEdge > underEdge
        return {
          player_id: p.player_id,
          player_name: p.player_name || `Player ${p.player_id}`,
          stat: p.stat,
          line: p.prop_line,
          direction: (isOver ? 'Over' : 'Under') as 'Over' | 'Under',
          edge: isOver ? overEdge : underEdge,
          team_abbrev: p.team_abbrev || 'UNK',
          opponent_abbrev: p.opponent_abbrev || 'UNK',
          game_time: p.game_time,
        }
      })

    setSlateError(null)
    try {
      const res = await fetch('/api/slate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ picks, date: selectedDate }),
      })
      if (!res.ok) throw new Error(`Failed to generate slate (${res.status})`)
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      setSlateImageUrl(url)
    } catch (err) {
      console.error('Slate generation error:', err)
      setSlateError(err instanceof Error ? err.message : 'Failed to generate slate')
    } finally {
      setSlateLoading(false)
    }
  }

  // Fetch predictions for a specific date
  const fetchPredictions = useCallback(async (date: string) => {
    setLoading(true)
    const supabase = createClient()

    const { data: predictionsData, error: predictionsError } = await supabase
      .from('daily_predictions')
      .select('*')
      .eq('prediction_date', date)
      .not('line', 'is', null)
      .order('is_recommended', { ascending: false, nullsFirst: false })
      .order('over_edge', { ascending: false })
      .limit(3000)

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

  // Initial load: fetch available dates
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

  // Derive tonight's games from predictions
  const tonightsGames: GameInfo[] = (() => {
    const gameMap = new Map<string, GameInfo>()
    for (const p of predictions) {
      const teams = [p.team_abbrev || 'UNK', p.opponent_abbrev || 'UNK'].sort() as [string, string]
      const key = `${teams[0]} vs ${teams[1]}`
      const existing = gameMap.get(key)
      if (existing) {
        existing.predictionCount++
        if (!existing.gameTime && p.game_time) existing.gameTime = p.game_time
      } else {
        gameMap.set(key, { matchupKey: key, teams, gameTime: p.game_time || null, predictionCount: 1 })
      }
    }
    return [...gameMap.values()].sort((a, b) => {
      if (a.gameTime && b.gameTime) return a.gameTime.localeCompare(b.gameTime)
      if (a.gameTime) return -1
      if (b.gameTime) return 1
      return a.matchupKey.localeCompare(b.matchupKey)
    })
  })()

  // Enrich predictions with effective edges (BL blending when active)
  // This ensures PropCard, PlayOfTheDay, and AnalysisModal all show consistent values
  const enrichedPredictions = predictions.map(p => {
    if (showModelPicks && p.bl_over_edge != null && p.bl_under_edge != null) {
      return {
        ...p,
        over_edge: p.bl_over_edge,
        under_edge: p.bl_under_edge,
        model_prob_over: p.bl_over_prob ?? p.model_prob_over,
        model_prob_under: p.bl_under_prob ?? p.model_prob_under,
      }
    }
    if (blTau !== null && p.pred_mean && p.pred_std) {
      const confidence = calculateBLConfidence(p.pred_mean, p.pred_std, p.prop_line)
      const blendedOver = blendProbability(p.model_prob_over, p.implied_prob_over, blTau, confidence)
      const blendedUnder = blendProbability(p.model_prob_under, p.implied_prob_under, blTau, confidence)
      return {
        ...p,
        over_edge: blendedOver - p.implied_prob_over,
        under_edge: blendedUnder - p.implied_prob_under,
        model_prob_over: blendedOver,
        model_prob_under: blendedUnder,
      }
    }
    return p
  })

  // Filter predictions by stat type, matchup, edge threshold, and Model Picks toggle
  const filteredPredictions = enrichedPredictions.filter(p => {
    if (showModelPicks && !p.is_recommended) return false
    if (filter !== 'all' && p.stat !== filter) return false
    if (teamFilter !== 'all') {
      const matchupTeams = teamFilter.split(' vs ')
      if (!matchupTeams.includes(p.team_abbrev || '') && !matchupTeams.includes(p.opponent_abbrev || '')) {
        return false
      }
    }
    // Skip edge threshold for Model Picks (is_recommended already guarantees BL edge >= 9%)
    if (showModelPicks) return true
    return Math.max(p.over_edge, p.under_edge) >= edgeThreshold
  })

  // Sort by max edge (effective edges already computed above)
  const sortedPredictions = [...filteredPredictions].sort((a, b) =>
    Math.max(b.over_edge, b.under_edge) - Math.max(a.over_edge, a.under_edge)
  )

  return (
    <main className={`flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8 ${slateMode ? 'pb-24' : ''}`}>
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
          {/* State Selector */}
          <select
            value={userState}
            onChange={(e) => {
              setUserState(e.target.value)
              localStorage.setItem('user_state', e.target.value)
            }}
            className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
          >
            {US_STATES.map((s) => (
              <option key={s.value} value={s.value}>
                {s.value ? s.value : 'All States'}
              </option>
            ))}
          </select>
          {/* Model Picks Toggle */}
          <div className="flex bg-slate-800 rounded-lg p-0.5 border border-slate-700">
            <button
              onClick={() => handleModelPicksToggle(false)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                !showModelPicks
                  ? 'bg-slate-700 text-slate-100'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              All Bets
            </button>
            <button
              onClick={() => handleModelPicksToggle(true)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                showModelPicks
                  ? 'bg-green-600 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              Model Picks
            </button>
          </div>
          {/* Build Slate Toggle */}
          <button
            onClick={() => {
              setSlateMode(prev => !prev)
              if (slateMode) setSelectedPicks(new Set())
            }}
            className={`px-3 py-1.5 text-sm font-medium rounded-lg border transition-colors ${
              slateMode
                ? 'bg-blue-600 border-blue-500 text-white'
                : 'bg-slate-800 border-slate-700 text-slate-400 hover:text-slate-200'
            }`}
          >
            {slateMode ? 'Exit Slate' : 'Build Slate'}
          </button>
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
          {/* Edge Threshold Filter */}
          <select
            value={edgeThreshold}
            onChange={(e) => setEdgeThreshold(Number(e.target.value))}
            className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
          >
            <option value={0}>Edge: All</option>
            <option value={0.03}>Edge: ≥3%</option>
            <option value={0.05}>Edge: ≥5%</option>
            <option value={0.07}>Edge: ≥7%</option>
            <option value={0.09}>Edge: ≥9% (Model)</option>
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
            <option value={0.10}>BL: τ=0.10</option>
            <option value={0.15}>BL: τ=0.15</option>
            <option value={0.25}>BL: τ=0.25</option>
            <option value={0.50}>BL: τ=0.50 (Model)</option>
          </select>
          <FilterTabs activeFilter={filter} onFilterChange={setFilter} />
        </div>
      </div>

      {/* Tonight's Games Strip */}
      {!loading && (
        <TonightsGames
          games={tonightsGames}
          activeMatchup={teamFilter}
          onSelectMatchup={setTeamFilter}
          isToday={selectedDate === getToday()}
        />
      )}

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
          onAnalyze={slateMode ? undefined : setSelectedPrediction}
          selectable={slateMode}
          selectedIds={selectedPicks}
          onToggleSelect={handleToggleSelect}
          takenIds={takenBets}
          onToggleTaken={handleToggleTaken}
        />
      )}

      {/* Slate Builder Floating Bar */}
      {slateMode && (
        <div className="fixed bottom-0 left-0 right-0 z-40 bg-slate-900/95 backdrop-blur border-t border-slate-700 px-4 py-3">
          <div className="max-w-7xl mx-auto flex items-center justify-between">
            <div className="text-slate-300 text-sm">
              <span className="font-semibold text-slate-50">{selectedPicks.size}</span>
              {' / '}{MAX_SLATE_PICKS} picks selected
              {selectedPicks.size >= MAX_SLATE_PICKS && (
                <span className="ml-2 text-yellow-400 text-xs">Max reached</span>
              )}
              {slateError && (
                <span className="ml-2 text-red-400 text-xs">{slateError}</span>
              )}
            </div>
            <div className="flex items-center gap-3">
              <button
                onClick={() => setSelectedPicks(new Set())}
                className="px-3 py-1.5 text-sm text-slate-400 hover:text-slate-200 transition-colors"
              >
                Clear
              </button>
              <button
                onClick={handleGenerateSlate}
                disabled={selectedPicks.size === 0 || slateLoading}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-500 disabled:bg-slate-700 disabled:text-slate-500 text-white text-sm font-medium rounded-lg transition-colors flex items-center gap-2"
              >
                {slateLoading ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white" />
                    Generating...
                  </>
                ) : (
                  'Generate Slate'
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Slate Image Modal */}
      {slateImageUrl && (
        <SlateModal
          imageUrl={slateImageUrl}
          onClose={() => {
            URL.revokeObjectURL(slateImageUrl)
            setSlateImageUrl(null)
          }}
        />
      )}

      {/* Analysis Modal */}
      {selectedPrediction && (
        <AnalysisModal
          prediction={selectedPrediction}
          onClose={() => setSelectedPrediction(null)}
        />
      )}
    </main>
  )
}
