'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { Badge, EdgeBadge } from '@/components/shared/Badge'
import { Last5Chart } from './Last5Chart'
import { QuantileSummary } from './QuantileSummary'
import { type Prediction, type PlayerGameStats, type StatType } from '@/types/predictions'
import { formatProb } from '@/lib/utils'

interface AnalysisModalProps {
  prediction: Prediction
  onClose: () => void
}

// Map stat type to column name in player_game_stats
const STAT_COLUMN_MAP: Record<StatType, keyof PlayerGameStats> = {
  pts: 'pts',
  reb: 'reb',
  ast: 'ast',
}

export function AnalysisModal({ prediction, onClose }: AnalysisModalProps) {
  const [history, setHistory] = useState<PlayerGameStats[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function fetchHistory() {
      const supabase = createClient()
      const { data, error } = await supabase
        .from('player_game_stats')
        .select('game_date, pts, reb, ast, fg3m, min')
        .eq('player_id', prediction.player_id)
        .order('game_date', { ascending: false })
        .limit(5)

      if (!error && data) {
        setHistory(data.reverse()) // Chronological order for chart
      }
      setLoading(false)
    }

    fetchHistory()
  }, [prediction.player_id])

  // Determine bet direction (with NaN safety)
  const overEdge = Number.isFinite(prediction.over_edge) ? prediction.over_edge : 0
  const underEdge = Number.isFinite(prediction.under_edge) ? prediction.under_edge : 0
  const isOverBet = overEdge > underEdge
  const edge = isOverBet ? overEdge : underEdge
  const direction = isOverBet ? 'Over' : 'Under'
  const probability = isOverBet
    ? (Number.isFinite(prediction.model_prob_over) ? prediction.model_prob_over : 0)
    : (Number.isFinite(prediction.model_prob_under) ? prediction.model_prob_under : 0)
  const marketProb = isOverBet
    ? (Number.isFinite(prediction.implied_prob_over) ? prediction.implied_prob_over : 0)
    : (Number.isFinite(prediction.implied_prob_under) ? prediction.implied_prob_under : 0)

  // Get stat values for chart
  const statColumn = STAT_COLUMN_MAP[prediction.stat]

  // Handle escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handleEscape)
    return () => window.removeEventListener('keydown', handleEscape)
  }, [onClose])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/70" onClick={onClose} />

      {/* Modal */}
      <div className="relative bg-slate-800 rounded-lg border border-slate-700 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        {/* Close button */}
        <button
          onClick={onClose}
          className="absolute top-4 right-4 text-slate-400 hover:text-slate-200 text-2xl"
        >
          &times;
        </button>

        {/* Header */}
        <div className="p-6 border-b border-slate-700">
          <div className="flex items-center space-x-4">
            <PlayerAvatar
              playerId={prediction.player_id}
              playerName={prediction.player_name || 'Unknown'}
              size="lg"
            />
            <div className="flex-1">
              <h2 className="text-2xl font-bold text-slate-50">
                {prediction.player_name || `Player ${prediction.player_id}`}
              </h2>
              <p className="text-slate-400">
                {prediction.team_abbrev || '???'} vs {prediction.opponent_abbrev || '???'}
              </p>
            </div>
            <div className="text-right">
              <Badge stat={prediction.stat} className="mb-1" />
              <div className="text-xl font-bold text-slate-50">
                {direction} {prediction.prop_line}
              </div>
            </div>
          </div>
        </div>

        {/* Last 5 Games Chart */}
        <div className="p-6 border-b border-slate-700">
          <h3 className="text-lg font-semibold text-slate-50 mb-4">Last 5 Games</h3>
          {loading ? (
            <div className="h-48 flex items-center justify-center text-slate-400">
              Loading...
            </div>
          ) : history.length > 0 ? (
            <Last5Chart
              games={history}
              stat={statColumn}
              line={prediction.prop_line}
            />
          ) : (
            <div className="h-48 flex items-center justify-center text-slate-400">
              No game history available
            </div>
          )}
        </div>

        {/* Quantile Summary */}
        <div className="p-6 border-b border-slate-700">
          <h3 className="text-lg font-semibold text-slate-50 mb-4">Model Summary</h3>
          <QuantileSummary
            q10={prediction.q10}
            q25={prediction.q25}
            q50={prediction.q50}
            q75={prediction.q75}
            q90={prediction.q90}
            line={prediction.prop_line}
          />
        </div>

        {/* Edge Summary */}
        <div className="p-6">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-slate-400 text-sm">Model Probability</div>
              <div className="text-xl font-semibold text-slate-50">
                {formatProb(probability)}
              </div>
            </div>
            <div>
              <div className="text-slate-400 text-sm">Market Implied</div>
              <div className="text-xl font-semibold text-slate-50">
                {formatProb(marketProb)}
              </div>
            </div>
            <div>
              <div className="text-slate-400 text-sm">Edge</div>
              <EdgeBadge edge={edge} className="text-lg" />
            </div>
          </div>
        </div>

        {/* Close button */}
        <div className="p-6 pt-0">
          <button
            onClick={onClose}
            className="w-full py-3 px-4 bg-slate-700 hover:bg-slate-600 text-slate-200 rounded-md font-medium transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  )
}
