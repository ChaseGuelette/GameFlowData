'use client'

import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { Badge, EdgeBadge } from '@/components/shared/Badge'
import { type Prediction } from '@/types/predictions'
import { formatProb, formatGameTime, isGameLive } from '@/lib/utils'

interface PlayOfTheDayProps {
  prediction: Prediction
  onAnalyze: (p: Prediction) => void
}

export function PlayOfTheDay({ prediction, onAnalyze }: PlayOfTheDayProps) {
  // Determine bet direction (NaN-safe)
  const overEdge = Number.isFinite(prediction.over_edge) ? prediction.over_edge : 0
  const underEdge = Number.isFinite(prediction.under_edge) ? prediction.under_edge : 0
  const isOverBet = overEdge > underEdge
  const edge = isOverBet ? overEdge : underEdge
  const direction = isOverBet ? 'Over' : 'Under'
  const probability = isOverBet
    ? (Number.isFinite(prediction.model_prob_over) ? prediction.model_prob_over : 0)
    : (Number.isFinite(prediction.model_prob_under) ? prediction.model_prob_under : 0)

  // Star rating (1-5 based on edge magnitude)
  const stars = Math.min(5, Math.max(1, Math.ceil(Math.abs(edge) * 50)))

  return (
    <div className="mb-6 bg-gradient-to-r from-amber-950/30 to-slate-800 rounded-lg border-2 border-amber-400/50 p-6">
      {/* Header badge */}
      <div className="flex items-center gap-2 mb-4">
        <span className="text-amber-400 text-lg">🏆</span>
        <span className="text-amber-400 font-bold text-sm tracking-wider uppercase">
          Play of the Day
        </span>
      </div>

      {/* Main content - responsive layout */}
      <div className="flex flex-col sm:flex-row sm:items-center gap-4">
        {/* Left: Player info */}
        <div className="flex items-center gap-4 flex-1">
          <PlayerAvatar
            playerId={prediction.player_id}
            playerName={prediction.player_name || 'Unknown'}
            size="lg"
          />
          <div>
            <h3 className="text-xl font-bold text-slate-50">
              {prediction.player_name || `Player ${prediction.player_id}`}
            </h3>
            <p className="text-slate-400">
              {prediction.team_abbrev || '???'} vs {prediction.opponent_abbrev || '???'}
              {' '}• {formatGameTime(prediction.game_time)}
              {isGameLive(prediction.game_time) && (
                <span className="ml-1.5 inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-bold uppercase bg-red-500/20 text-red-400 border border-red-500/30">
                  <span className="w-1.5 h-1.5 rounded-full bg-red-400 animate-pulse" />
                  Live
                </span>
              )}
            </p>
          </div>
        </div>

        {/* Center: Stat and line */}
        <div className="flex flex-col items-start sm:items-center gap-2">
          <Badge stat={prediction.stat} />
          <div className="text-2xl font-bold text-slate-50">
            {direction} {prediction.prop_line}
          </div>
          {/* Star rating */}
          <div className="text-amber-400 text-lg">
            {'★'.repeat(stars)}{'☆'.repeat(5 - stars)}
          </div>
        </div>

        {/* Right: Edge and probability */}
        <div className="flex flex-col items-start sm:items-end gap-2">
          <EdgeBadge edge={edge} className="text-lg px-3 py-1" />
          <div className="text-slate-400 text-sm">
            Model: <span className="text-slate-200 font-medium">{formatProb(probability)}</span>
          </div>
          <button
            onClick={() => onAnalyze(prediction)}
            className="mt-2 px-4 py-2 bg-amber-600 hover:bg-amber-500 text-white rounded-md font-medium transition-colors"
          >
            Analyze Pick
          </button>
        </div>
      </div>
    </div>
  )
}
