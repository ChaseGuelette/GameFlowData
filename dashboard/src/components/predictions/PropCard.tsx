'use client'

import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { Badge, EdgeBadge } from '@/components/shared/Badge'
import { type Prediction } from '@/types/predictions'
import { cn, getEdgeTier, formatProb, formatGameTime } from '@/lib/utils'

interface PropCardProps {
  prediction: Prediction
  onAnalyze?: (prediction: Prediction) => void
  selectable?: boolean
  selected?: boolean
  onToggleSelect?: (prediction: Prediction) => void
  taken?: boolean
  onToggleTaken?: (prediction: Prediction) => void
}

export function PropCard({ prediction, onAnalyze, selectable, selected, onToggleSelect, taken, onToggleTaken }: PropCardProps) {
  // Determine which direction has positive edge (with NaN safety)
  const overEdge = Number.isFinite(prediction.over_edge) ? prediction.over_edge : 0
  const underEdge = Number.isFinite(prediction.under_edge) ? prediction.under_edge : 0
  const isOverBet = overEdge > underEdge
  const edge = isOverBet ? overEdge : underEdge
  const direction = isOverBet ? 'Over' : 'Under'
  const probability = isOverBet ? prediction.model_prob_over : prediction.model_prob_under
  const edgeTier = getEdgeTier(edge)

  const borderColors = {
    high: 'border-green-500/50',
    medium: 'border-yellow-500/50',
    low: 'border-slate-700',
  }

  // Calculate stars (1-5 based on edge) with NaN safety
  const safeEdge = Number.isFinite(edge) ? edge : 0
  const stars = Math.min(5, Math.max(1, Math.ceil(Math.abs(safeEdge) * 50)))

  return (
    <div
      className={cn(
        'bg-slate-800 rounded-lg border-2 p-4 transition-all hover:bg-slate-750 relative',
        borderColors[edgeTier],
        selectable && 'cursor-pointer',
        selected && 'ring-2 ring-blue-500'
      )}
      onClick={selectable ? () => onToggleSelect?.(prediction) : undefined}
    >
      {/* Selection checkbox */}
      {selectable && (
        <button
          onClick={(e) => {
            e.stopPropagation()
            onToggleSelect?.(prediction)
          }}
          className={cn(
            'absolute top-3 right-3 w-6 h-6 rounded-full border-2 flex items-center justify-center transition-colors z-10',
            selected
              ? 'bg-blue-500 border-blue-500 text-white'
              : 'border-slate-500 hover:border-blue-400'
          )}
        >
          {selected && (
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
              <path d="M2 6L5 9L10 3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          )}
        </button>
      )}

      {/* Header: Player info */}
      <div className="flex items-start space-x-3 mb-3">
        <PlayerAvatar
          playerId={prediction.player_id}
          playerName={prediction.player_name || 'Unknown'}
          size="md"
        />
        <div className="flex-1 min-w-0">
          <h3 className="text-slate-50 font-semibold truncate">
            {prediction.player_name || `Player ${prediction.player_id}`}
          </h3>
          <p className="text-slate-400 text-sm">
            {prediction.team_abbrev || '???'} vs {prediction.opponent_abbrev || '???'}
            {prediction.game_time && (
              <span className="ml-2 text-slate-500">• {formatGameTime(prediction.game_time)}</span>
            )}
          </p>
        </div>
      </div>

      {/* Stat badge and line */}
      <div className="flex items-center justify-between mb-3">
        <Badge stat={prediction.stat} />
        <span className="text-slate-50 font-bold text-lg">
          {direction} {prediction.prop_line}
        </span>
      </div>

      {/* Probability and edge */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center space-x-1">
          {[...Array(5)].map((_, i) => (
            <span
              key={i}
              className={cn('text-lg', i < stars ? 'text-yellow-400' : 'text-slate-600')}
            >
              &#9733;
            </span>
          ))}
        </div>
        <div className="flex items-center space-x-2">
          <span className="text-slate-300 text-sm">{formatProb(probability)}</span>
          <EdgeBadge edge={edge} />
          <span className="text-slate-500 text-xs">min</span>
        </div>
      </div>

      {/* Analyze button + taken checkbox */}
      <div className="flex items-center gap-2">
        <button
          onClick={(e) => {
            e.stopPropagation()
            onToggleTaken?.(prediction)
          }}
          className={cn(
            'w-8 h-8 rounded-md border-2 flex items-center justify-center transition-colors shrink-0',
            taken
              ? 'bg-green-600 border-green-500 text-white'
              : 'border-slate-600 hover:border-green-500 text-transparent hover:text-green-500/50'
          )}
          title={taken ? 'Mark as not taken' : 'Mark as taken'}
        >
          <svg width="14" height="14" viewBox="0 0 12 12" fill="none">
            <path d="M2 6L5 9L10 3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </button>
        <button
          onClick={() => onAnalyze?.(prediction)}
          className="flex-1 py-2 px-4 bg-slate-700 hover:bg-slate-600 text-slate-200 rounded-md text-sm font-medium transition-colors"
        >
          Analyze
        </button>
      </div>
    </div>
  )
}
