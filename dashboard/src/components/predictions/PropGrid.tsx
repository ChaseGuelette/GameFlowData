'use client'

import { PropCard } from './PropCard'
import { type Prediction } from '@/types/predictions'

interface PropGridProps {
  predictions: Prediction[]
  onAnalyze?: (prediction: Prediction) => void
  selectable?: boolean
  selectedIds?: Set<string>
  onToggleSelect?: (prediction: Prediction) => void
}

export function PropGrid({ predictions, onAnalyze, selectable, selectedIds, onToggleSelect }: PropGridProps) {
  if (predictions.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 px-4">
        <div className="text-6xl mb-4">📊</div>
        <h3 className="text-xl font-semibold text-slate-300 mb-2">
          Inference not run yet
        </h3>
        <p className="text-slate-400 text-center max-w-md">
          No predictions available for today. The inference job runs daily at 6:30 PM ET
          to generate predictions for upcoming games.
        </p>
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
      {predictions.map((prediction) => (
        <PropCard
          key={`${prediction.player_id}-${prediction.stat}`}
          prediction={prediction}
          onAnalyze={onAnalyze}
          selectable={selectable}
          selected={selectedIds?.has(`${prediction.player_id}-${prediction.stat}`)}
          onToggleSelect={onToggleSelect}
        />
      ))}
    </div>
  )
}
