'use client'

import { type PaperBet } from '@/types/predictions'
import { BetCard } from './BetCard'

interface BetListProps {
  bets: PaperBet[]
  onRemove?: (id: number) => void
  onEdit?: (bet: PaperBet) => void
}

export function BetList({ bets, onRemove, onEdit }: BetListProps) {
  if (bets.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-center">
        <div className="text-4xl mb-4">📋</div>
        <h3 className="text-lg font-medium text-slate-300 mb-2">No betting history</h3>
        <p className="text-slate-500 max-w-md">
          Predictions will appear here after games are resolved. Check back after tonight&apos;s games!
        </p>
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {bets.map((bet) => (
        <BetCard key={bet.id} bet={bet} onRemove={onRemove} onEdit={onEdit} />
      ))}
    </div>
  )
}
