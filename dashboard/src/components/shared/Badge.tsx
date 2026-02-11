import { STAT_COLORS, STAT_LABELS, type StatType } from '@/types/predictions'
import { cn } from '@/lib/utils'

interface BadgeProps {
  stat: StatType
  className?: string
}

export function Badge({ stat, className }: BadgeProps) {
  return (
    <span
      className={cn(
        'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium text-white uppercase',
        STAT_COLORS[stat],
        className
      )}
    >
      {STAT_LABELS[stat]}
    </span>
  )
}

interface EdgeBadgeProps {
  edge: number
  className?: string
}

export function EdgeBadge({ edge, className }: EdgeBadgeProps) {
  // Handle NaN/undefined/null
  const safeEdge = Number.isFinite(edge) ? edge : 0
  const isPositive = safeEdge >= 0
  const absEdge = Math.abs(safeEdge)
  const tier = absEdge >= 0.07 ? 'high' : absEdge >= 0.05 ? 'medium' : 'low'

  const tierColors = {
    high: 'bg-green-500/20 text-green-400 border-green-500/50',
    medium: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/50',
    low: 'bg-slate-500/20 text-slate-400 border-slate-500/50',
  }

  // Show dash for invalid edge values
  if (!Number.isFinite(edge)) {
    return (
      <span className={cn('inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border', tierColors.low, className)}>
        —
      </span>
    )
  }

  return (
    <span
      className={cn(
        'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border',
        tierColors[tier],
        className
      )}
    >
      {isPositive ? '+' : ''}
      {(safeEdge * 100).toFixed(1)}%
    </span>
  )
}
