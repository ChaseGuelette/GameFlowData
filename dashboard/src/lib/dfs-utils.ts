import { DFS_SLIP_TYPES } from '@/types/dfs'

/**
 * Estimate probability of Under X using quantile interpolation.
 * P(stat < line) = probability the Under hits.
 * Higher line = easier Under = higher probability.
 * Lower line = harder Under = lower probability.
 */
export function estimateUnderProb(
  line: number,
  q10: number,
  q25: number,
  q50: number,
  q75: number,
  q90: number
): number {
  const points = [
    { val: q10, prob: 0.10 },
    { val: q25, prob: 0.25 },
    { val: q50, prob: 0.50 },
    { val: q75, prob: 0.75 },
    { val: q90, prob: 0.90 },
  ]

  // Extrapolate below q10
  if (line <= q10) {
    const slope = (points[1].prob - points[0].prob) / (points[1].val - points[0].val)
    const extrapolated = points[0].prob + slope * (line - points[0].val)
    return Math.max(0.01, Math.min(0.10, extrapolated))
  }

  // Extrapolate above q90
  if (line >= q90) {
    const slope = (points[4].prob - points[3].prob) / (points[4].val - points[3].val)
    const extrapolated = points[4].prob + slope * (line - points[4].val)
    return Math.max(0.90, Math.min(0.99, extrapolated))
  }

  // Interpolate between bracketing quantiles
  for (let i = 0; i < points.length - 1; i++) {
    if (line >= points[i].val && line <= points[i + 1].val) {
      const range = points[i + 1].val - points[i].val
      if (range === 0) return points[i].prob
      const fraction = (line - points[i].val) / range
      return points[i].prob + fraction * (points[i + 1].prob - points[i].prob)
    }
  }

  return 0.5 // Fallback
}

/**
 * Estimate probability of Over X using quantile interpolation.
 */
export function estimateOverProb(
  line: number,
  q10: number,
  q25: number,
  q50: number,
  q75: number,
  q90: number
): number {
  return 1 - estimateUnderProb(line, q10, q25, q50, q75, q90)
}

/**
 * Calculate DFS EV: model probability minus break-even threshold.
 * Positive = +EV pick.
 */
export function calcDfsEv(modelProb: number, breakEven: number): number {
  return modelProb - breakEven
}

/**
 * Calculate EV for all slip types given a model probability.
 */
export function calcAllSlipEvs(modelProb: number): Record<string, number> {
  const evs: Record<string, number> = {}
  for (const [key, slip] of Object.entries(DFS_SLIP_TYPES)) {
    evs[key] = calcDfsEv(modelProb, slip.breakEven)
  }
  return evs
}
