import { USER_DFS_SLIP_TYPES, type UserDfsSlipType, type SlipBuilderLeg } from '@/types/dfs-entries'
import type { EdgeMode } from '@/types/dfs'

/**
 * Get the effective probability for a leg based on the active edge mode.
 * - model: use model_prob
 * - market: use market_prob
 * - combined: use min(model_prob, market_prob) — conservative
 */
export function getEffectiveProb(leg: SlipBuilderLeg, edgeMode: EdgeMode): number | null {
  if (edgeMode === 'model') return leg.model_prob
  if (edgeMode === 'market') return leg.market_prob
  // combined: conservative min
  if (leg.model_prob !== null && leg.market_prob !== null) {
    return Math.min(leg.model_prob, leg.market_prob)
  }
  return leg.model_prob ?? leg.market_prob
}

/**
 * Calculate parlay Kelly stake for a set of legs.
 *
 * @param legs - Selected slip builder legs
 * @param slipType - Which slip type (determines payout multiplier)
 * @param bankroll - Current bankroll
 * @param kellyFraction - User's kelly fraction (e.g. 0.25 = quarter kelly)
 * @param edgeMode - Which probability source to use
 * @returns Calculated values or null if invalid
 */
export function calcParlayKelly(
  legs: SlipBuilderLeg[],
  slipType: UserDfsSlipType,
  bankroll: number,
  kellyFraction: number,
  edgeMode: EdgeMode,
): {
  combinedProb: number
  payoutMultiplier: number
  rawKelly: number
  stake: number
  ev: number
} | null {
  const config = USER_DFS_SLIP_TYPES[slipType]
  if (!config) return null
  if (legs.length !== config.legs) return null

  // Combined probability = product of per-leg probs
  let combinedProb = 1
  for (const leg of legs) {
    const prob = getEffectiveProb(leg, edgeMode)
    if (prob === null || prob <= 0) return null
    combinedProb *= prob
  }

  const b = config.payout - 1 // net odds (e.g. 3x payout → b=2)

  // Kelly criterion: f* = (p(b+1) - 1) / b
  const rawKelly = (combinedProb * (b + 1) - 1) / b

  // If raw Kelly is negative, there's no edge
  if (rawKelly <= 0) {
    return {
      combinedProb,
      payoutMultiplier: config.payout,
      rawKelly,
      stake: 0,
      ev: combinedProb * config.payout - 1,
    }
  }

  // Apply kelly fraction and cap at 10% of bankroll
  const maxStake = bankroll * 0.10
  const stake = Math.min(rawKelly * kellyFraction * bankroll, maxStake)

  // EV = combinedProb * payout - 1 (per dollar)
  const ev = combinedProb * config.payout - 1

  return {
    combinedProb,
    payoutMultiplier: config.payout,
    rawKelly,
    stake: Math.round(stake * 100) / 100,
    ev,
  }
}
