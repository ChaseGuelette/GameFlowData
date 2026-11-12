import type {
  NbaBlConfig,
  NbaExecutableOffer,
  NbaExecutableOfferResult,
} from '@/types/predictions'

export const NBA_POLICY_STATS = new Set(['pts', 'reb', 'ast'])
export const NBA_POLICY_MIN_ODDS = -200
export const NBA_POLICY_MAX_ODDS = 200
const MIN_PROB = 0.01
const MAX_PROB = 0.99

export interface CurrentNbaLine {
  player_id: number
  game_id: string
  bookmaker: string
  market_key: string
  line: number
  over_odds: number
  under_odds: number
  snapshot_time: string
}

export interface NbaOfferPredictionInput {
  prediction_id: number
  prediction_date: string
  player_id: number
  game_id: string
  stat: string
  reference_line: number
  bl_config: NbaBlConfig
}

export type NbaOfferPredictionBase = Omit<NbaOfferPredictionInput, 'bl_config'>

export function americanToRawImplied(odds: number): number {
  if (!Number.isFinite(odds) || odds === 0) return Number.NaN
  const decimal = odds > 0 ? 1 + odds / 100 : 1 + 100 / Math.abs(odds)
  return 1 / Math.max(decimal, 1.01)
}

export function devig(overOdds: number, underOdds: number): { over: number; under: number; booksum: number } | null {
  const rawOver = americanToRawImplied(overOdds)
  const rawUnder = americanToRawImplied(underOdds)
  const booksum = rawOver + rawUnder
  if (!Number.isFinite(booksum) || booksum <= 0) return null
  return { over: rawOver / booksum, under: rawUnder / booksum, booksum }
}

export function validateBlConfig(value: unknown): NbaBlConfig | null {
  if (!value || typeof value !== 'object') return null
  const row = value as Record<string, unknown>
  const tau = Number(row.tau)
  const zMax = Number(row.z_max)
  const maxWeight = Number(row.max_weight)
  const edgeThreshold = Number(row.edge_threshold)
  if (![tau, zMax, maxWeight, edgeThreshold].every(Number.isFinite)) return null
  if (tau <= 0 || zMax <= 0 || maxWeight <= 0 || maxWeight > 1 || edgeThreshold < 0 || edgeThreshold >= 1) return null
  return { tau, z_max: zMax, max_weight: maxWeight, edge_threshold: edgeThreshold }
}

function clampProbability(probability: number): number {
  return Math.min(Math.max(probability, MIN_PROB), MAX_PROB)
}

function blend(modelProb: number, marketProb: number, confidence: number, config: NbaBlConfig): number {
  const model = clampProbability(modelProb)
  const market = clampProbability(marketProb)
  const marketLogit = Math.log(market / (1 - market))
  const modelLogit = Math.log(model / (1 - model))
  const weight = Math.min(config.tau * confidence, config.max_weight)
  const posteriorLogit = marketLogit + weight * (modelLogit - marketLogit)
  return 1 / (1 + Math.exp(-posteriorLogit))
}

function meanAndStd(samples: Float64Array): { mean: number; std: number } | null {
  if (samples.length === 0) return null
  let sum = 0
  for (const value of samples) {
    if (!Number.isFinite(value)) return null
    sum += value
  }
  const mean = sum / samples.length
  let varianceSum = 0
  for (const value of samples) varianceSum += (value - mean) ** 2
  return { mean, std: Math.sqrt(varianceSum / samples.length) }
}

export function selectClosestLinePerBook(lines: CurrentNbaLine[], referenceLine: number): CurrentNbaLine[] {
  const selected = new Map<string, CurrentNbaLine>()
  const ordered = [...lines].sort((a, b) => {
    const distance = Math.abs(a.line - referenceLine) - Math.abs(b.line - referenceLine)
    if (distance !== 0) return distance
    const aBooksum = devig(a.over_odds, a.under_odds)?.booksum ?? Number.POSITIVE_INFINITY
    const bBooksum = devig(b.over_odds, b.under_odds)?.booksum ?? Number.POSITIVE_INFINITY
    if (aBooksum !== bBooksum) return aBooksum - bBooksum
    if (a.line !== b.line) return a.line - b.line
    return a.bookmaker.localeCompare(b.bookmaker)
  })
  for (const line of ordered) {
    if (!selected.has(line.bookmaker)) selected.set(line.bookmaker, line)
  }
  return [...selected.values()]
}

export function computeExecutableOffer(
  prediction: NbaOfferPredictionInput,
  line: CurrentNbaLine,
  samples: Float64Array,
): NbaExecutableOffer | null {
  const market = devig(line.over_odds, line.under_odds)
  const distribution = meanAndStd(samples)
  if (!market || !distribution || samples.length === 0) return null

  let overCount = 0
  let underCount = 0
  for (const value of samples) {
    if (value > line.line) overCount += 1
    if (value < line.line) underCount += 1
  }
  const modelOver = overCount / samples.length
  const modelUnder = underCount / samples.length
  const confidence = distribution.std < 1e-6
    ? 0
    : Math.min((Math.abs(distribution.mean - line.line) / distribution.std) / prediction.bl_config.z_max, 1)
  const blOver = blend(modelOver, market.over, confidence, prediction.bl_config)
  const blUnder = blend(modelUnder, market.under, confidence, prediction.bl_config)
  const blOverEdge = blOver - market.over
  const blUnderEdge = blUnder - market.under
  const selectedDirection = blOverEdge >= blUnderEdge ? 'over' : 'under'
  const selectedEdge = selectedDirection === 'over' ? blOverEdge : blUnderEdge
  const selectedOdds = selectedDirection === 'over' ? line.over_odds : line.under_odds
  const passesLocalPolicy = NBA_POLICY_STATS.has(prediction.stat) &&
    selectedDirection === 'under' &&
    selectedOdds >= NBA_POLICY_MIN_ODDS && selectedOdds <= NBA_POLICY_MAX_ODDS &&
    selectedEdge >= prediction.bl_config.edge_threshold

  return {
    bookmaker: line.bookmaker,
    line: line.line,
    over_odds: line.over_odds,
    under_odds: line.under_odds,
    snapshot_time: line.snapshot_time,
    reference_line: prediction.reference_line,
    line_distance: Math.abs(line.line - prediction.reference_line),
    booksum: market.booksum,
    model_prob_over: modelOver,
    model_prob_under: modelUnder,
    market_prob_over: market.over,
    market_prob_under: market.under,
    raw_over_edge: modelOver - market.over,
    raw_under_edge: modelUnder - market.under,
    bl_over_prob: blOver,
    bl_under_prob: blUnder,
    bl_over_edge: blOverEdge,
    bl_under_edge: blUnderEdge,
    bl_confidence: confidence,
    selected_direction: selectedDirection,
    selected_edge: selectedEdge,
    passes_local_policy: passesLocalPolicy,
    is_recommended: false,
  }
}

export function buildExecutableOfferResult(
  prediction: NbaOfferPredictionBase,
  config: NbaBlConfig | null,
  lines: CurrentNbaLine[],
  samples: Float64Array | null,
): NbaExecutableOfferResult {
  const base = {
    prediction_id: prediction.prediction_id,
    player_id: prediction.player_id,
    game_id: prediction.game_id,
    stat: prediction.stat,
  }
  if (!config || !Number.isFinite(prediction.reference_line)) {
    return { ...base, status: 'invalid_config', offers: [], selected_offer: null }
  }
  if (!samples || samples.length === 0) {
    return { ...base, status: 'missing_samples', offers: [], selected_offer: null }
  }
  const configuredPrediction: NbaOfferPredictionInput = { ...prediction, bl_config: config }
  const offers = selectClosestLinePerBook(lines, prediction.reference_line)
    .map(line => computeExecutableOffer(configuredPrediction, line, samples))
    .filter(offer => offer !== null)
  return {
    ...base,
    status: offers.length > 0 ? 'ready' : 'empty',
    offers,
    selected_offer: chooseDisplayOffer(offers),
  }
}

export function applyBestOpportunityPolicy(results: NbaExecutableOfferResult[]): void {
  const groups = new Map<string, Array<{ result: NbaExecutableOfferResult; offer: NbaExecutableOffer }>>()
  for (const result of results) {
    for (const offer of result.offers) offer.is_recommended = false
    if (!result.selected_offer) continue
    const key = `${result.player_id}:${result.game_id}`
    const group = groups.get(key) ?? []
    group.push({ result, offer: result.selected_offer })
    groups.set(key, group)
  }

  for (const group of groups.values()) {
    group.sort((a, b) => {
      if (a.offer.selected_edge !== b.offer.selected_edge) return b.offer.selected_edge - a.offer.selected_edge
      if (a.result.prediction_id !== b.result.prediction_id) return a.result.prediction_id - b.result.prediction_id
      return a.offer.bookmaker.localeCompare(b.offer.bookmaker)
    })
    const winner = group[0]
    if (winner.offer.passes_local_policy) winner.offer.is_recommended = true
  }
}

export function chooseDisplayOffer(offers: NbaExecutableOffer[]): NbaExecutableOffer | null {
  if (offers.length === 0) return null
  return [...offers].sort((a, b) => {
    if (a.selected_edge !== b.selected_edge) return b.selected_edge - a.selected_edge
    if (a.line_distance !== b.line_distance) return a.line_distance - b.line_distance
    if (a.booksum !== b.booksum) return a.booksum - b.booksum
    if (a.line !== b.line) return a.line - b.line
    return a.bookmaker.localeCompare(b.bookmaker)
  })[0]
}
